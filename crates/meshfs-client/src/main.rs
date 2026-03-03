#[cfg(feature = "fuse")]
mod fuse_mount;

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context;
use clap::{Parser, Subcommand};
use futures::{SinkExt, StreamExt};
use meshfs_types::{
    ChangeEvent, ChangeOp, DeviceActivateRequest, DevicePollRequest, DevicePollResponse,
    DeviceStartResponse, MetaResponse, NodeKind, PlanTier, SyncPullResponse,
};
use reqwest::{Client, StatusCode};
use rusqlite::{params, Connection, OptionalExtension};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::Message;

#[cfg(feature = "fuse")]
use crate::fuse_mount::{run_fuse_mount, FuseMountOptions};

#[cfg(not(feature = "fuse"))]
#[derive(Debug, Clone, Copy)]
struct FuseMountOptions {
    allow_other: bool,
    auto_unmount: bool,
    read_only: bool,
}

#[cfg(not(feature = "fuse"))]
fn run_fuse_mount(
    _server: &str,
    _token: &str,
    _target: &Path,
    options: FuseMountOptions,
) -> anyhow::Result<()> {
    let _ = (options.allow_other, options.auto_unmount, options.read_only);
    Err(anyhow::anyhow!(
        "FUSE support is disabled in this build. Rebuild meshfs-client with --features fuse"
    ))
}

#[derive(Debug, Parser)]
#[command(name = "meshfs")]
#[command(about = "MeshFS CLI client")]
struct Cli {
    #[arg(long, default_value = "http://127.0.0.1:8787")]
    server: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Login {
        #[arg(long)]
        auto_activate: bool,
        #[arg(long, default_value = "dev-user")]
        user_id: String,
        #[arg(long, default_value = "dev-tenant")]
        tenant_id: String,
        #[arg(long, value_enum, default_value_t = CliPlanTier::Free)]
        plan_tier: CliPlanTier,
    },
    Mount {
        #[arg(long)]
        remote: String,
        #[arg(long)]
        target: PathBuf,
        #[arg(long, default_value_t = false)]
        allow_other: bool,
        #[arg(long, default_value_t = false)]
        auto_unmount: bool,
        #[arg(long, default_value_t = false)]
        read_only: bool,
    },
    Sync {
        #[arg(long, default_value_t = 0)]
        cursor: u64,
        #[arg(long, default_value_t = 3)]
        reconnect_delay_seconds: u64,
        #[arg(long)]
        once: bool,
        #[arg(long)]
        target: Option<PathBuf>,
    },
}

#[derive(Debug, Clone, clap::ValueEnum)]
enum CliPlanTier {
    Free,
    Pro,
    Team,
}

impl From<CliPlanTier> for PlanTier {
    fn from(value: CliPlanTier) -> Self {
        match value {
            CliPlanTier::Free => PlanTier::Free,
            CliPlanTier::Pro => PlanTier::Pro,
            CliPlanTier::Team => PlanTier::Team,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let client = Client::builder()
        .timeout(Duration::from_secs(20))
        .connect_timeout(Duration::from_secs(5))
        .build()
        .context("failed to build http client")?;

    match cli.command {
        Command::Login {
            auto_activate,
            user_id,
            tenant_id,
            plan_tier,
        } => {
            run_login(
                &client,
                &cli.server,
                auto_activate,
                user_id,
                tenant_id,
                plan_tier.into(),
            )
            .await?;
        }
        Command::Mount {
            remote,
            target,
            allow_other,
            auto_unmount,
            read_only,
        } => {
            let server = if remote.starts_with("http://") || remote.starts_with("https://") {
                remote
            } else {
                cli.server
            };
            let db = ClientLocalDb::open_default()?;
            let token = load_token(&db)?;
            run_fuse_mount(
                &server,
                &token,
                &target,
                FuseMountOptions {
                    allow_other,
                    auto_unmount,
                    read_only,
                },
            )?;
        }
        Command::Sync {
            cursor,
            reconnect_delay_seconds,
            once,
            target,
        } => {
            let target = match target {
                Some(path) => path,
                None => default_sync_target_path()?,
            };
            run_sync(
                &client,
                &cli.server,
                cursor,
                reconnect_delay_seconds,
                once,
                target,
            )
            .await?;
        }
    }

    Ok(())
}

async fn run_login(
    client: &Client,
    server: &str,
    auto_activate: bool,
    user_id: String,
    tenant_id: String,
    plan_tier: PlanTier,
) -> anyhow::Result<()> {
    let start_url = format!("{server}/auth/device/start");
    let start_resp: DeviceStartResponse = client
        .post(start_url)
        .send()
        .await
        .context("start auth request failed")?
        .error_for_status()
        .context("start auth response status error")?
        .json()
        .await
        .context("failed to parse start auth response")?;

    println!("Open this URL in browser: {}", start_resp.verification_uri);
    println!("Enter user code: {}", start_resp.user_code);

    let _ = webbrowser::open(&start_resp.verification_uri);

    if auto_activate {
        let activate_url = format!("{server}/auth/device/activate");
        client
            .post(activate_url)
            .json(&DeviceActivateRequest {
                user_code: start_resp.user_code.clone(),
                user_id,
                tenant_id,
                plan_tier: Some(plan_tier),
            })
            .send()
            .await
            .context("auto activate request failed")?
            .error_for_status()
            .context("auto activate status error")?;
    }

    let poll_url = format!("{server}/auth/device/poll");
    loop {
        let resp = client
            .post(&poll_url)
            .json(&DevicePollRequest {
                device_code: start_resp.device_code.clone(),
            })
            .send()
            .await
            .context("poll request failed")?;

        if resp.status().is_success() {
            let tokens: DevicePollResponse =
                resp.json().await.context("failed to parse poll response")?;

            persist_token(&tokens.access_token)?;
            println!("login success, access token persisted to local SQLite");
            return Ok(());
        }

        tokio::time::sleep(Duration::from_secs(start_resp.interval_seconds)).await;
    }
}

async fn run_sync(
    client: &Client,
    server: &str,
    initial_cursor: u64,
    reconnect_delay_seconds: u64,
    once: bool,
    target: PathBuf,
) -> anyhow::Result<()> {
    fs::create_dir_all(&target)
        .with_context(|| format!("failed to create sync target dir {}", target.display()))?;

    let db = ClientLocalDb::open_default()?;
    let token = load_token(&db)?;
    let persisted_cursor = db.load_cursor(server)?;
    let start_cursor = if initial_cursor == 0 {
        persisted_cursor.unwrap_or(0)
    } else {
        initial_cursor
    };

    let first_pull = sync_pull_once(client, server, &token, start_cursor).await?;
    apply_events(client, server, &token, &target, &db, &first_pull.events).await?;
    let mut cursor = first_pull.next_cursor;
    db.save_cursor(server, cursor)?;

    if once {
        println!("sync finished once mode at cursor={cursor}");
        return Ok(());
    }

    let ws_base = to_ws_base(server)?;
    loop {
        let ws_url = format!("{ws_base}/sync/ws?cursor={cursor}");
        println!("sync ws connecting: {ws_url}");
        match sync_ws_loop(client, server, &ws_url, &token, cursor, &target, &db).await {
            Ok(new_cursor) => {
                cursor = new_cursor;
                println!(
                    "sync ws disconnected gracefully, reconnecting in {}s from cursor={cursor}",
                    reconnect_delay_seconds
                );
            }
            Err(err) => {
                println!(
                    "sync ws disconnected with error: {err}; reconnecting in {}s from cursor={cursor}",
                    reconnect_delay_seconds
                );
            }
        }

        tokio::time::sleep(Duration::from_secs(reconnect_delay_seconds)).await;
        let pull = sync_pull_once(client, server, &token, cursor).await?;
        apply_events(client, server, &token, &target, &db, &pull.events).await?;
        cursor = pull.next_cursor;
        db.save_cursor(server, cursor)?;
    }
}

async fn sync_pull_once(
    client: &Client,
    server: &str,
    token: &str,
    cursor: u64,
) -> anyhow::Result<SyncPullResponse> {
    let response = send_with_retry(
        || {
            client
                .get(format!("{server}/sync/pull?cursor={cursor}"))
                .bearer_auth(token)
        },
        "sync pull",
    )
    .await?
    .error_for_status()
    .context("sync pull response status error")?
    .json::<SyncPullResponse>()
    .await
    .context("failed to parse sync pull response")?;

    Ok(response)
}

async fn sync_ws_loop(
    client: &Client,
    server: &str,
    ws_url: &str,
    token: &str,
    mut cursor: u64,
    target: &Path,
    db: &ClientLocalDb,
) -> anyhow::Result<u64> {
    let mut request = ws_url.into_client_request().context("invalid ws url")?;
    request.headers_mut().insert(
        "Authorization",
        format!("Bearer {token}")
            .parse()
            .context("failed to build authorization header")?,
    );

    let (mut ws_stream, _) = connect_async(request)
        .await
        .context("failed to connect sync ws")?;

    while let Some(msg) = ws_stream.next().await {
        match msg.context("ws read failed")? {
            Message::Text(text) => {
                if let Ok(event) = serde_json::from_str::<ChangeEvent>(&text) {
                    cursor = cursor.max(event.cursor);
                    apply_change_event(client, server, token, target, db, &event).await?;
                    db.save_cursor(server, cursor)?;
                    print_change_event(&event, "ws");
                }
            }
            Message::Ping(payload) => {
                ws_stream
                    .send(Message::Pong(payload))
                    .await
                    .context("failed to send ws pong")?;
            }
            Message::Close(_) => break,
            Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
        }
    }

    Ok(cursor)
}

async fn apply_events(
    client: &Client,
    server: &str,
    token: &str,
    target: &Path,
    db: &ClientLocalDb,
    events: &[ChangeEvent],
) -> anyhow::Result<()> {
    for event in events {
        apply_change_event(client, server, token, target, db, event).await?;
        print_change_event(event, "pull");
    }
    Ok(())
}

async fn apply_change_event(
    client: &Client,
    server: &str,
    token: &str,
    target: &Path,
    db: &ClientLocalDb,
    event: &ChangeEvent,
) -> anyhow::Result<()> {
    match event.op {
        ChangeOp::Mkdir => {
            let local = local_path_for_remote(target, &event.path)?;
            fs::create_dir_all(&local)
                .with_context(|| format!("failed to create local dir {}", local.display()))?;
            db.save_node_path(server, &event.node_id, &event.path)?;
        }
        ChangeOp::Delete => {
            if let Some(old_path) = db.load_node_path(server, &event.node_id)? {
                remove_local_entry(target, &old_path)?;
            } else {
                remove_local_entry(target, &event.path)?;
            }
            db.delete_node_path(server, &event.node_id)?;
        }
        ChangeOp::Rename => {
            let old_path = db.load_node_path(server, &event.node_id)?;
            match old_path {
                Some(from_remote) => {
                    if rename_local_entry(target, &from_remote, &event.path).is_err() {
                        let _ = materialize_path_from_server(
                            client,
                            server,
                            token,
                            target,
                            &event.path,
                        )
                        .await?;
                    }
                }
                None => {
                    let _ =
                        materialize_path_from_server(client, server, token, target, &event.path)
                            .await?;
                }
            }
            db.save_node_path(server, &event.node_id, &event.path)?;
        }
        ChangeOp::Write | ChangeOp::Create | ChangeOp::Restore => {
            let materialized =
                materialize_path_from_server(client, server, token, target, &event.path).await?;
            if materialized.is_some() {
                db.save_node_path(server, &event.node_id, &event.path)?;
            } else {
                db.delete_node_path(server, &event.node_id)?;
            }
        }
    }
    Ok(())
}

async fn materialize_path_from_server(
    client: &Client,
    server: &str,
    token: &str,
    target: &Path,
    remote_path: &str,
) -> anyhow::Result<Option<NodeKind>> {
    let meta_resp = send_with_retry(
        || {
            client
                .get(format!("{server}/files/meta"))
                .bearer_auth(token)
                .query(&[("path", remote_path)])
        },
        "files meta",
    )
    .await?;

    if meta_resp.status() == StatusCode::NOT_FOUND {
        remove_local_entry(target, remote_path)?;
        return Ok(None);
    }

    let meta: MetaResponse = meta_resp
        .error_for_status()
        .context("files meta response status error")?
        .json()
        .await
        .context("failed to parse files meta response")?;

    let local = local_path_for_remote(target, &meta.node.path)?;
    match meta.node.kind {
        NodeKind::Dir => {
            fs::create_dir_all(&local)
                .with_context(|| format!("failed to create local dir {}", local.display()))?;
            Ok(Some(NodeKind::Dir))
        }
        NodeKind::File => {
            if let Some(parent) = local.parent() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("failed to create parent dir {}", parent.display()))?;
            }
            let download_resp = send_with_retry(
                || {
                    client
                        .get(format!("{server}/files/download"))
                        .bearer_auth(token)
                        .query(&[("path", meta.node.path.as_str())])
                },
                "files download",
            )
            .await?;
            let bytes = download_resp
                .error_for_status()
                .context("files download response status error")?
                .bytes()
                .await
                .context("failed to read download bytes")?;

            let tmp_path = local.with_extension("meshfs.tmp");
            fs::write(&tmp_path, &bytes)
                .with_context(|| format!("failed to write temp file {}", tmp_path.display()))?;
            fs::rename(&tmp_path, &local).with_context(|| {
                format!(
                    "failed to move temp file {} to {}",
                    tmp_path.display(),
                    local.display()
                )
            })?;
            Ok(Some(NodeKind::File))
        }
    }
}

fn rename_local_entry(target: &Path, from_remote: &str, to_remote: &str) -> anyhow::Result<()> {
    let from_local = local_path_for_remote(target, from_remote)?;
    let to_local = local_path_for_remote(target, to_remote)?;
    if !from_local.exists() {
        return Ok(());
    }
    if let Some(parent) = to_local.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create parent dir {}", parent.display()))?;
    }
    if to_local.exists() {
        remove_local_by_path(&to_local)?;
    }
    fs::rename(&from_local, &to_local).with_context(|| {
        format!(
            "failed to rename local path {} to {}",
            from_local.display(),
            to_local.display()
        )
    })?;
    Ok(())
}

fn remove_local_entry(target: &Path, remote_path: &str) -> anyhow::Result<()> {
    let local = local_path_for_remote(target, remote_path)?;
    remove_local_by_path(&local)
}

fn remove_local_by_path(path: &Path) -> anyhow::Result<()> {
    if !path.exists() {
        return Ok(());
    }
    if path.is_dir() {
        fs::remove_dir_all(path)
            .with_context(|| format!("failed to remove dir {}", path.display()))?;
    } else {
        fs::remove_file(path)
            .with_context(|| format!("failed to remove file {}", path.display()))?;
    }
    Ok(())
}

fn local_path_for_remote(target: &Path, remote_path: &str) -> anyhow::Result<PathBuf> {
    let normalized = normalize_remote_path(remote_path)?;
    if normalized == "/" {
        return Ok(target.to_path_buf());
    }

    let mut local = target.to_path_buf();
    for part in normalized.trim_start_matches('/').split('/') {
        if part.is_empty() || part == "." || part == ".." {
            return Err(anyhow::anyhow!(
                "invalid path segment in remote path: {part}"
            ));
        }
        local.push(part);
    }
    Ok(local)
}

fn normalize_remote_path(raw: &str) -> anyhow::Result<String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(anyhow::anyhow!("remote path cannot be empty"));
    }

    let mut parts = Vec::new();
    for part in raw.split('/') {
        if part.is_empty() || part == "." {
            continue;
        }
        if part == ".." {
            return Err(anyhow::anyhow!("path traversal not allowed"));
        }
        parts.push(part);
    }

    if parts.is_empty() {
        return Ok("/".to_string());
    }

    Ok(format!("/{}", parts.join("/")))
}

fn is_retryable_status(status: StatusCode) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

fn is_retryable_error(err: &reqwest::Error) -> bool {
    err.is_connect() || err.is_timeout()
}

async fn send_with_retry<F>(
    mut make_request: F,
    operation: &str,
) -> anyhow::Result<reqwest::Response>
where
    F: FnMut() -> reqwest::RequestBuilder,
{
    let max_attempts = 3u64;
    let mut attempt = 0u64;

    loop {
        attempt += 1;
        match make_request().send().await {
            Ok(resp) => {
                if is_retryable_status(resp.status()) && attempt < max_attempts {
                    tokio::time::sleep(Duration::from_millis(200 * attempt)).await;
                    continue;
                }
                return Ok(resp);
            }
            Err(err) => {
                if is_retryable_error(&err) && attempt < max_attempts {
                    tokio::time::sleep(Duration::from_millis(200 * attempt)).await;
                    continue;
                }
                return Err(err).with_context(|| format!("{operation} request failed"));
            }
        }
    }
}

fn print_change_event(event: &ChangeEvent, channel: &str) {
    println!(
        "[{channel}] cursor={} op={:?} path={} node_id={}",
        event.cursor, event.op, event.path, event.node_id
    );
}

fn to_ws_base(server: &str) -> anyhow::Result<String> {
    if let Some(rest) = server.strip_prefix("http://") {
        return Ok(format!("ws://{rest}"));
    }

    if let Some(rest) = server.strip_prefix("https://") {
        return Ok(format!("wss://{rest}"));
    }

    Err(anyhow::anyhow!(
        "server must start with http:// or https://, got: {server}"
    ))
}

fn load_token(db: &ClientLocalDb) -> anyhow::Result<String> {
    db.load_token()?
        .ok_or_else(|| anyhow::anyhow!("missing auth token in local SQLite; run `meshfs login`"))
}

fn persist_token(token: &str) -> anyhow::Result<()> {
    let db = ClientLocalDb::open_default()?;
    db.save_token(token)?;
    Ok(())
}

fn default_client_db_path() -> anyhow::Result<PathBuf> {
    let home = std::env::var("HOME").context("HOME env var missing")?;
    let dir = PathBuf::from(home).join(".meshfs");
    fs::create_dir_all(&dir).context("failed to create ~/.meshfs")?;
    Ok(dir.join("client.db"))
}

fn default_sync_target_path() -> anyhow::Result<PathBuf> {
    let home = std::env::var("HOME").context("HOME env var missing")?;
    let path = PathBuf::from(home)
        .join(".meshfs")
        .join("mirror")
        .join("default");
    Ok(path)
}

struct ClientLocalDb {
    conn: Connection,
}

impl ClientLocalDb {
    fn open_default() -> anyhow::Result<Self> {
        let path = default_client_db_path()?;
        let conn = Connection::open(path).context("failed to open local sqlite db")?;
        Self::bootstrap_schema(&conn)?;
        Ok(Self { conn })
    }

    #[cfg(test)]
    fn open_in_memory() -> anyhow::Result<Self> {
        let conn = Connection::open_in_memory().context("failed to open in-memory sqlite db")?;
        Self::bootstrap_schema(&conn)?;
        Ok(Self { conn })
    }

    fn bootstrap_schema(conn: &Connection) -> anyhow::Result<()> {
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS kv (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS node_paths (
              server TEXT NOT NULL,
              node_id TEXT NOT NULL,
              path TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              PRIMARY KEY (server, node_id)
            );
            "#,
        )
        .context("failed to bootstrap local sqlite schema")?;
        Ok(())
    }

    fn load_token(&self) -> anyhow::Result<Option<String>> {
        self.load_kv("auth.token")
    }

    fn save_token(&self, token: &str) -> anyhow::Result<()> {
        self.save_kv("auth.token", token)
    }

    fn load_cursor(&self, server: &str) -> anyhow::Result<Option<u64>> {
        let key = format!("sync.cursor.{server}");
        match self.load_kv(&key)? {
            Some(raw) => {
                let parsed = raw
                    .parse::<u64>()
                    .with_context(|| format!("invalid cursor value in sqlite for key {key}"))?;
                Ok(Some(parsed))
            }
            None => Ok(None),
        }
    }

    fn save_cursor(&self, server: &str, cursor: u64) -> anyhow::Result<()> {
        let key = format!("sync.cursor.{server}");
        self.save_kv(&key, &cursor.to_string())
    }

    fn load_node_path(&self, server: &str, node_id: &str) -> anyhow::Result<Option<String>> {
        self.conn
            .query_row(
                "SELECT path FROM node_paths WHERE server = ?1 AND node_id = ?2",
                params![server, node_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context("failed to query node path cache")
    }

    fn save_node_path(&self, server: &str, node_id: &str, path: &str) -> anyhow::Result<()> {
        self.conn
            .execute(
                r#"
                INSERT INTO node_paths(server, node_id, path, updated_at)
                VALUES (?1, ?2, ?3, datetime('now'))
                ON CONFLICT(server, node_id) DO UPDATE SET
                  path = excluded.path,
                  updated_at = excluded.updated_at
                "#,
                params![server, node_id, path],
            )
            .context("failed to upsert node path cache")?;
        Ok(())
    }

    fn delete_node_path(&self, server: &str, node_id: &str) -> anyhow::Result<()> {
        self.conn
            .execute(
                "DELETE FROM node_paths WHERE server = ?1 AND node_id = ?2",
                params![server, node_id],
            )
            .context("failed to delete node path cache")?;
        Ok(())
    }

    fn load_kv(&self, key: &str) -> anyhow::Result<Option<String>> {
        self.conn
            .query_row("SELECT value FROM kv WHERE key = ?1", params![key], |row| {
                row.get::<_, String>(0)
            })
            .optional()
            .context("failed to query kv from sqlite")
    }

    fn save_kv(&self, key: &str, value: &str) -> anyhow::Result<()> {
        self.conn
            .execute(
                r#"
                INSERT INTO kv(key, value, updated_at)
                VALUES (?1, ?2, datetime('now'))
                ON CONFLICT(key) DO UPDATE SET
                  value = excluded.value,
                  updated_at = excluded.updated_at
                "#,
                params![key, value],
            )
            .context("failed to upsert kv into sqlite")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use axum::extract::ws::{Message as WsMessage, WebSocketUpgrade};
    use axum::extract::{Query, State};
    use axum::http::StatusCode as AxumStatusCode;
    use axum::response::IntoResponse;
    use axum::routing::get;
    use axum::{Json, Router};
    use chrono::Utc;
    use meshfs_types::{
        ChangeEvent, ChangeOp, FileVersion, MetaResponse, Node, NodeKind, SyncPullResponse,
    };
    use reqwest::{Client, StatusCode as ReqwestStatusCode};
    use tokio::net::TcpListener;

    use super::{
        apply_events, is_retryable_status, local_path_for_remote, normalize_remote_path,
        remove_local_by_path, send_with_retry, sync_pull_once, sync_ws_loop, to_ws_base,
        ClientLocalDb,
    };

    #[derive(Default)]
    struct TestServerState {
        retry_attempts: AtomicUsize,
    }

    fn unique_temp_dir(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "meshfs-client-test-{name}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system time")
                .as_nanos()
        ));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    fn sync_events_from_cursor(cursor: u64) -> SyncPullResponse {
        let events = if cursor == 0 {
            vec![
                ChangeEvent {
                    event_id: "evt-mkdir".to_string(),
                    tenant_id: "tenant-test".to_string(),
                    node_id: "node-docs".to_string(),
                    op: ChangeOp::Mkdir,
                    version_id: None,
                    ts: Utc::now(),
                    actor: "tester".to_string(),
                    cursor: 1,
                    path: "/docs".to_string(),
                },
                ChangeEvent {
                    event_id: "evt-write".to_string(),
                    tenant_id: "tenant-test".to_string(),
                    node_id: "node-docs-a".to_string(),
                    op: ChangeOp::Write,
                    version_id: Some("ver-a".to_string()),
                    ts: Utc::now(),
                    actor: "tester".to_string(),
                    cursor: 2,
                    path: "/docs/a.txt".to_string(),
                },
            ]
        } else {
            Vec::new()
        };

        SyncPullResponse {
            next_cursor: events.last().map(|evt| evt.cursor).unwrap_or(cursor),
            events,
        }
    }

    async fn retry_handler(State(state): State<Arc<TestServerState>>) -> impl IntoResponse {
        let attempt = state.retry_attempts.fetch_add(1, Ordering::SeqCst);
        if attempt == 0 {
            (AxumStatusCode::INTERNAL_SERVER_ERROR, "retry me").into_response()
        } else {
            Json(serde_json::json!({"ok": true})).into_response()
        }
    }

    async fn sync_pull_handler(Query(query): Query<HashMap<String, String>>) -> impl IntoResponse {
        let cursor = query
            .get("cursor")
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(0);
        Json(sync_events_from_cursor(cursor))
    }

    async fn files_meta_handler(Query(query): Query<HashMap<String, String>>) -> impl IntoResponse {
        let path = query.get("path").map(String::as_str).unwrap_or("/");

        if path == "/docs" {
            let body = MetaResponse {
                node: Node {
                    node_id: "node-docs".to_string(),
                    tenant_id: "tenant-test".to_string(),
                    parent_id: Some("root".to_string()),
                    name: "docs".to_string(),
                    path: "/docs".to_string(),
                    kind: NodeKind::Dir,
                    logical_clock: 1,
                    deleted_at: None,
                },
                head_version: None,
            };
            return Json(body).into_response();
        }

        if path == "/docs/a.txt" {
            let body = MetaResponse {
                node: Node {
                    node_id: "node-docs-a".to_string(),
                    tenant_id: "tenant-test".to_string(),
                    parent_id: Some("node-docs".to_string()),
                    name: "a.txt".to_string(),
                    path: "/docs/a.txt".to_string(),
                    kind: NodeKind::File,
                    logical_clock: 1,
                    deleted_at: None,
                },
                head_version: Some(FileVersion {
                    version_id: "ver-a".to_string(),
                    node_id: "node-docs-a".to_string(),
                    blob_key: "blob-a".to_string(),
                    size: 5,
                    content_hash: None,
                    writer_device_id: Some("device-test".to_string()),
                    committed_at: Utc::now(),
                    overwrite_of_version_id: None,
                }),
            };
            return Json(body).into_response();
        }

        AxumStatusCode::NOT_FOUND.into_response()
    }

    async fn files_download_handler(
        Query(query): Query<HashMap<String, String>>,
    ) -> impl IntoResponse {
        let path = query.get("path").map(String::as_str).unwrap_or("/");
        if path == "/docs/a.txt" {
            return (
                [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
                "hello",
            )
                .into_response();
        }
        AxumStatusCode::NOT_FOUND.into_response()
    }

    async fn sync_ws_handler(ws: WebSocketUpgrade) -> impl IntoResponse {
        ws.on_upgrade(|mut socket| async move {
            let event = ChangeEvent {
                event_id: "evt-ws-write".to_string(),
                tenant_id: "tenant-test".to_string(),
                node_id: "node-docs-a".to_string(),
                op: ChangeOp::Write,
                version_id: Some("ver-a".to_string()),
                ts: Utc::now(),
                actor: "tester".to_string(),
                cursor: 7,
                path: "/docs/a.txt".to_string(),
            };
            let payload = serde_json::to_string(&event).expect("serialize ws event");
            let _ = socket.send(WsMessage::Text(payload.into())).await;
            let _ = socket.send(WsMessage::Close(None)).await;
        })
    }

    async fn start_test_server() -> (String, Arc<TestServerState>, tokio::task::JoinHandle<()>) {
        let state = Arc::new(TestServerState::default());
        let app = Router::new()
            .route("/retry-test", get(retry_handler))
            .route("/sync/pull", get(sync_pull_handler))
            .route("/sync/ws", get(sync_ws_handler))
            .route("/files/meta", get(files_meta_handler))
            .route("/files/download", get(files_download_handler))
            .with_state(Arc::clone(&state));

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let addr = listener.local_addr().expect("test listener addr");
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("run test server");
        });
        (format!("http://{addr}"), state, handle)
    }

    #[test]
    fn normalize_remote_path_rejects_traversal() {
        assert_eq!(normalize_remote_path("/a/b").unwrap(), "/a/b");
        assert!(normalize_remote_path("../../etc/passwd").is_err());
    }

    #[test]
    fn local_path_mapping_is_stable() {
        let base = std::env::temp_dir().join("meshfs-client-map-test");
        let local = local_path_for_remote(&base, "/docs/a.txt").unwrap();
        assert!(local.ends_with("docs/a.txt"));
    }

    #[test]
    fn remove_local_path_works_for_missing_entries() {
        let missing = std::env::temp_dir().join("meshfs-client-missing-file");
        remove_local_by_path(&missing).unwrap();
    }

    #[test]
    fn node_path_cache_roundtrip() {
        let db = ClientLocalDb::open_in_memory().unwrap();
        db.save_node_path("http://localhost:8787", "node-1", "/docs/a.txt")
            .unwrap();
        let loaded = db
            .load_node_path("http://localhost:8787", "node-1")
            .unwrap()
            .unwrap();
        assert_eq!(loaded, "/docs/a.txt");
        db.delete_node_path("http://localhost:8787", "node-1")
            .unwrap();
        assert!(db
            .load_node_path("http://localhost:8787", "node-1")
            .unwrap()
            .is_none());
    }

    #[test]
    fn ws_base_conversion_supports_http_and_https() {
        assert_eq!(
            to_ws_base("http://127.0.0.1:8787").unwrap(),
            "ws://127.0.0.1:8787"
        );
        assert_eq!(
            to_ws_base("https://meshfs.example").unwrap(),
            "wss://meshfs.example"
        );
        assert!(to_ws_base("meshfs.example").is_err());
    }

    #[test]
    fn retryable_status_classification_is_stable() {
        assert!(is_retryable_status(ReqwestStatusCode::TOO_MANY_REQUESTS));
        assert!(is_retryable_status(
            ReqwestStatusCode::INTERNAL_SERVER_ERROR
        ));
        assert!(!is_retryable_status(ReqwestStatusCode::BAD_REQUEST));
        assert!(!is_retryable_status(ReqwestStatusCode::OK));
    }

    #[tokio::test]
    async fn send_with_retry_retries_once_then_succeeds() {
        let (server, state, handle) = start_test_server().await;
        let client = Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .expect("build client");

        let resp = send_with_retry(|| client.get(format!("{server}/retry-test")), "retry-test")
            .await
            .expect("send with retry");
        assert_eq!(resp.status(), ReqwestStatusCode::OK);
        assert_eq!(state.retry_attempts.load(Ordering::SeqCst), 2);

        handle.abort();
    }

    #[tokio::test]
    async fn sync_pull_and_apply_events_materializes_local_files() {
        let (server, _state, handle) = start_test_server().await;
        let client = Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("build client");
        let target = unique_temp_dir("pull-materialize");
        let db = ClientLocalDb::open_in_memory().expect("open in-memory db");

        let pull = sync_pull_once(&client, &server, "test-token", 0)
            .await
            .expect("sync pull");
        assert_eq!(pull.events.len(), 2);
        assert_eq!(pull.next_cursor, 2);

        apply_events(&client, &server, "test-token", &target, &db, &pull.events)
            .await
            .expect("apply pulled events");

        assert!(target.join("docs").is_dir());
        let content = fs::read(target.join("docs").join("a.txt")).expect("read synced file");
        assert_eq!(content, b"hello");
        assert_eq!(
            db.load_node_path(&server, "node-docs-a")
                .expect("load path cache")
                .as_deref(),
            Some("/docs/a.txt")
        );

        handle.abort();
    }

    #[tokio::test]
    async fn sync_ws_loop_applies_push_event_and_updates_cursor() {
        let (server, _state, handle) = start_test_server().await;
        let client = Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("build client");
        let target = unique_temp_dir("ws-materialize");
        let db = ClientLocalDb::open_in_memory().expect("open in-memory db");

        let ws_base = to_ws_base(&server).expect("ws base");
        let ws_url = format!("{ws_base}/sync/ws?cursor=0");
        let cursor = sync_ws_loop(&client, &server, &ws_url, "test-token", 0, &target, &db)
            .await
            .expect("sync ws loop");

        assert_eq!(cursor, 7);
        let content = fs::read(target.join("docs").join("a.txt")).expect("read ws synced file");
        assert_eq!(content, b"hello");

        handle.abort();
    }
}
