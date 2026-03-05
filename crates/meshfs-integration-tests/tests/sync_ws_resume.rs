mod support;

use std::collections::HashSet;
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use futures::{SinkExt, StreamExt};
use meshfs_types::{ChangeEvent, SyncPullResponse};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

use crate::support::{
    create_remote_text_file, start_device_authorization, BinaryPaths, RunningServer, ServerConfig,
};

async fn create_remote_text_file_with_retry(
    server_url: &str,
    token: &str,
    remote_path: &str,
    payload: &[u8],
) -> anyhow::Result<()> {
    let attempts = std::env::var("MESHFS_TEST_API_MAX_ATTEMPTS")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(3);

    for attempt in 1..=attempts {
        match create_remote_text_file(server_url, token, remote_path, payload).await {
            Ok(()) => return Ok(()),
            Err(err) if attempt < attempts => {
                eprintln!(
                    "transient create_remote_text_file failure (attempt {attempt}/{attempts}) for {remote_path}: {err}"
                );
                tokio::time::sleep(Duration::from_millis(200 * u64::from(attempt))).await;
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!(
                        "create_remote_text_file failed after {attempts} attempts for {remote_path}"
                    )
                })
            }
        }
    }

    unreachable!("loop always returns")
}

async fn connect_sync_ws(
    server_url: &str,
    token: &str,
    cursor: u64,
) -> anyhow::Result<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
> {
    let ws_base = if let Some(rest) = server_url.strip_prefix("http://") {
        format!("ws://{rest}")
    } else if let Some(rest) = server_url.strip_prefix("https://") {
        format!("wss://{rest}")
    } else {
        bail!("invalid server url: {server_url}");
    };

    let ws_url = format!("{ws_base}/sync/ws?cursor={cursor}");
    let mut req = ws_url
        .into_client_request()
        .context("build websocket request")?;
    req.headers_mut().insert(
        "Authorization",
        format!("Bearer {token}")
            .parse()
            .context("build auth header")?,
    );

    let (stream, _) = connect_async(req).await.context("connect sync ws")?;
    Ok(stream)
}

async fn collect_until(
    stream: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    timeout: Duration,
    mut done: impl FnMut(&[ChangeEvent]) -> bool,
) -> anyhow::Result<Vec<ChangeEvent>> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut events = Vec::new();

    while tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let item = tokio::time::timeout(remaining, stream.next())
            .await
            .context("timed out waiting ws message")?;

        let Some(msg) = item else {
            break;
        };

        match msg.context("ws recv error")? {
            Message::Text(text) => {
                if let Ok(event) = serde_json::from_str::<ChangeEvent>(&text) {
                    events.push(event);
                    if done(&events) {
                        break;
                    }
                }
            }
            Message::Ping(payload) => {
                stream
                    .send(Message::Pong(payload))
                    .await
                    .context("send ws pong")?;
            }
            Message::Close(_) => break,
            Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
        }
    }

    Ok(events)
}

async fn sync_pull(server_url: &str, token: &str, cursor: u64) -> anyhow::Result<SyncPullResponse> {
    let client = reqwest::Client::new();
    client
        .get(format!("{server_url}/sync/pull"))
        .bearer_auth(token)
        .query(&[("cursor", cursor)])
        .send()
        .await
        .context("sync pull request")?
        .error_for_status()
        .context("sync pull status error")?
        .json::<SyncPullResponse>()
        .await
        .context("parse sync pull response")
}

#[tokio::test]
async fn sync_ws_reconnect_from_last_cursor_only_receives_new_events() -> anyhow::Result<()> {
    let Some(bins) = BinaryPaths::resolve_or_skip()? else {
        return Ok(());
    };

    let mut server = RunningServer::start(&bins.server_bin, ServerConfig::default()).await?;
    let (token, _) =
        start_device_authorization(&server.base_url, "sync-ws-user", "sync-ws-tenant").await?;

    let mut ws1 = connect_sync_ws(&server.base_url, &token, 0).await?;
    create_remote_text_file_with_retry(&server.base_url, &token, "/ws-r1/a.txt", b"one").await?;

    let first_batch = collect_until(&mut ws1, Duration::from_secs(10), |events| {
        events.iter().any(|e| e.path == "/ws-r1/a.txt")
    })
    .await?;
    if first_batch.is_empty() {
        bail!("first ws batch is empty");
    }

    let max_cursor = first_batch
        .iter()
        .map(|e| e.cursor)
        .max()
        .ok_or_else(|| anyhow!("first batch cursor missing"))?;

    ws1.send(Message::Close(None))
        .await
        .context("close first ws")?;

    let seen_cursors: HashSet<u64> = first_batch.iter().map(|e| e.cursor).collect();

    create_remote_text_file_with_retry(&server.base_url, &token, "/ws-r2/b.txt", b"two").await?;

    let mut ws2 = connect_sync_ws(&server.base_url, &token, max_cursor).await?;
    let second_batch = collect_until(&mut ws2, Duration::from_secs(10), |events| {
        events.iter().any(|e| e.path == "/ws-r2/b.txt")
    })
    .await?;

    if second_batch.is_empty() {
        bail!("second ws batch is empty");
    }

    for evt in &second_batch {
        assert!(
            evt.cursor > max_cursor,
            "expected cursor > {max_cursor}, got {}",
            evt.cursor
        );
        assert!(
            !seen_cursors.contains(&evt.cursor),
            "duplicate cursor replayed after resume: {}",
            evt.cursor
        );
    }

    server.finish_ok();
    Ok(())
}

#[tokio::test]
async fn sync_ws_resume_backlog_matches_sync_pull_for_same_cursor() -> anyhow::Result<()> {
    let Some(bins) = BinaryPaths::resolve_or_skip()? else {
        return Ok(());
    };

    let mut server = RunningServer::start(&bins.server_bin, ServerConfig::default()).await?;
    let (token, _) =
        start_device_authorization(&server.base_url, "sync-ws-user2", "sync-ws-tenant2").await?;

    let mut ws1 = connect_sync_ws(&server.base_url, &token, 0).await?;
    create_remote_text_file_with_retry(&server.base_url, &token, "/ws-m1/a.txt", b"one").await?;

    let first_batch = collect_until(&mut ws1, Duration::from_secs(10), |events| {
        events.iter().any(|e| e.path == "/ws-m1/a.txt")
    })
    .await?;
    let cursor = first_batch
        .iter()
        .map(|e| e.cursor)
        .max()
        .ok_or_else(|| anyhow!("first ws batch had no cursor"))?;

    ws1.send(Message::Close(None)).await.context("close ws1")?;

    create_remote_text_file_with_retry(&server.base_url, &token, "/ws-m2/b.txt", b"two").await?;

    let expected_backlog = sync_pull(&server.base_url, &token, cursor).await?;
    if expected_backlog.events.is_empty() {
        bail!("expected backlog from sync_pull is empty");
    }

    let mut ws2 = connect_sync_ws(&server.base_url, &token, cursor).await?;
    let expected_ids: HashSet<String> = expected_backlog
        .events
        .iter()
        .map(|e| e.event_id.clone())
        .collect();

    let resumed = collect_until(&mut ws2, Duration::from_secs(10), |events| {
        let got: HashSet<String> = events.iter().map(|e| e.event_id.clone()).collect();
        expected_ids.is_subset(&got)
    })
    .await?;

    let got_ids: HashSet<String> = resumed.iter().map(|e| e.event_id.clone()).collect();
    assert!(
        expected_ids.is_subset(&got_ids),
        "ws resume events must contain all sync_pull backlog events"
    );

    server.finish_ok();
    Ok(())
}
