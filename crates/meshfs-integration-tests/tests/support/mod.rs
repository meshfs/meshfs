use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Output, Stdio};
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use base64::Engine;
use reqwest::StatusCode;
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::json;
use tempfile::TempDir;

pub struct BinaryPaths {
    pub client_bin: PathBuf,
    pub server_bin: PathBuf,
}

impl BinaryPaths {
    pub fn resolve() -> anyhow::Result<Self> {
        Ok(Self {
            client_bin: resolve_binary_path("MESHFS_TEST_CLIENT_BIN", "meshfs")?,
            server_bin: resolve_binary_path("MESHFS_TEST_SERVER_BIN", "meshfs-control-plane")?,
        })
    }

    pub fn resolve_or_skip() -> anyhow::Result<Option<Self>> {
        match Self::resolve() {
            Ok(paths) => Ok(Some(paths)),
            Err(err) => {
                if std::env::var("MESHFS_TEST_STRICT").ok().as_deref() == Some("1") {
                    Err(err)
                } else {
                    eprintln!("skipped: integration binaries unavailable: {err}");
                    Ok(None)
                }
            }
        }
    }
}

#[derive(Default)]
pub struct ServerConfig {
    pub extra_env: Vec<(String, String)>,
}

pub struct RunningServer {
    pub base_url: String,
    pub work_dir: TempDir,
    child: Child,
}

impl RunningServer {
    pub async fn start(server_bin: &Path, config: ServerConfig) -> anyhow::Result<Self> {
        let work_dir = tempfile::tempdir().context("create server work dir")?;
        let port = pick_unused_port()?;
        let bind_addr = format!("127.0.0.1:{port}");
        let base_url = format!("http://{bind_addr}");
        let metadata_path = work_dir.path().join("metadata.db");

        let stdout_log = work_dir.path().join("control-plane.stdout.log");
        let stderr_log = work_dir.path().join("control-plane.stderr.log");

        let stdout = File::create(&stdout_log).context("create server stdout log")?;
        let stderr = File::create(&stderr_log).context("create server stderr log")?;

        let mut cmd = Command::new(server_bin);
        cmd.env("MESHFS_BIND_ADDR", &bind_addr)
            .env("MESHFS_METADATA_SQLITE_PATH", &metadata_path)
            .env("MESHFS_JWT_SECRET", "meshfs-integration-test-secret")
            .env("MESHFS_DEV_AUTO_APPROVE", "true")
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));

        for (key, value) in config.extra_env {
            cmd.env(key, value);
        }

        let child = cmd
            .spawn()
            .with_context(|| format!("spawn control plane: {}", server_bin.display()))?;

        let mut server = Self {
            base_url,
            work_dir,
            child,
        };

        server
            .wait_until_healthy(Duration::from_secs(20))
            .await
            .context("wait control plane healthy")?;

        Ok(server)
    }

    async fn wait_until_healthy(&mut self, timeout: Duration) -> anyhow::Result<()> {
        let deadline = std::time::Instant::now() + timeout;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .context("build healthcheck client")?;
        let health_url = format!("{}/healthz", self.base_url);

        loop {
            if let Some(status) = self
                .child
                .try_wait()
                .context("poll control plane process")?
            {
                return Err(self.server_exit_err(status));
            }

            match client.get(&health_url).send().await {
                Ok(resp) if resp.status().is_success() => return Ok(()),
                _ => {
                    if std::time::Instant::now() >= deadline {
                        bail!("control plane healthcheck timed out: {health_url}");
                    }
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            }
        }
    }

    fn server_exit_err(&self, status: ExitStatus) -> anyhow::Error {
        let stdout_log = self.work_dir.path().join("control-plane.stdout.log");
        let stderr_log = self.work_dir.path().join("control-plane.stderr.log");
        anyhow!(
            "control plane exited early with status {status}; logs: stdout={}, stderr={}",
            stdout_log.display(),
            stderr_log.display()
        )
    }
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

pub fn run_meshfs(client_bin: &Path, home_dir: &Path, args: &[String]) -> anyhow::Result<Output> {
    let output = Command::new(client_bin)
        .env("HOME", home_dir)
        .env("BROWSER", "true")
        .args(args)
        .output()
        .with_context(|| format!("run meshfs: {} {}", client_bin.display(), args.join(" ")))?;

    if !output.status.success() {
        bail!(
            "meshfs command failed (status={}):\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    Ok(output)
}

pub fn read_saved_access_token(home_dir: &Path) -> anyhow::Result<String> {
    let db_path = home_dir.join(".meshfs").join("client.db");
    let conn = Connection::open(&db_path)
        .with_context(|| format!("open client sqlite db: {}", db_path.display()))?;

    let token: Option<String> = conn
        .query_row(
            "SELECT value FROM kv WHERE key = ?1",
            params!["auth.token"],
            |row| row.get(0),
        )
        .optional()
        .context("query auth token")?;

    token.ok_or_else(|| anyhow!("auth token missing in sqlite db: {}", db_path.display()))
}

pub async fn create_remote_text_file(
    server_url: &str,
    token: &str,
    remote_path: &str,
    payload: &[u8],
) -> anyhow::Result<()> {
    let client = reqwest::Client::new();

    let parent = parent_path(remote_path)?;
    let mkdir_resp = client
        .post(format!("{server_url}/files/mkdir"))
        .bearer_auth(token)
        .json(&json!({ "path": parent }))
        .send()
        .await
        .context("mkdir request")?;

    if mkdir_resp.status() != StatusCode::CREATED && mkdir_resp.status() != StatusCode::CONFLICT {
        bail!(
            "mkdir failed: status={} path={}",
            mkdir_resp.status(),
            parent
        );
    }

    let init_resp = client
        .post(format!("{server_url}/files/upload/init"))
        .bearer_auth(token)
        .json(&json!({
            "path": remote_path,
            "size_hint": payload.len()
        }))
        .send()
        .await
        .context("upload init request")?;

    if !init_resp.status().is_success() {
        bail!("upload init failed: status={}", init_resp.status());
    }

    let init_body: serde_json::Value = init_resp.json().await.context("parse upload init body")?;
    let upload_id = init_body["upload_id"]
        .as_str()
        .ok_or_else(|| anyhow!("upload init response missing upload_id"))?;

    let part_resp = client
        .put(format!("{server_url}/files/upload/part"))
        .bearer_auth(token)
        .json(&json!({
            "upload_id": upload_id,
            "part_number": 1,
            "data_base64": base64::engine::general_purpose::STANDARD.encode(payload)
        }))
        .send()
        .await
        .context("upload part request")?;

    if part_resp.status() != StatusCode::NO_CONTENT {
        bail!("upload part failed: status={}", part_resp.status());
    }

    let commit_resp = client
        .post(format!("{server_url}/files/upload/commit"))
        .bearer_auth(token)
        .json(&json!({ "upload_id": upload_id }))
        .send()
        .await
        .context("upload commit request")?;

    if !commit_resp.status().is_success() {
        bail!("upload commit failed: status={}", commit_resp.status());
    }

    Ok(())
}

pub async fn start_device_authorization(
    server_url: &str,
    user_id: &str,
    tenant_id: &str,
) -> anyhow::Result<(String, String)> {
    let client = reqwest::Client::new();
    let start_resp = client
        .post(format!("{server_url}/auth/device/start"))
        .send()
        .await
        .context("device start request")?;

    if !start_resp.status().is_success() {
        bail!("device start failed: status={}", start_resp.status());
    }

    let start: serde_json::Value = start_resp.json().await.context("parse device start body")?;
    let user_code = start["user_code"]
        .as_str()
        .ok_or_else(|| anyhow!("device start missing user_code"))?
        .to_string();
    let device_code = start["device_code"]
        .as_str()
        .ok_or_else(|| anyhow!("device start missing device_code"))?
        .to_string();

    let activate_resp = client
        .post(format!("{server_url}/auth/device/activate"))
        .json(&json!({
            "user_code": user_code,
            "user_id": user_id,
            "tenant_id": tenant_id,
            "plan_tier": "free"
        }))
        .send()
        .await
        .context("device activate request")?;

    if activate_resp.status() != StatusCode::NO_CONTENT {
        bail!("device activate failed: status={}", activate_resp.status());
    }

    let poll_resp = client
        .post(format!("{server_url}/auth/device/poll"))
        .json(&json!({ "device_code": device_code }))
        .send()
        .await
        .context("device poll request")?;

    if !poll_resp.status().is_success() {
        bail!("device poll failed: status={}", poll_resp.status());
    }

    let poll: serde_json::Value = poll_resp.json().await.context("parse device poll body")?;
    let access = poll["access_token"]
        .as_str()
        .ok_or_else(|| anyhow!("poll missing access_token"))?
        .to_string();
    let refresh = poll["refresh_token"]
        .as_str()
        .ok_or_else(|| anyhow!("poll missing refresh_token"))?
        .to_string();

    Ok((access, refresh))
}

fn parent_path(path: &str) -> anyhow::Result<String> {
    let trimmed = path.trim();
    if !trimmed.starts_with('/') {
        bail!("remote path must start with '/': {trimmed}");
    }

    let mut parts: Vec<&str> = trimmed.split('/').filter(|p| !p.is_empty()).collect();
    if parts.is_empty() {
        return Ok("/".to_string());
    }

    parts.pop();
    if parts.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", parts.join("/")))
    }
}

fn resolve_binary_path(env_key: &str, binary_name: &str) -> anyhow::Result<PathBuf> {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .ok_or_else(|| anyhow!("failed to resolve workspace root"))?;

    if let Ok(path) = std::env::var(env_key) {
        let provided = PathBuf::from(path);
        if provided.exists() {
            return Ok(provided);
        }
        if provided.is_relative() {
            let joined = workspace_root.join(&provided);
            if joined.exists() {
                return Ok(joined);
            }
        }
        bail!(
            "provided binary path from {env_key} does not exist: {}",
            provided.display()
        );
    }

    let mut guessed = workspace_root
        .join("target")
        .join("debug")
        .join(binary_name);
    if cfg!(target_os = "windows") {
        guessed.set_extension("exe");
    }

    if guessed.exists() {
        Ok(guessed)
    } else {
        bail!(
            "binary not found for {binary_name}; set {env_key} or build it first at {}",
            guessed.display()
        )
    }
}

fn pick_unused_port() -> anyhow::Result<u16> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").context("bind test port")?;
    let port = listener.local_addr().context("read local addr")?.port();
    Ok(port)
}
