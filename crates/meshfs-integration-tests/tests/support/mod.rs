use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use base64::Engine;
use reqwest::StatusCode;
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::json;

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
    pub work_dir: PathBuf,
    cleanup_on_drop: bool,
    child: Child,
}

impl RunningServer {
    pub async fn start(server_bin: &Path, config: ServerConfig) -> anyhow::Result<Self> {
        let work_dir = create_test_dir("meshfs-int-server")?;
        let port = pick_unused_port()?;
        let bind_addr = format!("127.0.0.1:{port}");
        let base_url = format!("http://{bind_addr}");
        let metadata_path = work_dir.join("metadata.db");

        let stdout_log = work_dir.join("control-plane.stdout.log");
        let stderr_log = work_dir.join("control-plane.stderr.log");

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
            cleanup_on_drop: false,
            child,
        };

        server
            .wait_until_healthy(Duration::from_secs(20))
            .await
            .context("wait control plane healthy")?;

        Ok(server)
    }

    pub fn finish_ok(&mut self) {
        if !should_keep_artifacts() {
            self.cleanup_on_drop = true;
        }
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
                        bail!(
                            "control plane healthcheck timed out: {health_url}; {}",
                            self.server_diagnostics()
                        );
                    }
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
            }
        }
    }

    fn server_exit_err(&self, status: ExitStatus) -> anyhow::Error {
        anyhow!(
            "control plane exited early with status {status}; {}",
            self.server_diagnostics()
        )
    }

    fn server_diagnostics(&self) -> String {
        let stdout_log = self.work_dir.join("control-plane.stdout.log");
        let stderr_log = self.work_dir.join("control-plane.stderr.log");
        let stdout_tail = read_file_tail(&stdout_log, 30);
        let stderr_tail = read_file_tail(&stderr_log, 30);
        format!(
            "work_dir={} stdout_log={} stderr_log={} stdout_tail=\n{}\nstderr_tail=\n{}",
            self.work_dir.display(),
            stdout_log.display(),
            stderr_log.display(),
            stdout_tail,
            stderr_tail
        )
    }
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        if self.cleanup_on_drop && !std::thread::panicking() {
            let _ = fs::remove_dir_all(&self.work_dir);
        }
    }
}

pub fn run_meshfs(client_bin: &Path, home_dir: &Path, args: &[String]) -> anyhow::Result<Output> {
    let attempts = std::env::var("MESHFS_TEST_CLI_MAX_ATTEMPTS")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(2);

    for attempt in 1..=attempts {
        let output = Command::new(client_bin)
            .env("HOME", home_dir)
            .env("BROWSER", "true")
            .args(args)
            .output()
            .with_context(|| format!("run meshfs: {} {}", client_bin.display(), args.join(" ")))?;

        if output.status.success() {
            return Ok(output);
        }

        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let transient = is_transient_cli_failure(&stderr);
        if transient && attempt < attempts {
            std::thread::sleep(Duration::from_millis(200 * u64::from(attempt)));
            continue;
        }

        bail!(
            "meshfs command failed (attempt={attempt}/{attempts}, transient={transient}, status={}):\ncmd={} {}\nhome_dir={}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            client_bin.display(),
            args.join(" "),
            home_dir.display(),
            stdout,
            stderr
        );
    }

    unreachable!("loop always returns or bails")
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
    let attempts = api_attempts();
    for attempt in 1..=attempts {
        let result: anyhow::Result<()> = async {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .context("build api client")?;

            let parent = parent_path(remote_path)?;
            let mkdir_resp = client
                .post(format!("{server_url}/files/mkdir"))
                .bearer_auth(token)
                .json(&json!({ "path": parent }))
                .send()
                .await
                .context("mkdir request")?;

            if mkdir_resp.status() != StatusCode::CREATED
                && mkdir_resp.status() != StatusCode::CONFLICT
            {
                let status = mkdir_resp.status();
                let body = mkdir_resp.text().await.unwrap_or_default();
                bail!("mkdir failed: status={status} path={} body={body}", parent);
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
                let status = init_resp.status();
                let body = init_resp.text().await.unwrap_or_default();
                bail!("upload init failed: status={status} body={body}");
            }

            let init_body: serde_json::Value =
                init_resp.json().await.context("parse upload init body")?;
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
                let status = part_resp.status();
                let body = part_resp.text().await.unwrap_or_default();
                bail!("upload part failed: status={status} body={body}");
            }

            let commit_resp = client
                .post(format!("{server_url}/files/upload/commit"))
                .bearer_auth(token)
                .json(&json!({ "upload_id": upload_id }))
                .send()
                .await
                .context("upload commit request")?;

            if !commit_resp.status().is_success() {
                let status = commit_resp.status();
                let body = commit_resp.text().await.unwrap_or_default();
                bail!("upload commit failed: status={status} body={body}");
            }

            Ok(())
        }
        .await;

        match result {
            Ok(()) => return Ok(()),
            Err(err) if attempt < attempts && is_transient_api_error(&err) => {
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

pub async fn start_device_authorization(
    server_url: &str,
    user_id: &str,
    tenant_id: &str,
) -> anyhow::Result<(String, String)> {
    let attempts = api_attempts();
    for attempt in 1..=attempts {
        let result: anyhow::Result<(String, String)> = async {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .context("build auth client")?;
            let start_resp = client
                .post(format!("{server_url}/auth/device/start"))
                .send()
                .await
                .context("device start request")?;

            if !start_resp.status().is_success() {
                let status = start_resp.status();
                let body = start_resp.text().await.unwrap_or_default();
                bail!("device start failed: status={status} body={body}");
            }

            let start: serde_json::Value =
                start_resp.json().await.context("parse device start body")?;
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
                let status = activate_resp.status();
                let body = activate_resp.text().await.unwrap_or_default();
                bail!("device activate failed: status={status} body={body}");
            }

            let poll_resp = client
                .post(format!("{server_url}/auth/device/poll"))
                .json(&json!({ "device_code": device_code }))
                .send()
                .await
                .context("device poll request")?;

            if !poll_resp.status().is_success() {
                let status = poll_resp.status();
                let body = poll_resp.text().await.unwrap_or_default();
                bail!("device poll failed: status={status} body={body}");
            }

            let poll: serde_json::Value =
                poll_resp.json().await.context("parse device poll body")?;
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
        .await;

        match result {
            Ok(tokens) => return Ok(tokens),
            Err(err) if attempt < attempts && is_transient_api_error(&err) => {
                tokio::time::sleep(Duration::from_millis(200 * u64::from(attempt))).await;
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!(
                        "start_device_authorization failed after {attempts} attempts for user={user_id} tenant={tenant_id}"
                    )
                })
            }
        }
    }

    unreachable!("loop always returns")
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

fn is_transient_cli_failure(stderr: &str) -> bool {
    let s = stderr.to_ascii_lowercase();
    s.contains("connection refused")
        || s.contains("connection reset")
        || s.contains("timed out")
        || s.contains("failed to connect")
        || s.contains("dns error")
        || s.contains("temporary failure")
}

fn api_attempts() -> u32 {
    std::env::var("MESHFS_TEST_API_MAX_ATTEMPTS")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(3)
}

fn is_transient_api_error(err: &anyhow::Error) -> bool {
    let s = err.to_string().to_ascii_lowercase();
    s.contains("status=500")
        || s.contains("status=502")
        || s.contains("status=503")
        || s.contains("status=504")
        || s.contains("status=429")
        || s.contains("timed out")
        || s.contains("connection refused")
        || s.contains("connection reset")
        || s.contains("failed to connect")
        || s.contains("dns")
}

fn should_keep_artifacts() -> bool {
    std::env::var("MESHFS_TEST_KEEP_ARTIFACTS").ok().as_deref() == Some("1")
}

fn read_file_tail(path: &Path, max_lines: usize) -> String {
    match fs::read_to_string(path) {
        Ok(content) => {
            let mut lines: Vec<&str> = content.lines().rev().take(max_lines).collect();
            lines.reverse();
            lines.join("\n")
        }
        Err(err) => format!("<unavailable: {err}>"),
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

fn create_test_dir(prefix: &str) -> anyhow::Result<PathBuf> {
    static NEXT_DIR_ID: AtomicU64 = AtomicU64::new(1);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("system clock before unix epoch")?
        .as_nanos();
    let id = NEXT_DIR_ID.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let root_dir = test_artifacts_root_dir()?;
    fs::create_dir_all(&root_dir)
        .with_context(|| format!("create artifacts root dir {}", root_dir.display()))?;
    let path = root_dir.join(format!("{prefix}-{pid}-{nanos}-{id}"));
    fs::create_dir_all(&path).with_context(|| format!("create test dir {}", path.display()))?;
    Ok(path)
}

fn test_artifacts_root_dir() -> anyhow::Result<PathBuf> {
    match std::env::var("MESHFS_TEST_ARTIFACTS_DIR") {
        Ok(path) if !path.trim().is_empty() => {
            let root = PathBuf::from(path);
            if root.is_absolute() {
                Ok(root)
            } else {
                let cwd = std::env::current_dir()
                    .context("resolve current dir for MESHFS_TEST_ARTIFACTS_DIR")?;
                Ok(cwd.join(root))
            }
        }
        _ => Ok(std::env::temp_dir()),
    }
}

#[cfg(test)]
mod tests {
    use super::is_transient_cli_failure;

    #[test]
    fn transient_cli_failure_detection_is_stable() {
        assert!(is_transient_cli_failure(
            "request failed: connection refused"
        ));
        assert!(is_transient_cli_failure("operation Timed Out"));
        assert!(!is_transient_cli_failure("invalid request: unauthorized"));
    }
}
