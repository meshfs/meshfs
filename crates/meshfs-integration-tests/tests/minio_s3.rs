#[cfg(target_os = "linux")]
mod support;

#[cfg(not(target_os = "linux"))]
#[tokio::test]
async fn s3_backend_roundtrip_with_minio() {
    eprintln!("skipped: minio integration is only executed on Linux runners");
}

#[cfg(target_os = "linux")]
mod linux_minio {
    use std::fs;

    use anyhow::{anyhow, bail, Context};
    use base64::Engine;
    use reqwest::StatusCode;
    use tempfile::tempdir;

    use crate::support::{
        create_remote_text_file, read_saved_access_token, run_meshfs, start_device_authorization,
        BinaryPaths, RunningServer, ServerConfig,
    };

    fn minio_enabled() -> bool {
        std::env::var("MESHFS_TEST_ENABLE_MINIO").ok().as_deref() == Some("1")
    }

    fn minio_endpoint() -> String {
        std::env::var("MESHFS_TEST_MINIO_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:9000".to_string())
    }

    fn minio_access_key() -> String {
        std::env::var("MESHFS_TEST_MINIO_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".to_string())
    }

    fn minio_secret_key() -> String {
        std::env::var("MESHFS_TEST_MINIO_SECRET_KEY").unwrap_or_else(|_| "minioadmin".to_string())
    }

    fn minio_bucket() -> String {
        std::env::var("MESHFS_TEST_MINIO_BUCKET").unwrap_or_else(|_| "meshfs-int".to_string())
    }

    async fn start_s3_server(
        bins: &BinaryPaths,
        bucket: String,
        endpoint: String,
        access_key: String,
        secret_key: String,
    ) -> anyhow::Result<RunningServer> {
        RunningServer::start(
            &bins.server_bin,
            ServerConfig {
                extra_env: vec![
                    (
                        "MESHFS_OBJECT_STORE_BACKEND".to_string(),
                        "s3-compatible".to_string(),
                    ),
                    ("MESHFS_OBJECT_STORE_BUCKET".to_string(), bucket),
                    (
                        "MESHFS_OBJECT_STORE_REGION".to_string(),
                        "us-east-1".to_string(),
                    ),
                    ("MESHFS_OBJECT_STORE_ENDPOINT".to_string(), endpoint),
                    ("MESHFS_OBJECT_STORE_ACCESS_KEY_ID".to_string(), access_key),
                    (
                        "MESHFS_OBJECT_STORE_SECRET_ACCESS_KEY".to_string(),
                        secret_key,
                    ),
                    (
                        "MESHFS_OBJECT_STORE_FORCE_PATH_STYLE".to_string(),
                        "true".to_string(),
                    ),
                ],
            },
        )
        .await
    }

    async fn upload_text_and_capture_commit_error(
        server_url: &str,
        token: &str,
        remote_path: &str,
        payload: &[u8],
    ) -> anyhow::Result<(StatusCode, String)> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .context("build reqwest client")?;

        let parent = parent_path(remote_path)?;
        let mkdir_resp = client
            .post(format!("{server_url}/files/mkdir"))
            .bearer_auth(token)
            .json(&serde_json::json!({ "path": parent }))
            .send()
            .await
            .context("mkdir request")?;
        if mkdir_resp.status() != StatusCode::CREATED && mkdir_resp.status() != StatusCode::CONFLICT
        {
            bail!("mkdir failed unexpectedly: status={}", mkdir_resp.status());
        }

        let init_resp = client
            .post(format!("{server_url}/files/upload/init"))
            .bearer_auth(token)
            .json(&serde_json::json!({
                "path": remote_path,
                "size_hint": payload.len()
            }))
            .send()
            .await
            .context("upload init request")?;
        if !init_resp.status().is_success() {
            bail!(
                "upload init failed unexpectedly: status={}",
                init_resp.status()
            );
        }
        let init_body: serde_json::Value = init_resp.json().await.context("parse init body")?;
        let upload_id = init_body["upload_id"]
            .as_str()
            .ok_or_else(|| anyhow!("upload_id missing in init response"))?;

        let part_resp = client
            .put(format!("{server_url}/files/upload/part"))
            .bearer_auth(token)
            .json(&serde_json::json!({
                "upload_id": upload_id,
                "part_number": 1,
                "data_base64": base64::engine::general_purpose::STANDARD.encode(payload)
            }))
            .send()
            .await
            .context("upload part request")?;
        if part_resp.status() != StatusCode::NO_CONTENT {
            bail!(
                "upload part failed unexpectedly: status={}",
                part_resp.status()
            );
        }

        let commit_resp = client
            .post(format!("{server_url}/files/upload/commit"))
            .bearer_auth(token)
            .json(&serde_json::json!({ "upload_id": upload_id }))
            .send()
            .await
            .context("upload commit request")?;
        let status = commit_resp.status();
        let body = commit_resp.text().await.context("read commit body")?;
        Ok((status, body))
    }

    fn parent_path(path: &str) -> anyhow::Result<String> {
        let trimmed = path.trim();
        if !trimmed.starts_with('/') {
            bail!("path must start with '/': {trimmed}");
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

    #[tokio::test]
    async fn s3_backend_roundtrip_with_minio() -> anyhow::Result<()> {
        if !minio_enabled() {
            eprintln!("skipped: set MESHFS_TEST_ENABLE_MINIO=1 to run this test");
            return Ok(());
        }

        let Some(bins) = BinaryPaths::resolve_or_skip()? else {
            return Ok(());
        };

        let home_dir = tempdir().context("create home dir")?;
        let sync_target = tempdir().context("create sync target")?;

        let mut server = start_s3_server(
            &bins,
            minio_bucket(),
            minio_endpoint(),
            minio_access_key(),
            minio_secret_key(),
        )
        .await?;

        run_meshfs(
            &bins.client_bin,
            home_dir.path(),
            &[
                "--server".to_string(),
                server.base_url.clone(),
                "login".to_string(),
                "--auto-activate".to_string(),
                "--user-id".to_string(),
                "minio-user".to_string(),
                "--tenant-id".to_string(),
                "minio-tenant".to_string(),
            ],
        )?;

        let token = read_saved_access_token(home_dir.path())?;
        create_remote_text_file(
            &server.base_url,
            &token,
            "/bucket/probe.txt",
            b"hello-minio",
        )
        .await?;

        run_meshfs(
            &bins.client_bin,
            home_dir.path(),
            &[
                "--server".to_string(),
                server.base_url.clone(),
                "sync".to_string(),
                "--once".to_string(),
                "--target".to_string(),
                sync_target.path().to_string_lossy().to_string(),
            ],
        )?;

        let synced = sync_target.path().join("bucket").join("probe.txt");
        let bytes = fs::read(&synced)
            .with_context(|| format!("read synced minio-backed file: {}", synced.display()))?;
        assert_eq!(bytes, b"hello-minio");

        server.finish_ok();
        Ok(())
    }

    #[tokio::test]
    async fn s3_backend_missing_bucket_returns_internal_error_on_commit() -> anyhow::Result<()> {
        if !minio_enabled() {
            eprintln!("skipped: set MESHFS_TEST_ENABLE_MINIO=1 to run this test");
            return Ok(());
        }

        let Some(bins) = BinaryPaths::resolve_or_skip()? else {
            return Ok(());
        };

        let missing_bucket = format!(
            "meshfs-int-missing-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .context("system time")?
                .as_millis()
        );

        let mut server = start_s3_server(
            &bins,
            missing_bucket,
            minio_endpoint(),
            minio_access_key(),
            minio_secret_key(),
        )
        .await?;

        let (token, _) =
            start_device_authorization(&server.base_url, "missing-bucket-user", "tenant-a").await?;

        let (status, body) = upload_text_and_capture_commit_error(
            &server.base_url,
            &token,
            "/missing-bucket/probe.txt",
            b"payload",
        )
        .await?;

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(
            body.to_ascii_lowercase().contains("store")
                || body.to_ascii_lowercase().contains("bucket")
                || body.to_ascii_lowercase().contains("error"),
            "unexpected error body: {body}"
        );

        server.finish_ok();
        Ok(())
    }

    #[tokio::test]
    async fn s3_backend_invalid_credentials_returns_internal_error_on_commit() -> anyhow::Result<()>
    {
        if !minio_enabled() {
            eprintln!("skipped: set MESHFS_TEST_ENABLE_MINIO=1 to run this test");
            return Ok(());
        }

        let Some(bins) = BinaryPaths::resolve_or_skip()? else {
            return Ok(());
        };

        let mut server = start_s3_server(
            &bins,
            minio_bucket(),
            minio_endpoint(),
            "invalid-key".to_string(),
            "invalid-secret".to_string(),
        )
        .await?;

        let (token, _) =
            start_device_authorization(&server.base_url, "bad-creds-user", "tenant-a").await?;

        let (status, body) = upload_text_and_capture_commit_error(
            &server.base_url,
            &token,
            "/bad-creds/probe.txt",
            b"payload",
        )
        .await?;

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(
            body.to_ascii_lowercase().contains("store")
                || body.to_ascii_lowercase().contains("signature")
                || body.to_ascii_lowercase().contains("access")
                || body.to_ascii_lowercase().contains("error"),
            "unexpected error body: {body}"
        );

        server.finish_ok();
        Ok(())
    }

    #[tokio::test]
    async fn s3_backend_unreachable_endpoint_returns_internal_error_on_commit() -> anyhow::Result<()>
    {
        if !minio_enabled() {
            eprintln!("skipped: set MESHFS_TEST_ENABLE_MINIO=1 to run this test");
            return Ok(());
        }

        let Some(bins) = BinaryPaths::resolve_or_skip()? else {
            return Ok(());
        };

        let mut server = start_s3_server(
            &bins,
            minio_bucket(),
            "http://127.0.0.1:65530".to_string(),
            minio_access_key(),
            minio_secret_key(),
        )
        .await?;

        let (token, _) =
            start_device_authorization(&server.base_url, "unreachable-endpoint-user", "tenant-a")
                .await?;

        let (status, body) = upload_text_and_capture_commit_error(
            &server.base_url,
            &token,
            "/unreachable/probe.txt",
            b"payload",
        )
        .await?;

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(
            body.to_ascii_lowercase().contains("store")
                || body.to_ascii_lowercase().contains("connection")
                || body.to_ascii_lowercase().contains("timed")
                || body.to_ascii_lowercase().contains("error"),
            "unexpected error body: {body}"
        );

        server.finish_ok();
        Ok(())
    }
}
