#[cfg(target_os = "linux")]
mod support;

#[cfg(not(target_os = "linux"))]
#[tokio::test]
async fn s3_backend_roundtrip_with_minio() {
    eprintln!("skipped: minio integration is only executed on Linux runners");
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn s3_backend_roundtrip_with_minio() -> anyhow::Result<()> {
    use std::fs;

    use anyhow::Context;
    use tempfile::tempdir;

    use crate::support::{
        create_remote_text_file, read_saved_access_token, run_meshfs, BinaryPaths, RunningServer,
        ServerConfig,
    };

    if std::env::var("MESHFS_TEST_ENABLE_MINIO").ok().as_deref() != Some("1") {
        eprintln!("skipped: set MESHFS_TEST_ENABLE_MINIO=1 to run this test");
        return Ok(());
    }

    let Some(bins) = BinaryPaths::resolve_or_skip()? else {
        return Ok(());
    };
    let home_dir = tempdir().context("create home dir")?;
    let sync_target = tempdir().context("create sync target")?;

    let endpoint = std::env::var("MESHFS_TEST_MINIO_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:9000".to_string());
    let access_key =
        std::env::var("MESHFS_TEST_MINIO_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".to_string());
    let secret_key =
        std::env::var("MESHFS_TEST_MINIO_SECRET_KEY").unwrap_or_else(|_| "minioadmin".to_string());
    let bucket =
        std::env::var("MESHFS_TEST_MINIO_BUCKET").unwrap_or_else(|_| "meshfs-int".to_string());

    let server = RunningServer::start(
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

    Ok(())
}
