mod support;

use std::fs;

use anyhow::Context;
use reqwest::StatusCode;
use tempfile::tempdir;

use crate::support::{
    create_remote_text_file, read_saved_access_token, run_meshfs, start_device_authorization,
    BinaryPaths, RunningServer, ServerConfig,
};

#[tokio::test]
async fn cli_login_and_sync_once_materializes_remote_file() -> anyhow::Result<()> {
    let Some(bins) = BinaryPaths::resolve_or_skip()? else {
        return Ok(());
    };
    let home_dir = tempdir().context("create home dir")?;
    let sync_target = tempdir().context("create sync target")?;

    let mut server = RunningServer::start(&bins.server_bin, ServerConfig::default()).await?;

    run_meshfs(
        &bins.client_bin,
        home_dir.path(),
        &[
            "--server".to_string(),
            server.base_url.clone(),
            "login".to_string(),
            "--auto-activate".to_string(),
            "--user-id".to_string(),
            "integration-user".to_string(),
            "--tenant-id".to_string(),
            "integration-tenant".to_string(),
        ],
    )?;

    let token = read_saved_access_token(home_dir.path())?;
    create_remote_text_file(&server.base_url, &token, "/docs/a.txt", b"one").await?;

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

    let synced = sync_target.path().join("docs").join("a.txt");
    let bytes = fs::read(&synced)
        .with_context(|| format!("read synced file after sync once: {}", synced.display()))?;
    assert_eq!(bytes, b"one");

    server.finish_ok();
    Ok(())
}

#[tokio::test]
async fn unauthorized_request_is_rejected() -> anyhow::Result<()> {
    let Some(bins) = BinaryPaths::resolve_or_skip()? else {
        return Ok(());
    };
    let mut server = RunningServer::start(&bins.server_bin, ServerConfig::default()).await?;

    let response = reqwest::Client::new()
        .get(format!("{}/plans/current", server.base_url))
        .send()
        .await
        .context("plans/current request")?;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    server.finish_ok();
    Ok(())
}

#[tokio::test]
async fn refresh_and_logout_flow_rotates_and_revokes_refresh_token() -> anyhow::Result<()> {
    let Some(bins) = BinaryPaths::resolve_or_skip()? else {
        return Ok(());
    };
    let mut server = RunningServer::start(&bins.server_bin, ServerConfig::default()).await?;
    let client = reqwest::Client::new();

    let (access_token, refresh_token) =
        start_device_authorization(&server.base_url, "refresh-user", "refresh-tenant").await?;

    let refresh_resp = client
        .post(format!("{}/auth/refresh", server.base_url))
        .json(&serde_json::json!({ "refresh_token": refresh_token }))
        .send()
        .await
        .context("refresh request")?;
    assert_eq!(refresh_resp.status(), StatusCode::OK);

    let refresh_body: serde_json::Value = refresh_resp.json().await.context("refresh body")?;
    let rotated = refresh_body["refresh_token"]
        .as_str()
        .context("rotated refresh token missing")?
        .to_string();

    let logout_resp = client
        .post(format!("{}/auth/logout", server.base_url))
        .bearer_auth(&access_token)
        .json(&serde_json::json!({ "refresh_token": rotated }))
        .send()
        .await
        .context("logout request")?;
    assert_eq!(logout_resp.status(), StatusCode::NO_CONTENT);

    let second_refresh = client
        .post(format!("{}/auth/refresh", server.base_url))
        .json(&serde_json::json!({ "refresh_token": rotated }))
        .send()
        .await
        .context("refresh request after logout")?;

    assert_eq!(second_refresh.status(), StatusCode::UNAUTHORIZED);
    server.finish_ok();
    Ok(())
}
