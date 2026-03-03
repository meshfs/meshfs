pub mod auth;
pub mod config;
pub mod error;
pub mod routes;

use std::sync::Arc;

use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use meshfs_store::ObjectStore;
use serde_json::json;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;

use crate::config::AppConfig;
use meshfs_control_plane_core::state::AppState;

pub fn build_app(config: AppConfig, object_store: Arc<dyn ObjectStore>) -> Router {
    let state = Arc::new(AppState::new_with_runtime_config(
        config.jwt_secret,
        config.token_ttl_seconds,
        config.refresh_token_ttl_seconds,
        object_store,
        Some(config.metadata_sqlite_path),
        config.rate_limit_per_minute,
        config.tenant_storage_quota_bytes,
    ));

    Router::new()
        .route("/healthz", get(|| async { Json(json!({"status": "ok"})) }))
        .route("/auth/device/start", post(routes::auth_device_start))
        .route("/auth/device/poll", post(routes::auth_device_poll))
        .route("/auth/device/activate", post(routes::auth_device_activate))
        .route("/auth/refresh", post(routes::auth_refresh))
        .route("/auth/logout", post(routes::auth_logout))
        .route("/files/upload/init", post(routes::files_upload_init))
        .route("/files/upload/part", put(routes::files_upload_part))
        .route("/files/upload/commit", post(routes::files_upload_commit))
        .route("/files/mkdir", post(routes::files_mkdir))
        .route("/files/rename", post(routes::files_rename))
        .route("/files", delete(routes::files_delete))
        .route("/files/meta", get(routes::files_meta))
        .route("/files/list", get(routes::files_list))
        .route("/files/download", get(routes::files_download))
        .route("/files/{node_id}/versions", get(routes::versions_list))
        .route(
            "/files/{node_id}/versions/{version_id}/restore",
            post(routes::versions_restore),
        )
        .route("/sync/pull", get(routes::sync_pull))
        .route("/sync/stream", get(routes::sync_stream))
        .route("/sync/ws", get(routes::sync_ws))
        .route("/plans/current", get(routes::current_plan))
        .route("/retention/policy", get(routes::retention_policy))
        .route("/retention/apply", post(routes::retention_apply))
        .route("/audit/recent", get(routes::audit_recent))
        .layer(
            CorsLayer::new()
                .allow_methods(Any)
                .allow_headers(Any)
                .allow_origin(Any),
        )
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use meshfs_store::InMemoryObjectStore;
    use serde_json::{json, Value};
    use tower::util::ServiceExt;

    use super::build_app;
    use crate::config::{AppConfig, ObjectStoreBackend};

    fn test_config() -> AppConfig {
        AppConfig {
            bind_addr: "127.0.0.1:0".to_string(),
            jwt_secret: "meshfs-test-secret".to_string(),
            token_ttl_seconds: 3600,
            refresh_token_ttl_seconds: 30 * 24 * 3600,
            dev_auto_approve: true,
            metadata_sqlite_path: std::env::temp_dir()
                .join(format!(
                    "meshfs-route-test-{}.db",
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .expect("system time")
                        .as_nanos()
                ))
                .to_string_lossy()
                .to_string(),
            object_store_backend: ObjectStoreBackend::InMemory,
            object_store_bucket: None,
            object_store_region: "us-east-1".to_string(),
            object_store_endpoint: None,
            object_store_access_key_id: None,
            object_store_secret_access_key: None,
            object_store_force_path_style: false,
            object_store_r2_account_id: None,
            rate_limit_per_minute: 10_000,
            tenant_storage_quota_bytes: 10 * 1024 * 1024,
        }
    }

    fn request_with_json(
        method: &str,
        uri: &str,
        token: Option<&str>,
        payload: Value,
    ) -> Request<Body> {
        let mut builder = Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json");
        if let Some(token) = token {
            builder = builder.header("authorization", format!("Bearer {token}"));
        }
        builder
            .body(Body::from(payload.to_string()))
            .expect("request")
    }

    fn request_without_body(method: &str, uri: &str, token: Option<&str>) -> Request<Body> {
        let mut builder = Request::builder().method(method).uri(uri);
        if let Some(token) = token {
            builder = builder.header("authorization", format!("Bearer {token}"));
        }
        builder.body(Body::empty()).expect("request")
    }

    async fn response_json(resp: axum::response::Response) -> Value {
        let body = to_bytes(resp.into_body(), usize::MAX)
            .await
            .expect("read response body");
        serde_json::from_slice(&body).expect("valid json body")
    }

    #[tokio::test]
    async fn route_surface_supports_auth_file_sync_and_policy_flow() {
        let app = build_app(test_config(), Arc::new(InMemoryObjectStore::default()));

        let start = app
            .clone()
            .oneshot(request_without_body("POST", "/auth/device/start", None))
            .await
            .expect("auth device start");
        assert_eq!(start.status(), StatusCode::OK);
        let start_body = response_json(start).await;
        let device_code = start_body["device_code"].as_str().unwrap().to_string();
        let user_code = start_body["user_code"].as_str().unwrap().to_string();

        let activate = app
            .clone()
            .oneshot(request_with_json(
                "POST",
                "/auth/device/activate",
                None,
                json!({
                    "user_code": user_code,
                    "user_id": "u1",
                    "tenant_id": "t1",
                    "plan_tier": "free"
                }),
            ))
            .await
            .expect("auth device activate");
        assert_eq!(activate.status(), StatusCode::NO_CONTENT);

        let poll = app
            .clone()
            .oneshot(request_with_json(
                "POST",
                "/auth/device/poll",
                None,
                json!({"device_code": device_code}),
            ))
            .await
            .expect("auth device poll");
        assert_eq!(poll.status(), StatusCode::OK);
        let poll_body = response_json(poll).await;
        let token = poll_body["access_token"].as_str().unwrap().to_string();
        let refresh = poll_body["refresh_token"].as_str().unwrap().to_string();

        let mkdir = app
            .clone()
            .oneshot(request_with_json(
                "POST",
                "/files/mkdir",
                Some(&token),
                json!({"path": "/docs"}),
            ))
            .await
            .expect("files mkdir");
        assert_eq!(mkdir.status(), StatusCode::CREATED);

        let init = app
            .clone()
            .oneshot(request_with_json(
                "POST",
                "/files/upload/init",
                Some(&token),
                json!({"path": "/docs/a.txt", "size_hint": 3}),
            ))
            .await
            .expect("upload init");
        assert_eq!(init.status(), StatusCode::OK);
        let init_body = response_json(init).await;
        let upload_id = init_body["upload_id"].as_str().unwrap().to_string();

        let part = app
            .clone()
            .oneshot(request_with_json(
                "PUT",
                "/files/upload/part",
                Some(&token),
                json!({
                    "upload_id": upload_id,
                    "part_number": 1,
                    "data_base64": "b25l"
                }),
            ))
            .await
            .expect("upload part");
        assert_eq!(part.status(), StatusCode::NO_CONTENT);

        let commit = app
            .clone()
            .oneshot(request_with_json(
                "POST",
                "/files/upload/commit",
                Some(&token),
                json!({"upload_id": upload_id}),
            ))
            .await
            .expect("upload commit");
        assert_eq!(commit.status(), StatusCode::OK);
        let commit_body = response_json(commit).await;
        let node_id = commit_body["node_id"].as_str().unwrap().to_string();
        let version_id = commit_body["version_id"].as_str().unwrap().to_string();

        let meta = app
            .clone()
            .oneshot(request_without_body(
                "GET",
                "/files/meta?path=/docs/a.txt",
                Some(&token),
            ))
            .await
            .expect("files meta");
        assert_eq!(meta.status(), StatusCode::OK);

        let list = app
            .clone()
            .oneshot(request_without_body(
                "GET",
                "/files/list?path=/docs",
                Some(&token),
            ))
            .await
            .expect("files list");
        assert_eq!(list.status(), StatusCode::OK);

        let download = app
            .clone()
            .oneshot(request_without_body(
                "GET",
                "/files/download?path=/docs/a.txt",
                Some(&token),
            ))
            .await
            .expect("files download");
        assert_eq!(download.status(), StatusCode::OK);
        let download_body = to_bytes(download.into_body(), usize::MAX)
            .await
            .expect("download body");
        assert_eq!(&download_body[..], b"one");

        let versions = app
            .clone()
            .oneshot(request_without_body(
                "GET",
                &format!("/files/{node_id}/versions"),
                Some(&token),
            ))
            .await
            .expect("versions list");
        assert_eq!(versions.status(), StatusCode::OK);

        let restore = app
            .clone()
            .oneshot(request_without_body(
                "POST",
                &format!("/files/{node_id}/versions/{version_id}/restore"),
                Some(&token),
            ))
            .await
            .expect("version restore");
        assert_eq!(restore.status(), StatusCode::OK);

        let pull = app
            .clone()
            .oneshot(request_without_body(
                "GET",
                "/sync/pull?cursor=0",
                Some(&token),
            ))
            .await
            .expect("sync pull");
        assert_eq!(pull.status(), StatusCode::OK);

        let stream = app
            .clone()
            .oneshot(request_without_body("GET", "/sync/stream", Some(&token)))
            .await
            .expect("sync stream");
        assert_eq!(stream.status(), StatusCode::OK);

        let plan = app
            .clone()
            .oneshot(request_without_body("GET", "/plans/current", Some(&token)))
            .await
            .expect("current plan");
        assert_eq!(plan.status(), StatusCode::OK);

        let retention = app
            .clone()
            .oneshot(request_without_body(
                "GET",
                "/retention/policy",
                Some(&token),
            ))
            .await
            .expect("retention policy");
        assert_eq!(retention.status(), StatusCode::OK);

        let apply = app
            .clone()
            .oneshot(request_with_json(
                "POST",
                "/retention/apply",
                Some(&token),
                json!({
                    "tenant_id": "t1",
                    "policy": {
                        "plan_tier": "free",
                        "max_days": 7,
                        "max_versions": 20,
                        "hard_delete_grace_days": 7
                    }
                }),
            ))
            .await
            .expect("retention apply");
        assert_eq!(apply.status(), StatusCode::NO_CONTENT);

        let audit = app
            .clone()
            .oneshot(request_without_body(
                "GET",
                "/audit/recent?limit=10",
                Some(&token),
            ))
            .await
            .expect("audit recent");
        assert_eq!(audit.status(), StatusCode::OK);

        let refresh_resp = app
            .clone()
            .oneshot(request_with_json(
                "POST",
                "/auth/refresh",
                None,
                json!({ "refresh_token": refresh }),
            ))
            .await
            .expect("auth refresh");
        assert_eq!(refresh_resp.status(), StatusCode::OK);
        let refresh_body = response_json(refresh_resp).await;
        let rotated_refresh = refresh_body["refresh_token"].as_str().unwrap().to_string();

        let logout = app
            .clone()
            .oneshot(request_with_json(
                "POST",
                "/auth/logout",
                Some(&token),
                json!({ "refresh_token": rotated_refresh }),
            ))
            .await
            .expect("auth logout");
        assert_eq!(logout.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn unauthorized_requests_are_rejected() {
        let app = build_app(test_config(), Arc::new(InMemoryObjectStore::default()));
        let resp = app
            .oneshot(request_without_body("GET", "/plans/current", None))
            .await
            .expect("unauthorized request");
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }
}
