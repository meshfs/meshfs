use std::sync::Arc;

use meshfs_control_plane::build_app;
use meshfs_control_plane::config::{AppConfig, ObjectStoreBackend};
use meshfs_store::{InMemoryObjectStore, ObjectStore, S3CompatibleConfig, S3CompatibleObjectStore};
use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "meshfs_control_plane=info,tower_http=info".to_string()),
        )
        .init();

    let config = AppConfig::from_env();
    let object_store = build_object_store(&config).await?;
    let app = build_app(config.clone(), object_store);

    let listener = TcpListener::bind(&config.bind_addr).await?;
    info!(bind_addr = %config.bind_addr, "meshfs control plane listening");

    axum::serve(listener, app).await?;
    Ok(())
}

async fn build_object_store(config: &AppConfig) -> anyhow::Result<Arc<dyn ObjectStore>> {
    match config.object_store_backend {
        ObjectStoreBackend::InMemory => Ok(Arc::new(InMemoryObjectStore::default())),
        ObjectStoreBackend::S3Compatible => {
            let bucket =
                required_env_cfg("MESHFS_OBJECT_STORE_BUCKET", &config.object_store_bucket)?;
            let access_key_id = required_env_cfg(
                "MESHFS_OBJECT_STORE_ACCESS_KEY_ID",
                &config.object_store_access_key_id,
            )?;
            let secret_access_key = required_env_cfg(
                "MESHFS_OBJECT_STORE_SECRET_ACCESS_KEY",
                &config.object_store_secret_access_key,
            )?;

            let s3_config = if let Some(account_id) = &config.object_store_r2_account_id {
                S3CompatibleObjectStore::for_cloudflare_r2(
                    bucket,
                    account_id.clone(),
                    access_key_id,
                    secret_access_key,
                )
            } else {
                S3CompatibleConfig {
                    bucket,
                    region: config.object_store_region.clone(),
                    endpoint: config.object_store_endpoint.clone(),
                    access_key_id,
                    secret_access_key,
                    force_path_style: config.object_store_force_path_style,
                }
            };

            let store = S3CompatibleObjectStore::new(s3_config).await?;
            Ok(Arc::new(store))
        }
    }
}

fn required_env_cfg(name: &str, value: &Option<String>) -> anyhow::Result<String> {
    value
        .as_ref()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("missing required env: {name}"))
}

#[cfg(test)]
mod tests {
    use super::{build_object_store, required_env_cfg};
    use meshfs_control_plane::config::{AppConfig, ObjectStoreBackend};

    fn test_config(backend: ObjectStoreBackend) -> AppConfig {
        AppConfig {
            bind_addr: "127.0.0.1:0".to_string(),
            jwt_secret: "test-secret".to_string(),
            token_ttl_seconds: 3600,
            refresh_token_ttl_seconds: 30 * 24 * 3600,
            dev_auto_approve: true,
            metadata_sqlite_path: ":memory:".to_string(),
            object_store_backend: backend,
            object_store_bucket: None,
            object_store_region: "us-east-1".to_string(),
            object_store_endpoint: None,
            object_store_access_key_id: None,
            object_store_secret_access_key: None,
            object_store_force_path_style: false,
            object_store_r2_account_id: None,
            rate_limit_per_minute: 1200,
            tenant_storage_quota_bytes: 10 * 1024 * 1024 * 1024,
        }
    }

    #[test]
    fn required_env_cfg_enforces_presence() {
        assert_eq!(
            required_env_cfg("KEY", &Some("value".to_string())).unwrap(),
            "value"
        );
        let err = required_env_cfg("KEY", &None).expect_err("missing value should fail");
        assert!(err.to_string().contains("missing required env: KEY"));
    }

    #[tokio::test]
    async fn build_object_store_supports_in_memory_backend() {
        let cfg = test_config(ObjectStoreBackend::InMemory);
        let store = build_object_store(&cfg)
            .await
            .expect("in-memory backend should build");
        let err = store
            .get("missing")
            .await
            .expect_err("missing object should return error");
        assert!(err.to_string().contains("object not found"));
    }

    #[tokio::test]
    async fn build_object_store_s3_backend_requires_credentials() {
        let cfg = test_config(ObjectStoreBackend::S3Compatible);
        let err = build_object_store(&cfg)
            .await
            .err()
            .expect("missing s3 config should fail");
        assert!(err.to_string().contains("MESHFS_OBJECT_STORE_BUCKET"));
    }
}
