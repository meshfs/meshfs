use std::env;
use std::path::PathBuf;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObjectStoreBackend {
    InMemory,
    S3Compatible,
}

#[derive(Clone)]
pub struct AppConfig {
    pub bind_addr: String,
    pub jwt_secret: String,
    pub token_ttl_seconds: i64,
    pub refresh_token_ttl_seconds: i64,
    pub dev_auto_approve: bool,
    pub metadata_sqlite_path: String,
    pub object_store_backend: ObjectStoreBackend,
    pub object_store_bucket: Option<String>,
    pub object_store_region: String,
    pub object_store_endpoint: Option<String>,
    pub object_store_access_key_id: Option<String>,
    pub object_store_secret_access_key: Option<String>,
    pub object_store_force_path_style: bool,
    pub object_store_r2_account_id: Option<String>,
    pub rate_limit_per_minute: u64,
    pub tenant_storage_quota_bytes: u64,
}

impl AppConfig {
    pub fn from_env() -> Self {
        let backend_raw = env::var("MESHFS_OBJECT_STORE_BACKEND")
            .unwrap_or_else(|_| "in-memory".to_string())
            .to_lowercase();
        let object_store_backend = parse_object_store_backend(&backend_raw);

        Self {
            bind_addr: env::var("MESHFS_BIND_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:8787".to_string()),
            jwt_secret: env::var("MESHFS_JWT_SECRET")
                .unwrap_or_else(|_| "meshfs-dev-secret".to_string()),
            token_ttl_seconds: env::var("MESHFS_TOKEN_TTL_SECONDS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(3600),
            refresh_token_ttl_seconds: env::var("MESHFS_REFRESH_TOKEN_TTL_SECONDS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(30 * 24 * 3600),
            dev_auto_approve: env::var("MESHFS_DEV_AUTO_APPROVE")
                .ok()
                .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "True"))
                .unwrap_or(true),
            metadata_sqlite_path: env::var("MESHFS_METADATA_SQLITE_PATH")
                .unwrap_or_else(|_| default_metadata_sqlite_path()),
            object_store_backend,
            object_store_bucket: env::var("MESHFS_OBJECT_STORE_BUCKET").ok(),
            object_store_region: env::var("MESHFS_OBJECT_STORE_REGION")
                .unwrap_or_else(|_| "us-east-1".to_string()),
            object_store_endpoint: env::var("MESHFS_OBJECT_STORE_ENDPOINT").ok(),
            object_store_access_key_id: env::var("MESHFS_OBJECT_STORE_ACCESS_KEY_ID").ok(),
            object_store_secret_access_key: env::var("MESHFS_OBJECT_STORE_SECRET_ACCESS_KEY").ok(),
            object_store_force_path_style: env::var("MESHFS_OBJECT_STORE_FORCE_PATH_STYLE")
                .ok()
                .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "True"))
                .unwrap_or(false),
            object_store_r2_account_id: env::var("MESHFS_OBJECT_STORE_R2_ACCOUNT_ID").ok(),
            rate_limit_per_minute: env::var("MESHFS_RATE_LIMIT_PER_MINUTE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1200),
            tenant_storage_quota_bytes: env::var("MESHFS_TENANT_STORAGE_QUOTA_BYTES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10 * 1024 * 1024 * 1024),
        }
    }
}

fn default_metadata_sqlite_path() -> String {
    let mut base = if cfg!(target_os = "windows") {
        env::var_os("LOCALAPPDATA")
            .or_else(|| env::var_os("APPDATA"))
            .map(PathBuf::from)
    } else if cfg!(target_os = "macos") {
        env::var_os("HOME")
            .map(PathBuf::from)
            .map(|home| home.join("Library").join("Application Support"))
    } else {
        env::var_os("XDG_DATA_HOME").map(PathBuf::from).or_else(|| {
            env::var_os("HOME")
                .map(PathBuf::from)
                .map(|home| home.join(".local").join("share"))
        })
    }
    .unwrap_or_else(std::env::temp_dir);

    base.push("meshfs");
    base.push("control-plane");
    base.push("metadata.db");
    base.to_string_lossy().into_owned()
}

fn parse_object_store_backend(raw: &str) -> ObjectStoreBackend {
    match raw {
        "s3" | "s3-compatible" | "r2" => ObjectStoreBackend::S3Compatible,
        _ => ObjectStoreBackend::InMemory,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        default_metadata_sqlite_path, parse_object_store_backend, AppConfig, ObjectStoreBackend,
    };
    use std::path::Path;

    #[test]
    fn default_config_values_are_safe_for_local_dev() {
        let cfg = AppConfig::from_env();
        assert!(cfg.rate_limit_per_minute > 0);
        assert!(cfg.tenant_storage_quota_bytes > 0);
        assert!(cfg.refresh_token_ttl_seconds > cfg.token_ttl_seconds);
    }

    #[test]
    fn default_metadata_sqlite_path_is_not_repo_root_file() {
        let path = default_metadata_sqlite_path();
        let p = Path::new(&path);
        assert_eq!(p.file_name().and_then(|v| v.to_str()), Some("metadata.db"));
        assert_eq!(
            p.parent()
                .and_then(|v| v.file_name())
                .and_then(|v| v.to_str()),
            Some("control-plane")
        );
        assert!(
            !path.ends_with("meshfs-control-plane.db"),
            "default path should not use the old repository-root filename"
        );
    }

    #[test]
    fn backend_parser_handles_supported_values() {
        assert_eq!(
            parse_object_store_backend("in-memory"),
            ObjectStoreBackend::InMemory
        );
        assert_eq!(
            parse_object_store_backend("s3-compatible"),
            ObjectStoreBackend::S3Compatible
        );
        assert_eq!(
            parse_object_store_backend("r2"),
            ObjectStoreBackend::S3Compatible
        );
    }

    #[test]
    fn backend_parser_falls_back_to_in_memory_for_unknown_values() {
        assert_eq!(
            parse_object_store_backend("unknown-backend"),
            ObjectStoreBackend::InMemory
        );
    }
}
