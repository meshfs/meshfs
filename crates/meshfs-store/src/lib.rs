use std::collections::HashMap;
use std::sync::Arc;

#[cfg(not(target_arch = "wasm32"))]
use anyhow::Context;
use async_trait::async_trait;
#[cfg(not(target_arch = "wasm32"))]
use aws_config::BehaviorVersion;
#[cfg(not(target_arch = "wasm32"))]
use aws_sdk_s3::config::{Credentials, Region};
#[cfg(not(target_arch = "wasm32"))]
use aws_sdk_s3::types::CompletedMultipartUpload;
#[cfg(not(target_arch = "wasm32"))]
use aws_sdk_s3::Client;
use bytes::Bytes;
use tokio::sync::RwLock;

#[derive(thiserror::Error, Debug)]
pub enum StoreError {
    #[error("object not found: {0}")]
    NotFound(String),
    #[error("store operation failed: {0}")]
    Other(String),
}

pub type StoreResult<T> = Result<T, StoreError>;

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait ObjectStore: Send + Sync {
    async fn put(&self, key: &str, value: Bytes) -> StoreResult<()>;
    async fn get(&self, key: &str) -> StoreResult<Bytes>;
    async fn delete(&self, key: &str) -> StoreResult<()>;
    async fn copy(&self, src: &str, dst: &str) -> StoreResult<()>;
}

#[derive(Default, Clone)]
pub struct InMemoryObjectStore {
    inner: Arc<RwLock<HashMap<String, Bytes>>>,
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl ObjectStore for InMemoryObjectStore {
    async fn put(&self, key: &str, value: Bytes) -> StoreResult<()> {
        self.inner.write().await.insert(key.to_owned(), value);
        Ok(())
    }

    async fn get(&self, key: &str) -> StoreResult<Bytes> {
        let map = self.inner.read().await;
        map.get(key)
            .cloned()
            .ok_or_else(|| StoreError::NotFound(key.to_owned()))
    }

    async fn delete(&self, key: &str) -> StoreResult<()> {
        self.inner.write().await.remove(key);
        Ok(())
    }

    async fn copy(&self, src: &str, dst: &str) -> StoreResult<()> {
        let mut map = self.inner.write().await;
        let payload = map
            .get(src)
            .cloned()
            .ok_or_else(|| StoreError::NotFound(src.to_owned()))?;
        map.insert(dst.to_owned(), payload);
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
pub struct S3CompatibleConfig {
    pub bucket: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub force_path_style: bool,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
pub struct S3CompatibleObjectStore {
    bucket: String,
    client: Client,
}

#[cfg(not(target_arch = "wasm32"))]
impl S3CompatibleObjectStore {
    pub async fn new(config: S3CompatibleConfig) -> anyhow::Result<Self> {
        let mut loader = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(config.region.clone()))
            .credentials_provider(Credentials::new(
                config.access_key_id,
                config.secret_access_key,
                None,
                None,
                "meshfs",
            ));

        if let Some(endpoint) = &config.endpoint {
            loader = loader.endpoint_url(endpoint);
        }

        let shared = loader.load().await;
        let mut s3_builder = aws_sdk_s3::config::Builder::from(&shared);

        if config.force_path_style {
            s3_builder = s3_builder.force_path_style(true);
        }

        let client = Client::from_conf(s3_builder.build());

        Ok(Self {
            bucket: config.bucket,
            client,
        })
    }

    pub fn for_aws_s3(
        bucket: String,
        region: String,
        access_key_id: String,
        secret_access_key: String,
    ) -> S3CompatibleConfig {
        S3CompatibleConfig {
            bucket,
            region,
            endpoint: None,
            access_key_id,
            secret_access_key,
            force_path_style: false,
        }
    }

    pub fn for_cloudflare_r2(
        bucket: String,
        account_id: String,
        access_key_id: String,
        secret_access_key: String,
    ) -> S3CompatibleConfig {
        S3CompatibleConfig {
            bucket,
            region: "auto".to_string(),
            endpoint: Some(format!("https://{account_id}.r2.cloudflarestorage.com")),
            access_key_id,
            secret_access_key,
            force_path_style: true,
        }
    }

    #[allow(dead_code)]
    pub async fn complete_multipart_noop(&self) -> anyhow::Result<CompletedMultipartUpload> {
        let parts = Vec::new();
        Ok(CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build())
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl ObjectStore for S3CompatibleObjectStore {
    async fn put(&self, key: &str, value: Bytes) -> StoreResult<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(value.into())
            .send()
            .await
            .map_err(|err| StoreError::Other(err.to_string()))?;
        Ok(())
    }

    async fn get(&self, key: &str) -> StoreResult<Bytes> {
        let out = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|err| StoreError::Other(err.to_string()))?;

        let data = out
            .body
            .collect()
            .await
            .context("failed to collect S3 object body")
            .map_err(|err| StoreError::Other(err.to_string()))?;

        Ok(data.into_bytes())
    }

    async fn delete(&self, key: &str) -> StoreResult<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|err| StoreError::Other(err.to_string()))?;
        Ok(())
    }

    async fn copy(&self, src: &str, dst: &str) -> StoreResult<()> {
        let source = format!("{}/{}", self.bucket, src);
        self.client
            .copy_object()
            .bucket(&self.bucket)
            .copy_source(source)
            .key(dst)
            .send()
            .await
            .map_err(|err| StoreError::Other(err.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    #[cfg(not(target_arch = "wasm32"))]
    use super::S3CompatibleObjectStore;
    use super::{InMemoryObjectStore, ObjectStore, StoreError};

    #[tokio::test]
    async fn in_memory_store_roundtrip_and_copy() {
        let store = InMemoryObjectStore::default();
        store
            .put("src", Bytes::from_static(b"payload"))
            .await
            .expect("put");

        store.copy("src", "dst").await.expect("copy");
        let dst = store.get("dst").await.expect("get copied payload");
        assert_eq!(dst, Bytes::from_static(b"payload"));

        store.delete("src").await.expect("delete");
        let err = store
            .get("src")
            .await
            .expect_err("deleted object should not exist");
        assert!(matches!(err, StoreError::NotFound(_)));
    }

    #[tokio::test]
    async fn in_memory_copy_requires_existing_source() {
        let store = InMemoryObjectStore::default();
        let err = store
            .copy("missing", "dst")
            .await
            .expect_err("copy from missing source should fail");
        assert!(matches!(err, StoreError::NotFound(_)));
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn s3_config_builders_set_expected_defaults() {
        let aws = S3CompatibleObjectStore::for_aws_s3(
            "meshfs-bucket".to_string(),
            "us-east-1".to_string(),
            "ak".to_string(),
            "sk".to_string(),
        );
        assert_eq!(aws.bucket, "meshfs-bucket");
        assert_eq!(aws.region, "us-east-1");
        assert!(aws.endpoint.is_none());
        assert!(!aws.force_path_style);

        let r2 = S3CompatibleObjectStore::for_cloudflare_r2(
            "meshfs-r2".to_string(),
            "acc123".to_string(),
            "ak".to_string(),
            "sk".to_string(),
        );
        assert_eq!(r2.bucket, "meshfs-r2");
        assert_eq!(r2.region, "auto");
        assert_eq!(
            r2.endpoint.as_deref(),
            Some("https://acc123.r2.cloudflarestorage.com")
        );
        assert!(r2.force_path_style);
    }
}
