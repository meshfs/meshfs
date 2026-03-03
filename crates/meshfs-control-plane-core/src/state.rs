use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::Mutex;

use base64::Engine;
use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use meshfs_store::ObjectStore;
use meshfs_types::{
    ApplyRetentionRequest, AuditListResponse, AuditRecord, ChangeEvent, ChangeOp,
    CurrentPlanResponse, DeleteRequest, DeviceActivateRequest, DirectoryEntry, FileVersion,
    ListDirectoryResponse, ListVersionsResponse, MetaResponse, MkdirRequest, Node, NodeKind,
    PlanTier, RenameRequest, RestoreVersionResponse, RetentionPolicy, RetentionPolicyResponse,
    SyncPullResponse, UploadCommitResponse, UploadInitRequest, UploadInitResponse,
    UploadPartRequest,
};
#[cfg(not(target_arch = "wasm32"))]
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;
use tokio::sync::{broadcast, RwLock};
use uuid::Uuid;

use crate::error::{AppError, AppResult};

const ROOT_NODE_ID: &str = "root";

#[derive(Clone)]
pub struct AppState {
    pub jwt_secret: String,
    pub token_ttl_seconds: i64,
    pub refresh_token_ttl_seconds: i64,
    pub rate_limit_per_minute: u64,
    pub tenant_storage_quota_bytes: u64,
    pub object_store: Arc<dyn ObjectStore>,
    pub events_tx: broadcast::Sender<ChangeEvent>,
    inner: Arc<RwLock<MetadataStore>>,
    metadata_sqlite: Option<Arc<SqliteMetadataStore>>,
    rate_windows: Arc<Mutex<HashMap<String, RateWindow>>>,
}

impl AppState {
    pub fn new(
        jwt_secret: String,
        token_ttl_seconds: i64,
        object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self::new_with_runtime_config(
            jwt_secret,
            token_ttl_seconds,
            30 * 24 * 3600,
            object_store,
            None,
            1200,
            10 * 1024 * 1024 * 1024,
        )
    }

    pub fn new_with_metadata_sqlite(
        jwt_secret: String,
        token_ttl_seconds: i64,
        object_store: Arc<dyn ObjectStore>,
        metadata_sqlite_path: Option<String>,
    ) -> Self {
        Self::new_with_runtime_config(
            jwt_secret,
            token_ttl_seconds,
            30 * 24 * 3600,
            object_store,
            metadata_sqlite_path,
            1200,
            10 * 1024 * 1024 * 1024,
        )
    }

    pub fn new_with_runtime_config(
        jwt_secret: String,
        token_ttl_seconds: i64,
        refresh_token_ttl_seconds: i64,
        object_store: Arc<dyn ObjectStore>,
        metadata_sqlite_path: Option<String>,
        rate_limit_per_minute: u64,
        tenant_storage_quota_bytes: u64,
    ) -> Self {
        let (events_tx, _) = broadcast::channel(4096);
        let metadata_sqlite = metadata_sqlite_path
            .as_deref()
            .and_then(|path| SqliteMetadataStore::open(path).ok().map(Arc::new));
        let initial_state = metadata_sqlite
            .as_ref()
            .and_then(|store| store.load().ok())
            .unwrap_or_default();

        Self {
            jwt_secret,
            token_ttl_seconds,
            refresh_token_ttl_seconds,
            rate_limit_per_minute: rate_limit_per_minute.max(1),
            tenant_storage_quota_bytes,
            object_store,
            events_tx,
            inner: Arc::new(RwLock::new(initial_state)),
            metadata_sqlite,
            rate_windows: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn persist_metadata_snapshot(&self, store: &MetadataStore) -> AppResult<()> {
        if let Some(sqlite) = &self.metadata_sqlite {
            sqlite.save(store).map_err(|err| {
                AppError::Internal(format!("metadata sqlite persist failed: {err}"))
            })?;
        }
        Ok(())
    }

    pub async fn export_snapshot_json(&self) -> AppResult<String> {
        let guard = self.inner.read().await;
        serde_json::to_string(&*guard)
            .map_err(|err| AppError::Internal(format!("serialize metadata snapshot failed: {err}")))
    }

    pub async fn import_snapshot_json(&self, snapshot_json: &str) -> AppResult<()> {
        let parsed: MetadataStore = serde_json::from_str(snapshot_json).map_err(|err| {
            AppError::InvalidRequest(format!("invalid metadata snapshot json: {err}"))
        })?;
        let mut guard = self.inner.write().await;
        *guard = parsed;
        self.persist_metadata_snapshot(&guard)?;
        Ok(())
    }

    pub fn enforce_rate_limit(&self, tenant_id: &str) -> AppResult<()> {
        let mut guard = self
            .rate_windows
            .lock()
            .map_err(|_| AppError::Internal("rate-limit mutex poisoned".to_string()))?;
        let now = Utc::now();
        let window = guard.entry(tenant_id.to_string()).or_insert(RateWindow {
            window_start: now,
            count: 0,
        });

        if now.signed_duration_since(window.window_start) >= Duration::minutes(1) {
            window.window_start = now;
            window.count = 0;
        }

        if window.count >= self.rate_limit_per_minute {
            return Err(AppError::RateLimited(format!(
                "tenant rate limit exceeded: {}/min",
                self.rate_limit_per_minute
            )));
        }

        window.count += 1;
        Ok(())
    }

    pub async fn start_device_session(&self) -> (String, String, u64, u64) {
        let mut guard = self.inner.write().await;
        let device_code = format!("dc_{}", Uuid::new_v4());
        let user_code = generate_device_user_code();
        let interval_seconds = 3;
        let expires_in_seconds = 600;

        guard.device_sessions.insert(
            device_code.clone(),
            DeviceSession {
                user_code: user_code.clone(),
                approved: None,
                expires_at: Utc::now() + Duration::seconds(expires_in_seconds as i64),
            },
        );
        let _ = self.persist_metadata_snapshot(&guard);

        (device_code, user_code, interval_seconds, expires_in_seconds)
    }

    pub async fn activate_device_session(&self, req: DeviceActivateRequest) -> AppResult<()> {
        let mut guard = self.inner.write().await;
        let approved = ApprovedSession {
            user_id: req.user_id,
            tenant_id: req.tenant_id.clone(),
            plan_tier: req.plan_tier.unwrap_or(PlanTier::Free),
        };

        {
            let target = guard
                .device_sessions
                .iter_mut()
                .find(|(_, session)| session.user_code == req.user_code)
                .map(|(_, session)| session)
                .ok_or_else(|| AppError::NotFound("device session by user_code".to_string()))?;
            target.approved = Some(approved.clone());
        }

        let tenant = guard.ensure_tenant_mut(&req.tenant_id);
        tenant.plan_tier = approved.plan_tier.clone();
        tenant.retention_policy = retention_for_tier(&approved.plan_tier);
        self.persist_metadata_snapshot(&guard)?;

        Ok(())
    }

    pub async fn poll_device_session(
        &self,
        device_code: &str,
    ) -> AppResult<Option<(String, String, PlanTier)>> {
        let mut guard = self.inner.write().await;

        let session = guard
            .device_sessions
            .get_mut(device_code)
            .ok_or_else(|| AppError::NotFound("device session".to_string()))?;

        if session.expires_at < Utc::now() {
            return Err(AppError::InvalidRequest("device code expired".to_string()));
        }

        Ok(session.approved.as_ref().map(|approved| {
            (
                approved.user_id.clone(),
                approved.tenant_id.clone(),
                approved.plan_tier.clone(),
            )
        }))
    }

    pub async fn issue_refresh_token(
        &self,
        user_id: &str,
        tenant_id: &str,
        plan_tier: PlanTier,
    ) -> AppResult<String> {
        let mut guard = self.inner.write().await;
        let refresh_token = format!("rfr_{}", Uuid::new_v4());
        guard.refresh_sessions.insert(
            refresh_token.clone(),
            RefreshSession {
                user_id: user_id.to_string(),
                tenant_id: tenant_id.to_string(),
                plan_tier: plan_tier.clone(),
                expires_at: Utc::now() + Duration::seconds(self.refresh_token_ttl_seconds),
                revoked_at: None,
            },
        );
        push_audit_event(
            &mut guard,
            tenant_id.to_string(),
            user_id.to_string(),
            "auth.refresh.issue",
            None,
            "success",
        );
        self.persist_metadata_snapshot(&guard)?;
        Ok(refresh_token)
    }

    pub async fn exchange_refresh_token(
        &self,
        refresh_token: &str,
    ) -> AppResult<(String, String, PlanTier, String)> {
        let mut guard = self.inner.write().await;
        let session = guard
            .refresh_sessions
            .remove(refresh_token)
            .ok_or(AppError::Unauthorized)?;

        if session.revoked_at.is_some() || session.expires_at < Utc::now() {
            push_audit_event(
                &mut guard,
                session.tenant_id.clone(),
                session.user_id.clone(),
                "auth.refresh.exchange",
                None,
                "denied",
            );
            self.persist_metadata_snapshot(&guard)?;
            return Err(AppError::Unauthorized);
        }

        let rotated_refresh = format!("rfr_{}", Uuid::new_v4());
        guard.refresh_sessions.insert(
            rotated_refresh.clone(),
            RefreshSession {
                user_id: session.user_id.clone(),
                tenant_id: session.tenant_id.clone(),
                plan_tier: session.plan_tier.clone(),
                expires_at: Utc::now() + Duration::seconds(self.refresh_token_ttl_seconds),
                revoked_at: None,
            },
        );
        push_audit_event(
            &mut guard,
            session.tenant_id.clone(),
            session.user_id.clone(),
            "auth.refresh.exchange",
            None,
            "success",
        );
        self.persist_metadata_snapshot(&guard)?;

        Ok((
            session.user_id,
            session.tenant_id,
            session.plan_tier,
            rotated_refresh,
        ))
    }

    pub async fn revoke_refresh_token(
        &self,
        auth: AuthIdentity,
        refresh_token: &str,
    ) -> AppResult<()> {
        let mut guard = self.inner.write().await;
        let session = guard
            .refresh_sessions
            .get_mut(refresh_token)
            .ok_or(AppError::Unauthorized)?;

        if session.tenant_id != auth.tenant_id || session.user_id != auth.user_id {
            return Err(AppError::Forbidden);
        }

        session.revoked_at = Some(Utc::now());
        push_audit_event(
            &mut guard,
            auth.tenant_id,
            auth.user_id,
            "auth.refresh.revoke",
            None,
            "success",
        );
        self.persist_metadata_snapshot(&guard)?;
        Ok(())
    }

    pub async fn init_upload(
        &self,
        auth: AuthIdentity,
        req: UploadInitRequest,
    ) -> AppResult<UploadInitResponse> {
        self.enforce_rate_limit(&auth.tenant_id)?;
        let path = normalize_path(&req.path)?;
        let mut guard = self.inner.write().await;

        if path == "/" {
            return Err(AppError::InvalidRequest(
                "cannot upload into root".to_string(),
            ));
        }

        validate_parent_directory(&mut guard, &auth.tenant_id, &path)?;

        let upload_id = format!("upl_{}", Uuid::new_v4());
        let blob_key = format!("tenants/{}/objects/{}.blob", auth.tenant_id, Uuid::new_v4());

        guard.uploads.insert(
            upload_id.clone(),
            UploadSession {
                tenant_id: auth.tenant_id,
                path,
                blob_key: blob_key.clone(),
                content_hash: req.content_hash,
                writer_device_id: req.writer_device_id,
                size_hint: req.size_hint,
                parts: BTreeMap::new(),
            },
        );
        self.persist_metadata_snapshot(&guard)?;

        Ok(UploadInitResponse {
            upload_id,
            blob_key,
        })
    }

    pub async fn put_upload_part(
        &self,
        auth: AuthIdentity,
        req: UploadPartRequest,
    ) -> AppResult<()> {
        self.enforce_rate_limit(&auth.tenant_id)?;
        let mut guard = self.inner.write().await;

        let upload = guard
            .uploads
            .get_mut(&req.upload_id)
            .ok_or_else(|| AppError::NotFound("upload session".to_string()))?;

        if upload.tenant_id != auth.tenant_id {
            return Err(AppError::Forbidden);
        }

        let decoded = base64::engine::general_purpose::STANDARD
            .decode(req.data_base64)
            .map_err(|err| AppError::InvalidRequest(format!("invalid base64 payload: {err}")))?;

        upload.parts.insert(req.part_number, decoded);
        self.persist_metadata_snapshot(&guard)?;

        Ok(())
    }

    pub async fn commit_upload(
        &self,
        auth: AuthIdentity,
        upload_id: &str,
        idempotency_key: Option<String>,
    ) -> AppResult<UploadCommitResponse> {
        self.enforce_rate_limit(&auth.tenant_id)?;
        let mut guard = self.inner.write().await;

        if let Some(key) = idempotency_key.as_ref() {
            let id_key = format!("{}:{}", auth.tenant_id, key);
            if let Some(existing) = guard.idempotency.get(&id_key) {
                return Ok(existing.clone());
            }
        }

        let upload = guard
            .uploads
            .remove(upload_id)
            .ok_or_else(|| AppError::NotFound("upload session".to_string()))?;

        if upload.tenant_id != auth.tenant_id {
            return Err(AppError::Forbidden);
        }

        let mut payload = Vec::new();
        for (_part_no, data) in upload.parts {
            payload.extend(data);
        }

        if let Some(size_hint) = upload.size_hint {
            let actual = payload.len() as u64;
            if size_hint != actual {
                return Err(AppError::InvalidRequest(format!(
                    "size_hint mismatch, expected {size_hint}, got {actual}"
                )));
            }
        }

        let tenant_for_quota = guard.ensure_tenant_mut(&auth.tenant_id);
        let current_size = tenant_total_stored_bytes(tenant_for_quota);
        let projected_size = current_size.saturating_add(payload.len() as u64);
        if projected_size > self.tenant_storage_quota_bytes {
            return Err(AppError::QuotaExceeded(format!(
                "tenant storage quota exceeded: projected={} quota={}",
                projected_size, self.tenant_storage_quota_bytes
            )));
        }

        self.object_store
            .put(&upload.blob_key, Bytes::from(payload.clone()))
            .await
            .map_err(|err| AppError::Internal(err.to_string()))?;

        let committed_at = Utc::now();
        let tenant = guard.ensure_tenant_mut(&auth.tenant_id);
        let node_id = upsert_file_node(tenant, &upload.path)?;

        let previous_head = tenant.head_versions.get(&node_id).cloned();

        let version_id = format!("ver_{}", Uuid::new_v4());
        let new_version = FileVersion {
            version_id: version_id.clone(),
            node_id: node_id.clone(),
            blob_key: upload.blob_key,
            size: payload.len() as u64,
            content_hash: upload.content_hash,
            writer_device_id: upload.writer_device_id,
            committed_at,
            overwrite_of_version_id: previous_head,
        };

        tenant
            .versions
            .entry(node_id.clone())
            .or_default()
            .push(new_version.clone());
        tenant
            .head_versions
            .insert(node_id.clone(), version_id.clone());

        if let Some(node) = tenant.nodes.get_mut(&node_id) {
            node.logical_clock += 1;
            node.deleted_at = None;
        }

        apply_retention_for_node(tenant, &node_id);

        let cursor = tenant.next_cursor;
        tenant.next_cursor += 1;

        let node_path = tenant
            .nodes
            .get(&node_id)
            .map(|n| n.path.clone())
            .unwrap_or_else(|| upload.path.clone());

        let event = ChangeEvent {
            event_id: format!("evt_{}", Uuid::new_v4()),
            tenant_id: auth.tenant_id.clone(),
            node_id: node_id.clone(),
            op: ChangeOp::Write,
            version_id: Some(version_id.clone()),
            ts: committed_at,
            actor: auth.user_id.clone(),
            cursor,
            path: node_path,
        };
        tenant.changes.push(event.clone());
        let _ = self.events_tx.send(event);
        push_audit_event(
            &mut guard,
            auth.tenant_id.clone(),
            auth.user_id.clone(),
            "files.upload.commit",
            Some(upload.path.clone()),
            "success",
        );

        let response = UploadCommitResponse {
            node_id,
            version_id,
            cursor,
        };

        if let Some(key) = idempotency_key {
            let id_key = format!("{}:{}", auth.tenant_id, key);
            guard.idempotency.insert(id_key, response.clone());
        }
        self.persist_metadata_snapshot(&guard)?;

        Ok(response)
    }

    pub async fn mkdir(&self, auth: AuthIdentity, req: MkdirRequest) -> AppResult<MetaResponse> {
        self.enforce_rate_limit(&auth.tenant_id)?;
        let path = normalize_path(&req.path)?;
        if path == "/" {
            return Err(AppError::InvalidRequest("root already exists".to_string()));
        }

        let mut guard = self.inner.write().await;
        validate_parent_directory(&mut guard, &auth.tenant_id, &path)?;

        let tenant = guard.ensure_tenant_mut(&auth.tenant_id);
        if let Some(existing_node_id) = tenant.path_index.get(&path).cloned() {
            let node = tenant
                .nodes
                .get(&existing_node_id)
                .cloned()
                .ok_or_else(|| {
                    AppError::Internal("path index points to missing node".to_string())
                })?;
            return Ok(MetaResponse {
                node,
                head_version: None,
            });
        }

        let (parent_path, name) = split_parent_and_name(&path)?;
        let parent_id = tenant.path_index.get(&parent_path).cloned();

        let node_id = format!("node_{}", Uuid::new_v4());
        let node = Node {
            node_id: node_id.clone(),
            tenant_id: auth.tenant_id.clone(),
            parent_id,
            name,
            path: path.clone(),
            kind: NodeKind::Dir,
            logical_clock: 1,
            deleted_at: None,
        };

        tenant.path_index.insert(path.clone(), node_id.clone());
        tenant.nodes.insert(node_id.clone(), node.clone());

        let cursor = tenant.next_cursor;
        tenant.next_cursor += 1;

        let event = ChangeEvent {
            event_id: format!("evt_{}", Uuid::new_v4()),
            tenant_id: auth.tenant_id.clone(),
            node_id,
            op: ChangeOp::Mkdir,
            version_id: None,
            ts: Utc::now(),
            actor: auth.user_id.clone(),
            cursor,
            path: path.clone(),
        };
        tenant.changes.push(event.clone());
        let _ = self.events_tx.send(event);
        push_audit_event(
            &mut guard,
            auth.tenant_id.clone(),
            auth.user_id.clone(),
            "files.mkdir",
            Some(path.clone()),
            "success",
        );
        self.persist_metadata_snapshot(&guard)?;

        Ok(MetaResponse {
            node,
            head_version: None,
        })
    }

    pub async fn rename(&self, auth: AuthIdentity, req: RenameRequest) -> AppResult<()> {
        self.enforce_rate_limit(&auth.tenant_id)?;
        let from_path = normalize_path(&req.from_path)?;
        let to_path = normalize_path(&req.to_path)?;

        if from_path == "/" || to_path == "/" {
            return Err(AppError::InvalidRequest("cannot rename root".to_string()));
        }

        let mut guard = self.inner.write().await;
        validate_parent_directory(&mut guard, &auth.tenant_id, &to_path)?;

        let tenant = guard.ensure_tenant_mut(&auth.tenant_id);

        if tenant.path_index.contains_key(&to_path) {
            return Err(AppError::Conflict("target path already exists".to_string()));
        }

        let node_id = tenant
            .path_index
            .get(&from_path)
            .cloned()
            .ok_or_else(|| AppError::NotFound("source path".to_string()))?;

        let mut updates: Vec<(String, String)> = Vec::new();
        for (path, child_id) in tenant.path_index.clone() {
            if child_id == node_id || path.starts_with(&(from_path.clone() + "/")) {
                let suffix = path.trim_start_matches(&from_path);
                let new_path = format!("{}{}", to_path, suffix);
                updates.push((path, normalize_path(&new_path)?));
            }
        }

        for (old_path, new_path) in &updates {
            if let Some(id) = tenant.path_index.remove(old_path) {
                tenant.path_index.insert(new_path.clone(), id.clone());
                if let Some(node) = tenant.nodes.get_mut(&id) {
                    node.path = new_path.clone();
                    if node.node_id == node_id {
                        let (parent_path, name) = split_parent_and_name(new_path)?;
                        node.name = name;
                        node.parent_id = tenant.path_index.get(&parent_path).cloned();
                        node.logical_clock += 1;
                    }
                }
            }
        }

        let cursor = tenant.next_cursor;
        tenant.next_cursor += 1;

        let event = ChangeEvent {
            event_id: format!("evt_{}", Uuid::new_v4()),
            tenant_id: auth.tenant_id.clone(),
            node_id,
            op: ChangeOp::Rename,
            version_id: None,
            ts: Utc::now(),
            actor: auth.user_id.clone(),
            cursor,
            path: to_path.clone(),
        };
        tenant.changes.push(event.clone());
        let _ = self.events_tx.send(event);
        push_audit_event(
            &mut guard,
            auth.tenant_id.clone(),
            auth.user_id.clone(),
            "files.rename",
            Some(to_path.clone()),
            "success",
        );
        self.persist_metadata_snapshot(&guard)?;

        Ok(())
    }

    pub async fn delete(&self, auth: AuthIdentity, req: DeleteRequest) -> AppResult<()> {
        self.enforce_rate_limit(&auth.tenant_id)?;
        let path = normalize_path(&req.path)?;
        if path == "/" {
            return Err(AppError::InvalidRequest("cannot delete root".to_string()));
        }

        let mut guard = self.inner.write().await;
        let tenant = guard.ensure_tenant_mut(&auth.tenant_id);

        let target_node_id = tenant
            .path_index
            .get(&path)
            .cloned()
            .ok_or_else(|| AppError::NotFound("path".to_string()))?;

        let affected: Vec<String> = tenant
            .path_index
            .keys()
            .filter(|p| *p == &path || p.starts_with(&(path.clone() + "/")))
            .cloned()
            .collect();

        for item_path in affected {
            if let Some(node_id) = tenant.path_index.remove(&item_path) {
                if let Some(node) = tenant.nodes.get_mut(&node_id) {
                    node.deleted_at = Some(Utc::now());
                    node.logical_clock += 1;
                }
            }
        }

        let cursor = tenant.next_cursor;
        tenant.next_cursor += 1;

        let event = ChangeEvent {
            event_id: format!("evt_{}", Uuid::new_v4()),
            tenant_id: auth.tenant_id.clone(),
            node_id: target_node_id,
            op: ChangeOp::Delete,
            version_id: None,
            ts: Utc::now(),
            actor: auth.user_id.clone(),
            cursor,
            path: path.clone(),
        };
        tenant.changes.push(event.clone());
        let _ = self.events_tx.send(event);
        push_audit_event(
            &mut guard,
            auth.tenant_id.clone(),
            auth.user_id.clone(),
            "files.delete",
            Some(path.clone()),
            "success",
        );
        self.persist_metadata_snapshot(&guard)?;

        Ok(())
    }

    pub async fn get_meta_by_path(
        &self,
        auth: AuthIdentity,
        raw_path: &str,
    ) -> AppResult<MetaResponse> {
        let path = normalize_path(raw_path)?;
        let guard = self.inner.read().await;
        let tenant = guard
            .tenants
            .get(&auth.tenant_id)
            .ok_or_else(|| AppError::NotFound("tenant".to_string()))?;

        let node_id = tenant
            .path_index
            .get(&path)
            .ok_or_else(|| AppError::NotFound("path".to_string()))?;

        let node =
            tenant.nodes.get(node_id).cloned().ok_or_else(|| {
                AppError::Internal("path index points to missing node".to_string())
            })?;

        let head_version = tenant.head_versions.get(node_id).and_then(|head_id| {
            tenant
                .versions
                .get(node_id)?
                .iter()
                .find(|v| &v.version_id == head_id)
                .cloned()
        });

        Ok(MetaResponse { node, head_version })
    }

    pub async fn list_directory(
        &self,
        auth: AuthIdentity,
        raw_path: &str,
    ) -> AppResult<ListDirectoryResponse> {
        let path = normalize_path(raw_path)?;
        let guard = self.inner.read().await;
        let tenant = guard
            .tenants
            .get(&auth.tenant_id)
            .ok_or_else(|| AppError::NotFound("tenant".to_string()))?;

        let parent_node_id = tenant
            .path_index
            .get(&path)
            .ok_or_else(|| AppError::NotFound("path".to_string()))?;
        let parent = tenant
            .nodes
            .get(parent_node_id)
            .ok_or_else(|| AppError::Internal("path index points to missing node".to_string()))?;
        if parent.kind != NodeKind::Dir {
            return Err(AppError::Conflict("path is not a directory".to_string()));
        }

        let mut entries: Vec<DirectoryEntry> = tenant
            .nodes
            .values()
            .filter(|node| {
                node.deleted_at.is_none() && node.parent_id.as_deref() == Some(parent_node_id)
            })
            .map(|node| DirectoryEntry {
                node: node.clone(),
                head_version: tenant.head_versions.get(&node.node_id).and_then(|head_id| {
                    tenant
                        .versions
                        .get(&node.node_id)
                        .and_then(|versions| versions.iter().find(|v| &v.version_id == head_id))
                        .cloned()
                }),
            })
            .collect();
        entries.sort_by(|a, b| a.node.name.cmp(&b.node.name));

        Ok(ListDirectoryResponse { path, entries })
    }

    pub async fn get_bytes_by_path(&self, auth: AuthIdentity, raw_path: &str) -> AppResult<Bytes> {
        let path = normalize_path(raw_path)?;
        let blob_key = {
            let guard = self.inner.read().await;
            let tenant = guard
                .tenants
                .get(&auth.tenant_id)
                .ok_or_else(|| AppError::NotFound("tenant".to_string()))?;
            let node_id = tenant
                .path_index
                .get(&path)
                .ok_or_else(|| AppError::NotFound("path".to_string()))?;
            let head_id = tenant
                .head_versions
                .get(node_id)
                .ok_or_else(|| AppError::NotFound("head version".to_string()))?;
            tenant
                .versions
                .get(node_id)
                .and_then(|versions| versions.iter().find(|v| &v.version_id == head_id))
                .map(|v| v.blob_key.clone())
                .ok_or_else(|| AppError::NotFound("head version payload".to_string()))?
        };

        self.object_store
            .get(&blob_key)
            .await
            .map_err(|err| AppError::Internal(err.to_string()))
    }

    pub async fn list_versions(
        &self,
        auth: AuthIdentity,
        node_id: &str,
    ) -> AppResult<ListVersionsResponse> {
        let guard = self.inner.read().await;
        let tenant = guard
            .tenants
            .get(&auth.tenant_id)
            .ok_or_else(|| AppError::NotFound("tenant".to_string()))?;

        if !tenant.nodes.contains_key(node_id) {
            return Err(AppError::NotFound("node".to_string()));
        }

        let versions = tenant.versions.get(node_id).cloned().unwrap_or_default();

        Ok(ListVersionsResponse {
            node_id: node_id.to_string(),
            versions,
        })
    }

    pub async fn restore_version(
        &self,
        auth: AuthIdentity,
        node_id: &str,
        version_id: &str,
    ) -> AppResult<RestoreVersionResponse> {
        self.enforce_rate_limit(&auth.tenant_id)?;
        let mut guard = self.inner.write().await;
        let tenant = guard.ensure_tenant_mut(&auth.tenant_id);

        let current_versions = tenant
            .versions
            .get(node_id)
            .cloned()
            .ok_or_else(|| AppError::NotFound("node versions".to_string()))?;

        let target = current_versions
            .iter()
            .find(|v| v.version_id == version_id)
            .cloned()
            .ok_or_else(|| AppError::NotFound("version".to_string()))?;

        let new_version_id = format!("ver_{}", Uuid::new_v4());
        let overwrite_of = tenant.head_versions.get(node_id).cloned();
        let new_version = FileVersion {
            version_id: new_version_id.clone(),
            node_id: node_id.to_string(),
            blob_key: target.blob_key.clone(),
            size: target.size,
            content_hash: target.content_hash.clone(),
            writer_device_id: Some("restore".to_string()),
            committed_at: Utc::now(),
            overwrite_of_version_id: overwrite_of,
        };

        tenant
            .versions
            .entry(node_id.to_string())
            .or_default()
            .push(new_version.clone());
        tenant
            .head_versions
            .insert(node_id.to_string(), new_version_id.clone());
        apply_retention_for_node(tenant, node_id);

        if let Some(node) = tenant.nodes.get_mut(node_id) {
            node.logical_clock += 1;
            node.deleted_at = None;
        }

        let cursor = tenant.next_cursor;
        tenant.next_cursor += 1;
        let path = tenant
            .nodes
            .get(node_id)
            .map(|node| node.path.clone())
            .unwrap_or_else(|| "/".to_string());

        let event = ChangeEvent {
            event_id: format!("evt_{}", Uuid::new_v4()),
            tenant_id: auth.tenant_id.clone(),
            node_id: node_id.to_string(),
            op: ChangeOp::Restore,
            version_id: Some(new_version_id.clone()),
            ts: Utc::now(),
            actor: auth.user_id.clone(),
            cursor,
            path: path.clone(),
        };
        tenant.changes.push(event.clone());
        let _ = self.events_tx.send(event);
        push_audit_event(
            &mut guard,
            auth.tenant_id.clone(),
            auth.user_id.clone(),
            "files.version.restore",
            Some(path.clone()),
            "success",
        );
        self.persist_metadata_snapshot(&guard)?;

        Ok(RestoreVersionResponse {
            node_id: node_id.to_string(),
            restored_version_id: new_version_id,
            cursor,
        })
    }

    pub async fn sync_pull(&self, auth: AuthIdentity, cursor: u64) -> AppResult<SyncPullResponse> {
        self.enforce_rate_limit(&auth.tenant_id)?;
        let guard = self.inner.read().await;
        let tenant = guard
            .tenants
            .get(&auth.tenant_id)
            .ok_or_else(|| AppError::NotFound("tenant".to_string()))?;

        let events: Vec<_> = tenant
            .changes
            .iter()
            .filter(|event| event.cursor > cursor)
            .cloned()
            .collect();

        let next_cursor = events.last().map(|event| event.cursor).unwrap_or(cursor);

        Ok(SyncPullResponse {
            events,
            next_cursor,
        })
    }

    pub async fn current_plan(&self, auth: AuthIdentity) -> AppResult<CurrentPlanResponse> {
        let mut guard = self.inner.write().await;
        let tenant = guard.ensure_tenant_mut(&auth.tenant_id);
        let plan_tier = tenant.plan_tier.clone();
        self.persist_metadata_snapshot(&guard)?;

        Ok(CurrentPlanResponse {
            tenant_id: auth.tenant_id,
            plan_tier,
        })
    }

    pub async fn retention_policy(&self, auth: AuthIdentity) -> AppResult<RetentionPolicyResponse> {
        let mut guard = self.inner.write().await;
        let tenant = guard.ensure_tenant_mut(&auth.tenant_id);
        let policy = tenant.retention_policy.clone();
        self.persist_metadata_snapshot(&guard)?;
        Ok(RetentionPolicyResponse {
            tenant_id: auth.tenant_id,
            policy,
        })
    }

    pub async fn apply_retention_policy(
        &self,
        auth: AuthIdentity,
        req: ApplyRetentionRequest,
    ) -> AppResult<()> {
        self.enforce_rate_limit(&auth.tenant_id)?;
        if auth.tenant_id != req.tenant_id {
            return Err(AppError::Forbidden);
        }

        let mut guard = self.inner.write().await;
        let tenant = guard.ensure_tenant_mut(&req.tenant_id);

        tenant.plan_tier = req.policy.plan_tier.clone();
        tenant.retention_policy = req.policy;

        let node_ids: Vec<String> = tenant.versions.keys().cloned().collect();
        for node_id in node_ids {
            apply_retention_for_node(tenant, &node_id);
        }
        push_audit_event(
            &mut guard,
            req.tenant_id,
            auth.user_id,
            "retention.apply",
            None,
            "success",
        );
        self.persist_metadata_snapshot(&guard)?;

        Ok(())
    }

    pub async fn list_audit_events(
        &self,
        auth: AuthIdentity,
        limit: usize,
    ) -> AppResult<AuditListResponse> {
        let guard = self.inner.read().await;
        let mut events: Vec<AuditRecord> = guard
            .audit_events
            .iter()
            .filter(|evt| evt.tenant_id == auth.tenant_id)
            .cloned()
            .collect();
        events.sort_by_key(|evt| evt.ts);
        events.reverse();
        events.truncate(limit.max(1));
        Ok(AuditListResponse { events })
    }
}

#[derive(Debug, Clone)]
pub struct AuthIdentity {
    pub user_id: String,
    pub tenant_id: String,
}

#[derive(Debug, Clone)]
struct RateWindow {
    window_start: DateTime<Utc>,
    count: u64,
}

#[derive(Default, Serialize, Deserialize)]
struct MetadataStore {
    tenants: HashMap<String, TenantState>,
    uploads: HashMap<String, UploadSession>,
    idempotency: HashMap<String, UploadCommitResponse>,
    device_sessions: HashMap<String, DeviceSession>,
    refresh_sessions: HashMap<String, RefreshSession>,
    audit_events: Vec<AuditRecord>,
}

impl MetadataStore {
    fn ensure_tenant_mut(&mut self, tenant_id: &str) -> &mut TenantState {
        self.tenants
            .entry(tenant_id.to_string())
            .or_insert_with(|| TenantState::new(tenant_id.to_string()))
    }
}

#[derive(Serialize, Deserialize)]
struct TenantState {
    nodes: HashMap<String, Node>,
    path_index: HashMap<String, String>,
    versions: HashMap<String, Vec<FileVersion>>,
    head_versions: HashMap<String, String>,
    changes: Vec<ChangeEvent>,
    next_cursor: u64,
    plan_tier: PlanTier,
    retention_policy: RetentionPolicy,
}

impl TenantState {
    fn new(tenant_id: String) -> Self {
        let mut nodes = HashMap::new();
        let mut path_index = HashMap::new();

        nodes.insert(
            ROOT_NODE_ID.to_string(),
            Node {
                node_id: ROOT_NODE_ID.to_string(),
                tenant_id: tenant_id.clone(),
                parent_id: None,
                name: "/".to_string(),
                path: "/".to_string(),
                kind: NodeKind::Dir,
                logical_clock: 1,
                deleted_at: None,
            },
        );
        path_index.insert("/".to_string(), ROOT_NODE_ID.to_string());

        Self {
            nodes,
            path_index,
            versions: HashMap::new(),
            head_versions: HashMap::new(),
            changes: Vec::new(),
            next_cursor: 1,
            plan_tier: PlanTier::Free,
            retention_policy: RetentionPolicy::free(),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct UploadSession {
    tenant_id: String,
    path: String,
    blob_key: String,
    content_hash: Option<String>,
    writer_device_id: Option<String>,
    size_hint: Option<u64>,
    parts: BTreeMap<u32, Vec<u8>>,
}

#[derive(Serialize, Deserialize)]
struct DeviceSession {
    user_code: String,
    approved: Option<ApprovedSession>,
    expires_at: DateTime<Utc>,
}

#[derive(Clone, Serialize, Deserialize)]
struct ApprovedSession {
    user_id: String,
    tenant_id: String,
    plan_tier: PlanTier,
}

#[derive(Clone, Serialize, Deserialize)]
struct RefreshSession {
    user_id: String,
    tenant_id: String,
    plan_tier: PlanTier,
    expires_at: DateTime<Utc>,
    revoked_at: Option<DateTime<Utc>>,
}

#[cfg(not(target_arch = "wasm32"))]
struct SqliteMetadataStore {
    conn: Mutex<Connection>,
}

#[cfg(not(target_arch = "wasm32"))]
impl SqliteMetadataStore {
    fn open(path: &str) -> Result<Self, String> {
        let path = PathBuf::from(path);
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)
                    .map_err(|err| format!("failed to create metadata sqlite dir: {err}"))?;
            }
        }

        let conn = Connection::open(path)
            .map_err(|err| format!("failed to open metadata sqlite: {err}"))?;
        conn.execute_batch(
            r#"
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS tenants (
              tenant_id TEXT PRIMARY KEY,
              plan_tier TEXT NOT NULL,
              retention_policy_json TEXT NOT NULL,
              next_cursor INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS nodes (
              tenant_id TEXT NOT NULL,
              node_id TEXT PRIMARY KEY,
              parent_id TEXT,
              name TEXT NOT NULL,
              path TEXT NOT NULL,
              kind TEXT NOT NULL,
              logical_clock INTEGER NOT NULL,
              deleted_at TEXT
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_nodes_tenant_path ON nodes(tenant_id, path);

            CREATE TABLE IF NOT EXISTS file_versions (
              tenant_id TEXT NOT NULL,
              version_id TEXT PRIMARY KEY,
              node_id TEXT NOT NULL,
              blob_key TEXT NOT NULL,
              size INTEGER NOT NULL,
              content_hash TEXT,
              writer_device_id TEXT,
              committed_at TEXT NOT NULL,
              overwrite_of_version_id TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_file_versions_tenant_node ON file_versions(tenant_id, node_id, committed_at);

            CREATE TABLE IF NOT EXISTS head_versions (
              tenant_id TEXT NOT NULL,
              node_id TEXT NOT NULL,
              version_id TEXT NOT NULL,
              PRIMARY KEY (tenant_id, node_id)
            );

            CREATE TABLE IF NOT EXISTS change_events (
              tenant_id TEXT NOT NULL,
              event_id TEXT PRIMARY KEY,
              node_id TEXT NOT NULL,
              op TEXT NOT NULL,
              version_id TEXT,
              ts TEXT NOT NULL,
              actor TEXT NOT NULL,
              cursor INTEGER NOT NULL,
              path TEXT NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_change_events_tenant_cursor ON change_events(tenant_id, cursor);

            CREATE TABLE IF NOT EXISTS uploads (
              upload_id TEXT PRIMARY KEY,
              tenant_id TEXT NOT NULL,
              path TEXT NOT NULL,
              blob_key TEXT NOT NULL,
              content_hash TEXT,
              writer_device_id TEXT,
              size_hint INTEGER,
              parts_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS idempotency_keys (
              id_key TEXT PRIMARY KEY,
              response_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS device_sessions (
              device_code TEXT PRIMARY KEY,
              user_code TEXT NOT NULL,
              approved_json TEXT,
              expires_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS refresh_sessions (
              refresh_token TEXT PRIMARY KEY,
              user_id TEXT NOT NULL,
              tenant_id TEXT NOT NULL,
              plan_tier TEXT NOT NULL,
              expires_at TEXT NOT NULL,
              revoked_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_refresh_sessions_tenant ON refresh_sessions(tenant_id);

            CREATE TABLE IF NOT EXISTS audit_events (
              audit_id TEXT PRIMARY KEY,
              tenant_id TEXT NOT NULL,
              user_id TEXT NOT NULL,
              action TEXT NOT NULL,
              resource TEXT,
              outcome TEXT NOT NULL,
              ts TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_audit_events_tenant_ts ON audit_events(tenant_id, ts);
            "#,
        )
        .map_err(|err| format!("failed to bootstrap metadata sqlite schema: {err}"))?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn load(&self) -> Result<MetadataStore, String> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| "metadata sqlite mutex poisoned".to_string())?;
        let mut store = MetadataStore::default();

        {
            let mut stmt = conn
                .prepare(
                    "SELECT tenant_id, plan_tier, retention_policy_json, next_cursor FROM tenants",
                )
                .map_err(|err| format!("prepare tenants query failed: {err}"))?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                })
                .map_err(|err| format!("query tenants failed: {err}"))?;

            for row in rows {
                let (tenant_id, plan_tier_raw, retention_json, next_cursor_raw) =
                    row.map_err(|err| format!("decode tenant row failed: {err}"))?;
                let retention_policy: RetentionPolicy = serde_json::from_str(&retention_json)
                    .map_err(|err| format!("parse retention policy failed: {err}"))?;
                let next_cursor = u64::try_from(next_cursor_raw)
                    .map_err(|_| "next_cursor cannot be negative".to_string())?;
                let plan_tier = plan_tier_from_str(&plan_tier_raw)?;
                store.tenants.insert(
                    tenant_id,
                    TenantState {
                        nodes: HashMap::new(),
                        path_index: HashMap::new(),
                        versions: HashMap::new(),
                        head_versions: HashMap::new(),
                        changes: Vec::new(),
                        next_cursor,
                        plan_tier,
                        retention_policy,
                    },
                );
            }
        }

        {
            let mut stmt = conn
                .prepare(
                    "SELECT tenant_id, node_id, parent_id, name, path, kind, logical_clock, deleted_at FROM nodes",
                )
                .map_err(|err| format!("prepare nodes query failed: {err}"))?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, i64>(6)?,
                        row.get::<_, Option<String>>(7)?,
                    ))
                })
                .map_err(|err| format!("query nodes failed: {err}"))?;

            for row in rows {
                let (
                    tenant_id,
                    node_id,
                    parent_id,
                    name,
                    path,
                    kind_raw,
                    logical_clock_raw,
                    deleted_at_raw,
                ) = row.map_err(|err| format!("decode nodes row failed: {err}"))?;
                let kind = node_kind_from_str(&kind_raw)?;
                let logical_clock = u64::try_from(logical_clock_raw)
                    .map_err(|_| "logical_clock cannot be negative".to_string())?;
                let deleted_at = deleted_at_raw
                    .as_deref()
                    .map(parse_rfc3339_utc)
                    .transpose()?;
                let tenant = store
                    .tenants
                    .entry(tenant_id.clone())
                    .or_insert_with(|| TenantState::new(tenant_id.clone()));
                tenant.path_index.insert(path.clone(), node_id.clone());
                tenant.nodes.insert(
                    node_id.clone(),
                    Node {
                        node_id,
                        tenant_id,
                        parent_id,
                        name,
                        path,
                        kind,
                        logical_clock,
                        deleted_at,
                    },
                );
            }
        }

        {
            let mut stmt = conn
                .prepare(
                    "SELECT tenant_id, version_id, node_id, blob_key, size, content_hash, writer_device_id, committed_at, overwrite_of_version_id FROM file_versions",
                )
                .map_err(|err| format!("prepare versions query failed: {err}"))?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, String>(7)?,
                        row.get::<_, Option<String>>(8)?,
                    ))
                })
                .map_err(|err| format!("query versions failed: {err}"))?;
            for row in rows {
                let (
                    tenant_id,
                    version_id,
                    node_id,
                    blob_key,
                    size_raw,
                    content_hash,
                    writer_device_id,
                    committed_at_raw,
                    overwrite_of_version_id,
                ) = row.map_err(|err| format!("decode versions row failed: {err}"))?;
                let tenant = store
                    .tenants
                    .entry(tenant_id.clone())
                    .or_insert_with(|| TenantState::new(tenant_id.clone()));
                tenant
                    .versions
                    .entry(node_id.clone())
                    .or_default()
                    .push(FileVersion {
                        version_id,
                        node_id,
                        blob_key,
                        size: u64::try_from(size_raw)
                            .map_err(|_| "version size cannot be negative".to_string())?,
                        content_hash,
                        writer_device_id,
                        committed_at: parse_rfc3339_utc(&committed_at_raw)?,
                        overwrite_of_version_id,
                    });
            }
        }

        {
            let mut stmt = conn
                .prepare("SELECT tenant_id, node_id, version_id FROM head_versions")
                .map_err(|err| format!("prepare head_versions query failed: {err}"))?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })
                .map_err(|err| format!("query head_versions failed: {err}"))?;
            for row in rows {
                let (tenant_id, node_id, version_id) =
                    row.map_err(|err| format!("decode head_versions row failed: {err}"))?;
                let tenant = store
                    .tenants
                    .entry(tenant_id.clone())
                    .or_insert_with(|| TenantState::new(tenant_id.clone()));
                tenant.head_versions.insert(node_id, version_id);
            }
        }

        {
            let mut stmt = conn
                .prepare(
                    "SELECT tenant_id, event_id, node_id, op, version_id, ts, actor, cursor, path FROM change_events ORDER BY tenant_id, cursor ASC",
                )
                .map_err(|err| format!("prepare change_events query failed: {err}"))?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, i64>(7)?,
                        row.get::<_, String>(8)?,
                    ))
                })
                .map_err(|err| format!("query change_events failed: {err}"))?;
            for row in rows {
                let (
                    tenant_id,
                    event_id,
                    node_id,
                    op_raw,
                    version_id,
                    ts_raw,
                    actor,
                    cursor_raw,
                    path,
                ) = row.map_err(|err| format!("decode change_events row failed: {err}"))?;
                let tenant = store
                    .tenants
                    .entry(tenant_id.clone())
                    .or_insert_with(|| TenantState::new(tenant_id.clone()));
                tenant.changes.push(ChangeEvent {
                    event_id,
                    tenant_id,
                    node_id,
                    op: change_op_from_str(&op_raw)?,
                    version_id,
                    ts: parse_rfc3339_utc(&ts_raw)?,
                    actor,
                    cursor: u64::try_from(cursor_raw)
                        .map_err(|_| "event cursor cannot be negative".to_string())?,
                    path,
                });
            }
        }

        {
            let mut stmt = conn
                .prepare(
                    "SELECT upload_id, tenant_id, path, blob_key, content_hash, writer_device_id, size_hint, parts_json FROM uploads",
                )
                .map_err(|err| format!("prepare uploads query failed: {err}"))?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<i64>>(6)?,
                        row.get::<_, String>(7)?,
                    ))
                })
                .map_err(|err| format!("query uploads failed: {err}"))?;
            for row in rows {
                let (
                    upload_id,
                    tenant_id,
                    path,
                    blob_key,
                    content_hash,
                    writer_device_id,
                    size_hint_raw,
                    parts_json,
                ) = row.map_err(|err| format!("decode uploads row failed: {err}"))?;
                store.uploads.insert(
                    upload_id,
                    UploadSession {
                        tenant_id,
                        path,
                        blob_key,
                        content_hash,
                        writer_device_id,
                        size_hint: size_hint_raw
                            .map(u64::try_from)
                            .transpose()
                            .map_err(|_| "upload size_hint cannot be negative".to_string())?,
                        parts: serde_json::from_str(&parts_json)
                            .map_err(|err| format!("parse upload parts json failed: {err}"))?,
                    },
                );
            }
        }

        {
            let mut stmt = conn
                .prepare("SELECT id_key, response_json FROM idempotency_keys")
                .map_err(|err| format!("prepare idempotency query failed: {err}"))?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(|err| format!("query idempotency failed: {err}"))?;
            for row in rows {
                let (id_key, response_json) =
                    row.map_err(|err| format!("decode idempotency row failed: {err}"))?;
                let response: UploadCommitResponse = serde_json::from_str(&response_json)
                    .map_err(|err| format!("parse idempotency response json failed: {err}"))?;
                store.idempotency.insert(id_key, response);
            }
        }

        {
            let mut stmt = conn
                .prepare(
                    "SELECT device_code, user_code, approved_json, expires_at FROM device_sessions",
                )
                .map_err(|err| format!("prepare device_sessions query failed: {err}"))?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                })
                .map_err(|err| format!("query device_sessions failed: {err}"))?;
            for row in rows {
                let (device_code, user_code, approved_json, expires_at_raw) =
                    row.map_err(|err| format!("decode device_sessions row failed: {err}"))?;
                let approved = approved_json
                    .as_deref()
                    .map(serde_json::from_str::<ApprovedSession>)
                    .transpose()
                    .map_err(|err| format!("parse approved session json failed: {err}"))?;
                store.device_sessions.insert(
                    device_code,
                    DeviceSession {
                        user_code,
                        approved,
                        expires_at: parse_rfc3339_utc(&expires_at_raw)?,
                    },
                );
            }
        }

        {
            let mut stmt = conn
                .prepare(
                    "SELECT refresh_token, user_id, tenant_id, plan_tier, expires_at, revoked_at FROM refresh_sessions",
                )
                .map_err(|err| format!("prepare refresh_sessions query failed: {err}"))?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                    ))
                })
                .map_err(|err| format!("query refresh_sessions failed: {err}"))?;
            for row in rows {
                let (
                    refresh_token,
                    user_id,
                    tenant_id,
                    plan_tier_raw,
                    expires_at_raw,
                    revoked_at_raw,
                ) = row.map_err(|err| format!("decode refresh_sessions row failed: {err}"))?;
                store.refresh_sessions.insert(
                    refresh_token,
                    RefreshSession {
                        user_id,
                        tenant_id,
                        plan_tier: plan_tier_from_str(&plan_tier_raw)?,
                        expires_at: parse_rfc3339_utc(&expires_at_raw)?,
                        revoked_at: revoked_at_raw
                            .as_deref()
                            .map(parse_rfc3339_utc)
                            .transpose()?,
                    },
                );
            }
        }

        {
            let mut stmt = conn
                .prepare(
                    "SELECT audit_id, tenant_id, user_id, action, resource, outcome, ts FROM audit_events ORDER BY ts ASC",
                )
                .map_err(|err| format!("prepare audit_events query failed: {err}"))?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, String>(6)?,
                    ))
                })
                .map_err(|err| format!("query audit_events failed: {err}"))?;
            for row in rows {
                let (audit_id, tenant_id, user_id, action, resource, outcome, ts_raw) =
                    row.map_err(|err| format!("decode audit_events row failed: {err}"))?;
                store.audit_events.push(AuditRecord {
                    audit_id,
                    tenant_id,
                    user_id,
                    action,
                    resource,
                    outcome,
                    ts: parse_rfc3339_utc(&ts_raw)?,
                });
            }
        }

        for (tenant_id, tenant) in &mut store.tenants {
            if !tenant.nodes.contains_key(ROOT_NODE_ID) {
                tenant.nodes.insert(
                    ROOT_NODE_ID.to_string(),
                    Node {
                        node_id: ROOT_NODE_ID.to_string(),
                        tenant_id: tenant_id.clone(),
                        parent_id: None,
                        name: "/".to_string(),
                        path: "/".to_string(),
                        kind: NodeKind::Dir,
                        logical_clock: 1,
                        deleted_at: None,
                    },
                );
                tenant
                    .path_index
                    .insert("/".to_string(), ROOT_NODE_ID.to_string());
            }
        }

        Ok(store)
    }

    fn save(&self, store: &MetadataStore) -> Result<(), String> {
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| "metadata sqlite mutex poisoned".to_string())?;
        let tx = conn
            .transaction()
            .map_err(|err| format!("begin sqlite transaction failed: {err}"))?;

        tx.execute("DELETE FROM audit_events", [])
            .map_err(|err| format!("clear audit_events failed: {err}"))?;
        tx.execute("DELETE FROM refresh_sessions", [])
            .map_err(|err| format!("clear refresh_sessions failed: {err}"))?;
        tx.execute("DELETE FROM device_sessions", [])
            .map_err(|err| format!("clear device_sessions failed: {err}"))?;
        tx.execute("DELETE FROM idempotency_keys", [])
            .map_err(|err| format!("clear idempotency_keys failed: {err}"))?;
        tx.execute("DELETE FROM uploads", [])
            .map_err(|err| format!("clear uploads failed: {err}"))?;
        tx.execute("DELETE FROM change_events", [])
            .map_err(|err| format!("clear change_events failed: {err}"))?;
        tx.execute("DELETE FROM head_versions", [])
            .map_err(|err| format!("clear head_versions failed: {err}"))?;
        tx.execute("DELETE FROM file_versions", [])
            .map_err(|err| format!("clear file_versions failed: {err}"))?;
        tx.execute("DELETE FROM nodes", [])
            .map_err(|err| format!("clear nodes failed: {err}"))?;
        tx.execute("DELETE FROM tenants", [])
            .map_err(|err| format!("clear tenants failed: {err}"))?;

        for (tenant_id, tenant) in &store.tenants {
            let retention_json = serde_json::to_string(&tenant.retention_policy)
                .map_err(|err| format!("serialize retention policy failed: {err}"))?;
            let next_cursor = i64::try_from(tenant.next_cursor)
                .map_err(|_| "next_cursor overflow for sqlite i64".to_string())?;
            tx.execute(
                "INSERT INTO tenants(tenant_id, plan_tier, retention_policy_json, next_cursor) VALUES (?1, ?2, ?3, ?4)",
                params![tenant_id, plan_tier_to_str(&tenant.plan_tier), retention_json, next_cursor],
            )
            .map_err(|err| format!("insert tenant failed: {err}"))?;

            for node in tenant.nodes.values() {
                let deleted_at = node.deleted_at.as_ref().map(DateTime::<Utc>::to_rfc3339);
                let logical_clock = i64::try_from(node.logical_clock)
                    .map_err(|_| "logical_clock overflow for sqlite i64".to_string())?;
                tx.execute(
                    "INSERT INTO nodes(tenant_id, node_id, parent_id, name, path, kind, logical_clock, deleted_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    params![
                        tenant_id,
                        node.node_id,
                        node.parent_id,
                        node.name,
                        node.path,
                        node_kind_to_str(&node.kind),
                        logical_clock,
                        deleted_at
                    ],
                )
                .map_err(|err| format!("insert node failed: {err}"))?;
            }

            for versions in tenant.versions.values() {
                for version in versions {
                    let size = i64::try_from(version.size)
                        .map_err(|_| "version size overflow for sqlite i64".to_string())?;
                    tx.execute(
                        "INSERT INTO file_versions(tenant_id, version_id, node_id, blob_key, size, content_hash, writer_device_id, committed_at, overwrite_of_version_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                        params![
                            tenant_id,
                            version.version_id,
                            version.node_id,
                            version.blob_key,
                            size,
                            version.content_hash,
                            version.writer_device_id,
                            version.committed_at.to_rfc3339(),
                            version.overwrite_of_version_id
                        ],
                    )
                    .map_err(|err| format!("insert file version failed: {err}"))?;
                }
            }

            for (node_id, version_id) in &tenant.head_versions {
                tx.execute(
                    "INSERT INTO head_versions(tenant_id, node_id, version_id) VALUES (?1, ?2, ?3)",
                    params![tenant_id, node_id, version_id],
                )
                .map_err(|err| format!("insert head version failed: {err}"))?;
            }

            for event in &tenant.changes {
                let cursor = i64::try_from(event.cursor)
                    .map_err(|_| "event cursor overflow for sqlite i64".to_string())?;
                tx.execute(
                    "INSERT INTO change_events(tenant_id, event_id, node_id, op, version_id, ts, actor, cursor, path) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                    params![
                        tenant_id,
                        event.event_id,
                        event.node_id,
                        change_op_to_str(&event.op),
                        event.version_id,
                        event.ts.to_rfc3339(),
                        event.actor,
                        cursor,
                        event.path
                    ],
                )
                .map_err(|err| format!("insert change event failed: {err}"))?;
            }
        }

        for (upload_id, upload) in &store.uploads {
            let size_hint = upload
                .size_hint
                .map(i64::try_from)
                .transpose()
                .map_err(|_| "upload size_hint overflow for sqlite i64".to_string())?;
            let parts_json = serde_json::to_string(&upload.parts)
                .map_err(|err| format!("serialize upload parts failed: {err}"))?;
            tx.execute(
                "INSERT INTO uploads(upload_id, tenant_id, path, blob_key, content_hash, writer_device_id, size_hint, parts_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    upload_id,
                    upload.tenant_id,
                    upload.path,
                    upload.blob_key,
                    upload.content_hash,
                    upload.writer_device_id,
                    size_hint,
                    parts_json
                ],
            )
            .map_err(|err| format!("insert upload failed: {err}"))?;
        }

        for (id_key, response) in &store.idempotency {
            let response_json = serde_json::to_string(response)
                .map_err(|err| format!("serialize idempotency response failed: {err}"))?;
            tx.execute(
                "INSERT INTO idempotency_keys(id_key, response_json) VALUES (?1, ?2)",
                params![id_key, response_json],
            )
            .map_err(|err| format!("insert idempotency row failed: {err}"))?;
        }

        for (device_code, session) in &store.device_sessions {
            let approved_json = session
                .approved
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|err| format!("serialize approved session failed: {err}"))?;
            tx.execute(
                "INSERT INTO device_sessions(device_code, user_code, approved_json, expires_at) VALUES (?1, ?2, ?3, ?4)",
                params![device_code, session.user_code, approved_json, session.expires_at.to_rfc3339()],
            )
            .map_err(|err| format!("insert device session failed: {err}"))?;
        }

        for (refresh_token, session) in &store.refresh_sessions {
            let revoked_at = session.revoked_at.as_ref().map(DateTime::<Utc>::to_rfc3339);
            tx.execute(
                "INSERT INTO refresh_sessions(refresh_token, user_id, tenant_id, plan_tier, expires_at, revoked_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    refresh_token,
                    session.user_id,
                    session.tenant_id,
                    plan_tier_to_str(&session.plan_tier),
                    session.expires_at.to_rfc3339(),
                    revoked_at
                ],
            )
            .map_err(|err| format!("insert refresh session failed: {err}"))?;
        }

        for event in &store.audit_events {
            tx.execute(
                "INSERT INTO audit_events(audit_id, tenant_id, user_id, action, resource, outcome, ts) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    event.audit_id,
                    event.tenant_id,
                    event.user_id,
                    event.action,
                    event.resource,
                    event.outcome,
                    event.ts.to_rfc3339()
                ],
            )
            .map_err(|err| format!("insert audit event failed: {err}"))?;
        }

        tx.commit()
            .map_err(|err| format!("commit sqlite transaction failed: {err}"))?;
        Ok(())
    }
}

#[cfg(target_arch = "wasm32")]
struct SqliteMetadataStore;

#[cfg(target_arch = "wasm32")]
impl SqliteMetadataStore {
    fn open(_path: &str) -> Result<Self, String> {
        Err("metadata sqlite is not supported on wasm runtime".to_string())
    }

    fn load(&self) -> Result<MetadataStore, String> {
        Ok(MetadataStore::default())
    }

    fn save(&self, _store: &MetadataStore) -> Result<(), String> {
        Ok(())
    }
}

fn generate_device_user_code() -> String {
    let bytes = Uuid::new_v4().into_bytes();
    let left = u16::from_be_bytes([bytes[0], bytes[1]]);
    let right = u16::from_be_bytes([bytes[2], bytes[3]]);
    format!("{left:04X}-{right:04X}")
}

#[cfg(not(target_arch = "wasm32"))]
fn parse_rfc3339_utc(raw: &str) -> Result<DateTime<Utc>, String> {
    chrono::DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|err| format!("invalid RFC3339 datetime: {err}"))
}

#[cfg(not(target_arch = "wasm32"))]
fn plan_tier_to_str(plan: &PlanTier) -> &'static str {
    match plan {
        PlanTier::Free => "free",
        PlanTier::Pro => "pro",
        PlanTier::Team => "team",
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn plan_tier_from_str(raw: &str) -> Result<PlanTier, String> {
    match raw {
        "free" => Ok(PlanTier::Free),
        "pro" => Ok(PlanTier::Pro),
        "team" => Ok(PlanTier::Team),
        _ => Err(format!("invalid plan_tier value: {raw}")),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn node_kind_to_str(kind: &NodeKind) -> &'static str {
    match kind {
        NodeKind::File => "file",
        NodeKind::Dir => "dir",
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn node_kind_from_str(raw: &str) -> Result<NodeKind, String> {
    match raw {
        "file" => Ok(NodeKind::File),
        "dir" => Ok(NodeKind::Dir),
        _ => Err(format!("invalid node kind value: {raw}")),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn change_op_to_str(op: &ChangeOp) -> &'static str {
    match op {
        ChangeOp::Create => "create",
        ChangeOp::Write => "write",
        ChangeOp::Rename => "rename",
        ChangeOp::Delete => "delete",
        ChangeOp::Restore => "restore",
        ChangeOp::Mkdir => "mkdir",
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn change_op_from_str(raw: &str) -> Result<ChangeOp, String> {
    match raw {
        "create" => Ok(ChangeOp::Create),
        "write" => Ok(ChangeOp::Write),
        "rename" => Ok(ChangeOp::Rename),
        "delete" => Ok(ChangeOp::Delete),
        "restore" => Ok(ChangeOp::Restore),
        "mkdir" => Ok(ChangeOp::Mkdir),
        _ => Err(format!("invalid change op value: {raw}")),
    }
}

fn retention_for_tier(tier: &PlanTier) -> RetentionPolicy {
    match tier {
        PlanTier::Free => RetentionPolicy::free(),
        PlanTier::Pro => RetentionPolicy::pro(),
        PlanTier::Team => RetentionPolicy::team(),
    }
}

fn tenant_total_stored_bytes(tenant: &TenantState) -> u64 {
    tenant
        .versions
        .values()
        .flat_map(|versions| versions.iter().map(|v| v.size))
        .fold(0u64, |acc, size| acc.saturating_add(size))
}

fn push_audit_event(
    store: &mut MetadataStore,
    tenant_id: String,
    user_id: String,
    action: &str,
    resource: Option<String>,
    outcome: &str,
) {
    store.audit_events.push(AuditRecord {
        audit_id: format!("aud_{}", Uuid::new_v4()),
        tenant_id,
        user_id,
        action: action.to_string(),
        resource,
        outcome: outcome.to_string(),
        ts: Utc::now(),
    });
    if store.audit_events.len() > 50_000 {
        let drain_until = store.audit_events.len() - 50_000;
        store.audit_events.drain(0..drain_until);
    }
}

fn upsert_file_node(tenant: &mut TenantState, path: &str) -> AppResult<String> {
    if let Some(existing_node_id) = tenant.path_index.get(path).cloned() {
        let node = tenant
            .nodes
            .get_mut(&existing_node_id)
            .ok_or_else(|| AppError::Internal("path index points to missing node".to_string()))?;

        if node.kind != NodeKind::File {
            return Err(AppError::Conflict("path is not a file".to_string()));
        }

        node.deleted_at = None;
        return Ok(existing_node_id);
    }

    let (parent_path, name) = split_parent_and_name(path)?;
    let parent_id = tenant
        .path_index
        .get(&parent_path)
        .cloned()
        .ok_or_else(|| AppError::Conflict("parent directory does not exist".to_string()))?;

    let node_id = format!("node_{}", Uuid::new_v4());
    let node = Node {
        node_id: node_id.clone(),
        tenant_id: tenant
            .nodes
            .get(ROOT_NODE_ID)
            .map(|root| root.tenant_id.clone())
            .unwrap_or_else(|| "unknown".to_string()),
        parent_id: Some(parent_id),
        name,
        path: path.to_string(),
        kind: NodeKind::File,
        logical_clock: 1,
        deleted_at: None,
    };

    tenant.path_index.insert(path.to_string(), node_id.clone());
    tenant.nodes.insert(node_id.clone(), node);

    Ok(node_id)
}

fn apply_retention_for_node(tenant: &mut TenantState, node_id: &str) {
    let Some(versions) = tenant.versions.get_mut(node_id) else {
        return;
    };

    let policy = tenant.retention_policy.clone();
    let head_id = tenant.head_versions.get(node_id).cloned();
    let now = Utc::now();
    let day_cutoff = now - Duration::days(policy.max_days);

    versions.sort_by_key(|v| v.committed_at);

    versions.retain(|version| {
        if Some(version.version_id.clone()) == head_id {
            return true;
        }

        version.committed_at >= day_cutoff
    });

    if versions.len() <= policy.max_versions {
        return;
    }

    let mut i = 0usize;
    while versions.len() > policy.max_versions && i < versions.len() {
        let keep = head_id
            .as_ref()
            .map(|id| *id == versions[i].version_id)
            .unwrap_or(false);
        if keep {
            i += 1;
            continue;
        }

        versions.remove(i);
    }
}

pub fn normalize_path(raw: &str) -> AppResult<String> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(AppError::InvalidRequest("path cannot be empty".to_string()));
    }

    let mut parts = Vec::new();
    for part in raw.split('/') {
        if part.is_empty() || part == "." {
            continue;
        }
        if part == ".." {
            return Err(AppError::InvalidRequest(
                "path traversal is not allowed".to_string(),
            ));
        }
        parts.push(part);
    }

    if parts.is_empty() {
        return Ok("/".to_string());
    }

    Ok(format!("/{}", parts.join("/")))
}

fn split_parent_and_name(path: &str) -> AppResult<(String, String)> {
    let path = normalize_path(path)?;
    if path == "/" {
        return Err(AppError::InvalidRequest("root has no parent".to_string()));
    }

    if let Some(idx) = path.rfind('/') {
        let name = path[idx + 1..].to_string();
        let parent = if idx == 0 {
            "/".to_string()
        } else {
            path[..idx].to_string()
        };
        return Ok((parent, name));
    }

    Err(AppError::InvalidRequest("invalid path".to_string()))
}

fn validate_parent_directory(
    store: &mut MetadataStore,
    tenant_id: &str,
    path: &str,
) -> AppResult<()> {
    let tenant = store.ensure_tenant_mut(tenant_id);
    let (parent_path, _) = split_parent_and_name(path)?;

    let parent_node_id = tenant
        .path_index
        .get(&parent_path)
        .cloned()
        .ok_or_else(|| AppError::Conflict(format!("parent directory missing: {parent_path}")))?;

    let parent = tenant
        .nodes
        .get(&parent_node_id)
        .ok_or_else(|| AppError::Internal("parent id not found".to_string()))?;

    if parent.kind != NodeKind::Dir {
        return Err(AppError::Conflict(
            "parent path is not a directory".to_string(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use meshfs_store::InMemoryObjectStore;
    use meshfs_types::{
        ApplyRetentionRequest, NodeKind, PlanTier, RetentionPolicy, UploadCommitResponse,
        UploadInitRequest, UploadPartRequest,
    };

    use super::{normalize_path, AppState, AuthIdentity};
    use crate::error::AppError;

    fn auth() -> AuthIdentity {
        AuthIdentity {
            user_id: "user-1".to_string(),
            tenant_id: "tenant-1".to_string(),
        }
    }

    #[tokio::test]
    async fn lww_keeps_history_and_updates_head() {
        let state = AppState::new(
            "secret".to_string(),
            3600,
            Arc::new(InMemoryObjectStore::default()),
        );

        let init1 = state
            .init_upload(
                auth(),
                UploadInitRequest {
                    path: "/docs/a.txt".to_string(),
                    size_hint: Some(3),
                    content_hash: None,
                    writer_device_id: Some("d1".to_string()),
                },
            )
            .await;

        assert!(init1.is_err());

        state
            .mkdir(
                auth(),
                meshfs_types::MkdirRequest {
                    path: "/docs".to_string(),
                },
            )
            .await
            .expect("mkdir should work");

        let init1 = state
            .init_upload(
                auth(),
                UploadInitRequest {
                    path: "/docs/a.txt".to_string(),
                    size_hint: Some(3),
                    content_hash: None,
                    writer_device_id: Some("d1".to_string()),
                },
            )
            .await
            .expect("init1");

        state
            .put_upload_part(
                auth(),
                UploadPartRequest {
                    upload_id: init1.upload_id.clone(),
                    part_number: 1,
                    data_base64: "b25l".to_string(),
                },
            )
            .await
            .expect("part1");

        let commit1 = state
            .commit_upload(auth(), &init1.upload_id, None)
            .await
            .expect("commit1");

        let init2 = state
            .init_upload(
                auth(),
                UploadInitRequest {
                    path: "/docs/a.txt".to_string(),
                    size_hint: Some(3),
                    content_hash: None,
                    writer_device_id: Some("d2".to_string()),
                },
            )
            .await
            .expect("init2");

        state
            .put_upload_part(
                auth(),
                UploadPartRequest {
                    upload_id: init2.upload_id.clone(),
                    part_number: 1,
                    data_base64: "dHdv".to_string(),
                },
            )
            .await
            .expect("part2");

        let commit2 = state
            .commit_upload(auth(), &init2.upload_id, None)
            .await
            .expect("commit2");

        assert_ne!(commit1.version_id, commit2.version_id);

        let meta = state
            .get_meta_by_path(auth(), "/docs/a.txt")
            .await
            .expect("meta");
        assert_eq!(
            meta.head_version.expect("head").version_id,
            commit2.version_id
        );

        let versions = state
            .list_versions(auth(), &meta.node.node_id)
            .await
            .expect("versions");

        assert_eq!(versions.versions.len(), 2);
    }

    #[tokio::test]
    async fn idempotent_commit_reuses_response() {
        let state = AppState::new(
            "secret".to_string(),
            3600,
            Arc::new(InMemoryObjectStore::default()),
        );

        state
            .mkdir(
                auth(),
                meshfs_types::MkdirRequest {
                    path: "/docs".to_string(),
                },
            )
            .await
            .expect("mkdir should work");

        let init = state
            .init_upload(
                auth(),
                UploadInitRequest {
                    path: "/docs/a.txt".to_string(),
                    size_hint: Some(3),
                    content_hash: None,
                    writer_device_id: Some("d1".to_string()),
                },
            )
            .await
            .expect("init");

        state
            .put_upload_part(
                auth(),
                UploadPartRequest {
                    upload_id: init.upload_id.clone(),
                    part_number: 1,
                    data_base64: "b25l".to_string(),
                },
            )
            .await
            .expect("part");

        let first: UploadCommitResponse = state
            .commit_upload(auth(), &init.upload_id, Some("abc".to_string()))
            .await
            .expect("first commit");

        let second = state
            .commit_upload(auth(), "non-existent", Some("abc".to_string()))
            .await
            .expect("second commit should replay by idempotency key");

        assert_eq!(first.version_id, second.version_id);
        assert_eq!(first.node_id, second.node_id);
    }

    #[tokio::test]
    async fn retention_max_versions_applies() {
        let state = AppState::new(
            "secret".to_string(),
            3600,
            Arc::new(InMemoryObjectStore::default()),
        );

        state
            .mkdir(
                auth(),
                meshfs_types::MkdirRequest {
                    path: "/docs".to_string(),
                },
            )
            .await
            .expect("mkdir should work");

        state
            .apply_retention_policy(
                auth(),
                ApplyRetentionRequest {
                    tenant_id: "tenant-1".to_string(),
                    policy: RetentionPolicy {
                        plan_tier: PlanTier::Free,
                        max_days: 365,
                        max_versions: 2,
                        hard_delete_grace_days: 7,
                    },
                },
            )
            .await
            .expect("apply retention");

        for data in ["b25l", "dHdv", "dGhyZWU="] {
            let init = state
                .init_upload(
                    auth(),
                    UploadInitRequest {
                        path: "/docs/a.txt".to_string(),
                        size_hint: None,
                        content_hash: None,
                        writer_device_id: None,
                    },
                )
                .await
                .expect("init");

            state
                .put_upload_part(
                    auth(),
                    UploadPartRequest {
                        upload_id: init.upload_id.clone(),
                        part_number: 1,
                        data_base64: data.to_string(),
                    },
                )
                .await
                .expect("part");

            state
                .commit_upload(auth(), &init.upload_id, None)
                .await
                .expect("commit");
        }

        let meta = state
            .get_meta_by_path(auth(), "/docs/a.txt")
            .await
            .expect("meta");

        let versions = state
            .list_versions(auth(), &meta.node.node_id)
            .await
            .expect("versions");

        assert_eq!(versions.versions.len(), 2);
    }

    #[tokio::test]
    async fn list_directory_returns_immediate_children() {
        let state = AppState::new(
            "secret".to_string(),
            3600,
            Arc::new(InMemoryObjectStore::default()),
        );

        state
            .mkdir(
                auth(),
                meshfs_types::MkdirRequest {
                    path: "/docs".to_string(),
                },
            )
            .await
            .expect("mkdir /docs");
        state
            .mkdir(
                auth(),
                meshfs_types::MkdirRequest {
                    path: "/docs/nested".to_string(),
                },
            )
            .await
            .expect("mkdir /docs/nested");

        let init = state
            .init_upload(
                auth(),
                UploadInitRequest {
                    path: "/docs/a.txt".to_string(),
                    size_hint: Some(3),
                    content_hash: None,
                    writer_device_id: None,
                },
            )
            .await
            .expect("init upload");
        state
            .put_upload_part(
                auth(),
                UploadPartRequest {
                    upload_id: init.upload_id.clone(),
                    part_number: 1,
                    data_base64: "b25l".to_string(),
                },
            )
            .await
            .expect("put part");
        state
            .commit_upload(auth(), &init.upload_id, None)
            .await
            .expect("commit");

        let listed = state
            .list_directory(auth(), "/docs")
            .await
            .expect("list directory");
        assert_eq!(listed.entries.len(), 2);
        assert_eq!(listed.entries[0].node.name, "a.txt");
        assert_eq!(listed.entries[0].node.kind, NodeKind::File);
        assert!(listed.entries[0].head_version.is_some());
        assert_eq!(listed.entries[1].node.name, "nested");
        assert_eq!(listed.entries[1].node.kind, NodeKind::Dir);
        assert!(listed.entries[1].head_version.is_none());
    }

    #[test]
    fn path_normalization_works() {
        assert_eq!(normalize_path("/").unwrap(), "/");
        assert_eq!(normalize_path("docs/a").unwrap(), "/docs/a");
        assert_eq!(normalize_path("//docs///a//").unwrap(), "/docs/a");
        assert!(normalize_path("../../etc/passwd").is_err());
    }

    #[tokio::test]
    async fn metadata_sqlite_persists_across_restart() {
        let db_path = std::env::temp_dir().join(format!("meshfs-test-{}.db", uuid::Uuid::new_v4()));
        let db_path_str = db_path.to_string_lossy().to_string();

        let state_a = AppState::new_with_metadata_sqlite(
            "secret".to_string(),
            3600,
            Arc::new(InMemoryObjectStore::default()),
            Some(db_path_str.clone()),
        );

        state_a
            .mkdir(
                auth(),
                meshfs_types::MkdirRequest {
                    path: "/docs".to_string(),
                },
            )
            .await
            .expect("mkdir should persist");

        drop(state_a);

        let state_b = AppState::new_with_metadata_sqlite(
            "secret".to_string(),
            3600,
            Arc::new(InMemoryObjectStore::default()),
            Some(db_path_str),
        );

        let meta = state_b
            .get_meta_by_path(auth(), "/docs")
            .await
            .expect("metadata should be restored from sqlite");
        assert_eq!(meta.node.path, "/docs");

        let _ = std::fs::remove_file(db_path);
    }

    #[tokio::test]
    async fn refresh_token_exchange_and_revoke_work() {
        let state = AppState::new(
            "secret".to_string(),
            3600,
            Arc::new(InMemoryObjectStore::default()),
        );

        let refresh = state
            .issue_refresh_token("user-1", "tenant-1", PlanTier::Free)
            .await
            .expect("issue refresh");
        let (_user_id, _tenant_id, _tier, rotated) = state
            .exchange_refresh_token(&refresh)
            .await
            .expect("exchange refresh");
        assert!(state.exchange_refresh_token(&refresh).await.is_err());

        state
            .revoke_refresh_token(auth(), &rotated)
            .await
            .expect("revoke refresh");
        let err = state
            .exchange_refresh_token(&rotated)
            .await
            .expect_err("revoked refresh should fail");
        assert!(matches!(err, AppError::Unauthorized));
    }

    #[tokio::test]
    async fn retention_apply_requires_tenant_ownership() {
        let state = AppState::new(
            "secret".to_string(),
            3600,
            Arc::new(InMemoryObjectStore::default()),
        );
        let err = state
            .apply_retention_policy(
                AuthIdentity {
                    user_id: "user-2".to_string(),
                    tenant_id: "tenant-2".to_string(),
                },
                ApplyRetentionRequest {
                    tenant_id: "tenant-1".to_string(),
                    policy: RetentionPolicy::free(),
                },
            )
            .await
            .expect_err("cross-tenant apply should fail");
        assert!(matches!(err, AppError::Forbidden));
    }

    #[tokio::test]
    async fn storage_quota_is_enforced_on_commit() {
        let state = AppState::new_with_runtime_config(
            "secret".to_string(),
            3600,
            30 * 24 * 3600,
            Arc::new(InMemoryObjectStore::default()),
            None,
            1200,
            4,
        );

        state
            .mkdir(
                auth(),
                meshfs_types::MkdirRequest {
                    path: "/docs".to_string(),
                },
            )
            .await
            .expect("mkdir should work");

        let init = state
            .init_upload(
                auth(),
                UploadInitRequest {
                    path: "/docs/a.txt".to_string(),
                    size_hint: Some(5),
                    content_hash: None,
                    writer_device_id: None,
                },
            )
            .await
            .expect("init upload");

        state
            .put_upload_part(
                auth(),
                UploadPartRequest {
                    upload_id: init.upload_id.clone(),
                    part_number: 1,
                    data_base64: "aGVsbG8=".to_string(),
                },
            )
            .await
            .expect("put part");

        let err = state
            .commit_upload(auth(), &init.upload_id, None)
            .await
            .expect_err("commit should hit quota");
        assert!(matches!(err, AppError::QuotaExceeded(_)));
    }

    #[tokio::test]
    async fn tenant_rate_limit_is_enforced() {
        let state = AppState::new_with_runtime_config(
            "secret".to_string(),
            3600,
            30 * 24 * 3600,
            Arc::new(InMemoryObjectStore::default()),
            None,
            1,
            1024 * 1024,
        );

        state.current_plan(auth()).await.expect("warmup tenant");
        state.sync_pull(auth(), 0).await.expect("first pull");
        let err = state
            .sync_pull(auth(), 0)
            .await
            .expect_err("second pull in same window should be limited");
        assert!(matches!(err, AppError::RateLimited(_)));
    }

    #[tokio::test]
    async fn audit_events_persist_in_sqlite() {
        let db_path =
            std::env::temp_dir().join(format!("meshfs-audit-test-{}.db", uuid::Uuid::new_v4()));
        let db_path_str = db_path.to_string_lossy().to_string();

        let state_a = AppState::new_with_metadata_sqlite(
            "secret".to_string(),
            3600,
            Arc::new(InMemoryObjectStore::default()),
            Some(db_path_str.clone()),
        );

        state_a
            .mkdir(
                auth(),
                meshfs_types::MkdirRequest {
                    path: "/audit-demo".to_string(),
                },
            )
            .await
            .expect("mkdir should record audit");

        drop(state_a);

        let state_b = AppState::new_with_metadata_sqlite(
            "secret".to_string(),
            3600,
            Arc::new(InMemoryObjectStore::default()),
            Some(db_path_str),
        );
        let audits = state_b
            .list_audit_events(auth(), 100)
            .await
            .expect("list audit");
        assert!(audits.events.iter().any(|evt| evt.action == "files.mkdir"));

        let _ = std::fs::remove_file(db_path);
    }
}
