#![allow(clippy::module_name_repetitions)]

#[cfg(not(target_arch = "wasm32"))]
pub fn runtime_target() -> &'static str {
    "cloudflare-workers (build this crate for wasm32-unknown-unknown)"
}

#[cfg(target_arch = "wasm32")]
mod wasm_runtime {
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::{FutureExt, StreamExt};
    use meshfs_control_plane_core::auth::{
        issue_access_token, parse_auth_context_from_headers, AuthContext,
    };
    use meshfs_control_plane_core::error::AppError;
    use meshfs_control_plane_core::state::{AppState, AuthIdentity};
    use meshfs_store::{ObjectStore, StoreError, StoreResult};
    use meshfs_types::{
        ApplyRetentionRequest, DeleteRequest, DeviceActivateRequest, DevicePollRequest,
        DevicePollResponse, DeviceStartResponse, LogoutRequest, MkdirRequest, RefreshTokenRequest,
        RefreshTokenResponse, RenameRequest, UploadCommitRequest, UploadInitRequest,
        UploadPartRequest,
    };
    use serde::Deserialize;
    use serde_json::json;
    use worker::*;

    #[event(fetch, respond_with_errors)]
    pub async fn fetch(req: Request, env: Env, _ctx: Context) -> Result<Response> {
        console_error_panic_hook::set_once();

        if req.method() == Method::Get && req.path() == "/healthz" {
            return json_response(200, &json!({"status": "ok"}));
        }
        if req.method() == Method::Get && req.path() == "/edition" {
            return json_response(
                200,
                &json!({
                    "edition": "oss",
                    "pricing": "free",
                    "notes": "Rust/Wasm direct-worker runtime"
                }),
            );
        }
        if req.method() == Method::Get && req.path() == "/capabilities" {
            return json_response(200, &capabilities(&env));
        }

        let runtime = WorkerRuntime::new(env).await?;
        runtime.handle(req).await
    }

    fn capabilities(env: &Env) -> serde_json::Value {
        let metadata = if env.d1("MESHFS_DB").is_ok() {
            "d1"
        } else {
            "unbound"
        };

        let object_store = if env.bucket("MESHFS_R2").is_ok() {
            "r2"
        } else {
            "unbound"
        };

        json!({
            "runtime": "cloudflare-workers-direct",
            "gateway_mode": false,
            "metadata": metadata,
            "object_store": object_store
        })
    }

    struct WorkerRuntime {
        env: Env,
        state: Arc<AppState>,
        metadata: D1SnapshotMetadataAdapter,
    }

    impl WorkerRuntime {
        async fn new(env: Env) -> Result<Self> {
            let metadata_db = env.d1("MESHFS_DB")?;
            let metadata = D1SnapshotMetadataAdapter::new(metadata_db);
            metadata.ensure_schema().await?;

            let object_store: Arc<dyn ObjectStore> = if let Ok(bucket) = env.bucket("MESHFS_R2") {
                Arc::new(CloudflareR2ObjectStore::new(bucket))
            } else {
                Arc::new(meshfs_store::InMemoryObjectStore::default())
            };

            let jwt_secret = env
                .var("MESHFS_JWT_SECRET")
                .map(|v| v.to_string())
                .unwrap_or_else(|_| "meshfs-dev-secret".to_string());
            let token_ttl_seconds = env
                .var("MESHFS_TOKEN_TTL_SECONDS")
                .ok()
                .and_then(|v| v.to_string().parse::<i64>().ok())
                .unwrap_or(3600);
            let refresh_ttl_seconds = env
                .var("MESHFS_REFRESH_TOKEN_TTL_SECONDS")
                .ok()
                .and_then(|v| v.to_string().parse::<i64>().ok())
                .unwrap_or(30 * 24 * 3600);
            let rate_limit_per_minute = env
                .var("MESHFS_RATE_LIMIT_PER_MINUTE")
                .ok()
                .and_then(|v| v.to_string().parse::<u64>().ok())
                .unwrap_or(1200);
            let tenant_storage_quota_bytes = env
                .var("MESHFS_TENANT_STORAGE_QUOTA_BYTES")
                .ok()
                .and_then(|v| v.to_string().parse::<u64>().ok())
                .unwrap_or(10 * 1024 * 1024 * 1024);

            let state = Arc::new(AppState::new_with_runtime_config(
                jwt_secret,
                token_ttl_seconds,
                refresh_ttl_seconds,
                object_store,
                None,
                rate_limit_per_minute,
                tenant_storage_quota_bytes,
            ));

            if let Some(snapshot) = metadata.load_snapshot().await? {
                state
                    .import_snapshot_json(&snapshot)
                    .await
                    .map_err(core_error_to_worker)?;
            }

            Ok(Self {
                env,
                state,
                metadata,
            })
        }

        async fn persist_snapshot(&self) -> Result<()> {
            let snapshot = self
                .state
                .export_snapshot_json()
                .await
                .map_err(core_error_to_worker)?;
            self.metadata.save_snapshot(&snapshot).await
        }

        async fn handle(&self, mut req: Request) -> Result<Response> {
            let method = req.method();
            let path = req.path();

            match (method.clone(), path.as_str()) {
                (Method::Post, "/auth/device/start") => {
                    let (device_code, user_code, interval_seconds, expires_in_seconds) =
                        self.state.start_device_session().await;
                    self.persist_snapshot().await?;
                    json_response(
                        200,
                        &DeviceStartResponse {
                            device_code,
                            user_code,
                            verification_uri: verification_uri_for_request(&req),
                            interval_seconds,
                            expires_in_seconds,
                        },
                    )
                }
                (Method::Post, "/auth/device/activate") => {
                    let body: DeviceActivateRequest = req.json().await?;
                    if let Err(err) = self.state.activate_device_session(body).await {
                        return app_error_response(err);
                    }
                    self.persist_snapshot().await?;
                    empty_response(204)
                }
                (Method::Post, "/auth/device/poll") => {
                    let body: DevicePollRequest = req.json().await?;
                    let poll = self
                        .state
                        .poll_device_session(&body.device_code)
                        .await
                        .map_err(core_error_to_worker);
                    match poll {
                        Ok(Some((user_id, tenant_id, plan_tier))) => {
                            let access = issue_access_token(
                                &user_id,
                                &tenant_id,
                                plan_tier.clone(),
                                &self.state.jwt_secret,
                                self.state.token_ttl_seconds,
                            )
                            .map_err(core_error_to_worker)?;
                            let refresh = self
                                .state
                                .issue_refresh_token(&user_id, &tenant_id, plan_tier)
                                .await
                                .map_err(core_error_to_worker)?;
                            self.persist_snapshot().await?;
                            json_response(
                                200,
                                &DevicePollResponse {
                                    access_token: access,
                                    refresh_token: refresh,
                                    expires_in_seconds: self.state.token_ttl_seconds as u64,
                                    token_type: "Bearer".to_string(),
                                },
                            )
                        }
                        Ok(None) => json_response(202, &json!({"status": "authorization_pending"})),
                        Err(err) => Err(err),
                    }
                }
                (Method::Post, "/auth/refresh") => {
                    let body: RefreshTokenRequest = req.json().await?;
                    let exchange = self
                        .state
                        .exchange_refresh_token(&body.refresh_token)
                        .await
                        .map_err(core_error_to_worker)?;
                    let access = issue_access_token(
                        &exchange.0,
                        &exchange.1,
                        exchange.2,
                        &self.state.jwt_secret,
                        self.state.token_ttl_seconds,
                    )
                    .map_err(core_error_to_worker)?;
                    self.persist_snapshot().await?;
                    json_response(
                        200,
                        &RefreshTokenResponse {
                            access_token: access,
                            refresh_token: exchange.3,
                            expires_in_seconds: self.state.token_ttl_seconds as u64,
                            token_type: "Bearer".to_string(),
                        },
                    )
                }
                (Method::Post, "/auth/logout") => {
                    let auth = parse_auth_from_request(&req, &self.state.jwt_secret)?;
                    let body: LogoutRequest = req.json().await?;
                    if let Err(err) = self
                        .state
                        .revoke_refresh_token(to_identity(auth), &body.refresh_token)
                        .await
                    {
                        return app_error_response(err);
                    }
                    self.persist_snapshot().await?;
                    empty_response(204)
                }
                (Method::Get, "/sync/stream") => self.handle_sync_stream(req).await,
                (Method::Get, "/sync/ws") => self.handle_sync_ws(req).await,
                (Method::Get, "/sync/pull") => {
                    let auth = parse_auth_from_request(&req, &self.state.jwt_secret)?;
                    let cursor = query_param_u64(&req, "cursor")?.unwrap_or(0);
                    match self.state.sync_pull(to_identity(auth), cursor).await {
                        Ok(out) => json_response(200, &out),
                        Err(err) => app_error_response(err),
                    }
                }
                (Method::Post, "/files/mkdir") => {
                    let auth = parse_auth_from_request(&req, &self.state.jwt_secret)?;
                    let body: MkdirRequest = req.json().await?;
                    let out = self.state.mkdir(to_identity(auth), body).await;
                    match out {
                        Ok(out) => {
                            self.persist_snapshot().await?;
                            json_response(201, &out)
                        }
                        Err(err) => app_error_response(err),
                    }
                }
                (Method::Post, "/files/upload/init") => {
                    let auth = parse_auth_from_request(&req, &self.state.jwt_secret)?;
                    let body: UploadInitRequest = req.json().await?;
                    let out = self.state.init_upload(to_identity(auth), body).await;
                    match out {
                        Ok(out) => {
                            self.persist_snapshot().await?;
                            json_response(200, &out)
                        }
                        Err(err) => app_error_response(err),
                    }
                }
                (Method::Put, "/files/upload/part") => {
                    let auth = parse_auth_from_request(&req, &self.state.jwt_secret)?;
                    let body: UploadPartRequest = req.json().await?;
                    let out = self.state.put_upload_part(to_identity(auth), body).await;
                    match out {
                        Ok(()) => {
                            self.persist_snapshot().await?;
                            empty_response(204)
                        }
                        Err(err) => app_error_response(err),
                    }
                }
                (Method::Post, "/files/upload/commit") => {
                    let auth = parse_auth_from_request(&req, &self.state.jwt_secret)?;
                    let body: UploadCommitRequest = req.json().await?;
                    let idempotency_key = req.headers().get("idempotency-key")?;
                    let out = self
                        .state
                        .commit_upload(to_identity(auth), &body.upload_id, idempotency_key)
                        .await;
                    match out {
                        Ok(out) => {
                            self.persist_snapshot().await?;
                            json_response(200, &out)
                        }
                        Err(err) => app_error_response(err),
                    }
                }
                (Method::Post, "/files/rename") => {
                    let auth = parse_auth_from_request(&req, &self.state.jwt_secret)?;
                    let body: RenameRequest = req.json().await?;
                    let out = self.state.rename(to_identity(auth), body).await;
                    match out {
                        Ok(()) => {
                            self.persist_snapshot().await?;
                            empty_response(204)
                        }
                        Err(err) => app_error_response(err),
                    }
                }
                (Method::Delete, "/files") => {
                    let auth = parse_auth_from_request(&req, &self.state.jwt_secret)?;
                    let body: DeleteRequest = req.json().await?;
                    let out = self.state.delete(to_identity(auth), body).await;
                    match out {
                        Ok(()) => {
                            self.persist_snapshot().await?;
                            empty_response(204)
                        }
                        Err(err) => app_error_response(err),
                    }
                }
                (Method::Get, "/files/meta") => {
                    let auth = parse_auth_from_request(&req, &self.state.jwt_secret)?;
                    let path = required_query_param(&req, "path")?;
                    match self.state.get_meta_by_path(to_identity(auth), &path).await {
                        Ok(out) => json_response(200, &out),
                        Err(err) => app_error_response(err),
                    }
                }
                (Method::Get, "/files/list") => {
                    let auth = parse_auth_from_request(&req, &self.state.jwt_secret)?;
                    let path = required_query_param(&req, "path")?;
                    match self.state.list_directory(to_identity(auth), &path).await {
                        Ok(out) => json_response(200, &out),
                        Err(err) => app_error_response(err),
                    }
                }
                (Method::Get, "/files/download") => {
                    let auth = parse_auth_from_request(&req, &self.state.jwt_secret)?;
                    let path = required_query_param(&req, "path")?;
                    match self.state.get_bytes_by_path(to_identity(auth), &path).await {
                        Ok(bytes) => ResponseBuilder::new().from_bytes(bytes.to_vec()),
                        Err(err) => app_error_response(err),
                    }
                }
                (Method::Get, "/plans/current") => {
                    let auth = parse_auth_from_request(&req, &self.state.jwt_secret)?;
                    match self.state.current_plan(to_identity(auth)).await {
                        Ok(out) => {
                            self.persist_snapshot().await?;
                            json_response(200, &out)
                        }
                        Err(err) => app_error_response(err),
                    }
                }
                (Method::Get, "/retention/policy") => {
                    let auth = parse_auth_from_request(&req, &self.state.jwt_secret)?;
                    match self.state.retention_policy(to_identity(auth)).await {
                        Ok(out) => {
                            self.persist_snapshot().await?;
                            json_response(200, &out)
                        }
                        Err(err) => app_error_response(err),
                    }
                }
                (Method::Post, "/retention/apply") => {
                    let auth = parse_auth_from_request(&req, &self.state.jwt_secret)?;
                    let body: ApplyRetentionRequest = req.json().await?;
                    match self
                        .state
                        .apply_retention_policy(to_identity(auth), body)
                        .await
                    {
                        Ok(()) => {
                            self.persist_snapshot().await?;
                            empty_response(204)
                        }
                        Err(err) => app_error_response(err),
                    }
                }
                (Method::Get, "/audit/recent") => {
                    let auth = parse_auth_from_request(&req, &self.state.jwt_secret)?;
                    let limit = query_param_usize(&req, "limit")?.unwrap_or(100);
                    match self.state.list_audit_events(to_identity(auth), limit).await {
                        Ok(out) => json_response(200, &out),
                        Err(err) => app_error_response(err),
                    }
                }
                _ => {
                    if method == Method::Get {
                        if let Some(node_id) = versions_list_node_id(&path) {
                            let auth = parse_auth_from_request(&req, &self.state.jwt_secret)?;
                            return match self.state.list_versions(to_identity(auth), &node_id).await
                            {
                                Ok(out) => json_response(200, &out),
                                Err(err) => app_error_response(err),
                            };
                        }
                    }

                    if method == Method::Post {
                        if let Some((node_id, version_id)) = version_restore_ids(&path) {
                            let auth = parse_auth_from_request(&req, &self.state.jwt_secret)?;
                            return match self
                                .state
                                .restore_version(to_identity(auth), &node_id, &version_id)
                                .await
                            {
                                Ok(out) => {
                                    self.persist_snapshot().await?;
                                    json_response(200, &out)
                                }
                                Err(err) => app_error_response(err),
                            };
                        }
                    }

                    json_response(404, &json!({"error": "not_found"}))
                }
            }
        }

        async fn handle_sync_ws(&self, req: Request) -> Result<Response> {
            let auth = match parse_auth_from_request(&req, &self.state.jwt_secret) {
                Ok(auth) => auth,
                Err(_) => return Response::error("unauthorized", 401),
            };
            let initial_cursor = query_param_u64(&req, "cursor")?.unwrap_or(0);

            let pair = WebSocketPair::new()?;
            let client = pair.client;
            let server = pair.server;
            server.accept()?;

            let env = self.env.clone();
            let state = Arc::clone(&self.state);
            let identity = to_identity(auth);
            worker::wasm_bindgen_futures::spawn_local(async move {
                run_sync_ws_session(server, env, state, identity, initial_cursor).await;
            });

            Response::from_websocket(client)
        }

        async fn handle_sync_stream(&self, req: Request) -> Result<Response> {
            let auth = parse_auth_from_request(&req, &self.state.jwt_secret)?;
            let cursor = query_param_u64(&req, "cursor")?.unwrap_or(0);

            refresh_snapshot_from_d1(&self.env, &self.state).await?;
            let out = self
                .state
                .sync_pull(to_identity(auth), cursor)
                .await
                .map_err(core_error_to_worker)?;

            let mut payload = String::new();
            if out.events.is_empty() {
                payload.push_str(": keepalive\n\n");
            } else {
                for event in out.events {
                    let data = serde_json::to_string(&event).map_err(|err| {
                        Error::RustError(format!("sync stream encode event failed: {err}"))
                    })?;
                    payload.push_str("event: change\n");
                    payload.push_str("data: ");
                    payload.push_str(&data);
                    payload.push_str("\n\n");
                }
            }

            let mut builder = ResponseBuilder::new().with_status(200);
            builder = builder.with_header("content-type", "text/event-stream; charset=utf-8")?;
            builder = builder.with_header("cache-control", "no-store")?;
            builder = builder.with_header("x-meshfs-stream-mode", "snapshot")?;
            Ok(builder.fixed(payload.into_bytes()))
        }
    }

    #[derive(Deserialize)]
    struct SyncWsPullRequest {
        #[serde(rename = "type")]
        request_type: Option<String>,
        cursor: Option<u64>,
    }

    async fn run_sync_ws_session(
        socket: WebSocket,
        env: Env,
        state: Arc<AppState>,
        auth: AuthIdentity,
        mut cursor: u64,
    ) {
        const POLL_INTERVAL_SECONDS: u64 = 2;
        const HEARTBEAT_INTERVAL_MILLIS: u64 = 25_000;

        let mut last_activity_ms = worker::Date::now().as_millis();

        if emit_sync_events(&socket, &env, &state, &auth, cursor, &mut cursor)
            .await
            .is_err()
        {
            let _ = socket.close(Some(1011), Some("sync bootstrap failed"));
            return;
        }

        let mut events = match socket.events() {
            Ok(events) => events,
            Err(_) => {
                let _ = socket.close(Some(1011), Some("sync stream open failed"));
                return;
            }
        };

        loop {
            let next_event = events.next().fuse();
            let tick = Delay::from(Duration::from_secs(POLL_INTERVAL_SECONDS)).fuse();
            futures::pin_mut!(next_event, tick);

            futures::select! {
                incoming = next_event => {
                    match incoming {
                        Some(Ok(WebsocketEvent::Message(msg))) => {
                            let Some(text) = msg.text() else {
                                continue;
                            };

                            // Backward compatible: older clients may still send pull frames.
                            if let Ok(request) = serde_json::from_str::<SyncWsPullRequest>(&text) {
                                if request.request_type.as_deref() == Some("pull") || request.request_type.is_none() {
                                    let from_cursor = request.cursor.unwrap_or(cursor).min(cursor);
                                    if emit_sync_events(&socket, &env, &state, &auth, from_cursor, &mut cursor).await.is_err() {
                                        break;
                                    }
                                }
                            }
                        }
                        Some(Ok(WebsocketEvent::Close(_))) => break,
                        Some(Err(_)) | None => break,
                    }
                }
                _ = tick => {
                    let sent = match emit_sync_events(&socket, &env, &state, &auth, cursor, &mut cursor).await {
                        Ok(sent) => sent,
                        Err(_) => break,
                    };
                    let now = worker::Date::now().as_millis();
                    if sent {
                        last_activity_ms = now;
                    } else if now.saturating_sub(last_activity_ms) >= HEARTBEAT_INTERVAL_MILLIS {
                        let heartbeat = json!({
                            "type": "heartbeat",
                            "cursor": cursor,
                            "ts_ms": now
                        })
                        .to_string();
                        if socket.send_with_str(heartbeat).is_err() {
                            break;
                        }
                        last_activity_ms = now;
                    }
                }
            }
        }

        let _ = socket.close(Some(1000), Some("sync closed"));
    }

    async fn emit_sync_events(
        socket: &WebSocket,
        env: &Env,
        state: &Arc<AppState>,
        auth: &AuthIdentity,
        from_cursor: u64,
        cursor_out: &mut u64,
    ) -> Result<bool> {
        refresh_snapshot_from_d1(env, state).await?;

        let pull = state
            .sync_pull(auth.clone(), from_cursor)
            .await
            .map_err(core_error_to_worker)?;

        let mut sent = false;
        for event in pull.events {
            let payload = serde_json::to_string(&event)
                .map_err(|err| Error::RustError(format!("ws encode event failed: {err}")))?;
            socket.send_with_str(payload)?;
            *cursor_out = (*cursor_out).max(event.cursor);
            sent = true;
        }

        Ok(sent)
    }

    async fn refresh_snapshot_from_d1(env: &Env, state: &Arc<AppState>) -> Result<()> {
        let metadata = D1SnapshotMetadataAdapter::new(env.d1("MESHFS_DB")?);
        if let Some(snapshot) = metadata.load_snapshot().await? {
            state
                .import_snapshot_json(&snapshot)
                .await
                .map_err(core_error_to_worker)?;
        }
        Ok(())
    }

    fn versions_list_node_id(path: &str) -> Option<String> {
        let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();
        if parts.len() == 3 && parts[0] == "files" && parts[2] == "versions" {
            Some(parts[1].to_string())
        } else {
            None
        }
    }

    fn version_restore_ids(path: &str) -> Option<(String, String)> {
        let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();
        if parts.len() == 5
            && parts[0] == "files"
            && parts[2] == "versions"
            && parts[4] == "restore"
        {
            Some((parts[1].to_string(), parts[3].to_string()))
        } else {
            None
        }
    }

    fn verification_uri_for_request(req: &Request) -> String {
        let fallback = "https://meshfs.example/auth/device/activate".to_string();
        let url = match req.url() {
            Ok(url) => url,
            Err(_) => return fallback,
        };
        let host = match url.host_str() {
            Some(host) => host,
            None => return fallback,
        };

        let mut origin = format!("{}://{host}", url.scheme());
        if let Some(port) = url.port() {
            origin.push(':');
            origin.push_str(&port.to_string());
        }
        format!("{origin}/auth/device/activate")
    }

    fn to_identity(auth: AuthContext) -> AuthIdentity {
        AuthIdentity {
            user_id: auth.user_id,
            tenant_id: auth.tenant_id,
        }
    }

    fn parse_auth_from_request(req: &Request, jwt_secret: &str) -> Result<AuthContext> {
        let mut headers = http::HeaderMap::new();
        if let Some(authz) = req.headers().get("authorization")? {
            let value = authz
                .parse()
                .map_err(|_| Error::RustError("invalid authorization header".to_string()))?;
            headers.insert(http::header::AUTHORIZATION, value);
        }
        parse_auth_context_from_headers(&headers, jwt_secret).map_err(core_error_to_worker)
    }

    fn required_query_param(req: &Request, name: &str) -> Result<String> {
        req.url()?
            .query_pairs()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.to_string())
            .ok_or_else(|| Error::RustError(format!("missing query param: {name}")))
    }

    fn query_param_u64(req: &Request, name: &str) -> Result<Option<u64>> {
        let value = req
            .url()?
            .query_pairs()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.to_string());
        value
            .map(|raw| {
                raw.parse::<u64>()
                    .map_err(|_| Error::RustError(format!("invalid query param: {name}")))
            })
            .transpose()
    }

    fn query_param_usize(req: &Request, name: &str) -> Result<Option<usize>> {
        let value = req
            .url()?
            .query_pairs()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.to_string());
        value
            .map(|raw| {
                raw.parse::<usize>()
                    .map_err(|_| Error::RustError(format!("invalid query param: {name}")))
            })
            .transpose()
    }

    fn app_error_status(err: &AppError) -> u16 {
        match err {
            AppError::Unauthorized => 401,
            AppError::Forbidden => 403,
            AppError::NotFound(_) => 404,
            AppError::InvalidRequest(_) => 400,
            AppError::Conflict(_) => 409,
            AppError::RateLimited(_) => 429,
            AppError::QuotaExceeded(_) => 413,
            AppError::Internal(_) => 500,
        }
    }

    fn app_error_response(err: AppError) -> Result<Response> {
        let status = app_error_status(&err);
        json_response(status, &json!({"error": err.to_string()}))
    }

    fn json_response<T: serde::Serialize>(status: u16, payload: &T) -> Result<Response> {
        ResponseBuilder::new()
            .with_status(status)
            .from_json(payload)
    }

    fn empty_response(status: u16) -> Result<Response> {
        Ok(ResponseBuilder::new().with_status(status).empty())
    }

    fn core_error_to_worker(err: AppError) -> Error {
        Error::RustError(err.to_string())
    }

    struct D1SnapshotMetadataAdapter {
        db: D1Database,
    }

    impl D1SnapshotMetadataAdapter {
        fn new(db: D1Database) -> Self {
            Self { db }
        }

        async fn ensure_schema(&self) -> Result<()> {
            self.db
                .exec(
                    "CREATE TABLE IF NOT EXISTS meshfs_metadata_snapshot (snapshot_id INTEGER PRIMARY KEY CHECK (snapshot_id = 1), snapshot_json TEXT NOT NULL, updated_at TEXT NOT NULL)",
                )
                .await?;
            Ok(())
        }

        async fn load_snapshot(&self) -> Result<Option<String>> {
            #[derive(Deserialize)]
            struct Row {
                snapshot_json: String,
            }

            let row = self
                .db
                .prepare("SELECT snapshot_json FROM meshfs_metadata_snapshot WHERE snapshot_id = 1")
                .first::<Row>(None)
                .await?;
            Ok(row.map(|r| r.snapshot_json))
        }

        async fn save_snapshot(&self, snapshot_json: &str) -> Result<()> {
            let now = worker::Date::now().as_millis().to_string();
            self.db
                .prepare(
                    "INSERT INTO meshfs_metadata_snapshot(snapshot_id, snapshot_json, updated_at) VALUES (?1, ?2, ?3)
                     ON CONFLICT(snapshot_id) DO UPDATE SET snapshot_json = excluded.snapshot_json, updated_at = excluded.updated_at",
                )
                .bind(&[1i32.into(), snapshot_json.into(), now.into()])?
                .run()
                .await?;
            Ok(())
        }
    }

    struct CloudflareR2ObjectStore {
        bucket: Bucket,
    }

    impl CloudflareR2ObjectStore {
        fn new(bucket: Bucket) -> Self {
            Self { bucket }
        }
    }

    // SAFETY: Workers run in a single-threaded isolate; this wrapper is not shared across host threads.
    unsafe impl Send for CloudflareR2ObjectStore {}
    // SAFETY: Workers run in a single-threaded isolate; this wrapper is not shared across host threads.
    unsafe impl Sync for CloudflareR2ObjectStore {}

    #[async_trait(?Send)]
    impl ObjectStore for CloudflareR2ObjectStore {
        async fn put(&self, key: &str, value: Bytes) -> StoreResult<()> {
            self.bucket
                .put(key, value.to_vec())
                .execute()
                .await
                .map_err(|err| StoreError::Other(err.to_string()))?;
            Ok(())
        }

        async fn get(&self, key: &str) -> StoreResult<Bytes> {
            let maybe = self
                .bucket
                .get(key)
                .execute()
                .await
                .map_err(|err| StoreError::Other(err.to_string()))?;
            let object = maybe.ok_or_else(|| StoreError::NotFound(key.to_string()))?;
            let bytes = object
                .body()
                .ok_or_else(|| StoreError::Other("missing r2 object body".to_string()))?
                .bytes()
                .await
                .map_err(|err| StoreError::Other(err.to_string()))?;
            Ok(Bytes::from(bytes))
        }

        async fn delete(&self, key: &str) -> StoreResult<()> {
            self.bucket
                .delete(key)
                .await
                .map_err(|err| StoreError::Other(err.to_string()))?;
            Ok(())
        }

        async fn copy(&self, src: &str, dst: &str) -> StoreResult<()> {
            let payload = self.get(src).await?;
            self.put(dst, payload).await
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn runtime_target_mentions_workers() {
        assert!(super::runtime_target().contains("cloudflare-workers"));
    }
}
