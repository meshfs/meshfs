use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::IntoResponse;
use axum::Json;
use meshfs_types::{
    ApplyRetentionRequest, DeleteRequest, DeviceActivateRequest, DevicePollRequest,
    DevicePollResponse, DeviceStartResponse, ListDirectoryResponse, LogoutRequest, MkdirRequest,
    RefreshTokenRequest, RefreshTokenResponse, RenameRequest, UploadCommitRequest,
    UploadCommitResponse, UploadInitRequest, UploadInitResponse, UploadPartRequest,
};
use serde::Deserialize;
use serde_json::json;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

use crate::auth::{issue_access_token, parse_auth_context_from_headers, AuthContext};
use crate::error::AppResult;
use meshfs_control_plane_core::state::{AppState, AuthIdentity};

fn to_identity(auth: AuthContext) -> AuthIdentity {
    AuthIdentity {
        user_id: auth.user_id,
        tenant_id: auth.tenant_id,
    }
}

pub async fn auth_device_start(
    State(state): State<Arc<AppState>>,
) -> AppResult<Json<DeviceStartResponse>> {
    let (device_code, user_code, interval_seconds, expires_in_seconds) =
        state.start_device_session().await;

    Ok(Json(DeviceStartResponse {
        device_code,
        user_code,
        verification_uri: "http://localhost:8787/auth/device/activate".to_string(),
        interval_seconds,
        expires_in_seconds,
    }))
}

pub async fn auth_device_activate(
    State(state): State<Arc<AppState>>,
    Json(req): Json<DeviceActivateRequest>,
) -> AppResult<StatusCode> {
    state.activate_device_session(req).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn auth_device_poll(
    State(state): State<Arc<AppState>>,
    Json(req): Json<DevicePollRequest>,
) -> AppResult<impl IntoResponse> {
    if let Some((user_id, tenant_id, plan_tier)) =
        state.poll_device_session(&req.device_code).await?
    {
        let access_token = issue_access_token(
            &user_id,
            &tenant_id,
            plan_tier.clone(),
            &state.jwt_secret,
            state.token_ttl_seconds,
        )?;
        let refresh_token = state
            .issue_refresh_token(&user_id, &tenant_id, plan_tier)
            .await?;
        let body = DevicePollResponse {
            access_token,
            refresh_token,
            expires_in_seconds: state.token_ttl_seconds as u64,
            token_type: "Bearer".to_string(),
        };
        Ok((StatusCode::OK, Json(json!(body))))
    } else {
        Ok((
            StatusCode::ACCEPTED,
            Json(json!({"status": "authorization_pending"})),
        ))
    }
}

pub async fn auth_refresh(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RefreshTokenRequest>,
) -> AppResult<Json<RefreshTokenResponse>> {
    let (user_id, tenant_id, plan_tier, rotated_refresh) =
        state.exchange_refresh_token(&req.refresh_token).await?;
    let access_token = issue_access_token(
        &user_id,
        &tenant_id,
        plan_tier,
        &state.jwt_secret,
        state.token_ttl_seconds,
    )?;

    Ok(Json(RefreshTokenResponse {
        access_token,
        refresh_token: rotated_refresh,
        expires_in_seconds: state.token_ttl_seconds as u64,
        token_type: "Bearer".to_string(),
    }))
}

pub async fn auth_logout(
    State(state): State<Arc<AppState>>,
    auth: AuthContext,
    Json(req): Json<LogoutRequest>,
) -> AppResult<StatusCode> {
    state
        .revoke_refresh_token(to_identity(auth), &req.refresh_token)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn files_upload_init(
    State(state): State<Arc<AppState>>,
    auth: AuthContext,
    Json(req): Json<UploadInitRequest>,
) -> AppResult<Json<UploadInitResponse>> {
    let res = state.init_upload(to_identity(auth), req).await?;
    Ok(Json(res))
}

pub async fn files_upload_part(
    State(state): State<Arc<AppState>>,
    auth: AuthContext,
    Json(req): Json<UploadPartRequest>,
) -> AppResult<StatusCode> {
    state.put_upload_part(to_identity(auth), req).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn files_upload_commit(
    State(state): State<Arc<AppState>>,
    auth: AuthContext,
    headers: HeaderMap,
    Json(req): Json<UploadCommitRequest>,
) -> AppResult<Json<UploadCommitResponse>> {
    let idempotency_key = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let out = state
        .commit_upload(to_identity(auth), &req.upload_id, idempotency_key)
        .await?;

    Ok(Json(out))
}

pub async fn files_mkdir(
    State(state): State<Arc<AppState>>,
    auth: AuthContext,
    Json(req): Json<MkdirRequest>,
) -> AppResult<impl IntoResponse> {
    let out = state.mkdir(to_identity(auth), req).await?;
    Ok((StatusCode::CREATED, Json(out)))
}

pub async fn files_rename(
    State(state): State<Arc<AppState>>,
    auth: AuthContext,
    Json(req): Json<RenameRequest>,
) -> AppResult<StatusCode> {
    state.rename(to_identity(auth), req).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn files_delete(
    State(state): State<Arc<AppState>>,
    auth: AuthContext,
    Json(req): Json<DeleteRequest>,
) -> AppResult<StatusCode> {
    state.delete(to_identity(auth), req).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
pub struct PathQuery {
    pub path: String,
}

pub async fn files_meta(
    State(state): State<Arc<AppState>>,
    auth: AuthContext,
    Query(query): Query<PathQuery>,
) -> AppResult<impl IntoResponse> {
    let out = state
        .get_meta_by_path(to_identity(auth), &query.path)
        .await?;
    Ok((StatusCode::OK, Json(out)))
}

pub async fn files_list(
    State(state): State<Arc<AppState>>,
    auth: AuthContext,
    Query(query): Query<PathQuery>,
) -> AppResult<Json<ListDirectoryResponse>> {
    let out = state.list_directory(to_identity(auth), &query.path).await?;
    Ok(Json(out))
}

pub async fn files_download(
    State(state): State<Arc<AppState>>,
    auth: AuthContext,
    Query(query): Query<PathQuery>,
) -> AppResult<impl IntoResponse> {
    let bytes = state
        .get_bytes_by_path(to_identity(auth), &query.path)
        .await?;

    Ok((
        [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
        bytes,
    ))
}

pub async fn versions_list(
    State(state): State<Arc<AppState>>,
    auth: AuthContext,
    Path(node_id): Path<String>,
) -> AppResult<impl IntoResponse> {
    let out = state.list_versions(to_identity(auth), &node_id).await?;
    Ok((StatusCode::OK, Json(out)))
}

pub async fn versions_restore(
    State(state): State<Arc<AppState>>,
    auth: AuthContext,
    Path((node_id, version_id)): Path<(String, String)>,
) -> AppResult<impl IntoResponse> {
    let out = state
        .restore_version(to_identity(auth), &node_id, &version_id)
        .await?;
    Ok((StatusCode::OK, Json(out)))
}

#[derive(Deserialize)]
pub struct SyncPullQuery {
    pub cursor: Option<u64>,
}

pub async fn sync_pull(
    State(state): State<Arc<AppState>>,
    auth: AuthContext,
    Query(query): Query<SyncPullQuery>,
) -> AppResult<impl IntoResponse> {
    let out = state
        .sync_pull(to_identity(auth), query.cursor.unwrap_or(0))
        .await?;
    Ok((StatusCode::OK, Json(out)))
}

pub async fn sync_stream(
    State(state): State<Arc<AppState>>,
    auth: AuthContext,
) -> AppResult<Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>>> {
    let tenant_id = auth.tenant_id;
    let rx = state.events_tx.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(move |item| match item {
        Ok(event) if event.tenant_id == tenant_id => {
            let json = serde_json::to_string(&event).ok()?;
            Some(Ok(Event::default().event("change").data(json)))
        }
        _ => None,
    });

    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

pub async fn sync_ws(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<SyncPullQuery>,
) -> Result<impl IntoResponse, StatusCode> {
    let auth = parse_auth_context_from_headers(&headers, &state.jwt_secret)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;

    Ok(ws.on_upgrade(move |socket| async move {
        handle_sync_ws(socket, state, auth, query.cursor.unwrap_or(0)).await;
    }))
}

async fn handle_sync_ws(
    mut socket: WebSocket,
    state: Arc<AppState>,
    auth: AuthContext,
    mut cursor: u64,
) {
    let identity = to_identity(auth.clone());

    if let Ok(backlog) = state.sync_pull(identity.clone(), cursor).await {
        for event in backlog.events {
            if send_change_event(&mut socket, &event).await.is_err() {
                let _ = socket.send(Message::Close(None)).await;
                return;
            }
            cursor = event.cursor;
        }
    }

    let mut rx = state.events_tx.subscribe();
    let mut ping_interval = tokio::time::interval(Duration::from_secs(25));
    loop {
        tokio::select! {
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(Message::Ping(payload))) => {
                        if socket.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Text(_))) | Some(Ok(Message::Binary(_))) | Some(Ok(Message::Pong(_))) => {}
                    Some(Err(_)) => break,
                }
            }
            event_result = rx.recv() => {
                match event_result {
                    Ok(event) => {
                        if event.tenant_id == identity.tenant_id && event.cursor > cursor {
                            if send_change_event(&mut socket, &event).await.is_err() {
                                break;
                            }
                            cursor = event.cursor;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        if let Ok(backlog) = state.sync_pull(identity.clone(), cursor).await {
                            for event in backlog.events {
                                if send_change_event(&mut socket, &event).await.is_err() {
                                    let _ = socket.send(Message::Close(None)).await;
                                    return;
                                }
                                cursor = event.cursor;
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
            _ = ping_interval.tick() => {
                if socket.send(Message::Ping(Vec::new().into())).await.is_err() {
                    break;
                }
            }
        }
    }

    let _ = socket.send(Message::Close(None)).await;
}

async fn send_change_event(
    socket: &mut WebSocket,
    event: &meshfs_types::ChangeEvent,
) -> Result<(), ()> {
    let payload = serde_json::to_string(event).map_err(|_| ())?;
    socket
        .send(Message::Text(payload.into()))
        .await
        .map_err(|_| ())
}

pub async fn current_plan(
    State(state): State<Arc<AppState>>,
    auth: AuthContext,
) -> AppResult<impl IntoResponse> {
    let out = state.current_plan(to_identity(auth)).await?;
    Ok((StatusCode::OK, Json(out)))
}

pub async fn retention_policy(
    State(state): State<Arc<AppState>>,
    auth: AuthContext,
) -> AppResult<impl IntoResponse> {
    let out = state.retention_policy(to_identity(auth)).await?;
    Ok((StatusCode::OK, Json(out)))
}

pub async fn retention_apply(
    State(state): State<Arc<AppState>>,
    auth: AuthContext,
    Json(req): Json<ApplyRetentionRequest>,
) -> AppResult<StatusCode> {
    state.apply_retention_policy(to_identity(auth), req).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
pub struct AuditQuery {
    pub limit: Option<usize>,
}

pub async fn audit_recent(
    State(state): State<Arc<AppState>>,
    auth: AuthContext,
    Query(query): Query<AuditQuery>,
) -> AppResult<impl IntoResponse> {
    let out = state
        .list_audit_events(to_identity(auth), query.limit.unwrap_or(100))
        .await?;
    Ok((StatusCode::OK, Json(out)))
}
