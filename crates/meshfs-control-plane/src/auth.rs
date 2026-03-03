use std::sync::Arc;

use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::http::HeaderMap;
use meshfs_control_plane_core::auth as core_auth;
use meshfs_control_plane_core::error::AppError;
use meshfs_types::PlanTier;

use crate::error::HttpError;
use meshfs_control_plane_core::state::AppState;

#[derive(Debug, Clone)]
pub struct AuthContext {
    pub user_id: String,
    pub tenant_id: String,
    pub plan_tier: PlanTier,
}

impl AuthContext {
    pub fn default_dev() -> Self {
        Self {
            user_id: "dev-user".to_string(),
            tenant_id: "dev-tenant".to_string(),
            plan_tier: PlanTier::Free,
        }
    }
}

impl FromRequestParts<Arc<AppState>> for AuthContext {
    type Rejection = HttpError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        parse_auth_context_from_headers(&parts.headers, &state.jwt_secret).map_err(HttpError::from)
    }
}

pub fn parse_auth_context_from_headers(
    headers: &HeaderMap,
    jwt_secret: &str,
) -> Result<AuthContext, AppError> {
    let auth = core_auth::parse_auth_context_from_headers(headers, jwt_secret)?;
    Ok(AuthContext {
        user_id: auth.user_id,
        tenant_id: auth.tenant_id,
        plan_tier: auth.plan_tier,
    })
}

pub fn issue_access_token(
    user_id: &str,
    tenant_id: &str,
    plan_tier: PlanTier,
    secret: &str,
    ttl_seconds: i64,
) -> Result<String, AppError> {
    core_auth::issue_access_token(user_id, tenant_id, plan_tier, secret, ttl_seconds)
}
