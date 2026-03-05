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

#[cfg(test)]
mod tests {
    use axum::http::header::AUTHORIZATION;
    use axum::http::HeaderMap;
    use meshfs_control_plane_core::auth::decode_access_token;
    use meshfs_types::PlanTier;

    use super::{issue_access_token, parse_auth_context_from_headers, AuthContext};

    #[test]
    fn default_dev_context_is_stable() {
        let auth = AuthContext::default_dev();
        assert_eq!(auth.user_id, "dev-user");
        assert_eq!(auth.tenant_id, "dev-tenant");
        assert_eq!(auth.plan_tier, PlanTier::Free);
    }

    #[test]
    fn parse_auth_context_from_headers_accepts_valid_bearer_token() {
        let token = issue_access_token("user-1", "tenant-1", PlanTier::Team, "secret", 120)
            .expect("issue access token");
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            format!("Bearer {token}")
                .parse()
                .expect("authorization header"),
        );

        let auth = parse_auth_context_from_headers(&headers, "secret").expect("parse auth context");
        assert_eq!(auth.user_id, "user-1");
        assert_eq!(auth.tenant_id, "tenant-1");
        assert_eq!(auth.plan_tier, PlanTier::Team);
    }

    #[test]
    fn parse_auth_context_from_headers_rejects_missing_or_non_bearer_header() {
        let missing = HeaderMap::new();
        assert!(parse_auth_context_from_headers(&missing, "secret").is_err());

        let mut non_bearer = HeaderMap::new();
        non_bearer.insert(AUTHORIZATION, "Basic deadbeef".parse().expect("header"));
        assert!(parse_auth_context_from_headers(&non_bearer, "secret").is_err());
    }

    #[test]
    fn issue_access_token_wraps_core_jwt_issuer() {
        let token = issue_access_token("user-2", "tenant-2", PlanTier::Pro, "secret", 120)
            .expect("issue access token");
        let claims = decode_access_token(&token, "secret").expect("decode access token");
        assert_eq!(claims.sub, "user-2");
        assert_eq!(claims.tenant_id, "tenant-2");
        assert_eq!(claims.plan_tier, PlanTier::Pro);
    }
}
