use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::{Duration, Utc};
use hmac::{Hmac, Mac};
use http::HeaderMap;
use meshfs_types::PlanTier;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use crate::error::{AppError, AppResult};

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub tenant_id: String,
    pub plan_tier: PlanTier,
    pub exp: usize,
    pub iat: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JwtHeader {
    alg: String,
    typ: String,
}

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

pub fn parse_auth_context_from_headers(
    headers: &HeaderMap,
    jwt_secret: &str,
) -> AppResult<AuthContext> {
    let auth_value = headers
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or(AppError::Unauthorized)?;

    if !auth_value.starts_with("Bearer ") {
        return Err(AppError::Unauthorized);
    }

    let token = auth_value.trim_start_matches("Bearer ");
    decode_access_token(token, jwt_secret).map(|claims| AuthContext {
        user_id: claims.sub,
        tenant_id: claims.tenant_id,
        plan_tier: claims.plan_tier,
    })
}

pub fn issue_access_token(
    user_id: &str,
    tenant_id: &str,
    plan_tier: PlanTier,
    secret: &str,
    ttl_seconds: i64,
) -> AppResult<String> {
    let now = Utc::now();
    let exp = (now + Duration::seconds(ttl_seconds)).timestamp() as usize;
    let iat = now.timestamp() as usize;
    let claims = Claims {
        sub: user_id.to_string(),
        tenant_id: tenant_id.to_string(),
        plan_tier,
        exp,
        iat,
    };

    let header = JwtHeader {
        alg: "HS256".to_string(),
        typ: "JWT".to_string(),
    };

    let header_segment = encode_segment(&header)?;
    let payload_segment = encode_segment(&claims)?;
    let signing_input = format!("{header_segment}.{payload_segment}");
    let signature = sign_hs256(signing_input.as_bytes(), secret.as_bytes())?;
    let signature_segment = URL_SAFE_NO_PAD.encode(signature);

    Ok(format!("{signing_input}.{signature_segment}"))
}

pub fn decode_access_token(token: &str, secret: &str) -> AppResult<Claims> {
    let mut parts = token.split('.');
    let header_segment = parts.next().ok_or(AppError::Unauthorized)?;
    let payload_segment = parts.next().ok_or(AppError::Unauthorized)?;
    let signature_segment = parts.next().ok_or(AppError::Unauthorized)?;

    if parts.next().is_some() {
        return Err(AppError::Unauthorized);
    }

    let header: JwtHeader = decode_segment(header_segment)?;
    if header.alg != "HS256" {
        return Err(AppError::Unauthorized);
    }

    let signing_input = format!("{header_segment}.{payload_segment}");
    let got = URL_SAFE_NO_PAD
        .decode(signature_segment)
        .map_err(|_| AppError::Unauthorized)?;
    verify_hs256(signing_input.as_bytes(), secret.as_bytes(), &got)?;

    let claims: Claims = decode_segment(payload_segment)?;
    let now = Utc::now().timestamp() as usize;
    if claims.exp <= now {
        return Err(AppError::Unauthorized);
    }

    Ok(claims)
}

fn sign_hs256(data: &[u8], secret: &[u8]) -> AppResult<Vec<u8>> {
    let mut mac = HmacSha256::new_from_slice(secret)
        .map_err(|err| AppError::Internal(format!("invalid hmac key: {err}")))?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn verify_hs256(data: &[u8], secret: &[u8], signature: &[u8]) -> AppResult<()> {
    let mut mac = HmacSha256::new_from_slice(secret)
        .map_err(|err| AppError::Internal(format!("invalid hmac key: {err}")))?;
    mac.update(data);
    mac.verify_slice(signature)
        .map_err(|_| AppError::Unauthorized)
}

fn encode_segment<T: Serialize>(value: &T) -> AppResult<String> {
    let json = serde_json::to_vec(value)
        .map_err(|err| AppError::Internal(format!("serialize jwt segment failed: {err}")))?;
    Ok(URL_SAFE_NO_PAD.encode(json))
}

fn decode_segment<T: DeserializeOwned>(segment: &str) -> AppResult<T> {
    let bytes = URL_SAFE_NO_PAD
        .decode(segment)
        .map_err(|_| AppError::Unauthorized)?;
    serde_json::from_slice(&bytes).map_err(|_| AppError::Unauthorized)
}

#[cfg(test)]
mod tests {
    use super::{decode_access_token, issue_access_token};
    use meshfs_types::PlanTier;

    #[test]
    fn issue_and_decode_token_roundtrip() {
        let token =
            issue_access_token("user-a", "tenant-a", PlanTier::Free, "test-secret", 60).unwrap();
        let claims = decode_access_token(&token, "test-secret").unwrap();
        assert_eq!(claims.sub, "user-a");
        assert_eq!(claims.tenant_id, "tenant-a");
        assert_eq!(claims.plan_tier, PlanTier::Free);
    }

    #[test]
    fn expired_token_is_rejected() {
        let token =
            issue_access_token("user-a", "tenant-a", PlanTier::Free, "test-secret", -1).unwrap();
        assert!(decode_access_token(&token, "test-secret").is_err());
    }

    #[test]
    fn tampered_token_is_rejected() {
        let token =
            issue_access_token("user-a", "tenant-a", PlanTier::Free, "test-secret", 60).unwrap();
        let mut parts: Vec<String> = token.split('.').map(ToString::to_string).collect();
        parts[1].push('x');
        let tampered = parts.join(".");
        assert!(decode_access_token(&tampered, "test-secret").is_err());
    }
}
