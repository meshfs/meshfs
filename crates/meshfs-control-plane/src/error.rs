use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use meshfs_control_plane_core::error::AppError as CoreAppError;
use serde::Serialize;

#[derive(Debug)]
pub struct HttpError(pub CoreAppError);

impl From<CoreAppError> for HttpError {
    fn from(value: CoreAppError) -> Self {
        Self(value)
    }
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let status = match &self.0 {
            CoreAppError::Unauthorized => StatusCode::UNAUTHORIZED,
            CoreAppError::Forbidden => StatusCode::FORBIDDEN,
            CoreAppError::NotFound(_) => StatusCode::NOT_FOUND,
            CoreAppError::InvalidRequest(_) => StatusCode::BAD_REQUEST,
            CoreAppError::Conflict(_) => StatusCode::CONFLICT,
            CoreAppError::RateLimited(_) => StatusCode::TOO_MANY_REQUESTS,
            CoreAppError::QuotaExceeded(_) => StatusCode::PAYLOAD_TOO_LARGE,
            CoreAppError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        let body = Json(ErrorBody {
            error: self.0.to_string(),
        });

        (status, body).into_response()
    }
}

pub type AppResult<T> = Result<T, HttpError>;

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use meshfs_control_plane_core::error::AppError;
    use serde_json::Value;

    use super::HttpError;

    async fn assert_http_error(err: AppError, expected_status: StatusCode, expected_msg: &str) {
        let response = HttpError(err).into_response();
        assert_eq!(response.status(), expected_status);

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read body");
        let body_json: Value = serde_json::from_slice(&body).expect("json body");
        assert_eq!(
            body_json["error"].as_str().expect("error string"),
            expected_msg
        );
    }

    #[tokio::test]
    async fn maps_core_error_variants_to_http_status_and_body() {
        assert_http_error(
            AppError::Unauthorized,
            StatusCode::UNAUTHORIZED,
            "unauthorized",
        )
        .await;
        assert_http_error(AppError::Forbidden, StatusCode::FORBIDDEN, "forbidden").await;
        assert_http_error(
            AppError::NotFound("node".into()),
            StatusCode::NOT_FOUND,
            "not found: node",
        )
        .await;
        assert_http_error(
            AppError::InvalidRequest("bad payload".into()),
            StatusCode::BAD_REQUEST,
            "invalid request: bad payload",
        )
        .await;
        assert_http_error(
            AppError::Conflict("already exists".into()),
            StatusCode::CONFLICT,
            "conflict: already exists",
        )
        .await;
        assert_http_error(
            AppError::RateLimited("too many requests".into()),
            StatusCode::TOO_MANY_REQUESTS,
            "rate limited: too many requests",
        )
        .await;
        assert_http_error(
            AppError::QuotaExceeded("quota".into()),
            StatusCode::PAYLOAD_TOO_LARGE,
            "quota exceeded: quota",
        )
        .await;
        assert_http_error(
            AppError::Internal("db unavailable".into()),
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal: db unavailable",
        )
        .await;
    }
}
