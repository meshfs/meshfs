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
