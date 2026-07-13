use actix_web::{HttpResponse, ResponseError, http::StatusCode};
use serde_json::json;
use thiserror::Error;

/// Central application error type.
///
/// Every layer (cache, storage, bootstrap) speaks in these terms and never
/// references HTTP directly. The mapping from a variant to an HTTP status code
/// lives in a single place: the `ResponseError` implementation below.
#[derive(Debug, Error)]
pub enum AppError {
    #[error("Client not found")]
    ClientNotFound,

    #[error("Insufficient funds")]
    InsufficientFunds,

    #[error("A client with that document already exists")]
    DocumentInUse,

    #[error("Client creation already in progress")]
    DocumentInFlight,

    #[error("Internal lock was poisoned")]
    LockPoisoned,

    #[error("Storage failure: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization failure: {0}")]
    Serde(#[from] serde_json::Error),

    /// Startup-time failures (missing control files, corrupted storage, nonce
    /// mismatches, etc.)
    #[error("Bootstrap failure: {0}")]
    Bootstrap(String),
}

impl ResponseError for AppError {
    fn status_code(&self) -> StatusCode {
        match self {
            AppError::ClientNotFound => StatusCode::NOT_FOUND,
            AppError::InsufficientFunds => StatusCode::UNPROCESSABLE_ENTITY,
            AppError::DocumentInUse | AppError::DocumentInFlight => StatusCode::CONFLICT,
            AppError::LockPoisoned
            | AppError::Io(_)
            | AppError::Serde(_)
            | AppError::Bootstrap(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn error_response(&self) -> HttpResponse {
        HttpResponse::build(self.status_code()).json(json!({
            "error": self.to_string(),
        }))
    }
}
