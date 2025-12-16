use crate::commands::CommandError;
use redis_protocol::resp2::types::Frame;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, AppError>;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Database error: {0}")]
    Database(String),

    #[error("Command error: {0}")]
    Command(#[from] CommandError), // Use CommandError directly

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Encoding error: {0}")]
    Encoding(String),

    #[error("utf8 error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),

    #[error("Invalid command")]
    InvalidCommand,

    #[error("Other: {0}")]
    Other(String),
}

impl From<serde_json::Error> for AppError {
    fn from(err: serde_json::Error) -> Self {
        AppError::Serialization(err.to_string())
    }
}

// Convert AppError to Redis Frame for client response
impl From<AppError> for Frame {
    fn from(err: AppError) -> Self {
        match err {
            AppError::Command(cmd_err) => Frame::Error(cmd_err.to_string().into()),
            AppError::Io(_) => Frame::Error("ERR internal io error".to_string().into()),
            _ => Frame::Error(format!("ERR {}", err).into()),
        }
    }
}
