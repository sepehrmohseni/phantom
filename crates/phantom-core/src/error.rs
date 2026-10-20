use thiserror::Error;
use uuid::Uuid;

pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type PhantomResult<T> = Result<T, PhantomError>;

#[derive(Debug, Error)]
pub enum PhantomError {
    #[error("storage engine failure: {0}")]
    Storage(BoxError),
    #[error("payload codec failure: {0}")]
    Codec(BoxError),
    #[error("traffic session {0} does not exist")]
    SessionNotFound(Uuid),
    #[error("invalid target: {0}")]
    InvalidTarget(String),
    #[error("malformed http message: {0}")]
    InvalidMessage(String),
    #[error("invalid ignore pattern `{0}`")]
    InvalidPattern(String),
    #[error("payload exceeds the {limit} byte capture limit")]
    PayloadTooLarge { limit: usize },
    #[error("upstream `{target}` failed: {reason}")]
    Upstream { target: String, reason: String },
    #[error("i/o failure: {0}")]
    Io(#[from] std::io::Error),
    #[error("internal failure: {0}")]
    Internal(String),
}

impl PhantomError {
    pub fn storage(source: impl Into<BoxError>) -> Self {
        Self::Storage(source.into())
    }

    pub fn codec(source: impl Into<BoxError>) -> Self {
        Self::Codec(source.into())
    }

    pub fn status_code(&self) -> u16 {
        match self {
            Self::SessionNotFound(_) => 404,
            Self::InvalidTarget(_) | Self::InvalidMessage(_) | Self::InvalidPattern(_) => 400,
            Self::PayloadTooLarge { .. } => 413,
            Self::Upstream { .. } => 502,
            Self::Storage(_) | Self::Codec(_) | Self::Io(_) | Self::Internal(_) => 500,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Self::Storage(_) => "storage_failure",
            Self::Codec(_) => "codec_failure",
            Self::SessionNotFound(_) => "session_not_found",
            Self::InvalidTarget(_) => "invalid_target",
            Self::InvalidMessage(_) => "invalid_message",
            Self::InvalidPattern(_) => "invalid_pattern",
            Self::PayloadTooLarge { .. } => "payload_too_large",
            Self::Upstream { .. } => "upstream_failure",
            Self::Io(_) => "io_failure",
            Self::Internal(_) => "internal_failure",
        }
    }
}
