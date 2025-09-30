pub type Result<T> = std::result::Result<T, CommandError>;

#[derive(Debug, PartialEq, thiserror::Error)]
pub enum CommandError {
    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,

    #[error("ERR value is not an integer or out of range")]
    InvalidInteger,

    #[error("ERR value is not a valid float")]
    InvalidFloat,

    #[error("ERR syntax error")]
    SyntaxError,

    #[error("ERR command not found")]
    CommandNotFound,

    #[error("ERR wrong number of arguments for command")]
    WrongNumberOfArguments,

    #[error("ERR unknown command")]
    UnknownCommand,

    #[error("ERR invalid command format")]
    InvalidCommandFormat,

    #[error("ERR key not found")]
    KeyNotFound,

    #[error("ERR index out of range")]
    IndexOutOfRange,

    #[error("ERR invalid range")]
    InvalidRange,

    #[error("ERR operation not permitted")]
    OperationNotPermitted,

    #[error("ERR maximum number of clients reached")]
    MaxClientsReached,

    #[error("ERR command disabled")]
    CommandDisabled,

    #[error("ERR readonly mode")]
    ReadOnly,

    #[error("ERR out of memory")]
    OutOfMemory,

    #[error("ERR internal error")]
    InternalError,

    // List-specific errors
    #[error("ERR only BEFORE|AFTER allowed for LINSERT")]
    InvalidInsertDirection,

    #[error("ERR pivot not found in list")]
    PivotNotFound,

    // Hash-specific errors
    #[error("ERR hash field not found")]
    FieldNotFound,

    // Set-specific errors
    #[error("ERR member not found in set")]
    MemberNotFound,

    // Sorted set-specific errors
    #[error("ERR member not found in sorted set")]
    SortedSetMemberNotFound,

    // Generic error with custom message
    #[error("ERR {0}")]
    Custom(String),
}

impl CommandError {
    pub fn to_redis_error(&self) -> String {
        format!("-ERR {}\r\n", self)
    }
}
