/// Custom error type for database operations.
#[derive(Debug)]
pub enum DbError {
    /// The key already exists in the database (duplicate create).
    KeyAlreadyExists(String),
    /// The key was not found in the database.
    KeyNotFound(String),
    /// An error occurred while encoding/decoding protobuf data.
    SerializationError(String),
    /// An error propagated from the underlying RocksDB engine.
    RocksDb(rocksdb::Error),
}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbError::KeyAlreadyExists(key) => write!(f, "key '{}' already exists", key),
            DbError::KeyNotFound(key) => write!(f, "key '{}' not found", key),
            DbError::SerializationError(msg) => write!(f, "serialization error: {}", msg),
            DbError::RocksDb(e) => write!(f, "rocksdb error: {}", e),
        }
    }
}

impl std::error::Error for DbError {}

impl From<rocksdb::Error> for DbError {
    fn from(e: rocksdb::Error) -> Self {
        DbError::RocksDb(e)
    }
}