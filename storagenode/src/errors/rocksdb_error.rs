/// Custom error type for database operations.
#[derive(Debug)]
pub enum DbError<'a> {
    /// The key already exists in the database (duplicate create).
    KeyAlreadyExists(&'a str),
    /// The key was not found in the database.
    KeyNotFound(&'a str),
    /// An error occurred while encoding/decoding protobuf data.
    SerializationError(&'a str),
    /// An error propagated from the underlying RocksDB engine.
    RocksDb(rocksdb::Error),
}

impl<'a> std::fmt::Display for DbError<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbError::KeyAlreadyExists(key) => write!(f, "key '{}' already exists", key),
            DbError::KeyNotFound(key) => write!(f, "key '{}' not found", key),
            DbError::SerializationError(msg) => write!(f, "serialization error: {}", msg),
            DbError::RocksDb(e) => write!(f, "rocksdb error: {}", e),
        }
    }
}

impl<'a> std::error::Error for DbError<'a> {}

impl<'a> From<rocksdb::Error> for DbError<'a> {
    fn from(e: rocksdb::Error) -> Self {
        DbError::RocksDb(e)
    }
}