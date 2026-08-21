use rocksdb::DB;
use std::path::PathBuf;

use crate::errors::rocksdb_error::DbError;


// Database connection helpers

/// Returns the path to the persistent RocksDB data directory.
/// Creates the directory if it does not exist.
fn db_path() -> PathBuf {
    let path = PathBuf::from("./gridrock_data");

    //check if directory exists: if not exist create dir at that location
    if !path.exists() {
        std::fs::create_dir_all(&path).expect("Failed to create DB directory");
    }
    path
}

/// Opens a RocksDB connection at a persistent path so data survives across calls.
pub fn get_db_connection() -> Result<DB, rocksdb::Error> {
    let path = db_path();
    let db = DB::open_default(path)?;
    Ok(db)
}


// CRUD operations

/// Creates a new entry in the database.
/// Stores the raw `value` bytes under the given `key`.
/// Returns an error if the key already exists.
pub fn db_create<'a>(db: &'a DB, key: &'a str, value: &[u8]) -> Result<&'a str, DbError<'a>> {
    let key_bytes = key.as_bytes();

    // Guard: reject duplicate keys
    if db.get(key_bytes)?.is_some() {
        return Err(DbError::KeyAlreadyExists(key));
    }

    db.put(key_bytes, value)?;
    Ok(key)
}


/// Reads an entry from the database by its key.
/// Returns the raw bytes if found, or a `KeyNotFound` error.
pub fn db_read<'a>(db: &'a DB, key: &'a str) -> Result<Vec<u8>, DbError<'a>> {
    let key_bytes = key.as_bytes();

    match db.get(key_bytes)? {
        Some(raw_bytes) => Ok(raw_bytes.to_vec()),
        None => Err(DbError::KeyNotFound(key)),
    }
}


/// Updates an existing entry with new raw bytes.
/// Returns an error if the key does not exist.
pub fn db_update<'a>(db: &'a DB, key: &'a str, value: &[u8]) -> Result<&'a str, DbError<'a>> {
    let key_bytes = key.as_bytes();

    // Guard: reject updating non-existent keys
    if db.get(key_bytes)?.is_none() {
        return Err(DbError::KeyNotFound(key));
    }

    db.put(key_bytes, value)?;
    Ok(key)
}


/// Deletes an entry from the database by its key.
/// Returns an error if the key does not exist.
pub fn db_delete<'a>(db: &DB, key: &'a str) -> Result<&'a str, DbError<'a>> {
    let key_bytes = key.as_bytes();

    // Guard: reject deleting non-existent keys
    if db.get(key_bytes)?.is_none() {
        return Err(DbError::KeyNotFound(key));
    }

    db.delete(key_bytes)?;
    Ok(key)
}

/// Checks whether a key exists in the database without reading the value.
pub fn db_exists<'a>(db: &DB, key: &str) -> Result<bool, DbError<'a>> {
    let key_bytes = key.as_bytes();
    Ok(db.get(key_bytes)?.is_some())
}


// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Helper: opens a RocksDB instance in a temporary directory.
    fn open_test_db() -> (DB, TempDir) {
        let tmp = TempDir::new().expect("failed to create temp dir");
        let db = DB::open_default(tmp.path()).expect("failed to open test db");
        (db, tmp)
    }

    #[test]
    fn test_create_and_read() {
        let (db, _tmp) = open_test_db();
        let value = b"hello gridrock";

        db_create(&db, "key_1", value).unwrap();
        let result = db_read(&db, "key_1").unwrap();

        assert_eq!(result, value);
    }

    #[test]
    fn test_create_duplicate_fails() {
        let (db, _tmp) = open_test_db();

        db_create(&db, "key_dup", b"first").unwrap();

        match db_create(&db, "key_dup", b"second") {
            Err(DbError::KeyAlreadyExists(_)) => {} // expected
            other => panic!("expected KeyAlreadyExists, got {:?}", other),
        }
    }

    #[test]
    fn test_read_missing_key() {
        let (db, _tmp) = open_test_db();

        match db_read(&db, "does_not_exist") {
            Err(DbError::KeyNotFound(_)) => {} // expected
            other => panic!("expected KeyNotFound, got {:?}", other),
        }
    }

    #[test]
    fn test_update() {
        let (db, _tmp) = open_test_db();

        db_create(&db, "key_upd", b"old_value").unwrap();
        db_update(&db, "key_upd", b"new_value").unwrap();

        let result = db_read(&db, "key_upd").unwrap();
        assert_eq!(result, b"new_value");
    }

    #[test]
    fn test_update_missing_key_fails() {
        let (db, _tmp) = open_test_db();

        match db_update(&db, "ghost", b"data") {
            Err(DbError::KeyNotFound(_)) => {} // expected
            other => panic!("expected KeyNotFound, got {:?}", other),
        }
    }

    #[test]
    fn test_delete() {
        let (db, _tmp) = open_test_db();

        db_create(&db, "key_del", b"data").unwrap();
        db_delete(&db, "key_del").unwrap();

        assert!(!db_exists(&db, "key_del").unwrap());
    }

    #[test]
    fn test_delete_missing_key_fails() {
        let (db, _tmp) = open_test_db();

        match db_delete(&db, "phantom") {
            Err(DbError::KeyNotFound(_)) => {} // expected
            other => panic!("expected KeyNotFound, got {:?}", other),
        }
    }

    #[test]
    fn test_exists() {
        let (db, _tmp) = open_test_db();

        assert!(!db_exists(&db, "key_exists").unwrap());
        db_create(&db, "key_exists", b"data").unwrap();
        assert!(db_exists(&db, "key_exists").unwrap());
    }
}

