use prost::Message;
use rocksdb::DB;
use std::path::PathBuf;

use crate::storage_proto::CreateRequest;
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
/// The `CreateRequest` is serialized to protobuf bytes and stored under
/// the `unique_id` key. Returns an error if the key already exists.
pub fn db_create(db: &DB, request: &CreateRequest) -> Result<(), DbError> {
    let key = request.unique_id.as_bytes();

    // Guard: reject duplicate keys
    if db.get(key)?.is_some() {
        return Err(DbError::KeyAlreadyExists(request.unique_id.clone()));
    }

    let value = request.encode_to_vec();
    db.put(key, value)?;
    Ok(())
}


/// Reads an entry from the database by its `unique_id`.
/// Returns the deserialized `CreateRequest` if found, or a `KeyNotFound` error.
pub fn db_read(db: &DB, unique_id: &str) -> Result<CreateRequest, DbError> {
    let key = unique_id.as_bytes();

    match db.get(key)? {
        Some(raw_bytes) => {
            let entry = CreateRequest::decode(raw_bytes.as_slice())
                .map_err(|e| DbError::SerializationError(e.to_string()))?;
            Ok(entry)
        }
        None => Err(DbError::KeyNotFound(unique_id.to_string())),
    }
}


/// Updates an existing entry's balance.
/// Fetches the current record, applies the new balance, re-serializes,
/// and writes back. Returns an error if the key does not exist.
pub fn db_update(db: &DB, unique_id: &str, new_balance: u64) -> Result<(), DbError> {
    let key = unique_id.as_bytes();

    // Fetch existing record
    let raw_bytes = db
        .get(key)?
        .ok_or_else(|| DbError::KeyNotFound(unique_id.to_string()))?;

    let mut entry = CreateRequest::decode(raw_bytes.as_slice())
        .map_err(|e| DbError::SerializationError(e.to_string()))?;

    // Apply the update
    entry.balance = new_balance;

    // Write the modified record back
    let value = entry.encode_to_vec();
    db.put(key, value)?;
    Ok(())
}


/// Deletes an entry from the database by its `unique_id`.
/// Returns an error if the key does not exist.
pub fn db_delete(db: &DB, unique_id: &str) -> Result<(), DbError> {
    let key = unique_id.as_bytes();

    // Guard: reject deleting non-existent keys
    if db.get(key)?.is_none() {
        return Err(DbError::KeyNotFound(unique_id.to_string()));
    }

    db.delete(key)?;
    Ok(())
}

/// Checks whether a key exists in the database without deserializing the value.
pub fn db_exists(db: &DB, unique_id: &str) -> Result<bool, DbError> {
    let key = unique_id.as_bytes();
    Ok(db.get(key)?.is_some())
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

    /// Helper: builds a sample `CreateRequest`.
    fn sample_request(id: &str, balance: u64) -> CreateRequest {
        CreateRequest {
            unique_id: id.to_string(),
            balance,
            executable: false,
            rent_epoch: 0,
            data_hash: String::new(),
            last_updated_slot: 0,
        }
    }

    #[test]
    fn test_create_and_read() {
        let (db, _tmp) = open_test_db();
        let req = sample_request("acc_1", 1000);

        db_create(&db, &req).unwrap();
        let entry = db_read(&db, "acc_1").unwrap();

        assert_eq!(entry.unique_id, "acc_1");
        assert_eq!(entry.balance, 1000);
    }

    #[test]
    fn test_create_duplicate_fails() {
        let (db, _tmp) = open_test_db();
        let req = sample_request("acc_dup", 500);

        db_create(&db, &req).unwrap();

        match db_create(&db, &req) {
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
        let req = sample_request("acc_upd", 100);

        db_create(&db, &req).unwrap();
        db_update(&db, "acc_upd", 9999).unwrap();

        let entry = db_read(&db, "acc_upd").unwrap();
        assert_eq!(entry.balance, 9999);
    }

    #[test]
    fn test_update_missing_key_fails() {
        let (db, _tmp) = open_test_db();

        match db_update(&db, "ghost", 42) {
            Err(DbError::KeyNotFound(_)) => {} // expected
            other => panic!("expected KeyNotFound, got {:?}", other),
        }
    }

    #[test]
    fn test_delete() {
        let (db, _tmp) = open_test_db();
        let req = sample_request("acc_del", 200);

        db_create(&db, &req).unwrap();
        db_delete(&db, "acc_del").unwrap();

        assert!(!db_exists(&db, "acc_del").unwrap());
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
        let req = sample_request("acc_exists", 300);

        assert!(!db_exists(&db, "acc_exists").unwrap());
        db_create(&db, &req).unwrap();
        assert!(db_exists(&db, "acc_exists").unwrap());
    }
}
