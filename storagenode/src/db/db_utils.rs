use rocksdb::DB;
use std::path::PathBuf;

/// Returns a path to the persistent RocksDB data directory.
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
