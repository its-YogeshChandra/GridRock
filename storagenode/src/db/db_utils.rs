use rocksdb::{DB, Options};
use tempfile;

pub fn getDBconnection()-> Result<DB, rocksdb::Error>{
    let tempdir = tempfile::Builder::new().prefix("").tempdir().expect("failed to create tmeporary path");
    let path = tempdir.path();
    let db = DB::open_default(path)?;
    Ok(db)
}