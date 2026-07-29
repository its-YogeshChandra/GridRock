use rocksdb::{DB, Options};
use tempfile;

pub fn getDBconnection()-> Result<DB, rocksdb::Error>{
    let tempdir = tempfile::Builder::new().prefix("").tempdir().expect("failed to create tmeporary path");
    let path = tempdir.path();
    let db = DB::open_default(path)?;
    Ok(db)
}

pub struct MainVal {
    pub unique_id: String,
    pub balance: u64,
    pub executable: bool,
    pub rent_epoch: u64,
    pub data_hash: String,
    pub last_updated_slot: u64,
}    
pub struct RocksdbRequest{ 
    key: String,
    value : MainVal 
}
