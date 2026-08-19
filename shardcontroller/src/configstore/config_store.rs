use std::sync::{OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard};
use xxhash_rust::xxh3::xxh3_64;

//struct definition for config Val : the main struct for the config
#[derive(Debug, Clone)]
pub struct ConfigVal {
    pub tick_value: u64,
    pub address: String,
}

impl ConfigVal {
    //place a server on the ring : its position (tick value) is the hash of its address
    pub fn new(address: String) -> Self {
        let tick_value = hashing_function(address.clone());
        Self {
            tick_value,
            address,
        }
    }
}

#[derive(Debug)]
pub struct Config {
    config: Vec<ConfigVal>,
}


 impl Config {
    // 1. Create new config and SORT it immediately
    pub fn new(mut config: Vec<ConfigVal>) -> Self {
        config.sort_by_key(|val| val.tick_value);
        Self { config }
    }
    
    

    //build the whole config directly from server addresses :
    //every address becomes a ConfigVal hashed onto the ring, then sorted
    pub fn from_addresses(addresses: Vec<String>) -> Self {
        let config: Vec<ConfigVal> = addresses.into_iter().map(ConfigVal::new).collect();  //used fp 
        Config::new(config)
    }

    // 2. Update config by extending and re-sorting
    pub fn update_config(&mut self, values: Vec<ConfigVal>) {
        self.config.extend(values);
        self.config.sort_by_key(|val| val.tick_value);
    }

    // 3. Add a single entry using BINARY SEARCH to maintain sorted order
    pub fn add_entry(&mut self, entry: ConfigVal) {
        // partition_point finds the exact insertion index to keep the array sorted
        let idx = self
            .config
            .partition_point(|val| val.tick_value < entry.tick_value);
        self.config.insert(idx, entry);
    }

    // 4. Remove an entry by address. Returns true if an entry was removed.
    pub fn remove_entry(&mut self, address: &str) -> bool {
        let before_len = self.config.len();
        self.config.retain(|val| val.address != address);
        self.config.len() < before_len
    }

    // 5. Find nearest server using BINARY SEARCH
    pub fn find_nearest(&self, key_tick_value: u64) -> Option<&str> {
        if self.config.is_empty() {
            return None;
        }

        // Binary search: finds the FIRST server where tick_value >= key_tick_value
        let idx = self
            .config
            .partition_point(|val| val.tick_value < key_tick_value);

        // Wrap around the ring if the key is larger than all server ticks
        let actual_idx = if idx == self.config.len() { 0 } else { idx };

        // Return a reference to avoid unnecessary String cloning
        Some(&self.config[actual_idx].address)
    }

    // 6. Get a snapshot of all entries (cloned) — used by gRPC responses
    pub fn get_entries(&self) -> Vec<ConfigVal> {
        self.config.clone()
    }

    // 7. Get unique server addresses — used by GetServers RPC
    pub fn get_unique_addresses(&self) -> Vec<String> {
        let mut addrs: Vec<String> = self.config.iter().map(|v| v.address.clone()).collect();
        addrs.sort();
        addrs.dedup();
        addrs
    }
}

///has to add error handling on this function
pub fn hashing_function(value: String) -> u64 {
    //take the value and convert to give the hash of that value
    let hashed_value = xxh3_64(value.as_bytes());
    hashed_value
}


// -----------------------------------------
// Global in-memory store — call `init_config_store` once from main,
// then use `read_config_store` / `write_config_store` anywhere.
//
// Uses OnceLock<RwLock<Config>> so:
//   - OnceLock ensures one-time initialization
//   - RwLock allows concurrent reads + exclusive writes (add/remove server)
// -----------------------------------------

static CONFIG_STORE: OnceLock<RwLock<Config>> = OnceLock::new();

///store the config globally. call this once at startup.
///panics if called more than once.
pub fn init_config_store(addresses: Vec<String>) {
    let config = Config::from_addresses(addresses);
    CONFIG_STORE
        .set(RwLock::new(config))
        .expect("config store already initialized");
}

///acquire a read lock on the global config.
///returns None if `init_config_store` was never called.
///panics if the RwLock is poisoned.
pub fn read_config_store() -> Option<RwLockReadGuard<'static, Config>> {
    CONFIG_STORE.get().map(|lock| lock.read().expect("config store lock poisoned"))
}

///acquire a write lock on the global config.
///returns None if `init_config_store` was never called.
///panics if the RwLock is poisoned.
pub fn write_config_store() -> Option<RwLockWriteGuard<'static, Config>> {
    CONFIG_STORE.get().map(|lock| lock.write().expect("config store lock poisoned"))
}
