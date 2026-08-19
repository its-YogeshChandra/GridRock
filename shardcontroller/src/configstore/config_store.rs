use std::sync::OnceLock;
use xxhash_rust::xxh3::xxh3_64;

//struct definition for config Val : the main struct for the config
#[derive(Debug)]
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
        let config: Vec<ConfigVal> = addresses.into_iter().map(ConfigVal::new).collect();
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

    // 4. Find nearest server using BINARY SEARCH
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
}

///has to add error handling on this function
pub fn hashing_function(value: String) -> u64 {
    //take the value and convert to give the hash of that value
    let hashed_value = xxh3_64(value.as_bytes());
    hashed_value
}


// -----------------------------------------
// Global in-memory store — call `init_config_store` once from main,
// then `get_config_store` returns a &'static Config reference anywhere.
// -----------------------------------------

//in-memory store : set once from main, keeps the config state alive for the
//whole lifetime of the program. Every read hands out a 'static reference to
//the SAME config, so no cloning is needed anywhere.
static CONFIG_STORE: OnceLock<Config> = OnceLock::new();

///store the config globally. call this once at startup.
///panics if called more than once.
pub fn init_config_store(addresses: Vec<String>) {
    let config = Config::from_addresses(addresses);
    CONFIG_STORE
        .set(config)
        .expect("config store already initialized");
}

///retrieve the globally stored config as a static reference.
///returns None if `init_config_store` was never called.
pub fn get_config_store() -> Option<&'static Config> {
    CONFIG_STORE.get()
}
