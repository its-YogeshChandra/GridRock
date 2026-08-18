use xxhash_rust::xxh3::xxh3_64;

//struct definition for config Val : the main struct for the config
#[derive(Debug)]
struct ConfigVal {
    tick_value: u64,
    address: String,
}

pub struct Config {
    config: Vec<ConfigVal>,
}

impl Config {
    // 1. Create new config and SORT it immediately
    fn new(mut config: Vec<ConfigVal>) -> Self {
        config.sort_by_key(|val| val.tick_value);
        Self { config }
    }

    // 2. Update config by extending and re-sorting
    fn update_config(&mut self, values: Vec<ConfigVal>) {
        self.config.extend(values);
        self.config.sort_by_key(|val| val.tick_value);
    }

    // 3. Add a single entry using BINARY SEARCH to maintain sorted order
    fn add_entry(&mut self, entry: ConfigVal) {
        // partition_point finds the exact insertion index to keep the array sorted
        let idx = self
            .config
            .partition_point(|val| val.tick_value < entry.tick_value);
        self.config.insert(idx, entry);
    }

    // 4. Find nearest server using BINARY SEARCH
    fn find_nearest(&self, key_tick_value: u64) -> Option<&str> {
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
