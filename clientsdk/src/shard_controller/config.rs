use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

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

// Combined hash function for both servers and keys
fn get_hash_value(value: &str) -> u64 {
    let mut string_hash = DefaultHasher::new();
    value.hash(&mut string_hash);

    // NOTE: Removed `% 18446744073709551615`.
    // finish() already returns a full u64. Modulo by u64::MAX is mathematically flawed
    // because it maps the maximum possible value back to 0.
    // The raw output IS your position on the 2^64 ring!
    let response = string_hash.finish();
    response
}

fn main() {
    let mac_addr = [
        "192.168.1.1",
        "10.0.0.25",
        "172.16.254.1",
        "8.8.8.8",
        "1.1.1.1",
        "127.0.0.1",
        "192.168.0.100",
        "10.10.10.10",
        "203.0.113.5",
        "198.51.100.2",
    ];

    let key_string_arr = [
        "bTdhNzY4ZThkMWIxNmVmNA==",
        "ZTQ2NTVmMTdjZTVkZmE2OQ==",
        "NTZjYTk1NjEyMjI4ZjMzNw==",
        "YmMzZTYzYzRjODNkYzdiOA==",
        "YzExOGU2MDFhMjg3ZmFiYw==",
    ];

    let mut vec_config_arr: Vec<ConfigVal> = Vec::new();

    println!("--- Initializing Servers ---");
    for val in mac_addr {
        let result = get_hash_value(val);
        println!("Server {} tick: {}", val, result);

        vec_config_arr.push(ConfigVal {
            tick_value: result,
            address: val.to_string(),
        });
    }

    // Create config (this will sort the Vec internally)
    let mut config = Config::new(vec_config_arr);

    println!("\n--- Finding Servers for Keys ---");
    for val in key_string_arr {
        let tick_value = get_hash_value(val);
        println!("Key {} tick: {}", val, tick_value);

        // Idiomatic Rust: call the method directly on the instance
        if let Some(server_address) = config.find_nearest(tick_value) {
            println!("-> Assigned to server: {}\n", server_address);
        }
    }

    // --- Let's test adding a new server dynamically! ---
    println!("--- Adding a new server dynamically ---");
    let new_server_addr = "192.168.50.50";
    let new_server_tick = get_hash_value(new_server_addr);
    println!("New Server {} tick: {}", new_server_addr, new_server_tick);

    // This uses binary search to insert it in the exact right sorted position!
    config.add_entry(ConfigVal {
        tick_value: new_server_tick,
        address: new_server_addr.to_string(),
    });

    println!("\nTesting the first key again to see if it moved:");
    let first_key = key_string_arr[0];
    let first_key_tick = get_hash_value(first_key);
    if let Some(server_address) = config.find_nearest(first_key_tick) {
        println!("-> Assigned to server: {}", server_address);
    }
}

