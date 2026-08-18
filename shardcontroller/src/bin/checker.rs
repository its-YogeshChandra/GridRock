//take the address string and then proces it to get the value
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

struct ConfigVal {
    tick_value: u64,
    address: String,
}

pub struct Config {
    config: [ConfigVal; 3],
}

impl Config {
    fn new(&self, value: Vec<ConfigVal>) {
        for val in value {}
    }
}

//give server mod
fn give_server_mod(value: &str) {
    let mut string_hash = DefaultHasher::new();
    value.hash(&mut string_hash);

    let mod_val: u64 = 18446744073709551615;
    let response = string_hash.finish() % mod_val;
    println!("the server value is : {}", response)
}

//give key mod
fn give_key_mod(value: &str) {
    let mut string_hash = DefaultHasher::new();
    value.hash(&mut string_hash);

    let mod_val: u64 = 18446744073709551615;
    let response = string_hash.finish() % mod_val;
    println!("the key value is : {}", response)
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
        "NTlmOWI4YTlmMjczZjA4YQ==",
        "MzY3ZDhlNGE3MTJjZDlmNg==",
        "ODJhYTZhYTBjMzliOWRmYw==",
        "M2NlNTRkYWU4ZDBmNjlmNw==",
        "OWY3MDJiYWVjMDFkMjFhMQ==",
    ];

    for val in key_string_arr {
        give_key_mod(val);
    }

    for val in mac_addr {
        give_server_mod(val);
    }
}
