use rand::Rng;
use serde::Serialize;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Serialize, Debug)]
struct NodeStatus {
    ip_address: String,
    gossip_port: u16,
    tpu_port: u16,
    version: String,
    stake_weight: u64,
    last_shred_received: u64,
    timestamp: u64,
}

pub fn generate_node_status_kv() -> (String, String) {
    let mut rng = rand::thread_rng();

    let pubkey = generate_fake_base58(&mut rng, 44);
    let key = format!("node:status:{}", pubkey);

    let status = NodeStatus {
        ip_address: format!(
            "{}.{}.{}.{}",
            rng.gen_range(1..255),
            rng.gen_range(0..255),
            rng.gen_range(0..255),
            rng.gen_range(1..255)
        ),
        gossip_port: 8001,
        tpu_port: 8004,
        version: "1.17.2".to_string(),
        stake_weight: rng.gen_range(10_000_000..50_000_000_000),
        last_shred_received: rng.gen_range(1_000_000..5_000_000),
        timestamp: current_timestamp(),
    };

    let value = serde_json::to_string(&status).expect("Failed to serialize NodeStatus");
    (key, value)
}

#[derive(Serialize, Debug)]
struct PendingTransaction {
    sender: String,
    recent_blockhash: String,
    fee_tier: String,
    payload_size_bytes: u32,
    raw_instruction_blob: String,
    received_at: u64,
}

pub fn generate_pending_tx_kv() -> (String, String) {
    let mut rng = rand::thread_rng();

    let signature = generate_fake_base58(&mut rng, 88);
    let key = format!("tx:pending:{}", signature);

    let tiers = ["low", "medium", "high"];
    let selected_tier = tiers[rng.gen_range(0..tiers.len())];

    let tx = PendingTransaction {
        sender: generate_fake_base58(&mut rng, 44),
        recent_blockhash: generate_fake_base58(&mut rng, 44),
        fee_tier: selected_tier.to_string(),
        payload_size_bytes: rng.gen_range(200..1232),
        raw_instruction_blob: format!("0x{}", generate_hex_string(&mut rng, 64)),
        received_at: current_timestamp(),
    };

    let value = serde_json::to_string(&tx).expect("Failed to serialize PendingTransaction");
    (key, value)
}

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before UNIX epoch")
        .as_secs()
}

/// Generates a random alphanumeric string to simulate Base58 addresses
fn generate_fake_base58(rng: &mut impl Rng, length: usize) -> String {
    const CHARSET: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    (0..length)
        .map(|_| CHARSET[rng.gen_range(0..CHARSET.len())] as char)
        .collect()
}

/// Generates a random hex string to simulate raw byte payloads
fn generate_hex_string(rng: &mut impl Rng, length: usize) -> String {
    const CHARSET: &[u8] = b"0123456789abcdef";
    (0..length)
        .map(|_| CHARSET[rng.gen_range(0..CHARSET.len())] as char)
        .collect()
}

fn main() {
    println!("--- Generating Node Status Data ---");
    let (node_key, node_value) = generate_node_status_kv();
    println!("Key: {}", node_key);
    println!("Value: {}\n", node_value);

    println!("--- Generating Pending Tx Data ---");
    let (tx_key, tx_value) = generate_pending_tx_kv();
    println!("Key: {}", tx_key);
    println!("Value: {}", tx_value);
}
