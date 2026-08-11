use rand::RngExt;

// Adjust this to match your tonic-build generated module path,
// e.g. `pub mod storage_system { tonic::include_proto!("storage_system"); }`
use crate::storage_proto::{CreateRequest, DelValRequest, GetValRequest, UpdateRequest};

// -----------------------------------------
// Core idea: unique_id is the shared key across
// Create / Update / Get / Delete. Generate it ONCE
// per "test entity" and reuse it everywhere so a
// full CRUD lifecycle test is actually coherent.
// -----------------------------------------

/// Represents one fake "account"/entity and every request
/// variant you might want to test against it, all sharing
/// the same unique_id.

#[derive(Debug, Clone)]
pub struct TestEntity {
    pub unique_id: String,
    pub initial_balance: u64,
    pub updated_balance: u64,
    pub executable: bool,
    pub rent_epoch: u64,
    pub data_hash: String,
    pub last_updated_slot: u64,
}

impl TestEntity {
    /// Generates a new fake entity with a fresh unique_id and
    /// randomized field values.
    pub fn generate() -> Self {
        let mut rng = rand::rng(); // <- was thread_rng()

        TestEntity {
            unique_id: generate_fake_base58(&mut rng, 44),
            initial_balance: rng.random_range(0..1_000_000_000),
            updated_balance: rng.random_range(0..1_000_000_000),
            executable: rng.random_bool(0.2), // was gen_bool
            rent_epoch: rng.random_range(0..500),
            data_hash: generate_hex_string(&mut rng, 32),
            last_updated_slot: rng.random_range(1_000_000..5_000_000),
        }
    }

    /// Builds the CreateRequest for this entity.
    pub fn to_create_request(&self) -> CreateRequest {
        CreateRequest {
            unique_id: self.unique_id.clone(),
            balance: self.initial_balance,
            executable: self.executable,
            rent_epoch: self.rent_epoch,
            data_hash: self.data_hash.clone(),
            last_updated_slot: self.last_updated_slot,
        }
    }

    /// Builds an UpdateRequest for this entity — same unique_id,
    /// different balance, simulating a later state change.
    pub fn to_update_request(&self) -> UpdateRequest {
        UpdateRequest {
            unique_id: self.unique_id.clone(),
            balance: self.updated_balance,
        }
    }

    /// Builds a GetValRequest for this entity's unique_id.
    pub fn to_get_request(&self) -> GetValRequest {
        GetValRequest {
            unique_id: self.unique_id.clone(),
        }
    }

    /// Builds a DelValRequest for this entity's unique_id.
    pub fn to_delete_request(&self) -> DelValRequest {
        DelValRequest {
            unique_id: self.unique_id.clone(),
        }
    }
}

/// Generates a pool of N distinct test entities, useful for
/// bulk/load testing or exercising multiple keys at once.
pub fn generate_entity_pool(count: usize) -> Vec<TestEntity> {
    (0..count).map(|_| TestEntity::generate()).collect()
}

// -----------------------------------------
// Helper Functions
// -----------------------------------------
fn generate_fake_base58(rng: &mut impl RngExt, length: usize) -> String {
    const CHARSET: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    (0..length)
        .map(|_| CHARSET[rng.random_range(0..CHARSET.len())] as char)
        .collect()
}

fn generate_hex_string(rng: &mut impl RngExt, length: usize) -> String {
    const CHARSET: &[u8] = b"0123456789abcdef";
    (0..length)
        .map(|_| CHARSET[rng.random_range(0..CHARSET.len())] as char)
        .collect()
}

// -----------------------------------------
// Example usage: full CRUD lifecycle against one entity
// -----------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entity_lifecycle_requests_share_same_id() {
        let entity = TestEntity::generate();

        let create_req = entity.to_create_request();
        let update_req = entity.to_update_request();
        let get_req = entity.to_get_request();
        let del_req = entity.to_delete_request();

        // All four requests must reference the same key
        assert_eq!(create_req.unique_id, entity.unique_id);
        assert_eq!(update_req.unique_id, entity.unique_id);
        assert_eq!(get_req.unique_id, entity.unique_id);
        assert_eq!(del_req.unique_id, entity.unique_id);
    }
}
