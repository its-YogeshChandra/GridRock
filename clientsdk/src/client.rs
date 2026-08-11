pub mod storage_proto {
    tonic::include_proto!("storage_system");
}

#[path = "utils.rs"]
mod utils;
use utils::TestEntity;
use crate::storage_proto::grid_rock_client::GridRockClient;
use tonic::transport::Channel;
use rand::RngExt;

// -----------------------------------------
// CRUD functions — take an existing client + entity,
// so the same unique_id flows through Create -> Update -> Get -> Delete
// -----------------------------------------

fn get_random_value<T: Copy>(values: &[T]) -> T {
    let mut rng = rand::rng();
    let index = rng.random_range(0..values.len());
    values[index]
}


pub async fn create_val(
    client: &mut GridRockClient<Channel>,
    entity: &TestEntity,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = client
        .create_valin_storage(entity.to_create_request())
        .await;
    match response {
        Ok(resp) => {
            println!("[CREATE] {:#?}", resp.into_inner());
        }
        Err(e) => {
            eprintln!("[CREATE] failed for id {}: {:?}", entity.unique_id, e);
            return Err(Box::new(e));
        }
    }
    Ok(())
}

pub async fn update_val(
    client: &mut GridRockClient<Channel>,
    entity: &TestEntity,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = client
        .update_valin_storage(entity.to_update_request())
        .await?;
    println!("[UPDATE] {:#?}", response.into_inner());
    Ok(())
}

pub async fn get_val(
    client: &mut GridRockClient<Channel>,
    entity: &TestEntity,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = client.get_valfrom_storage(entity.to_get_request()).await?;
    println!("[GET] {:#?}", response.into_inner());
    Ok(())
}

pub async fn delete_val(
    client: &mut GridRockClient<Channel>,
    entity: &TestEntity,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = client
        .del_valfrom_storage(entity.to_delete_request())
        .await?;
    println!("[DELETE] {:#?}", response.into_inner());
    Ok(())
}

// -----------------------------------------
// Full lifecycle test for ONE entity: Create -> Get -> Update -> Get -> Delete -> Get (should fail/not found)
// -----------------------------------------
async fn run_lifecycle(
    client: &mut GridRockClient<Channel>,
    entity: &TestEntity,
) -> Result<(), Box<dyn std::error::Error>> {
    create_val(client, entity).await?;
    get_val(client, entity).await?; // confirm it exists with initial_balance
    update_val(client, entity).await?;
    get_val(client, entity).await?; // confirm balance changed to updated_balance
    delete_val(client, entity).await?;

    // this call should now come back with success = false / not found,
    // depending on how your server signals "missing key"
    let post_delete_get = client.get_valfrom_storage(entity.to_get_request()).await?;
    println!("[GET after DELETE] {:#?}", post_delete_get.into_inner());

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let urls:  [&str; 3] = [
        "http://[::1]:50051",
        "http://[::1]:50052",
        "http://[::1]:50053",];
    
    //get the random url from the array 
    let random_url = get_random_value(&urls);
    
    
    let mut client = GridRockClient::connect(random_url).await?;
    
    // Bulk test: 1000 DISTINCT entities, each with its own unique_id,
    // each pushed through a full create/get/update/get/delete cycle.
    let pool = utils::generate_entity_pool(1000);

    for entity in &pool {
        run_lifecycle(&mut client, entity).await?;
    }

    Ok(())
}
