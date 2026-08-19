use crate::configstore::config_store::{ConfigVal, read_config_store, write_config_store};
use crate::shard_config_service::shard_controller_server::ShardController;
use crate::shard_config_service::{
    AddServerRequest, AddServerResponse, ConfigVal as ProtoConfigVal, GetFullConfigRequest,
    GetFullConfigResponse, GetServersRequest, GetServersResponse, RemoveServerRequest,
    RemoveServerResponse, ServerInfo,
};
use tonic::{Request, Response, Status};

//create the handler
pub struct ConfigController;

// helper: convert the in-memory Vec<ConfigVal> into the proto repeated ConfigVal
fn config_to_proto(entries: &[ConfigVal]) -> Vec<ProtoConfigVal> {
    entries
        .iter()
        .map(|e| ProtoConfigVal {
            tick_value: e.tick_value,
            address: e.address.clone(),
        })
        .collect()
}

#[tonic::async_trait]
impl ShardController for ConfigController {
    //function to add a new server to the config
    async fn add_server(
        &self,
        request: Request<AddServerRequest>,
    ) -> Result<Response<AddServerResponse>, Status> {
        let req = request.into_inner();

        // extract the ServerInfo from the request
        let server_info = req
            .server
            .ok_or_else(|| Status::invalid_argument("missing server field"))?;

        // build the address string the config store expects (host:port)
        let full_address = format!("{}:{}", server_info.address, server_info.port);

        // acquire a write lock and add the entry
        let mut config =
            write_config_store().ok_or_else(|| Status::internal("config store not initialized"))?;

        config.add_entry(ConfigVal::new(full_address));

        // snapshot the updated config for the response
        let proto_config = config_to_proto(&config.get_entries());

        Ok(Response::new(AddServerResponse {
            success: true,
            message: "server added successfully".to_string(),
            config: proto_config,
        }))
    }

    //function to remove a server from the config
    async fn remove_server(
        &self,
        request: Request<RemoveServerRequest>,
    ) -> Result<Response<RemoveServerResponse>, Status> {
        let req = request.into_inner();
        let server_id = req.server_id;

        if server_id.is_empty() {
            return Err(Status::invalid_argument("server_id must not be empty"));
        }

        // acquire a write lock and remove entries matching this address/id
        let mut config =
            write_config_store().ok_or_else(|| Status::internal("config store not initialized"))?;

        let removed = config.remove_entry(&server_id);

        if !removed {
            return Err(Status::not_found(format!(
                "no server found with id/address '{}'",
                server_id
            )));
        }

        // snapshot the updated config for the response
        let proto_config = config_to_proto(&config.get_entries());

        Ok(Response::new(RemoveServerResponse {
            success: true,
            message: format!("server '{}' removed successfully", server_id),
            config: proto_config,
        }))
    }

    //function to get the full config (entire shard-to-server ring)
    async fn get_full_config(
        &self,
        _request: Request<GetFullConfigRequest>,
    ) -> Result<Response<GetFullConfigResponse>, Status> {
        let config =
            read_config_store().ok_or_else(|| Status::internal("config store not initialized"))?;

        let proto_config = config_to_proto(&config.get_entries());

        Ok(Response::new(GetFullConfigResponse {
            success: true,
            message: "full config retrieved".to_string(),
            config: proto_config,
        }))
    }

    //function to get the list of registered servers
    async fn get_servers(
        &self,
        _request: Request<GetServersRequest>,
    ) -> Result<Response<GetServersResponse>, Status> {
        let config =
            read_config_store().ok_or_else(|| Status::internal("config store not initialized"))?;

        // get unique addresses and map them into ServerInfo messages
        let servers: Vec<ServerInfo> = config
            .get_unique_addresses()
            .into_iter()
            .map(|addr| {
                // parse "host:port" back into ServerInfo fields
                let (host, port) = match addr.rsplit_once(':') {
                    Some((h, p)) => (h.to_string(), p.parse::<u32>().unwrap_or(0)),
                    None => (addr.clone(), 0),
                };
                ServerInfo {
                    server_id: addr, // the full address acts as the id
                    address: host,
                    port,
                }
            })
            .collect();

        Ok(Response::new(GetServersResponse { servers }))
    }
}
