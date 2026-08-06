//create the enum for the errros 
use std::fmt;
use std::error::Error;

#[derive(Debug)]
pub enum ClientGrpcRequestProcessingError{
   NodeUnaware,
   NotLeader
}


impl fmt::Display for ClientGrpcRequestProcessingError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ClientGrpcRequestProcessingError::NodeUnaware => write!(f, "the node is not aware of the request"),
            ClientGrpcRequestProcessingError::NotLeader => write!(f, "the node is not a leader"),
    }
}
}

impl Error for ClientGrpcRequestProcessingError {}