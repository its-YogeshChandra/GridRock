//create the enum for the errros 
use std::fmt;
use std::error::Error;

#[derive(Debug)]
pub enum ClientGrpcRequestProcessingError{
   NodeUnaware,
   NotLeader,
   GetRequestNotSupported,
   DbResponseFailed,
   RequestForwardingFailed 
}


impl fmt::Display for ClientGrpcRequestProcessingError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ClientGrpcRequestProcessingError::NodeUnaware => write!(f, "the node is not aware of the request"),
            ClientGrpcRequestProcessingError::NotLeader => write!(f, "the node is not a leader"),
            ClientGrpcRequestProcessingError::GetRequestNotSupported => write!(f, "the get request is not supported "),
            ClientGrpcRequestProcessingError::DbResponseFailed => write!(f, "the db response failed"),
            ClientGrpcRequestProcessingError::RequestForwardingFailed => write!(f, "the request forwarding to the leader failed"),
    }
}
}

impl Error for ClientGrpcRequestProcessingError {}