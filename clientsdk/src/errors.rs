use std::fmt;

// --- Error A ---
#[derive(Debug)]
pub enum ErrorA {
    // Passes a custom string message through the enum variant
    Message(String),
}

impl fmt::Display for ErrorA {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorA::Message(msg) => write!(f, "ErrorA: {}", msg),
        }
    }
}

impl std::error::Error for ErrorA {}


// --- Error B ---
#[derive(Debug)]
pub enum ErrorB {
    Message(String),
}

impl fmt::Display for ErrorB {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorB::Message(msg) => write!(f, "ErrorB: {}", msg),
        }
    }
}

impl std::error::Error for ErrorB {}


// --- Error C ---
#[derive(Debug)]
pub enum ErrorC {
    Message(String),
}

impl fmt::Display for ErrorC {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorC::Message(msg) => write!(f, "ErrorC: {}", msg),
        }
    }
}

impl std::error::Error for ErrorC {}


// --- Error D ---
#[derive(Debug)]
pub enum ErrorD {
    Message(String),
}

impl fmt::Display for ErrorD {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorD::Message(msg) => write!(f, "ErrorD: {}", msg),
        }
    }
}

impl std::error::Error for ErrorD {}