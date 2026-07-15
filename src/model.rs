use crate::api::dto::ClientDetails;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type Document = String;

pub enum TransactionDirection {
    Credit,
    Debit,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub enum Country {
    Uruguay,
    Peru,
    Chile,
    Argentina,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Client {
    pub client_id: Uuid,
    pub details: ClientDetails,
}

impl Client {
    pub fn new(details: ClientDetails) -> Self {
        Self {
            client_id: Uuid::now_v7(),
            details,
        }
    }
}
