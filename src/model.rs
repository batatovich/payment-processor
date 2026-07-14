use crate::api::dto::NewClientBody;
use rust_decimal::{Decimal, dec};
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
    pub details: NewClientBody,
    pub balance: Decimal,
}

impl Client {
    pub fn new(body: NewClientBody) -> Self {
        Self {
            client_id: Uuid::now_v7(),
            details: body.clone(),
            balance: dec!(0),
        }
    }
}
