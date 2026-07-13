use super::dto::NewClientBody;
use chrono::NaiveDate;
use rust_decimal::{Decimal, dec};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
pub type Document = String;

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
    pub client_name: String,
    pub country: Country,
    pub document_number: Document,
    pub birth_date: NaiveDate,
    pub balance: Decimal,
}

impl Client {
    pub fn new(body: NewClientBody) -> Self {
        Self {
            client_id: Uuid::now_v7(),
            client_name: body.client_name,
            country: body.country,
            document_number: body.document_number,
            birth_date: body.birth_date,
            balance: dec!(0),
        }
    }
}
