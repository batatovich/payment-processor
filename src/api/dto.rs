use crate::model::{Country, Document};
use chrono::NaiveDate;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NewClientBody {
    pub client_name: String,
    pub birth_date: NaiveDate,
    pub document_number: Document,
    pub country: Country,
}

#[derive(Deserialize)]
pub struct NewCreditTransactionBody {
    pub client_id: Uuid,
    pub credit_amount: Decimal,
}

#[derive(Deserialize)]
pub struct NewDebitTransactionBody {
    pub client_id: Uuid,
    pub debit_amount: Decimal,
}

#[derive(Deserialize)]
pub struct GetBalanceQuery {
    pub client_id: Uuid,
}

#[derive(Serialize)]
pub struct GetBalanceResponse {
    pub client_id: Uuid,
    pub document_number: Document,
    pub balance: Decimal,
}
