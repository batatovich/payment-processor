use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicI32;
use tokio::sync::Mutex;
use uuid::Uuid;

// Whitelisted countries
#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum Country {
    Uruguay,
    Peru,
    Chile,
    Argentina,
}

// ==========================================
// Request Payloads (DTOs)
// ==========================================

#[derive(Deserialize, Serialize)]
pub struct NewClientBody {
    pub client_name: String,
    pub birth_date: NaiveDate,
    pub document_number: String,
    pub country: Country,
}

#[derive(Deserialize, Serialize)]
pub struct NewCreditTransactionBody {
    pub client_id: Uuid,
    pub credit_amount: f64,
}

#[derive(Deserialize, Serialize)]
pub struct NewDebitTransactionBody {
    pub client_id: Uuid,
    pub debit_amount: f64,
}

// Client Model
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Client {
    pub client_id: Uuid,
    pub client_name: String,
    pub country: Country,
    pub document_number: String,
    pub birth_date: NaiveDate,
    pub balance: f64,
}

// Enum representing a debit or credit transaction.
pub enum Transaction {
    Credit(NewCreditTransactionBody),
    Debit(NewDebitTransactionBody),
}

// Shared App State
pub struct Cache {
    pub in_flight: Mutex<HashSet<String>>,
    pub clients: Mutex<HashMap<String, Client>>,
    pub transactions: Mutex<Vec<Transaction>>,
    pub nonce: AtomicI32,
}

impl Cache {
    /// Inserts a new client to the cache (if not existing) and persists it to storage.
    ///
    /// We use an in-flight strategy to avoid writing the new client to the cache before writing it to storage, in case something fails.
    pub async fn insert_client(&self, client: Client) -> Result<(), String> {
        let document_number = &client.document_number.clone();
        // Check clients cache first. If document exists returns with an error.
        {
            let clients = self.clients.lock().await;
            if clients.contains_key(document_number) {
                return Err("Exists".into());
            }
        }

        // Set in-flight to flag client creation.
        {
            let mut in_flight = self.in_flight.lock().await;
            if in_flight.contains(document_number) {
                return Err("Processing".into());
            }
            in_flight.insert(document_number.clone());
        }

        // Write the new client to storage
        // If this returns an error, the client document will be removed from the in_flight cache
        let result = crate::utils::save_client_to_storage(&client).await;

        // Add the new client to the clients cache
        if result.is_ok() {
            let mut clients = self.clients.lock().await;
            clients.insert(document_number.clone(), client);
        }

        // Remove new client document from in-flight cache
        let mut in_flight = self.in_flight.lock().await;
        in_flight.remove(document_number);

        result
    }
}
