use rust_decimal::{Decimal, dec};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::sync::atomic::AtomicI32;
use tokio::sync::RwLock;
use uuid::Uuid;

use super::client::{Client, Document};
use crate::TransactionDirection;

// Shared App State

pub struct Cache {
    // Map of all existing clients.
    // The key is the cliend_id and the value is a tuple: (client document, mutex(net balance, delta))
    // The outer async rwlock allows us to aquire a read lock when checking for client existence or quering client balance.
    // If we need to modify a client balance, we aquire the inner std::Mutex.
    pub clients: RwLock<HashMap<Uuid, (Document, Mutex<(Decimal, Decimal)>)>>,
    // Latest nonce
    pub nonce: AtomicI32,
    // In-flight clients: new clients being processed and waiting for storage and cache sync up
    pub in_flight: RwLock<HashSet<Document>>,
    // Keeps track of clients with balance changes in memory
    pub dirty_clients: Mutex<HashSet<Uuid>>,
    // Lock used to prevent data races when writing to storage
    pub store_lock: tokio::sync::Mutex<()>,
}

impl Cache {
    /// Inserts a new client to the cache (if not existing) and persists it to the storage.
    pub async fn insert_client(&self, client: Client) -> Result<(), String> {
        let document_number = &client.document_number.clone();
        // Check clients cache first. If document exists returns with an error.
        {
            // Read lock
            let clients = self.clients.read().await;
            // Check if the document is already in use
            let document_exists = clients
                .values()
                .any(|(doc, _balance)| doc == document_number);

            if document_exists {
                return Err("Exists".into());
            }
        }

        // Set in-flight to flag new client creation. This helps us sync cache with storage.
        // The clients cache will only reflect the new client once its safely saved to storage.
        {
            let mut in_flight = self.in_flight.write().await;
            if in_flight.contains(document_number) {
                return Err("Processing".into());
            }
            in_flight.insert(document_number.clone());
        }

        // Write the new client to storage
        // If this returns an error, the client document will be removed from the in_flight cache
        // and never added to the clients cache
        let result = crate::utils::save_client_to_storage(&client).await;

        // Add the new client to the clients cache
        if result.is_ok() {
            let mut clients = self.clients.write().await;
            clients.insert(
                client.client_id.clone(),
                (
                    client.document_number,
                    Mutex::new((client.balance, dec!(0))),
                ),
            );
        }

        // Remove new client document from in-flight cache
        let mut in_flight = self.in_flight.write().await;
        in_flight.remove(document_number);

        result
    }

    // Updates the current balance delta of a given client by delta.
    pub async fn apply_balance_change(
        &self,
        client_id: Uuid,
        delta: Decimal,
        direction: TransactionDirection,
    ) -> Result<Decimal, &'static str> {
        // Acquire outer read lock
        let clients = self.clients.read().await;

        if let Some((_document, balance_mutex)) = clients.get(&client_id) {
            // Lock only this client synchronously
            let mut balance_lock = balance_mutex.lock().map_err(|_| "Mutex poisoned")?;

            // Destructure the lock taking a mutable reference, as we need to modify the current delta
            let (base_balance, current_delta) = &mut *balance_lock;

            match direction {
                TransactionDirection::Credit => {
                    // No balance check needed as a credit tx always increases balance
                    let new_balance = *base_balance + *current_delta + delta;

                    // Increase current delta by |delta|
                    *current_delta += delta;
                    Ok(new_balance)
                }
                TransactionDirection::Debit => {
                    let new_balance = *base_balance + *current_delta - delta;
                    // Only update if new balance is positive
                    if new_balance >= dec!(0) {
                        // Decrement current delta by |delta|
                        *current_delta -= delta;
                        Ok(new_balance)
                    } else {
                        Err("Insufficient funds")
                    }
                }
            }
        } else {
            Err("Client not found")
        }
    }
}
