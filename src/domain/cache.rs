use rust_decimal::{Decimal, dec};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering::Relaxed;
use uuid::Uuid;

use super::client::{Client, Document};
use super::error::AppError;
use crate::TransactionDirection;

// Shared App State

pub struct Cache {
    // Map of all existing clients.
    // The key is the cliend_id and the value is a tuple: (client document, mutex(net balance, delta))
    // The outer async rwlock allows us to aquire a read lock when checking for client existence or quering client balance.
    // We only acquire the write lock if we need to insert a new client
    pub clients:
        tokio::sync::RwLock<HashMap<Uuid, (Document, std::sync::Mutex<(Decimal, Decimal)>)>>,
    // Latest nonce
    pub nonce: AtomicI32,
    // In-flight clients: new clients being processed and waiting for storage and cache sync up
    pub in_flight: std::sync::Mutex<HashSet<Document>>,
    // Keeps track of clients with balance changes in memory
    pub dirty_clients: std::sync::Mutex<HashSet<Uuid>>,
    // Lock used to prevent data races when writing to storage
    pub store_lock: tokio::sync::Mutex<()>,
}

impl Cache {
    /// Inserts a new client into the cache, initializing its balance with a zero delta.
    pub async fn insert_client(&self, client: &Client) {
        let mut clients = self.clients.write().await;
        clients.insert(
            client.client_id,
            (
                client.document_number.clone(),
                Mutex::new((client.balance, dec!(0))),
            ),
        );
    }

    pub async fn apply_balance_change(
        &self,
        client_id: Uuid,
        delta: Decimal,
        direction: TransactionDirection,
    ) -> Result<Decimal, AppError> {
        // Acquire outer lock in read mode.
        // No other task can add or remove elements to the hashmap while we hold this lock
        let clients = self.clients.read().await;

        if let Some((_document, balance_mutex)) = clients.get(&client_id) {
            let result = {
                // Lock this client balance mutex
                let mut balance_lock = balance_mutex.lock().map_err(|_| AppError::LockPoisoned)?;

                // Destructure the lock taking a mutable reference, as we need to modify the current delta
                let (base_balance, current_delta) = &mut *balance_lock;

                match direction {
                    TransactionDirection::Credit => {
                        let new_balance = *base_balance + *current_delta + delta;
                        *current_delta += delta;
                        Ok(new_balance)
                    }
                    TransactionDirection::Debit => {
                        let new_balance = *base_balance + *current_delta - delta;
                        if new_balance >= dec!(0) {
                            *current_delta -= delta;
                            Ok(new_balance)
                        } else {
                            return Err(AppError::InsufficientFunds);
                        }
                    }
                }

                // balance_lock is droped when leaving this scope
            };

            // If we successfully updated the client delta in the cache, add it to the dirty clients list
            if result.is_ok() {
                self.insert_dirty_client(client_id)?;
            }

            return result;
        } else {
            return Err(AppError::ClientNotFound);
        }
    }

    pub async fn apply_persisted_deltas(
        &self,
        clear_deltas: &[(Uuid, Decimal)],
    ) -> Result<(), AppError> {
        // Acquire the outer async RwLock read guard
        let clients_read_guard = self.clients.read().await;

        // Once we got the outer rwlock, we lock the std mutex of dirty clients to update the balance and delta
        let mut dirty_guard = self.dirty_clients.lock().map_err(|_| AppError::LockPoisoned)?;

        // Iterate over the clients we just successfully persisted to storage
        for (client_id, delta) in clear_deltas {
            if let Some((_doc, inner_mutex)) = clients_read_guard.get(client_id) {
                // Isolate the synchronous client lock inside a block so it releases immediately
                let is_delta_zero = {
                    let mut balance_lock =
                        inner_mutex.lock().map_err(|_| AppError::LockPoisoned)?;
                    let (base_balance, current_delta) = &mut *balance_lock;

                    *base_balance += delta;
                    *current_delta -= delta;

                    *current_delta == dec!(0)
                };

                // 4. Safely clean up the dirty tracker list if all deltas are cleared
                if is_delta_zero {
                    dirty_guard.remove(client_id);
                }
            }
        }

        Ok(())
    }

    /// Returns the client's document number together with its current balance,
    /// computed as the persisted base balance plus any in-memory delta that has
    /// not yet been flushed to storage.
    pub async fn get_client_snapshot(
        &self,
        client_id: Uuid,
    ) -> Result<(Document, Decimal), AppError> {
        let clients = self.clients.read().await;

        let (document, balance_mutex) =
            clients.get(&client_id).ok_or(AppError::ClientNotFound)?;

        let balance_lock = balance_mutex.lock().map_err(|_| AppError::LockPoisoned)?;
        let (base_balance, current_delta) = &*balance_lock;

        Ok((document.clone(), *base_balance + *current_delta))
    }

    pub async fn is_document_in_use(&self, document_number: &Document) -> bool {
        // Read lock
        let clients = self.clients.read().await;

        // Check if the document is already in use
        clients
            .values()
            .any(|(doc, _balance)| *doc == *document_number)
    }

    pub fn set_document_in_flight(&self, document_number: &Document) -> Result<(), AppError> {
        let mut in_flight = self.in_flight.lock().map_err(|_| AppError::LockPoisoned)?;
        if in_flight.contains(document_number) {
            return Err(AppError::DocumentInFlight);
        }
        in_flight.insert(document_number.clone());

        Ok(())
    }

    pub fn remove_document_in_flight(&self, document_number: &Document) -> Result<(), AppError> {
        let mut in_flight = self.in_flight.lock().map_err(|_| AppError::LockPoisoned)?;

        in_flight.remove(document_number);

        Ok(())
    }

    pub fn insert_dirty_client(&self, client_id: Uuid) -> Result<(), AppError> {
        let mut dirty_clients = self.dirty_clients.lock().map_err(|_| AppError::LockPoisoned)?;

        dirty_clients.insert(client_id);

        Ok(())
    }

    pub fn remove_dirty_client(&self, client_id: &Uuid) -> Result<(), AppError> {
        let mut dirty_clients = self.dirty_clients.lock().map_err(|_| AppError::LockPoisoned)?;

        dirty_clients.remove(client_id);

        Ok(())
    }

    pub fn get_dirty_clients(&self) -> Result<HashSet<Uuid>, AppError> {
        let dirty_clients = self.dirty_clients.lock().map_err(|_| AppError::LockPoisoned)?;

        Ok(dirty_clients.clone())
    }

    /// Collects the current balance deltas for a given set of dirty client IDs.
    pub async fn collect_batch_deltas(
        &self,
        dirty_clients: &HashSet<Uuid>,
    ) -> Result<Vec<(Uuid, Decimal)>, AppError> {
        // Acquire the outer async RwLock read guard
        let clients_read_guard = self.clients.read().await;

        let mut deltas_to_write = Vec::with_capacity(dirty_clients.len());

        // Iterate and individually look up and lock each client
        for client_id in dirty_clients {
            if let Some((_doc, inner_mutex)) = clients_read_guard.get(client_id) {
                let current_delta = {
                    let balance_lock =
                        inner_mutex.lock().map_err(|_| AppError::LockPoisoned)?;

                    let (_base_balance, current_delta) = &*balance_lock;
                    *current_delta
                };

                deltas_to_write.push((*client_id, current_delta));
            }
        }

        Ok(deltas_to_write)
    }

    pub fn increment_nonce(&self) {
        self.nonce.fetch_add(1, Relaxed);
    }
}
