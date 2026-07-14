use rust_decimal::{Decimal, dec};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering::Relaxed;
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

use crate::error::AppError;
use crate::model::{Client, Document, TransactionDirection};

/// Represents the in-memory state of a single client.
pub struct ClientState {
    pub document: Document,
    pub balances: Mutex<ClientBalances>,
}
/// Tracks the active balance and pending balance change of a client.
#[derive(Debug, Default, Clone, Copy)]
pub struct ClientBalances {
    /// The permanently persisted, settled balance of the client.
    pub settled_balance: Decimal,
    /// The temporary, uncommitted change (delta) to be written to the next ledger file.
    pub delta_balance: Decimal,
}

pub type ClientsMap = HashMap<Uuid, ClientState>;

// Shared App State
pub struct Cache {
    /// The latest nonce successfully committed to storage.
    latest_nonce: AtomicI32,

    /// In-memory registry of all clients.
    /// Read-lock for balance queries and existence checks; write-lock only to register new clients.
    clients: RwLock<ClientsMap>,

    /// Unique document numbers currently undergoing registration.
    /// Prevents duplicate sign-up race conditions before they are persisted.
    pending_registrations: Mutex<HashSet<Document>>,

    /// Client IDs that have memory-only balance updates (deltas) waiting to be flushed.
    dirty_client_ids: Mutex<HashSet<Uuid>>,

    /// Serializes writing operations to guarantee atomic updates and prevent races.
    pub persistence_lock: Mutex<()>,
}

impl Cache {
    /// Builds a cache from a hydrated clients map and the latest ledger nonce.
    pub fn new(clients: ClientsMap, nonce: i32) -> Self {
        Cache {
            clients: RwLock::new(clients),
            latest_nonce: AtomicI32::new(nonce),
            pending_registrations: Mutex::new(HashSet::new()),
            dirty_client_ids: Mutex::new(HashSet::new()),
            persistence_lock: Mutex::new(()),
        }
    }
}

impl Cache {
    /// Inserts a new client into the cache, initializing its balance with a zero delta.
    pub async fn insert_client(&self, client: &Client) {
        let mut clients = self.clients.write().await;
        clients.insert(
            client.client_id,
            ClientState {
                document: client.details.document_number.clone(),
                balances: Mutex::new(ClientBalances {
                    settled_balance: client.balance,
                    delta_balance: dec!(0),
                }),
            },
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

        if let Some(client_state) = clients.get(&client_id) {
            let result = {
                // Lock this client's balances
                let mut balances = client_state.balances.lock().await;

                match direction {
                    TransactionDirection::Credit => {
                        let new_balance = balances.settled_balance + balances.delta_balance + delta;
                        balances.delta_balance += delta;
                        Ok(new_balance)
                    }
                    TransactionDirection::Debit => {
                        let new_balance = balances.settled_balance + balances.delta_balance - delta;
                        if new_balance >= dec!(0) {
                            balances.delta_balance -= delta;
                            Ok(new_balance)
                        } else {
                            return Err(AppError::InsufficientFunds);
                        }
                    }
                }

                // balances lock is dropped when leaving this scope
            };

            // If we successfully updated the client delta in the cache, add it to the dirty clients list
            if result.is_ok() {
                self.insert_dirty_client(client_id).await?;
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
        let mut dirty_guard = self.dirty_client_ids.lock().await;

        // Iterate over the clients we just successfully persisted to storage
        for (client_id, delta) in clear_deltas {
            if let Some(client_state) = clients_read_guard.get(client_id) {
                // Isolate the synchronous client lock inside a block so it releases immediately
                let is_delta_zero = {
                    let mut balances = client_state.balances.lock().await;

                    balances.settled_balance += delta;
                    balances.delta_balance -= delta;

                    balances.delta_balance == dec!(0)
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
    /// computed as the persisted settled balance plus any in-memory delta that has
    /// not yet been flushed to storage.
    pub async fn get_client_snapshot(
        &self,
        client_id: Uuid,
    ) -> Result<(Document, Decimal), AppError> {
        let clients = self.clients.read().await;

        let client_state = clients.get(&client_id).ok_or(AppError::ClientNotFound)?;

        let balances = client_state.balances.lock().await;

        Ok((
            client_state.document.clone(),
            balances.settled_balance + balances.delta_balance,
        ))
    }

    pub async fn is_document_in_use(&self, document_number: &Document) -> bool {
        // Read lock
        let clients = self.clients.read().await;

        // Check if the document is already in use
        clients
            .values()
            .any(|state| state.document == *document_number)
    }

    pub async fn set_document_in_flight(&self, document_number: &Document) -> Result<(), AppError> {
        let mut pending_registrations = self.pending_registrations.lock().await;
        if pending_registrations.contains(document_number) {
            return Err(AppError::DocumentInFlight);
        }
        pending_registrations.insert(document_number.clone());

        Ok(())
    }

    pub async fn remove_document_in_flight(
        &self,
        document_number: &Document,
    ) -> Result<(), AppError> {
        let mut pending_registrations = self.pending_registrations.lock().await;

        pending_registrations.remove(document_number);

        Ok(())
    }

    pub async fn insert_dirty_client(&self, client_id: Uuid) -> Result<(), AppError> {
        let mut dirty_client_ids = self.dirty_client_ids.lock().await;

        dirty_client_ids.insert(client_id);

        Ok(())
    }

    pub async fn remove_dirty_client(&self, client_id: &Uuid) -> Result<(), AppError> {
        let mut dirty_client_ids = self.dirty_client_ids.lock().await;

        dirty_client_ids.remove(client_id);

        Ok(())
    }

    pub async fn get_dirty_clients(&self) -> Result<HashSet<Uuid>, AppError> {
        let dirty_client_ids = self.dirty_client_ids.lock().await;

        Ok(dirty_client_ids.clone())
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
            if let Some(client_state) = clients_read_guard.get(client_id) {
                let current_delta = {
                    let balances = client_state.balances.lock().await;
                    balances.delta_balance
                };

                deltas_to_write.push((*client_id, current_delta));
            }
        }

        Ok(deltas_to_write)
    }
}

// Nonce operations
impl Cache {
    pub fn increment_nonce(&self) {
        self.latest_nonce.fetch_add(1, Relaxed);
    }

    pub fn get_nonce(&self) -> i32 {
        self.latest_nonce.load(Relaxed)
    }
}
