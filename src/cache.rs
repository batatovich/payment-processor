use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering::Relaxed;
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

use crate::api::dto::NewClientBody as ClientDetails;
use crate::error::AppError;
use crate::model::{Client, Document, TransactionDirection};

/// Represents the in-memory state of a single client.
pub struct ClientCache {
    pub client_details: ClientDetails,
    /// The client's running balance. Starts at zero on boot, accumulates credits
    /// and debits (may go negative), and is flushed back to zero whenever it is
    /// persisted by `store_balances`.
    pub balance: Mutex<Decimal>,
}

pub type ClientsMap = HashMap<Uuid, ClientCache>;

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

    /// Client IDs whose balance has memory-only changes waiting to be flushed.
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

// Client registration
impl Cache {
    /// Returns whether a document number already belongs to a registered client.
    pub async fn is_document_in_use(&self, document_number: &Document) -> bool {
        let clients = self.clients.read().await;
        clients
            .values()
            .any(|client_cache| client_cache.client_details.document_number == *document_number)
    }

    /// Reserves a document number for an in-progress registration, rejecting a
    /// concurrent sign-up that races on the same document before it is persisted.
    pub async fn reserve_document(&self, document_number: &Document) -> Result<(), AppError> {
        let mut pending = self.pending_registrations.lock().await;
        if pending.contains(document_number) {
            return Err(AppError::DocumentInFlight);
        }
        pending.insert(document_number.clone());
        Ok(())
    }

    /// Releases a reserved document number once registration finishes, whether it
    /// succeeded or failed.
    pub async fn release_document(&self, document_number: &Document) {
        self.pending_registrations
            .lock()
            .await
            .remove(document_number);
    }

    /// Inserts a newly registered client, seeding it with a zero balance.
    pub async fn insert_client(&self, client: &Client) {
        let mut clients = self.clients.write().await;
        clients.insert(
            client.client_id,
            ClientCache {
                client_details: client.details.clone(),
                balance: Mutex::new(Decimal::ZERO),
            },
        );
    }
}

// Balance operations
impl Cache {
    /// Applies a credit or debit to a client's balance and returns the resulting
    /// balance. Debits are always accepted and may drive the balance negative.
    pub async fn apply_balance_change(
        &self,
        client_id: Uuid,
        amount: Decimal,
        direction: TransactionDirection,
    ) -> Result<Decimal, AppError> {
        // Read lock keeps the map stable; only registration takes the write lock.
        let clients = self.clients.read().await;
        let client_state = clients.get(&client_id).ok_or(AppError::ClientNotFound)?;

        let new_balance = {
            let mut balance = client_state.balance.lock().await;
            match direction {
                TransactionDirection::Credit => *balance += amount,
                TransactionDirection::Debit => *balance -= amount,
            }
            *balance
        };

        self.mark_dirty(client_id).await;
        Ok(new_balance)
    }

    /// Returns the client's document number and current balance.
    pub async fn get_client_state(&self, client_id: Uuid) -> Result<(Document, Decimal), AppError> {
        let clients = self.clients.read().await;
        let client_state = clients.get(&client_id).ok_or(AppError::ClientNotFound)?;

        let balance = *client_state.balance.lock().await;
        Ok((client_state.client_details.document_number.clone(), balance))
    }
}

// Persistence / flush
impl Cache {
    /// Snapshots the current balance of every dirty client, producing the batch to
    /// be written to the next balance file.
    pub async fn snapshot_dirty_balances(&self) -> Vec<(Uuid, Decimal)> {
        // Copy the dirty id set and release its lock before touching client balances.
        let dirty_ids = self.dirty_client_ids.lock().await.clone();

        let clients = self.clients.read().await;
        let mut balances = Vec::with_capacity(dirty_ids.len());
        for client_id in dirty_ids {
            if let Some(client_state) = clients.get(&client_id) {
                let balance = *client_state.balance.lock().await;
                balances.push((client_id, balance));
            }
        }
        balances
    }

    /// Resets each persisted balance back to zero, clearing the client from the
    /// dirty set. Subtracting the persisted amount (rather than hard-setting to
    /// zero) preserves any transaction that landed after the snapshot was taken;
    /// such a client stays dirty for the next flush.
    pub async fn reset_persisted_balances(&self, persisted_balances: &[(Uuid, Decimal)]) {
        let clients = self.clients.read().await;
        let mut dirty_ids = self.dirty_client_ids.lock().await;

        for (client_id, persisted) in persisted_balances {
            if let Some(client_state) = clients.get(client_id) {
                // Isolate the balance lock so it releases before the next iteration.
                let fully_flushed = {
                    let mut balance = client_state.balance.lock().await;
                    *balance -= persisted;
                    balance.is_zero()
                };

                if fully_flushed {
                    dirty_ids.remove(client_id);
                }
            }
        }
    }

    /// Flags a client as having in-memory changes awaiting persistence.
    async fn mark_dirty(&self, client_id: Uuid) {
        self.dirty_client_ids.lock().await.insert(client_id);
    }
}

// Nonce
impl Cache {
    pub fn increment_nonce(&self) {
        self.latest_nonce.fetch_add(1, Relaxed);
    }

    pub fn get_nonce(&self) -> i32 {
        self.latest_nonce.load(Relaxed)
    }
}
