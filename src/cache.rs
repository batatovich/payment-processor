use crate::api::dto::ClientDetails;
use crate::error::AppError;
use crate::model::{Client, Document, TransactionDirection};
use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering::Relaxed;
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

/// In-memory state of a single registered client.
pub struct ClientState {
    pub details: ClientDetails,
    /// The client's running balance. Starts at zero on boot, accumulates credits
    /// and debits (may go negative), and is flushed back to zero whenever it is
    /// persisted by `store_balances`.
    pub balance: Mutex<Decimal>,
}

pub type ClientsMap = HashMap<Uuid, ClientState>;

/// Shared application state, accessed concurrently by every request handler.
pub struct Cache {
    /// The latest nonce successfully committed to storage.
    latest_nonce: AtomicI32,

    /// In-memory registry of all clients.
    /// Read-lock for balance queries and existence checks; write-lock only to register new clients.
    clients: RwLock<ClientsMap>,

    /// Document numbers of all registered clients, kept in sync with `clients`.
    /// Enables faster duplicate detection without scanning the whole registry.
    registered_documents: RwLock<HashSet<Document>>,

    /// Unique document numbers currently undergoing registration.
    /// Prevents duplicate sign-up race conditions before they are persisted.
    pending_registrations: Mutex<HashSet<Document>>,

    /// Client IDs with balance changes waiting to be flushed.
    dirty_client_ids: Mutex<HashSet<Uuid>>,

    /// Guards writing operations to prevent races.
    pub persistence_lock: Mutex<()>,
}

impl Cache {
    /// Builds a cache from a hydrated clients map and the latest ledger nonce.
    pub fn new(clients: ClientsMap, nonce: i32) -> Self {
        let registered_documents = clients
            .values()
            .map(|client| client.details.document_number.clone())
            .collect();

        Cache {
            clients: RwLock::new(clients),
            registered_documents: RwLock::new(registered_documents),
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
        self.registered_documents
            .read()
            .await
            .contains(document_number)
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

    /// Inserts a newly registered client, seeding it with a zero balance and
    /// recording its document number in the duplicate-detection index.
    pub async fn insert_client(&self, client: &Client) {
        let mut clients = self.clients.write().await;
        let mut documents = self.registered_documents.write().await;

        documents.insert(client.details.document_number.clone());
        clients.insert(
            client.client_id,
            ClientState {
                details: client.details.clone(),
                balance: Mutex::new(Decimal::ZERO),
            },
        );
    }
}

// Balance operations
impl Cache {
    /// Applies a credit or debit to a client's balance and returns the resulting
    /// balance. Debits are always accepted and may drive the balance negative.
    pub async fn process_transaction(
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

    /// Returns the client's details and current balance.
    pub async fn get_client(&self, client_id: Uuid) -> Result<(ClientDetails, Decimal), AppError> {
        let clients = self.clients.read().await;
        let client_state = clients.get(&client_id).ok_or(AppError::ClientNotFound)?;

        let balance = *client_state.balance.lock().await;
        Ok((client_state.details.clone(), balance))
    }
}

// Persistence / flush
impl Cache {
    /// Snapshots the current balance of every dirty client.
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

    /// Flushes dirty clients balances, subtracting the persisted amount (rather than hard-setting to
    /// zero), which preserves any transaction that landed after the snapshot was taken;
    /// such a client stays dirty for the next flush.
    pub async fn flush_balances(&self, persisted_balances: &[(Uuid, Decimal)]) {
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
