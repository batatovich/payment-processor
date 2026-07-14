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

// Client registration
impl Cache {
    /// Returns whether a document number already belongs to a registered client.
    pub async fn is_document_in_use(&self, document_number: &Document) -> bool {
        let clients = self.clients.read().await;
        clients
            .values()
            .any(|state| state.document == *document_number)
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

    /// Inserts a newly registered client, seeding it with its settled balance and a zero delta.
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
}

// Balance operations
impl Cache {
    /// Applies a credit or debit to a client's in-memory delta and returns the resulting
    /// balance. Debits that would overdraw the account are rejected, leaving it untouched.
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
            let mut balances = client_state.balances.lock().await;
            let current = balances.settled_balance + balances.delta_balance;

            match direction {
                TransactionDirection::Credit => {
                    balances.delta_balance += amount;
                    current + amount
                }
                TransactionDirection::Debit => {
                    let projected = current - amount;
                    if projected < dec!(0) {
                        return Err(AppError::InsufficientFunds);
                    }
                    balances.delta_balance -= amount;
                    projected
                }
            }
        };

        self.mark_dirty(client_id).await;
        Ok(new_balance)
    }

    /// Returns the client's document number and current balance (settled + pending delta).
    pub async fn get_client_state(&self, client_id: Uuid) -> Result<(Document, Decimal), AppError> {
        let clients = self.clients.read().await;
        let client_state = clients.get(&client_id).ok_or(AppError::ClientNotFound)?;

        let balances = client_state.balances.lock().await;
        Ok((
            client_state.document.clone(),
            balances.settled_balance + balances.delta_balance,
        ))
    }
}

// Persistence / flush
impl Cache {
    /// Snapshots the pending delta of every dirty client, producing the batch to be
    /// written to the next ledger file.
    pub async fn snapshot_dirty_deltas(&self) -> Vec<(Uuid, Decimal)> {
        // Copy the dirty id set and release its lock before touching client balances.
        let dirty_ids = self.dirty_client_ids.lock().await.clone();

        let clients = self.clients.read().await;
        let mut deltas = Vec::with_capacity(dirty_ids.len());
        for client_id in dirty_ids {
            if let Some(client_state) = clients.get(&client_id) {
                let delta = client_state.balances.lock().await.delta_balance;
                deltas.push((client_id, delta));
            }
        }
        deltas
    }

    /// Folds persisted deltas into each client's settled balance, clearing a client
    /// from the dirty set once its delta is fully settled.
    pub async fn apply_persisted_deltas(&self, persisted_deltas: &[(Uuid, Decimal)]) {
        let clients = self.clients.read().await;
        let mut dirty_ids = self.dirty_client_ids.lock().await;

        for (client_id, delta) in persisted_deltas {
            if let Some(client_state) = clients.get(client_id) {
                // Isolate the balances lock so it releases before the next iteration.
                let fully_settled = {
                    let mut balances = client_state.balances.lock().await;
                    balances.settled_balance += delta;
                    balances.delta_balance -= delta;
                    balances.delta_balance == dec!(0)
                };

                if fully_settled {
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
