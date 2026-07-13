use crate::constants::{DATA_DIR, FILE_CLIENTS_METADATA, FILE_LAST_NONCE};
use crate::domain::cache::Cache;
use crate::domain::client::{Client, Document};
use crate::domain::error::AppError;
use crate::storage::{nonce_sanity_checks, verify_or_init_directory};
use rust_decimal::dec;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;

/// Boostraping function. It runs each time the server starts.
/// It handles cache initialization and sanity checks.
pub fn bootstrap() -> Result<Cache, AppError> {
    println!("🔍 Running system sanity checks...");
    let path = Path::new(DATA_DIR);
    let nonce_path = path.join(FILE_LAST_NONCE);
    let clients_path = path.join(FILE_CLIENTS_METADATA);

    // Ensure directory exists and has required files
    verify_or_init_directory(path, &nonce_path, &clients_path)?;

    // Read nonce from file
    let last_nonce = fs::read_to_string(&nonce_path)
        .map_err(|e| AppError::Bootstrap(format!("Failed to read {nonce_path:?}: {e}")))?
        .trim()
        .parse::<i32>()
        .map_err(|_| {
            AppError::Bootstrap("Invalid integer format reading last nonce from storage".to_string())
        })?;

    // Nonce Sanity checks
    nonce_sanity_checks(path, last_nonce)?;

    // Hydrate clients mapping
    let mut clients_map = HashMap::new();
    let content = fs::read_to_string(&clients_path)
        .map_err(|e| AppError::Bootstrap(format!("Failed to read clients from storage: {e}")))?;

    for (idx, line) in content.lines().map(|l| l.trim()).enumerate() {
        if !line.is_empty() {
            let client: Client = serde_json::from_str(line).map_err(|e| {
                AppError::Bootstrap(format!(
                    "Corrupted record inside clients storage at line {}: {e}",
                    idx + 1
                ))
            })?;

            // (document, mutex(balance, delta))
            let value = (
                client.document_number as Document,
                std::sync::Mutex::new((client.balance, dec!(0))),
            );

            clients_map.insert(client.client_id, value);
        }
    }

    println!("✅ [BOOTSTRAP SUCCESS]",);

    Ok(Cache {
        clients: tokio::sync::RwLock::new(clients_map),
        nonce: std::sync::atomic::AtomicI32::new(last_nonce),
        in_flight: std::sync::Mutex::new(HashSet::new()),
        dirty_clients: std::sync::Mutex::new(HashSet::new()),
        store_lock: tokio::sync::Mutex::new(()),
    })
}
