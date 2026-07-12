use chrono::NaiveDate;
use std::collections::{HashMap, HashSet};
use std::fs;
// use std::io::Write;
use std::path::Path;

use crate::constants::{DATA_DIR, FILE_CLIENTS_METADATA, FILE_LAST_NONCE};
use crate::domain::cache::Cache;
use crate::domain::client::{Client, Document};

use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

/// Appends a single client record to the clients storage asynchronously
pub async fn save_client_to_storage(client: &Client) -> Result<(), String> {
    // Serialize the `Client` struct
    let mut serialized =
        serde_json::to_string(client).map_err(|e| format!("Failed to serialize client: {e}"))?;

    serialized.push('\n');

    // Resolve target path securely
    let file_path = Path::new(DATA_DIR).join(FILE_CLIENTS_METADATA);

    // Open the file in append mode
    let mut file = OpenOptions::new()
        .write(true)
        .append(true)
        .open(file_path)
        .await
        .map_err(|e| format!("Failed to open clients file: {e}"))?;

    // Write the client to the file
    file.write_all(serialized.as_bytes())
        .await
        .map_err(|e| format!("Failed to append new client: {e}"))?;

    file.flush()
        .await
        .map_err(|e| format!("Failed to flush file buffers: {e}"))?;

    Ok(())
}

/// Boostraping function. It runs each time the server starts.
/// It handles cache initialization and sanity checks.
pub fn bootstrap() -> Result<Cache, String> {
    println!("🔍 Running system sanity checks...");
    let path = Path::new(DATA_DIR);
    let nonce_path = path.join(FILE_LAST_NONCE);
    let clients_path = path.join(FILE_CLIENTS_METADATA);

    // Ensure directory exists and has required files
    verify_or_init_directory(path, &nonce_path, &clients_path)?;

    // Read nonce from file
    let last_nonce = fs::read_to_string(&nonce_path)
        .map_err(|e| format!("Failed to read {:?}: {e}", nonce_path))?
        .trim()
        .parse::<i32>()
        .map_err(|_| "Invalid integer format reading last nonce from storage".to_string())?;

    // Nonce Sanity checks
    nonce_sanity_checks(path, last_nonce)?;

    // Hydrate clients mapping
    let mut clients_map = HashMap::new();
    let content = fs::read_to_string(&clients_path)
        .map_err(|e| format!("Failed to read clients from storage: {e}"))?;

    for (idx, line) in content.lines().map(|l| l.trim()).enumerate() {
        if !line.is_empty() {
            let client: Client = serde_json::from_str(line).map_err(|e| {
                format!(
                    "Corrupted record inside clients storage at line {}: {e}",
                    idx + 1
                )
            })?;

            // (document, mutex(balance, delta))
            let value = (
                client.document_number as Document,
                std::sync::Mutex::new((client.balance, 0.0)),
            );

            clients_map.insert(client.client_id, value);
        }
    }

    println!("✅ [BOOTSTRAP SUCCESS]",);

    // Construimos la instancia definitiva de Cache envolviendo las estructuras
    Ok(Cache {
        clients: tokio::sync::RwLock::new(clients_map),
        nonce: std::sync::atomic::AtomicI32::new(last_nonce),
        in_flight: tokio::sync::RwLock::new(HashSet::new()),
    })
}

/// Verifies co-existence of core tracking files or initializes a clean structure.
fn verify_or_init_directory(
    base_path: &Path,
    nonce_path: &Path,
    clients_path: &Path,
) -> Result<(), String> {
    if !base_path.exists() {
        fs::create_dir_all(base_path)
            .map_err(|e| format!("Failed to create data directory: {e}"))?;
        fs::write(nonce_path, "0")
            .map_err(|e| format!("Failed to initialize {}: {e}", FILE_LAST_NONCE))?;
        fs::write(clients_path, "")
            .map_err(|e| format!("Failed to initialize {}: {e}", FILE_CLIENTS_METADATA))?;
        println!("No historical storage detected. Clean directory layout initialized.");
    } else {
        if !nonce_path.exists() || !clients_path.exists() {
            return Err(
                "❌ [CRITICAL] Structural system layout breach: Control files missing.".to_string(),
            );
        }
    }

    Ok(())
}
/// Performs nonce related sanity checks
fn nonce_sanity_checks(path: &Path, last_nonce: i32) -> Result<(), String> {
    let mut detected_nonces = HashSet::with_capacity(last_nonce.max(0) as usize);
    let entries = fs::read_dir(path).map_err(|e| format!("Failed to read data directory: {e}"))?;

    // Parse entries
    for entry in entries.flatten().filter(|e| e.path().is_file()) {
        let file_path = entry.path();
        let file_ext = file_path
            .extension()
            .and_then(|s| s.to_str())
            .map(|s| s.to_lowercase());

        match file_ext.as_deref() {
            // Uncommitted writes must pause initialization to prevent data corruption
            Some("tmp") => {
                return Err("❌ [CRITICAL ERROR] Found orphan .tmp file.".to_string());
            }

            Some("dat") => {
                // Extract, validate, and track ledger nonces
                if let Some(counter) = file_path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|name| name.split_once('_'))
                    .and_then(|(date_str, counter_str)| {
                        let is_date = NaiveDate::parse_from_str(date_str, "%d%m%Y").is_ok();
                        let counter = counter_str.parse::<u32>().ok()?;
                        is_date.then_some(counter)
                    })
                {
                    detected_nonces.insert(counter);
                }
            }
            _ => {}
        }
    }

    // last_nonce should match the number of files with format ddmmyyyy_counter.dat
    if last_nonce != detected_nonces.len() as i32 {
        return Err("❌ [CRITICAL ERROR] NONCE_MISMATCH: Discovered file count mismatch against logical index.".to_string());
    }

    // nonces should be in sequence from 1 to last_nonce
    for i in 1..=last_nonce {
        if !detected_nonces.contains(&(i as u32)) {
            return Err(format!(
                "❌ [CRITICAL ERROR] NONCE_MISMATCH: Broken sequential sequence chain at ID {i}."
            ));
        }
    }
    Ok(())
}
