use crate::cache::Cache;
use crate::cache::ClientsMap;
use crate::constants::{DATA_DIR, FILE_CLIENTS_METADATA};
use crate::error::AppError;
use crate::model::{Client, Document};
use rust_decimal::dec;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use tokio::sync::Mutex;

/// Bootstrapping routine. Runs each time the server starts.
pub fn run() -> Result<Cache, AppError> {
    println!("Bootstrapping...");
    let path = Path::new(DATA_DIR);
    let clients_path = path.join(FILE_CLIENTS_METADATA);

    init_directory(path, &clients_path)?;

    let nonces = collect_nonces(path)?;
    let current_nonce = validate_nonce_sequence(nonces)?;
    let clients_map = hydrate_clients(&clients_path)?;

    println!("✅ [BOOTSTRAP SUCCESS]");
    Ok(Cache::new(clients_map, current_nonce as i32))
}

/// Initializes the data directory and an empty clients ledger on first run.
fn init_directory(base_path: &Path, clients_path: &Path) -> Result<(), AppError> {
    if !base_path.exists() {
        fs::create_dir_all(base_path)
            .map_err(|e| AppError::Bootstrap(format!("Failed to create data directory: {e}")))?;
        fs::write(clients_path, "").map_err(|e| {
            AppError::Bootstrap(format!("Failed to initialize {FILE_CLIENTS_METADATA}: {e}"))
        })?;
        println!("✅ [DATA DIRECTORY INITIALIZED]");
    }
    Ok(())
}

/// Single-pass directory scanner. Rejects orphan `.tmp` files and collects
/// nonces from valid `ddmmyyyy_<nonce>.dat` filenames.
fn collect_nonces(path: &Path) -> Result<Vec<u32>, AppError> {
    let entries = fs::read_dir(path)
        .map_err(|e| AppError::Bootstrap(format!("Failed to read data directory: {e}")))?;

    let mut nonces = Vec::new();

    for entry in entries.flatten() {
        let p = entry.path();
        if !p.is_file() {
            continue;
        }

        if has_extension(&p, "tmp") {
            return Err(AppError::Bootstrap(
                "❌ [CRITICAL ERROR] Found orphan .tmp file.".to_string(),
            ));
        }

        if has_extension(&p, "dat") {
            nonces.extend(extract_nonce_from_filename(&p));
        }
    }

    Ok(nonces)
}

/// Validates that nonces form an unbroken sequence from 1 to N, returning N.
/// An empty input (fresh system) resolves to `0`.
fn validate_nonce_sequence(mut nonces: Vec<u32>) -> Result<u32, AppError> {
    nonces.sort_unstable();

    for (index, &nonce) in nonces.iter().enumerate() {
        let expected = (index + 1) as u32;
        if nonce != expected {
            return Err(AppError::Bootstrap(format!(
                "❌ [CRITICAL ERROR] Broken or duplicate nonce sequence at index {expected}."
            )));
        }
    }

    Ok(nonces.last().copied().unwrap_or(0))
}

/// Reads the clients ledger and hydrates it into an in-memory map.
fn hydrate_clients(clients_path: &Path) -> Result<ClientsMap, AppError> {
    let content = fs::read_to_string(clients_path)
        .map_err(|e| AppError::Bootstrap(format!("Failed to read clients from storage: {e}")))?;

    let mut clients_map = HashMap::new();
    for (idx, line) in content.lines().map(str::trim).enumerate() {
        if line.is_empty() {
            continue;
        }

        let client: Client = serde_json::from_str(line).map_err(|e| {
            AppError::Bootstrap(format!(
                "Corrupted record inside clients storage at line {}: {e}",
                idx + 1
            ))
        })?;

        clients_map.insert(
            client.client_id,
            (
                client.document_number as Document,
                Mutex::new((client.balance, dec!(0))),
            ),
        );
    }

    Ok(clients_map)
}

/// Explicitly extracts nonces ONLY from filenames that match the pattern `ddmmyyyy_<nonce>.dat`
fn extract_nonce_from_filename(file_path: &Path) -> Option<u32> {
    let stem = file_path.file_stem()?.to_str()?;
    let (date_str, counter_str) = stem.split_once('_')?;

    // Validates both the date format and that the counter is a valid u32
    chrono::NaiveDate::parse_from_str(date_str, "%d%m%Y").ok()?;
    counter_str.parse::<u32>().ok()
}

fn has_extension(path: &Path, ext: &str) -> bool {
    path.extension()
        .and_then(|s| s.to_str())
        .is_some_and(|e| e.eq_ignore_ascii_case(ext))
}
