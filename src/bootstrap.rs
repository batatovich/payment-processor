use crate::cache::{Cache, ClientState, ClientsMap};
use crate::constants::{DATA_DIR, FILE_CLIENTS_METADATA};
use crate::error::AppError;
use crate::model::Client;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use tokio::sync::Mutex;

/// Bootstrapping routine. Runs each time the server starts.
///
/// Client balances always start at zero on boot.
/// The existing balance files are scanned only to recover the next nonce so new
/// writes don't collide with prior ones.
pub fn run() -> Result<Cache, AppError> {
    println!("Bootstrapping system state...");
    let path = Path::new(DATA_DIR);
    let clients_path = path.join(FILE_CLIENTS_METADATA);

    init_directory(path, &clients_path)?;

    let balance_files = scan_balance_files(path)?;
    let current_nonce = validate_nonce_sequence(balance_files.keys().copied().collect())?;

    let clients_map = hydrate_clients(&clients_path)?;
    println!("✅ [BOOTSTRAP SUCCESS]");
    Ok(Cache::new(clients_map, current_nonce as i32))
}

/// Ensures the data directory and an empty clients file exist.
fn init_directory(base_path: &Path, clients_path: &Path) -> Result<(), AppError> {
    fs::create_dir_all(base_path)
        .map_err(|e| AppError::Bootstrap(format!("Failed to create data directory: {e}")))?;

    if !clients_path.exists() {
        fs::write(clients_path, "").map_err(|e| {
            AppError::Bootstrap(format!("Failed to initialize {FILE_CLIENTS_METADATA}: {e}"))
        })?;
        println!("✅ [DATA DIRECTORY INITIALIZED]");
    }
    Ok(())
}

/// Single-pass directory scan collecting balance files by nonce. Any orphan
/// `.tmp` file (an interrupted write) aborts boot.
fn scan_balance_files(path: &Path) -> Result<HashMap<u32, PathBuf>, AppError> {
    let entries = fs::read_dir(path)
        .map_err(|e| AppError::Bootstrap(format!("Failed to read data directory: {e}")))?;

    let mut balance_files = HashMap::new();

    for entry in entries.flatten() {
        let p = entry.path();
        if !p.is_file() {
            continue;
        }

        let extension = p.extension().and_then(|s| s.to_str()).unwrap_or("");

        if extension.eq_ignore_ascii_case("tmp") {
            return Err(AppError::Bootstrap(
                "❌ [CRITICAL ERROR] Found orphan .tmp file.".to_string(),
            ));
        }

        if !extension.eq_ignore_ascii_case("dat") {
            continue;
        }

        if let Some(nonce) = extract_nonce_from_filename(&p)
            && balance_files.insert(nonce, p).is_some()
        {
            return Err(AppError::Bootstrap(format!(
                "❌ [CRITICAL ERROR] Duplicate balance file nonce {nonce}."
            )));
        }
    }

    Ok(balance_files)
}

/// Validates that nonces form an unbroken, non-duplicate `1..=N` sequence.
/// Returns `N`, or `0` if there are no balance files yet.
fn validate_nonce_sequence(mut nonces: Vec<u32>) -> Result<u32, AppError> {
    nonces.sort_unstable();

    for (index, &nonce) in nonces.iter().enumerate() {
        let expected = (index + 1) as u32;
        if nonce != expected {
            return Err(AppError::Bootstrap(format!(
                "❌ [CRITICAL ERROR] Broken balance file sequence: expected nonce {expected}, found {nonce}."
            )));
        }
    }

    Ok(nonces.last().copied().unwrap_or(0))
}

/// Reads the append-only client metadata ledger (name, document, etc) and builds
/// the in-memory cache map, seeding every client with a zero balance — balances
/// always start at zero on boot.
fn hydrate_clients(clients_path: &Path) -> Result<ClientsMap, AppError> {
    let content = fs::read_to_string(clients_path)
        .map_err(|e| AppError::Bootstrap(format!("Failed to read clients from storage: {e}")))?;

    let mut clients = ClientsMap::new();
    for (idx, line) in content.lines().map(str::trim).enumerate() {
        if line.is_empty() {
            continue;
        }

        let record: Client = serde_json::from_str(line).map_err(|e| {
            AppError::Bootstrap(format!("Corrupted metadata at line {}: {e}", idx + 1))
        })?;

        clients.insert(
            record.client_id,
            ClientState {
                details: record.details,
                balance: Mutex::new(Decimal::ZERO),
            },
        );
    }

    Ok(clients)
}

/// Extracts the nonce from a balance filename matching `ddmmyyyy_<nonce>.dat`.
fn extract_nonce_from_filename(file_path: &Path) -> Option<u32> {
    let stem = file_path.file_stem()?.to_str()?;
    let (date_str, counter_str) = stem.split_once('_')?;
    chrono::NaiveDate::parse_from_str(date_str, "%d%m%Y").ok()?;
    counter_str.parse::<u32>().ok()
}
