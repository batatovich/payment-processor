use crate::cache::{Cache, ClientBalances, ClientState, ClientsMap};
use crate::constants::{DATA_DIR, FILE_CLIENTS_METADATA};
use crate::error::AppError;
use crate::model::Client;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use tokio::sync::Mutex;
use uuid::Uuid;

/// Bootstrapping routine. Runs each time the server starts.
///
/// Rebuilds balance state from scratch by replaying every delta file in
/// order, then hydrates the in-memory client cache. No checkpointing —
/// simple, at the cost of replaying the full delta history on every boot.
///
/// For a more sophisticated approach, we could use a checkpoint file to only replay the deltas since the last checkpoint.
pub fn run() -> Result<Cache, AppError> {
    println!("Bootstrapping system state...");
    let path = Path::new(DATA_DIR);
    let clients_path = path.join(FILE_CLIENTS_METADATA);

    init_directory(path, &clients_path)?;

    let deltas = scan_delta_files(path)?;
    let current_nonce = validate_nonce_sequence(deltas.keys().copied().collect())?;
    let metadata = hydrate_metadata(&clients_path)?;

    let mut balances = HashMap::new();
    replay_deltas(&mut balances, &deltas, &metadata)?;

    let clients_map = build_clients_map(metadata, &balances);
    println!("✅ [BOOTSTRAP SUCCESS]");
    Ok(Cache::new(clients_map, current_nonce as i32))
}

/// Creates the data directory and an empty clients ledger on first run.
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

/// Single-pass directory scan collecting delta files by nonce. Any orphan
/// `.tmp` file (an interrupted write) aborts boot.
fn scan_delta_files(path: &Path) -> Result<HashMap<u32, PathBuf>, AppError> {
    let entries = fs::read_dir(path)
        .map_err(|e| AppError::Bootstrap(format!("Failed to read data directory: {e}")))?;

    let mut deltas = HashMap::new();

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

        if let Some(nonce) = extract_nonce_from_filename(&p) {
            if deltas.insert(nonce, p).is_some() {
                return Err(AppError::Bootstrap(format!(
                    "❌ [CRITICAL ERROR] Duplicate delta nonce {nonce}."
                )));
            }
        }
    }

    Ok(deltas)
}

/// Validates that nonces form an unbroken, non-duplicate `1..=N` sequence.
/// Returns `N`, or `0` if there are no deltas yet.
fn validate_nonce_sequence(mut nonces: Vec<u32>) -> Result<u32, AppError> {
    nonces.sort_unstable();

    for (index, &nonce) in nonces.iter().enumerate() {
        let expected = (index + 1) as u32;
        if nonce != expected {
            return Err(AppError::Bootstrap(format!(
                "❌ [CRITICAL ERROR] Broken or duplicate sequence chain at index {expected}."
            )));
        }
    }

    Ok(nonces.last().copied().unwrap_or(0))
}

/// Reads the append-only client metadata ledger (name, document, etc).
/// Does not carry balance — balance is sourced entirely from delta files.
fn hydrate_metadata(clients_path: &Path) -> Result<HashMap<Uuid, Client>, AppError> {
    let content = fs::read_to_string(clients_path)
        .map_err(|e| AppError::Bootstrap(format!("Failed to read clients from storage: {e}")))?;

    let mut metadata = HashMap::new();
    for (idx, line) in content.lines().map(str::trim).enumerate() {
        if line.is_empty() {
            continue;
        }

        let record: Client = serde_json::from_str(line).map_err(|e| {
            AppError::Bootstrap(format!("Corrupted metadata at line {}: {e}", idx + 1))
        })?;
        metadata.insert(record.client_id, record);
    }

    Ok(metadata)
}

/// Replays every delta file, accumulating each client's balance from zero.
/// Order doesn't matter here — deltas are summed, and `scan_delta_files` /
/// `validate_nonce_sequence` already guarantee the set has no gaps or dupes.
fn replay_deltas(
    balances: &mut HashMap<Uuid, Decimal>,
    deltas: &HashMap<u32, PathBuf>,
    metadata: &HashMap<Uuid, Client>,
) -> Result<(), AppError> {
    for delta_path in deltas.values() {
        let content = fs::read_to_string(delta_path).map_err(|e| {
            AppError::Bootstrap(format!(
                "Failed to read delta file {}: {e}",
                delta_path.display()
            ))
        })?;

        for (idx, line) in content.lines().map(str::trim).enumerate() {
            if line.is_empty() {
                continue;
            }

            let (client_id, delta) = parse_balance_line(line, delta_path, idx + 1)?;
            if !metadata.contains_key(&client_id) {
                return Err(AppError::Bootstrap(format!(
                    "Delta file {}:{} references unregistered client {client_id}",
                    delta_path.display(),
                    idx + 1
                )));
            }
            *balances.entry(client_id).or_insert(Decimal::ZERO) += delta;
        }
    }

    Ok(())
}

/// Merges client metadata with replayed balances into the final in-memory cache map.
fn build_clients_map(
    metadata: HashMap<Uuid, Client>,
    balances: &HashMap<Uuid, Decimal>,
) -> ClientsMap {
    metadata
        .into_iter()
        .map(|(id, meta)| {
            let settled_balance = balances.get(&id).copied().unwrap_or(Decimal::ZERO);
            (
                id,
                ClientState {
                    document: meta.details.document_number,
                    balances: Mutex::new(ClientBalances {
                        settled_balance,
                        delta_balance: Decimal::ZERO,
                    }),
                },
            )
        })
        .collect()
}

/// Parses a single `<uuid> <decimal>` ledger line.
fn parse_balance_line(
    line: &str,
    source: &Path,
    line_no: usize,
) -> Result<(Uuid, Decimal), AppError> {
    let (id_str, value_str) = line.split_once(' ').ok_or_else(|| {
        AppError::Bootstrap(format!(
            "Malformed entry at {}:{}",
            source.display(),
            line_no
        ))
    })?;

    let client_id = Uuid::parse_str(id_str.trim()).map_err(|e| {
        AppError::Bootstrap(format!(
            "Invalid client ID at {}:{}: {e}",
            source.display(),
            line_no
        ))
    })?;

    let value = Decimal::from_str_exact(value_str.trim()).map_err(|e| {
        AppError::Bootstrap(format!(
            "Invalid decimal value at {}:{}: {e}",
            source.display(),
            line_no
        ))
    })?;

    Ok((client_id, value))
}

/// Extracts the nonce from a delta filename matching `ddmmyyyy_<nonce>.dat`.
fn extract_nonce_from_filename(file_path: &Path) -> Option<u32> {
    let stem = file_path.file_stem()?.to_str()?;
    let (date_str, counter_str) = stem.split_once('_')?;
    chrono::NaiveDate::parse_from_str(date_str, "%d%m%Y").ok()?;
    counter_str.parse::<u32>().ok()
}
