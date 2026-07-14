use crate::cache::{Cache, ClientBalances, ClientState, ClientsMap};
use crate::constants::{CHECKPOINT_HEADER_PREFIX, DATA_DIR, FILE_CLIENTS_METADATA};
use crate::error::AppError;
use crate::model::Client;
use rust_decimal::{Decimal, dec};
use std::collections::HashMap;
use std::fmt::Write;
use std::fs;
use std::path::{Path, PathBuf};
use tokio::sync::Mutex;
use uuid::Uuid;

/// Bootstrapping routine. Runs each time the server starts.
///
/// Hydrates in-memory cache balances by loading the baseline checkpoint (`clients.dat`)
/// and replaying any subsequent balance change files ("deltas") on top.
pub fn run() -> Result<Cache, AppError> {
    println!("Bootstrapping system state...");
    let path = Path::new(DATA_DIR);
    let clients_path = path.join(FILE_CLIENTS_METADATA);

    init_directory(path, &clients_path)?;

    let delta_files = collect_delta_files(path)?;
    let latest_nonce = validate_nonce_sequence(delta_files.keys().copied().collect())?;

    let (mut clients, checkpoint_nonce) = hydrate_clients(&clients_path)?;

    // If there are newer deltas on disk than what the checkpoint has folded in,
    // apply them, update the checkpoint baseline, and rewrite it atomically.
    if latest_nonce > checkpoint_nonce {
        replay_deltas(&mut clients, &delta_files, checkpoint_nonce, latest_nonce)?;
        update_client_balances(&clients_path, clients.values(), latest_nonce)?;
        println!("✅ [CHECKPOINT ADVANCED nonce {checkpoint_nonce} -> {latest_nonce}]");
    }

    // Convert into active cache registry
    let clients_map: ClientsMap = clients
        .into_iter()
        .map(|(id, client)| {
            (
                id,
                ClientState {
                    document: client.details.document_number.clone(),
                    balances: Mutex::new(ClientBalances {
                        settled_balance: client.balance,
                        delta_balance: dec!(0),
                    }),
                },
            )
        })
        .collect();

    println!("✅ [BOOTSTRAP SUCCESS]");
    Ok(Cache::new(clients_map, latest_nonce as i32))
}

/// Initializes data directory and empty clients baseline metadata.
fn init_directory(base_path: &Path, clients_path: &Path) -> Result<(), AppError> {
    if !base_path.exists() {
        fs::create_dir_all(base_path)
            .map_err(|e| AppError::Bootstrap(format!("Failed to create data directory: {e}")))?;
        fs::write(clients_path, format!("{CHECKPOINT_HEADER_PREFIX}0\n")).map_err(|e| {
            AppError::Bootstrap(format!("Failed to initialize {FILE_CLIENTS_METADATA}: {e}"))
        })?;
        println!("✅ [DATA DIRECTORY INITIALIZED]");
    }
    Ok(())
}

/// Scans the directory for valid `ddmmyyyy_<nonce>.dat` delta files and catches
/// interrupted write remnants (orphan .tmp files).
fn collect_delta_files(path: &Path) -> Result<HashMap<u32, PathBuf>, AppError> {
    let entries = fs::read_dir(path)
        .map_err(|e| AppError::Bootstrap(format!("Failed to read data directory: {e}")))?;

    let mut delta_files = HashMap::new();

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
            if let Some(nonce) = extract_nonce_from_filename(&p) {
                if delta_files.insert(nonce, p).is_some() {
                    return Err(AppError::Bootstrap(format!(
                        "❌ [CRITICAL ERROR] Duplicate delta nonce {nonce}."
                    )));
                }
            }
        }
    }

    Ok(delta_files)
}

/// Validates that delta nonces form an unbroken sequence from 1 to N.
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

/// Reads the baseline clients checkpoint file and returns current balances + folded nonce.
fn hydrate_clients(clients_path: &Path) -> Result<(HashMap<Uuid, Client>, u32), AppError> {
    let content = fs::read_to_string(clients_path)
        .map_err(|e| AppError::Bootstrap(format!("Failed to read clients from storage: {e}")))?;

    let mut clients = HashMap::new();
    let mut checkpoint_nonce = 0;

    for (idx, line) in content.lines().map(str::trim).enumerate() {
        if line.is_empty() {
            continue;
        }

        if let Some(rest) = line.strip_prefix(CHECKPOINT_HEADER_PREFIX) {
            checkpoint_nonce = rest.trim().parse::<u32>().map_err(|e| {
                AppError::Bootstrap(format!(
                    "Invalid checkpoint header at line {}: {e}",
                    idx + 1
                ))
            })?;
            continue;
        }

        let client: Client = serde_json::from_str(line).map_err(|e| {
            AppError::Bootstrap(format!(
                "Corrupted record inside clients storage at line {}: {e}",
                idx + 1
            ))
        })?;

        clients.insert(client.client_id, client);
    }

    Ok((clients, checkpoint_nonce))
}

/// Replays balance change lines in sequence, updating client balances.
fn replay_deltas(
    clients: &mut HashMap<Uuid, Client>,
    delta_files: &HashMap<u32, PathBuf>,
    checkpoint_nonce: u32,
    latest_nonce: u32,
) -> Result<(), AppError> {
    for nonce in (checkpoint_nonce + 1)..=latest_nonce {
        let path = delta_files
            .get(&nonce)
            .ok_or_else(|| AppError::Bootstrap(format!("Missing delta file for nonce {nonce}")))?;

        let content = fs::read_to_string(path).map_err(|e| {
            AppError::Bootstrap(format!("Failed to read delta file for nonce {nonce}: {e}"))
        })?;

        for (idx, line) in content.lines().map(str::trim).enumerate() {
            if line.is_empty() {
                continue;
            }

            let (id_str, delta_str) = line.split_once(' ').ok_or_else(|| {
                AppError::Bootstrap(format!(
                    "Malformed delta entry at {}:{}",
                    path.display(),
                    idx + 1
                ))
            })?;

            let client_id = Uuid::parse_str(id_str.trim()).map_err(|e| {
                AppError::Bootstrap(format!(
                    "Invalid client ID in delta file {}:{}: {e}",
                    path.display(),
                    idx + 1
                ))
            })?;

            let delta = Decimal::from_str_exact(delta_str.trim()).map_err(|e| {
                AppError::Bootstrap(format!(
                    "Invalid delta amount in file {}:{}: {e}",
                    path.display(),
                    idx + 1
                ))
            })?;

            let client = clients.get_mut(&client_id).ok_or_else(|| {
                AppError::Bootstrap(format!(
                    "Delta file {}:{} references unknown client {client_id}",
                    path.display(),
                    idx + 1
                ))
            })?;

            client.balance += delta;
        }
    }

    Ok(())
}

/// Writes client baseline checkpoint atomically using a tmp file swap.
fn update_client_balances<'a>(
    clients_path: &Path,
    clients: impl Iterator<Item = &'a Client>,
    checkpoint_nonce: u32,
) -> Result<(), AppError> {
    let mut buf = String::new();
    let _ = writeln!(buf, "{CHECKPOINT_HEADER_PREFIX}{checkpoint_nonce}");

    for client in clients {
        let record = serde_json::to_string(client)?;
        buf.push_str(&record);
        buf.push('\n');
    }

    let path_tmp = clients_path.with_file_name(format!("{FILE_CLIENTS_METADATA}.tmp"));
    fs::write(&path_tmp, buf.as_bytes())
        .map_err(|e| AppError::Bootstrap(format!("Failed to write checkpoint: {e}")))?;

    fs::rename(&path_tmp, clients_path)
        .map_err(|e| AppError::Bootstrap(format!("Failed to commit checkpoint: {e}")))?;

    Ok(())
}

fn extract_nonce_from_filename(file_path: &Path) -> Option<u32> {
    let stem = file_path.file_stem()?.to_str()?;
    let (_, counter_str) = stem.split_once('_')?;
    counter_str.parse::<u32>().ok()
}

fn has_extension(path: &Path, ext: &str) -> bool {
    path.extension()
        .and_then(|s| s.to_str())
        .is_some_and(|e| e.eq_ignore_ascii_case(ext))
}