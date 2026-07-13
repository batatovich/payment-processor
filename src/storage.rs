use crate::constants::{DATA_DIR, FILE_CLIENTS_METADATA, FILE_LAST_NONCE};
use crate::domain::client::Client;
use rust_decimal::Decimal;
use std::path::Path;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

/// Appends a single client record to the clients storage asynchronously
pub async fn save_client_to_storage(client: &Client) -> Result<(), String> {
    // Serialize the `Client` struct
    let mut serialized =
        serde_json::to_string(client).map_err(|e| format!("Failed to serialize client: {e}"))?;

    serialized.push('\n');

    // Resolve target path securely
    let file_path = Path::new(DATA_DIR).join(FILE_CLIENTS_METADATA);

    // Open the file in append mode
    let mut file = fs::OpenOptions::new()
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

pub async fn save_balance_changes(
    nonce: i32,
    deltas_to_write: &Vec<(Uuid, Decimal)>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Write to file.
    // If anything here fails, the cache was never modified and we could retry the operation if needed.
    let now = chrono::Utc::now();
    let file_name_final = format!("{}_{}.dat", now.format("%d%m%Y"), nonce);
    let file_name_tmp = format!("{file_name_final}.tmp");
    let data_dir = Path::new(DATA_DIR);
    let path_tmp = data_dir.join(&file_name_tmp);
    let path_final = data_dir.join(&file_name_final);

    // First dump everything to a .tmp file. Once this succeeds, we strip the .tmp.
    // This way we mimic atomicity: either all the deltas are properly persisted, or we are sure to get an error.
    // We use the async tokio::fs module to avoid blocking the thread will we wait to write.
    let mut file_tmp = fs::File::create(&path_tmp)
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;
    for (client_id, delta) in deltas_to_write {
        let line = format!("{} {}\n", client_id, delta);
        file_tmp.write_all(line.as_bytes()).await?
    }

    // Flush buffers
    file_tmp.flush().await?;

    // Rename
    fs::rename(&path_tmp, &path_final).await?;

    Ok(())
}

/// Verifies co-existence of core tracking files or initializes a clean structure.
pub fn verify_or_init_directory(
    base_path: &Path,
    nonce_path: &Path,
    clients_path: &Path,
) -> Result<(), String> {
    if !base_path.exists() {
        std::fs::create_dir_all(base_path)
            .map_err(|e| format!("Failed to create data directory: {e}"))?;
        std::fs::write(nonce_path, "0")
            .map_err(|e| format!("Failed to initialize {}: {e}", FILE_LAST_NONCE))?;
        std::fs::write(clients_path, "")
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

/// Nonce sanity checks
pub fn nonce_sanity_checks(path: &Path, last_nonce: i32) -> Result<(), String> {
    let mut detected_nonces = std::collections::HashSet::with_capacity(last_nonce.max(0) as usize);
    let entries =
        std::fs::read_dir(path).map_err(|e| format!("Failed to read data directory: {e}"))?;

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
                        let is_date = chrono::NaiveDate::parse_from_str(date_str, "%d%m%Y").is_ok();
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
