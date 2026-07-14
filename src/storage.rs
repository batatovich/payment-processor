use crate::constants::{DATA_DIR, FILE_CLIENTS_METADATA};
use crate::error::AppError;
use crate::model::Client;
use rust_decimal::Decimal;
use std::fmt::Write;
use std::fs;
use std::io::{BufWriter, Write as IoWrite};
use std::path::Path;
use tokio::task;
use uuid::Uuid;

/// Appends a single client record to storage.
///
/// Serialization runs on the async task; the blocking file I/O is offloaded to a
/// dedicated blocking thread, so callers can simply `.await` this safely.
pub async fn save_client_to_storage(client: &Client) -> Result<(), AppError> {
    // Serialize the `Client` struct (cheap, in-memory work)
    let mut serialized = serde_json::to_string(client)?;
    serialized.push('\n');

    let file_path = Path::new(DATA_DIR).join(FILE_CLIENTS_METADATA);

    // Offload the blocking write onto a dedicated thread.
    task::spawn_blocking(move || {
        // Open file in append mode
        let mut file = fs::OpenOptions::new()
            .write(true)
            .append(true)
            .open(file_path)?;

        file.write_all(serialized.as_bytes())?;
        file.flush()?;

        Ok(())
    })
    .await?
}

/// Writes balance changes atomically using a temporary file.
pub async fn save_balance_changes(
    nonce: i32,
    balance_changes: &Vec<(Uuid, Decimal)>,
) -> Result<(), AppError> {
    // Build the full payload up front
    let mut buf = String::new();
    for (client_id, delta) in balance_changes {
        let _ = writeln!(buf, "{} {}", client_id, delta);
    }

    let now = chrono::Utc::now();
    let file_name = format!("{}_{}.dat", now.format("%d%m%Y"), nonce);
    let file_name_tmp = format!("{file_name}.tmp");

    let data_dir = Path::new(DATA_DIR);
    let path: std::path::PathBuf = data_dir.join(&file_name);
    let path_tmp = data_dir.join(&file_name_tmp);

    // Offload the write to a dedicated blocking thread.
    task::spawn_blocking(move || -> Result<(), AppError> {
        let file_tmp = fs::File::create(&path_tmp)?;

        // Use BufWriter to keep disk writes batch-buffered in memory
        let mut writer = BufWriter::new(file_tmp);
        writer.write_all(buf.as_bytes())?;

        // Flush the remaining buffer to disk
        writer.flush()?;

        // Atomically rename the file
        fs::rename(&path_tmp, &path)?;

        Ok(())
    })
    .await?
}
