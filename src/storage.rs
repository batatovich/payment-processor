use crate::constants::{DATA_DIR, FILE_CLIENTS_METADATA};
use crate::error::AppError;
use crate::model::Client;
use rust_decimal::Decimal;
use std::fmt::Write;
use std::path::Path;
use tokio::fs;
use tokio::io::{AsyncWriteExt, BufWriter};
use uuid::Uuid;

/// Appends a client to the clients metadata file.
pub async fn save_client_to_storage(client: &Client) -> Result<(), AppError> {
    let mut serialized = serde_json::to_string(client)?;
    serialized.push('\n');

    let file_path = Path::new(DATA_DIR).join(FILE_CLIENTS_METADATA);

    // Open file in append mode
    let mut file = fs::OpenOptions::new()
        .write(true)
        .append(true)
        .open(file_path)
        .await?;

    file.write_all(serialized.as_bytes()).await?;
    file.flush().await?;

    Ok(())
}

/// Writes the balances of dirty clients to storage.
///
/// Filename format: {date}_{nonce}.dat
/// Data format: {client_id} {balance}
///
/// Balances may be negative. The data is written to a .tmp file and then renamed
/// to the final filename to ensure all data is written.
pub async fn save_balances(nonce: i32, balances: &[(Uuid, Decimal)]) -> Result<(), AppError> {
    let mut buf = String::new();
    for (client_id, balance) in balances {
        let _ = writeln!(buf, "{} {}", client_id, balance);
    }

    let now = chrono::Utc::now();
    let file_name = format!("{}_{}.dat", now.format("%d%m%Y"), nonce);
    let file_name_tmp = format!("{file_name}.tmp");

    let data_dir = Path::new(DATA_DIR);
    let path: std::path::PathBuf = data_dir.join(&file_name);
    let path_tmp = data_dir.join(&file_name_tmp);

    // Create the temporary file
    let file_tmp = fs::File::create(&path_tmp).await?;

    // Write the data to the temporary file
    let mut writer = BufWriter::new(file_tmp);
    writer.write_all(buf.as_bytes()).await?;

    // Flush the buffer
    writer.flush().await?;

    // Atomically rename the file
    fs::rename(&path_tmp, &path).await?;

    Ok(())
}
