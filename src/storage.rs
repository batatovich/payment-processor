use crate::constants::{DATA_DIR, FILE_CLIENTS_METADATA};
use crate::error::AppError;
use crate::model::Client;
use rust_decimal::Decimal;
use std::fmt::Write;
use std::path::Path;
use tokio::fs;
use tokio::io::{AsyncWriteExt, BufWriter};
use uuid::Uuid;

/// Appends a client as a JSON line to the clients metadata ledger.
pub async fn save_client_to_storage(client: &Client) -> Result<(), AppError> {
    let mut serialized = serde_json::to_string(client)?;
    serialized.push('\n');

    let file_path = Path::new(DATA_DIR).join(FILE_CLIENTS_METADATA);

    let mut file = fs::OpenOptions::new().append(true).open(file_path).await?;

    file.write_all(serialized.as_bytes()).await?;
    file.sync_all().await?;

    Ok(())
}

/// Persists the balances of dirty clients to a nonce-stamped file.
///
/// Filename format: `{ddmmyyyy}_{nonce}.dat`
/// Line format: `{client_id} {balance}` (balances may be negative)
///
/// Data is written to a `.tmp` file, fsynced, and then atomically renamed into
/// place so a crash can never leave a partially written balance file behind.
pub async fn save_balances(nonce: i32, balances: &[(Uuid, Decimal)]) -> Result<(), AppError> {
    let mut buf = String::new();
    for (client_id, balance) in balances {
        let _ = writeln!(buf, "{client_id} {balance}");
    }

    let file_name = format!("{}_{}.dat", chrono::Utc::now().format("%d%m%Y"), nonce);
    let data_dir = Path::new(DATA_DIR);
    let path = data_dir.join(&file_name);
    let path_tmp = data_dir.join(format!("{file_name}.tmp"));

    let mut writer = BufWriter::new(fs::File::create(&path_tmp).await?);
    writer.write_all(buf.as_bytes()).await?;
    writer.flush().await?;
    writer.get_ref().sync_all().await?;

    fs::rename(&path_tmp, &path).await?;

    Ok(())
}
