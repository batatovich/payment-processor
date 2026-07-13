use crate::constants::{DATA_DIR, FILE_CLIENTS_METADATA};
use crate::domain::client::Client;
use crate::domain::error::AppError;
use rust_decimal::Decimal;
use std::path::Path;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

/// Appends a single client record to the clients storage asynchronously
pub async fn save_client_to_storage(client: &Client) -> Result<(), AppError> {
    // Serialize the `Client` struct
    let mut serialized = serde_json::to_string(client)?;

    serialized.push('\n');

    // Resolve target path securely
    let file_path = Path::new(DATA_DIR).join(FILE_CLIENTS_METADATA);

    // Open the file in append mode
    let mut file = fs::OpenOptions::new()
        .write(true)
        .append(true)
        .open(file_path)
        .await?;

    // Write the client to the file
    file.write_all(serialized.as_bytes()).await?;

    file.flush().await?;

    Ok(())
}

pub async fn save_balance_changes(
    nonce: i32,
    deltas_to_write: &Vec<(Uuid, Decimal)>,
) -> Result<(), AppError> {
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
    let mut file_tmp = fs::File::create(&path_tmp).await?;
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
