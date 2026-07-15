use actix_web::{HttpResponse, Responder, get, post, web};

use crate::api::dto::{
    ClientDetails, GetBalanceRequest, GetBalanceResponse, NewCreditTransaction, NewDebitTransaction,
};
use crate::cache::Cache;
use crate::error::AppError;
use crate::model::{Client, TransactionDirection};
use crate::storage;

/// Registers a new client: rejects duplicate documents, persists the client to
/// storage, and only then adds it to the in-memory cache
#[post("/new_client")]
pub async fn new_client(
    req_body: web::Json<ClientDetails>,
    cache: web::Data<Cache>,
) -> Result<impl Responder, AppError> {
    let body = req_body.into_inner();
    body.validate()?;

    let document_number = body.document_number.clone();

    if cache.is_document_in_use(&document_number).await {
        return Err(AppError::DocumentInUse);
    }

    // Reserve the document so a concurent sign-up on the same number is rejected
    // while this one is still being persisted (it isn't in the cache yet).
    cache.reserve_document(&document_number).await?;

    let client = Client::new(body);

    // Persist first, then publish to the cache. On write failure the reservation
    // is released and the client is never made visible so the call can be retried.
    if let Err(e) = storage::save_client_to_storage(&client).await {
        cache.release_document(&document_number).await;
        return Err(e);
    }

    // Insert the client into the cache
    cache.insert_client(&client).await;

    // Release the document number
    cache.release_document(&document_number).await;

    Ok(HttpResponse::Ok().json(client.client_id.to_string()))
}

/// Flushes the balances of all dirty clients to a storage file, then resets those
/// balances  to zero in memory.
#[post("/store_balances")]
pub async fn store_balances(cache: web::Data<Cache>) -> Result<impl Responder, AppError> {
    // Serializes this whole flow (snapshot, disk write, cache update) against any other
    // concurrent call to this same endpoint. Without it, two overlaping calls
    // could race to write the same data.
    let _store_guard = cache.persistence_lock.lock().await;

    // Snapshot the balances of dirty clients to persist.
    let balances = cache.snapshot_dirty_balances().await;

    // Nothing changed since the last call - avoid writing an empty file and
    // burning a nonce for no reason
    if balances.is_empty() {
        return Ok(HttpResponse::Ok().finish());
    }

    let nonce = cache.get_nonce() + 1;

    // Write first, mutate cache state second: if this fails, nothing in memory
    // has changed yet, so the client can safely retry the call.
    storage::save_balances(nonce, &balances).await?;

    // Subtracts the persisted amounts rather than zeroing outright, so any
    // transaction that landed after the snapshot above stays dirty and is
    // picked up by the next call instead of being silently droped.
    cache.settle_persisted_balances(&balances).await;

    // Only advance the nonce after the write suceeds, so a failed write never
    // leaves a gap in the nonce sequence.
    cache.increment_nonce();

    Ok(HttpResponse::Ok().finish())
}

/// Credits a client's balance and returns the updated balance.
#[post("/new_credit_transaction")]
pub async fn new_credit_transaction(
    req_body: web::Json<NewCreditTransaction>,
    cache: web::Data<Cache>,
) -> Result<impl Responder, AppError> {
    req_body.validate()?;

    let new_balance = cache
        .process_transaction(
            req_body.client_id,
            req_body.credit_amount,
            TransactionDirection::Credit,
        )
        .await?;

    Ok(HttpResponse::Ok().json(new_balance))
}

/// Debits a client's balance (may go negative) and returns the updated balance
#[post("/new_debit_transaction")]
pub async fn new_debit_transaction(
    req_body: web::Json<NewDebitTransaction>,
    cache: web::Data<Cache>,
) -> Result<impl Responder, AppError> {
    req_body.validate()?;

    let new_balance = cache
        .process_transaction(
            req_body.client_id,
            req_body.debit_amount,
            TransactionDirection::Debit,
        )
        .await?;

    Ok(HttpResponse::Ok().json(new_balance))
}

/// Returns a client's document number and current balance.
#[get("/get_balance")]
pub async fn get_balance(
    query: web::Query<GetBalanceRequest>,
    cache: web::Data<Cache>,
) -> Result<impl Responder, AppError> {
    let client_id = query.client_id;

    let (details, balance) = cache.get_client(client_id).await?;

    Ok(HttpResponse::Ok().json(GetBalanceResponse {
        client_id,
        details,
        balance,
    }))
}
