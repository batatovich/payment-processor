use actix_web::{HttpResponse, Responder, get, post, web};

use crate::api::dto::{
    GetBalanceQuery, GetBalanceResponse, NewClientBody, NewCreditTransactionBody,
    NewDebitTransactionBody,
};
use crate::cache::Cache;
use crate::error::AppError;
use crate::model::{Client, TransactionDirection};
use crate::storage;

#[get("/")]
pub async fn index() -> impl Responder {
    let endpoints = [
        ("POST /new_client", "Register a new client"),
        ("POST /new_credit_transaction", "Record a credit deposit"),
        ("POST /new_debit_transaction", "Record a debit withdrawal"),
        ("POST /store_balances", "Persist current client balances"),
        ("GET /get_balance", "Retrieve a client's current balance"),
    ];

    HttpResponse::Ok().json(&endpoints)
}

/// Add a new client to the system. If the document is already in use, an error is returned.
/// If the document is not in use, it is added to the in-flight cache.
/// The client is then written to storage and added to the clients cache.
/// If the write to storage fails, the client is removed from the in-flight cache and an error is returned.
/// If the write to storage succeeds, the client is added to the clients cache.
/// If the write to storage succeeds, the client is added to the clients cache.
#[post("/new_client")]
pub async fn new_client(
    req_body: web::Json<NewClientBody>,
    cache: web::Data<Cache>,
) -> Result<impl Responder, AppError> {
    let body = req_body.into_inner();
    let document_number = &body.document_number.clone();

    // Check if document is already in use
    if cache.is_document_in_use(document_number).await {
        return Err(AppError::DocumentInUse);
    }

    // Reserve the document to mark that this client is being created and has yet to be persisted to storage.
    // The cache will only reflect the new client once its safely saved to storage.
    cache.reserve_document(document_number).await?;

    // New Client
    let new_client = Client::new(body);

    // Write the new client to storage
    // If this returns an error, the reserved document is released
    // and the client is never added to the clients cache
    if let Err(e) = storage::save_client_to_storage(&new_client).await {
        cache.release_document(document_number).await;
        return Err(e);
    }

    // Add the new client to the clients cache
    cache.insert_client(&new_client).await;

    // Release the reserved document now that registration is complete
    cache.release_document(document_number).await;

    // Return the client id
    Ok(HttpResponse::Ok().json(new_client.client_id.to_string()))
}

#[post("/store_balances")]
pub async fn store_balances(cache: web::Data<Cache>) -> Result<impl Responder, AppError> {
    // Lock the store guard.
    // If we receive many requests to store_balances at the same time, we will effectively be making a queue here,
    // waiting for earlier calls to finish writing to storage and udpating dirty clients cache.
    // The lock is released when the function returns,
    // and only there can another task try to store balances again.
    let _store_guard = cache.persistence_lock.lock().await;

    // Snapshot the pending balance changes to persist.
    let balance_changes = cache.snapshot_dirty_deltas().await;

    // Early return if there's nothing new to write.
    if balance_changes.is_empty() {
        return Ok(HttpResponse::Ok().finish());
    }

    let nonce = cache.get_nonce() + 1;

    // Append the ledger file recording this batch of deltas. This is the sole
    // durability point for balance changes between checkpoints: bootstrap replays
    // these ledgers on top of clients.dat, so no full metadata rewrite is needed here.
    // If this fails, the cache was never modified and the operation can be retried.
    storage::save_balance_changes(nonce, &balance_changes).await?;

    cache.apply_persisted_deltas(&balance_changes).await;

    cache.increment_nonce();

    Ok(HttpResponse::Ok().finish())
}

#[post("/new_credit_transaction")]
pub async fn new_credit_transaction(
    req_body: web::Json<NewCreditTransactionBody>,
    cache: web::Data<Cache>,
) -> Result<impl Responder, AppError> {
    let new_balance = cache
        .apply_balance_change(
            req_body.client_id,
            req_body.credit_amount,
            TransactionDirection::Credit,
        )
        .await?;

    Ok(HttpResponse::Ok().json(new_balance))
}

#[post("/new_debit_transaction")]
pub async fn new_debit_transaction(
    req_body: web::Json<NewDebitTransactionBody>,
    cache: web::Data<Cache>,
) -> Result<impl Responder, AppError> {
    let new_balance = cache
        .apply_balance_change(
            req_body.client_id,
            req_body.debit_amount,
            TransactionDirection::Debit,
        )
        .await?;

    Ok(HttpResponse::Ok().json(new_balance))
}

#[get("/get_balance")]
pub async fn get_balance(
    query: web::Query<GetBalanceQuery>,
    cache: web::Data<Cache>,
) -> Result<impl Responder, AppError> {
    let client_id = query.client_id;

    let (document_number, balance) = cache.get_client_state(client_id).await?;

    Ok(HttpResponse::Ok().json(GetBalanceResponse {
        client_id,
        document_number,
        balance,
    }))
}
