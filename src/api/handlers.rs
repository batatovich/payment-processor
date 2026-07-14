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

    // Set document in-flight to mark that this client is being created and has yet to be persisted to storage.
    // The cache will only reflect the new client once its safely saved to storage.
    cache.set_document_in_flight(document_number).await?;

    // New Client
    let new_client = Client::new(body);

    // Write the new client to storage
    // If this returns an error, the client document will be removed from the in_flight cache
    // and never added to the clients cache
    if let Err(e) = storage::save_client_to_storage(&new_client).await {
        cache.remove_document_in_flight(document_number).await?;
        return Err(e);
    }

    // Add the new client to the clients cache
    cache.insert_client(&new_client).await;

    // Remove new client document from in-flight cache
    cache.remove_document_in_flight(document_number).await?;

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

    // Snapshot which clients have balance changes to save
    let dirty_clients = cache.get_dirty_clients().await?;

    // Early return if no news
    if dirty_clients.is_empty() {
        return Ok(HttpResponse::Ok().finish());
    }
    let balance_changes = cache.collect_batch_deltas(&dirty_clients).await?;

    let nonce = cache.get_nonce() + 1;

    // Write to file.
    // If anything here fails, the cache was never modified and we could retry the operation if needed.
    storage::save_balance_changes(nonce, &balance_changes).await?;

    cache.apply_persisted_deltas(&balance_changes).await?;

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

    let (document_number, balance) = cache.get_client_snapshot(client_id).await?;

    Ok(HttpResponse::Ok().json(GetBalanceResponse {
        client_id,
        document_number,
        balance,
    }))
}
