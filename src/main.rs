mod constants;
mod domain;
mod storage;
mod utils;

use actix_web::{App, HttpResponse, HttpServer, Responder, Result, get, post, web};
use std::sync::atomic::Ordering::Relaxed;

use crate::domain::cache::Cache;
use crate::domain::client::Client;
use crate::domain::dto::{
    GetBalanceQuery, GetBalanceResponse, NewClientBody, NewCreditTransactionBody,
    NewDebitTransactionBody,
};
use crate::utils::bootstrap;

pub enum TransactionDirection {
    Credit,
    Debit,
}

/// Endpoints

#[get("/")]
async fn index() -> impl Responder {
    let endpoints = [
        ("POST /new_client", "Register a new client"),
        ("POST /new_credit_transaction", "Record a credit deposit"),
        ("POST /new_debit_transaction", "Record a debit withdrawal"),
        ("POST /store_balances", "Persist current client balances"),
        ("GET /get_balance", "Retrieve a client's current balance"),
    ];

    HttpResponse::Ok().json(&endpoints)
}

#[post("/new_client")]
async fn new_client(
    req_body: web::Json<NewClientBody>,
    cache: web::Data<Cache>,
) -> Result<impl Responder> {
    let body = req_body.into_inner();
    let document_number = &body.document_number.clone();

    // Check if document is already in use
    if cache.is_document_in_use(document_number).await {
        return Err(actix_web::error::ErrorConflict(
            "A client with that document already exists.",
        ));
    }

    // Set document in-flight to mark that this client is being created and has yet to be persisted to storage.
    // The clients cache will only reflect the new client once its safely saved to storage.
    cache
        .set_document_in_flight(document_number)
        .map_err(|e| actix_web::error::ErrorConflict(e))?;

    // New Client
    let new_client = Client::new(body);

    // Write the new client to storage
    // If this returns an error, the client document will be removed from the in_flight cache
    // and never added to the clients cache
    if let Err(e) = crate::storage::save_client_to_storage(&new_client).await {
        cache
            .remove_document_in_flight(document_number)
            .map_err(|e| actix_web::error::ErrorInternalServerError(e))?;
        return Err(actix_web::error::ErrorInternalServerError(e));
    }

    // Add the new client to the clients cache
    cache.insert_client(&new_client).await;

    // Remove new client document from in-flight cache
    cache
        .remove_document_in_flight(document_number)
        .map_err(actix_web::error::ErrorInternalServerError)?;

    // Return the client id
    Ok(HttpResponse::Ok().json(new_client.client_id.to_string()))
}

#[post("/new_credit_transaction")]
async fn new_credit_transaction(
    req_body: web::Json<NewCreditTransactionBody>,
    cache: web::Data<Cache>,
) -> Result<impl Responder> {
    let new_balance = cache
        .apply_balance_change(
            req_body.client_id,
            req_body.credit_amount,
            TransactionDirection::Credit,
        )
        .await
        .map_err(|e| actix_web::error::ErrorInternalServerError(e))?;

    Ok(HttpResponse::Ok().json(new_balance))
}

#[post("/new_debit_transaction")]
async fn new_debit_transaction(
    req_body: web::Json<NewDebitTransactionBody>,
    cache: web::Data<Cache>,
) -> Result<impl Responder> {
    let new_balance = cache
        .apply_balance_change(
            req_body.client_id,
            req_body.debit_amount,
            TransactionDirection::Debit,
        )
        .await
        .map_err(|e| actix_web::error::ErrorInternalServerError(e))?;

    Ok(HttpResponse::Ok().json(new_balance))
}

#[post("/store_balances")]
async fn store_balances(cache: web::Data<Cache>) -> Result<impl Responder> {
    // Lock the store guard.
    // If we receive many requests to store_balances at the same time, we will effectively be making a queue here,
    // waiting for earlier calls to finish writing to storage and udpating dirty clients cache.
    // The lock is released when the function returns,
    // and only there can another thread/task try to store balances again.
    let _store_guard = cache.store_lock.lock().await;

    // Snapshot which clients have balance changes to save
    let dirty_clients = cache.get_dirty_clients()?;

    // Early return if no news
    if dirty_clients.is_empty() {
        return Ok(HttpResponse::Ok());
    }
    let deltas_to_write = cache.collect_batch_deltas(&dirty_clients).await?;

    let nonce = cache.nonce.load(Relaxed) + 1;

    // Write to file.
    // If anything here fails, the cache was never modified and we could retry the operation if needed.
    crate::storage::save_balance_changes(nonce, &deltas_to_write).await?;

    cache.apply_persisted_deltas(&deltas_to_write).await?;

    cache.increment_nonce();

    Ok(HttpResponse::Ok().into())
}

#[get("/get_balance")]
async fn get_balance(
    query: web::Query<GetBalanceQuery>,
    cache: web::Data<Cache>,
) -> Result<impl Responder> {
    let client_id = query.client_id;

    let (document_number, balance) = cache
        .get_client_snapshot(client_id)
        .await
        .map_err(|e| actix_web::error::ErrorNotFound(e))?;

    Ok(HttpResponse::Ok().json(GetBalanceResponse {
        client_id,
        document_number,
        balance,
    }))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("Starting server");

    let cache = match bootstrap() {
        Ok(cache) => web::Data::new(cache),
        Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::Other, e)),
    };

    HttpServer::new(move || {
        App::new()
            .app_data(cache.clone())
            .service(index)
            .service(new_client)
            .service(new_debit_transaction)
            .service(new_credit_transaction)
            .service(store_balances)
            .service(get_balance)
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
