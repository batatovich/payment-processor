mod constants;
mod domain;
mod utils;

use actix_web::{App, HttpResponse, HttpServer, Responder, Result, get, post, web};
use std::fs;
use std::path::Path;
use std::sync::atomic::Ordering::Relaxed;

use crate::domain::cache::Cache;
use crate::domain::client::Client;
use crate::domain::dto::{NewClientBody, NewCreditTransactionBody, NewDebitTransactionBody};

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
        ("GET /client_balance", "Retrieve a client's current balance"),
    ];

    HttpResponse::Ok().json(&endpoints)
}

#[post("/new_client")]
async fn new_client(
    req_body: web::Json<NewClientBody>,
    cache: web::Data<Cache>,
) -> Result<impl Responder> {
    let body = req_body.into_inner();

    // New Client
    let new_client = Client::new(body);

    let new_client_id = new_client.client_id.clone();

    // Insert the new client in the cache and save it to storage
    cache.insert_client(new_client).await.map_err(|e| {
        actix_web::error::ErrorInternalServerError(format!("Error inserting client: {e}"))
    })?;

    // Return the client id
    Ok(HttpResponse::Ok().json(new_client_id.to_string()))
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
        .await;

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
        .await;
    Ok(HttpResponse::Ok().json(new_balance))
}

#[post("/store_balances")]
async fn store_balances() -> impl Responder {
    HttpResponse::Ok()
}

#[get("/client_balance")]
async fn client_balance() -> impl Responder {
    HttpResponse::Ok().body("The client balance is 3.14")
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
            .service(client_balance)
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
