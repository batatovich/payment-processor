mod constants;
mod models;
mod utils;

use actix_web::{App, HttpResponse, HttpServer, Responder, Result, get, post, web};
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering::Relaxed;
use tokio::sync::Mutex;
use uuid::Uuid;

use models::{Cache, Client, NewClientBody, NewCreditTransactionBody, NewDebitTransactionBody};

use crate::utils::bootstrap;

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

    // Generate a unique client id
    let client_id = Uuid::now_v7();

    // New Client
    let new_client = Client {
        client_id,
        client_name: body.client_name,
        country: body.country,
        document_number: body.document_number,
        birth_date: body.birth_date,
        balance: 0f64,
    };

    // Insert the new client in the cache and save it to storage
    cache.insert_client(new_client).await.map_err(|e| {
        actix_web::error::ErrorInternalServerError(format!("Error inserting client: {e}"))
    })?;

    // Return the client id
    Ok(HttpResponse::Ok().json(client_id.to_string()))
}

#[post("/new_credit_transaction")]
async fn new_credit_transaction(req_body: web::Json<NewCreditTransactionBody>) -> impl Responder {
    HttpResponse::Ok().json(req_body)
}

#[post("/new_debit_transaction")]
async fn new_debit_transaction(req_body: web::Json<NewDebitTransactionBody>) -> impl Responder {
    HttpResponse::Ok().json(req_body)
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

    let (last_nonce, clients) = match bootstrap() {
        Ok((nonce, clients)) => (nonce, clients),
        Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::Other, e)),
    };

    // Initialize cache
    let cache = web::Data::new(Cache {
        in_flight: Mutex::new(HashSet::new()),
        clients: Mutex::new(clients),
        transactions: Mutex::new(vec![]),
        nonce: AtomicI32::new(last_nonce),
    });

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
