use actix_web::{App, HttpResponse, HttpServer, Responder, get, post, web};
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

/// Request body definitions 

#[derive(Deserialize, Serialize)]
struct NewClientBody {
    client_name: String,
    birth_date: NaiveDate,
    document_number: String,
    country: String,
}

#[derive(Deserialize, Serialize)]
struct NewCreditTransactionBody {
    client_id: i32,
    credit_amount: f64,
}

#[derive(Deserialize, Serialize)]
struct NewDebitTransactionBody {
    client_id: i32,
    debit_amount: f64,
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
async fn new_client(req_body: web::Json<NewClientBody>) -> impl Responder {
    let body = &req_body.into_inner();
    let response_string = format!(
        "Received new client request for {} with document numer {}",
        body.client_name, body.document_number
    );
    let response = &response_string;
    HttpResponse::Ok().json(response)
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

    HttpServer::new(|| {
        App::new()
            .service(index)
            .service(new_client)
            .service(new_debit_transaction)
            .service(new_credit_transaction)
            .service(store_balances)
            .service(client_balance)
    })
    .bind(("127.0.0.1", 8081))?
    .run()
    .await
}
