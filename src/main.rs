mod api;
mod bootstrap;
mod cache;
mod constants;
mod error;
mod model;
mod storage;

use actix_web::{App, HttpServer, web};

use crate::api::handlers::{
    get_balance, new_client, new_credit_transaction, new_debit_transaction, store_balances,
};
use crate::constants::{SERVER_HOST, SERVER_PORT};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let cache = match bootstrap::run() {
        Ok(cache) => web::Data::new(cache),
        Err(e) => return Err(std::io::Error::other(e.to_string())),
    };

    println!("Starting server on {SERVER_HOST}:{SERVER_PORT}");

    HttpServer::new(move || {
        App::new()
            .app_data(cache.clone())
            .service(new_client)
            .service(new_debit_transaction)
            .service(new_credit_transaction)
            .service(store_balances)
            .service(get_balance)
    })
    .bind((SERVER_HOST, SERVER_PORT))?
    .run()
    .await
}
