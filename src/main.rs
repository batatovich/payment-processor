mod api;
mod bootstrap;
mod cache;
mod constants;
mod error;
mod model;
mod storage;

use actix_web::{App, HttpServer, web};

use crate::api::handlers::{
    get_balance, index, new_client, new_credit_transaction, new_debit_transaction, store_balances,
};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("Starting server");

    let cache = match bootstrap::run() {
        Ok(cache) => web::Data::new(cache),
        Err(e) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ));
        }
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
