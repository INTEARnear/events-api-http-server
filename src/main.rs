use std::{fs::File, io::BufReader};

mod nft_events;
mod potlock_events;
mod trade_events;
pub mod utils;

use actix_cors::Cors;
use actix_web::{middleware, web, App, HttpServer};
use log::LevelFilter;
use serde::Deserialize;
use sqlx::PgPool;

const MAX_BLOCKS_PER_REQUEST: i64 = 50;

struct AppState {
    pg_pool: PgPool,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    dotenvy::dotenv().ok();
    simple_logger::SimpleLogger::new()
        .with_level(LevelFilter::Info)
        .init()
        .unwrap();

    let pg_pool = PgPool::connect(
        &std::env::var("DATABASE_URL").expect("DATABASE_URL environment variable must be set"),
    )
    .await
    .expect("Failed to connect to Postgres");

    let tls_config = if let Ok(files) = std::env::var("SSL") {
        #[allow(clippy::iter_nth_zero)]
        let mut certs_file = BufReader::new(File::open(files.split(',').nth(0).unwrap()).unwrap());
        let mut key_file = BufReader::new(File::open(files.split(',').nth(1).unwrap()).unwrap());
        let tls_certs = rustls_pemfile::certs(&mut certs_file)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let tls_key = rustls_pemfile::pkcs8_private_keys(&mut key_file)
            .next()
            .unwrap()
            .unwrap();
        Some(
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(tls_certs, rustls::pki_types::PrivateKeyDer::Pkcs8(tls_key))
                .unwrap(),
        )
    } else {
        None
    };

    let server = HttpServer::new(move || {
        let cors = Cors::default()
            .allow_any_origin()
            .allowed_methods(vec!["GET"])
            .max_age(3600)
            .supports_credentials();

        let nft = web::scope("/nft")
            .service(nft_events::nft_mint)
            .service(nft_events::nft_transfer)
            .service(nft_events::nft_burn);

        let potlock = web::scope("/potlock")
            .service(potlock_events::potlock_donation)
            .service(potlock_events::potlock_pot_project_donation)
            .service(potlock_events::potlock_pot_donation);

        let trade = web::scope("/trade")
            .service(trade_events::trade_pool)
            .service(trade_events::trade_swap)
            .service(trade_events::trade_pool_change);

        let api_v0 = web::scope("/v0")
            .service(nft)
            .service(potlock)
            .service(trade);

        let state = AppState {
            pg_pool: pg_pool.clone(),
        };

        App::new()
            .app_data(web::Data::new(state))
            .service(api_v0)
            .wrap(cors)
            .wrap(middleware::Logger::new(
                "%{r}a %a \"%r\"	Code: %s Size: %b bytes \"%{Referer}i\" \"%{User-Agent}i\" %T",
            ))
    });

    let server = if let Some(tls_config) = tls_config {
        server.bind_rustls_0_22(
            std::env::var("BIND_ADDRESS").unwrap_or("0.0.0.0:8080".to_string()),
            tls_config,
        )?
    } else {
        server.bind(std::env::var("BIND_ADDRESS").unwrap_or("0.0.0.0:8080".to_string()))?
    };

    server.run().await
}

#[derive(Deserialize)]
struct PaginationInfo {
    #[serde(default)]
    start_block_timestamp_nanosec: i64,
    #[serde(default = "default_blocks_per_request")]
    blocks: i64,
}

fn default_blocks_per_request() -> i64 {
    10
}
