use actix_cors::Cors;
use actix_web::{middleware, web, App, HttpServer};
use log::LevelFilter;
use serde::Deserialize;
use sqlx::PgPool;

mod nft_events;

const MAX_BLOCKS_PER_REQUEST: u64 = 50;

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

    let pg_pool = PgPool::connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .expect("Failed to connect to Postgres");

    HttpServer::new(move || {
        let cors = Cors::default()
            .allow_any_origin()
            .allowed_methods(vec!["GET"])
            .max_age(3600)
            .supports_credentials();

        let nft = web::scope("/nft")
            .service(nft_events::nft_mint)
            .service(nft_events::nft_transfer)
            .service(nft_events::nft_burn);

        let api_v0 = web::scope("/v0").service(nft);

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
    })
    .bind(std::env::var("BIND_ADDRESS").unwrap_or("0.0.0.0:8080".to_string()))?
    .run()
    .await
}

#[derive(Deserialize)]
struct PaginationInfo {
    #[serde(default)]
    start_block_timestamp_nanosec: u64,
    #[serde(default = "default_blocks_per_request")]
    blocks: u64,
}

fn default_blocks_per_request() -> u64 {
    10
}
