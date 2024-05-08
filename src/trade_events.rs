use actix_web::{get, web, HttpResponse, Responder};
use chrono::prelude::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{utils::Balance, AppState, PaginationInfo, MAX_BLOCKS_PER_REQUEST};

type TransactionId = String;
type ReceiptId = String;
type AccountId = String;
type BlockHeight = i64;
type PoolId = String;

#[derive(Debug, Serialize, Deserialize)]
pub struct TradePoolEvent {
    pub trader: AccountId,
    pub block_height: BlockHeight,
    #[serde(
        with = "chrono::serde::ts_nanoseconds",
        rename = "block_timestamp_nanosec"
    )]
    pub timestamp: DateTime<Utc>,
    pub transaction_id: TransactionId,
    pub receipt_id: ReceiptId,

    pub pool: PoolId,
    pub token_in: AccountId,
    pub token_out: AccountId,
    pub amount_in: Balance,
    pub amount_out: Balance,
}

#[derive(Deserialize)]
struct TradePoolFilter {
    pool_id: Option<String>,
    account_id: Option<String>,
}

#[get("/trade_pool")]
pub async fn trade_pool(
    state: web::Data<AppState>,
    pagination: web::Query<PaginationInfo>,
    filter: web::Query<TradePoolFilter>,
) -> impl Responder {
    if pagination.blocks > MAX_BLOCKS_PER_REQUEST {
        return HttpResponse::BadRequest().body(format!(
            "Blocks per request must be less or equal to {MAX_BLOCKS_PER_REQUEST}"
        ));
    }

    if let Ok(res) = sqlx::query_as!(TradePoolEvent,
        r#"
        WITH blocks AS (
            SELECT DISTINCT timestamp as t
            FROM trade_pool
            WHERE extract(epoch from timestamp) * 1_000_000_000 >= $1
                AND ($3::TEXT IS NULL OR pool = $3)
                AND ($4::TEXT IS NULL OR trader = $4)
            ORDER BY t
            LIMIT $2
        )
        SELECT trader, block_height, timestamp, transaction_id, receipt_id, pool, token_in, token_out, amount_in, amount_out
        FROM trade_pool
        INNER JOIN blocks ON timestamp = blocks.t
        WHERE ($3::TEXT IS NULL OR pool = $3)
            AND ($4::TEXT IS NULL OR trader = $4)
        ORDER BY timestamp ASC
        "#,
        pagination.start_block_timestamp_nanosec as i64,
        pagination.blocks,
        filter.pool_id.as_deref(),
        filter.account_id.as_deref(),
    ).fetch_all(&state.pg_pool).await {
        HttpResponse::Ok().json(res)
    } else {
        HttpResponse::InternalServerError().finish()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TradeSwapEvent {
    pub trader: AccountId,
    pub block_height: BlockHeight,
    #[serde(
        with = "chrono::serde::ts_nanoseconds",
        rename = "block_timestamp_nanosec"
    )]
    pub timestamp: DateTime<Utc>,
    pub transaction_id: TransactionId,
    pub receipt_id: ReceiptId,

    pub balance_changes: Value, // account_id: String -> balance_change: Balance
}

#[derive(Deserialize)]
struct TradeSwapFilter {
    account_id: Option<String>,
    involved_token_account_ids: Option<String>,
}

#[get("/trade_swap")]
pub async fn trade_swap(
    state: web::Data<AppState>,
    pagination: web::Query<PaginationInfo>,
    filter: web::Query<TradeSwapFilter>,
) -> impl Responder {
    if pagination.blocks > MAX_BLOCKS_PER_REQUEST {
        return HttpResponse::BadRequest().body(format!(
            "Blocks per request must be less or equal to {MAX_BLOCKS_PER_REQUEST}"
        ));
    }

    let involved_tokens = filter
        .involved_token_account_ids
        .as_ref()
        .map(|s| s.split(',').map(ToOwned::to_owned).collect::<Vec<String>>());
    if let Ok(res) = sqlx::query_as!(
        TradeSwapEvent,
        r#"
        WITH blocks AS (
            SELECT DISTINCT timestamp as t
            FROM trade_swap
            WHERE extract(epoch from timestamp) * 1_000_000_000 >= $1
                AND ($3::TEXT IS NULL OR trader = $3)
                AND ($4::TEXT[] IS NULL OR balance_changes ?& $4)
            ORDER BY t
            LIMIT $2
        )
        SELECT trader, block_height, timestamp, transaction_id, receipt_id, balance_changes
        FROM trade_swap
        INNER JOIN blocks ON timestamp = blocks.t
        WHERE ($3::TEXT IS NULL OR trader = $3)
            AND ($4::TEXT[] IS NULL OR balance_changes ?& $4)
        ORDER BY timestamp ASC
        "#,
        pagination.start_block_timestamp_nanosec as i64,
        pagination.blocks,
        filter.account_id.as_deref(),
        involved_tokens.as_deref(),
    )
    .fetch_all(&state.pg_pool)
    .await
    {
        HttpResponse::Ok().json(res)
    } else {
        HttpResponse::InternalServerError().finish()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TradePoolChangeEvent {
    pub pool_id: PoolId,
    pub receipt_id: ReceiptId,
    #[serde(
        with = "chrono::serde::ts_nanoseconds",
        rename = "block_timestamp_nanosec"
    )]
    pub timestamp: DateTime<Utc>,
    pub block_height: BlockHeight,
    pub pool: Value,
}

#[derive(Deserialize)]
struct TradePoolChangeFilter {
    pool_id: Option<String>,
}

#[get("/trade_pool_change")]
pub async fn trade_pool_change(
    state: web::Data<AppState>,
    pagination: web::Query<PaginationInfo>,
    filter: web::Query<TradePoolChangeFilter>,
) -> impl Responder {
    if pagination.blocks > MAX_BLOCKS_PER_REQUEST {
        return HttpResponse::BadRequest().body(format!(
            "Blocks per request must be less or equal to {MAX_BLOCKS_PER_REQUEST}"
        ));
    }

    if let Ok(res) = sqlx::query_as!(
        TradePoolChangeEvent,
        r#"
        WITH blocks AS (
            SELECT DISTINCT timestamp as t
            FROM trade_pool_change
            WHERE extract(epoch from timestamp) * 1_000_000_000 >= $1
                AND ($3::TEXT IS NULL OR pool_id = $3)
            ORDER BY t
            LIMIT $2
        )
        SELECT pool_id, receipt_id, timestamp, block_height, pool
        FROM trade_pool_change
        INNER JOIN blocks ON timestamp = blocks.t
        WHERE ($3::TEXT IS NULL OR pool_id = $3)
        ORDER BY timestamp ASC
        "#,
        pagination.start_block_timestamp_nanosec as i64,
        pagination.blocks,
        filter.pool_id.as_deref(),
    )
    .fetch_all(&state.pg_pool)
    .await
    {
        HttpResponse::Ok().json(res)
    } else {
        HttpResponse::InternalServerError().finish()
    }
}
