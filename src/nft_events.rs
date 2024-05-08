use actix_web::{get, web, HttpResponse, Responder};
use chrono::prelude::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::utils::VecBalance;
use crate::{AppState, PaginationInfo, MAX_BLOCKS_PER_REQUEST};

type TransactionId = String;
type ReceiptId = String;
type AccountId = String;
type NftTokenId = String;
type BlockHeight = i64;

#[derive(Debug, Serialize, Deserialize)]
pub struct NftMintEvent {
    pub owner_id: AccountId,
    pub token_ids: Vec<NftTokenId>,
    pub memo: Option<String>,

    pub transaction_id: TransactionId,
    pub receipt_id: ReceiptId,
    pub block_height: BlockHeight,
    #[serde(
        with = "chrono::serde::ts_nanoseconds",
        rename = "block_timestamp_nanosec"
    )]
    pub timestamp: DateTime<Utc>,
    pub contract_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NftTransferEvent {
    pub old_owner_id: AccountId,
    pub new_owner_id: AccountId,
    pub token_ids: Vec<NftTokenId>,
    pub memo: Option<String>,
    pub token_prices_near: VecBalance,

    pub transaction_id: TransactionId,
    pub receipt_id: ReceiptId,
    pub block_height: BlockHeight,
    #[serde(
        with = "chrono::serde::ts_nanoseconds",
        rename = "block_timestamp_nanosec"
    )]
    pub timestamp: DateTime<Utc>,
    pub contract_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NftBurnEvent {
    pub owner_id: AccountId,
    pub token_ids: Vec<NftTokenId>,
    pub memo: Option<String>,

    pub transaction_id: TransactionId,
    pub receipt_id: ReceiptId,
    pub block_height: BlockHeight,
    #[serde(
        with = "chrono::serde::ts_nanoseconds",
        rename = "block_timestamp_nanosec"
    )]
    pub timestamp: DateTime<Utc>,
    pub contract_id: String,
}

#[derive(Deserialize)]
struct NftMintFilter {
    token_account_id: Option<String>,
    account_id: Option<String>,
}

#[get("/nft_mint")]
pub async fn nft_mint(
    state: web::Data<AppState>,
    pagination: web::Query<PaginationInfo>,
    filter: web::Query<NftMintFilter>,
) -> impl Responder {
    if pagination.blocks > MAX_BLOCKS_PER_REQUEST {
        return HttpResponse::BadRequest().body(format!(
            "Blocks per request must be less or equal to {MAX_BLOCKS_PER_REQUEST}"
        ));
    }

    if let Ok(res) = sqlx::query_as!(NftMintEvent,
        r#"
        WITH blocks AS (
            SELECT DISTINCT timestamp as t
            FROM nft_mint
            WHERE extract(epoch from timestamp) * 1_000_000_000 >= $1
                AND ($3::TEXT IS NULL OR contract_id = $3)
                AND ($4::TEXT IS NULL OR owner_id = $4)
            ORDER BY t
            LIMIT $2
        )
        SELECT owner_id, token_ids, memo, transaction_id, receipt_id, block_height, timestamp, contract_id
        FROM nft_mint
        INNER JOIN blocks ON timestamp = blocks.t
        WHERE ($3::TEXT IS NULL OR contract_id = $3)
            AND ($4::TEXT IS NULL OR owner_id = $4)
        ORDER BY timestamp ASC
        "#,
        pagination.start_block_timestamp_nanosec as i64,
        pagination.blocks,
        filter.token_account_id.as_deref(),
        filter.account_id.as_deref(),
    ).fetch_all(&state.pg_pool).await {
        HttpResponse::Ok().json(res)
    } else {
        HttpResponse::InternalServerError().finish()
    }
}

#[derive(Deserialize)]
struct NftTransferFilter {
    token_account_id: Option<String>,
    old_owner_id: Option<String>,
    new_owner_id: Option<String>,
    involved_account_ids: Option<String>,
}

#[get("/nft_transfer")]
pub async fn nft_transfer(
    state: web::Data<AppState>,
    pagination: web::Query<PaginationInfo>,
    filter: web::Query<NftTransferFilter>,
) -> impl Responder {
    if pagination.blocks > MAX_BLOCKS_PER_REQUEST {
        return HttpResponse::BadRequest().body(format!(
            "Blocks per request must be less or equal to {MAX_BLOCKS_PER_REQUEST}"
        ));
    }

    let involved_account_ids = filter
        .involved_account_ids
        .as_ref()
        .map(|s| s.split(',').map(ToOwned::to_owned).collect::<Vec<String>>());
    if let Ok(res) = sqlx::query_as!(NftTransferEvent,
        r#"
        WITH blocks AS (
            SELECT DISTINCT timestamp as t
            FROM nft_transfer
            WHERE extract(epoch from timestamp) * 1_000_000_000 >= $1
                AND ($3::TEXT IS NULL OR contract_id = $3)
                AND ($4::TEXT IS NULL OR old_owner_id = $4)
                AND ($5::TEXT IS NULL OR new_owner_id = $5)
                AND ($6::TEXT[] IS NULL OR ARRAY[old_owner_id, new_owner_id] @> $6)
            ORDER BY t
            LIMIT $2
        )
        SELECT old_owner_id, new_owner_id, token_ids, memo, token_prices_near, transaction_id, receipt_id, block_height, timestamp, contract_id
        FROM nft_transfer
        INNER JOIN blocks ON timestamp = blocks.t
        WHERE ($3::TEXT IS NULL OR contract_id = $3)
            AND ($4::TEXT IS NULL OR old_owner_id = $4)
            AND ($5::TEXT IS NULL OR new_owner_id = $5)
            AND ($6::TEXT IS NULL OR ARRAY[old_owner_id, new_owner_id] @> $6)
        ORDER BY timestamp ASC
        "#,
        pagination.start_block_timestamp_nanosec as i64,
        pagination.blocks,
        filter.token_account_id.as_deref(),
        filter.old_owner_id.as_deref(),
        filter.new_owner_id.as_deref(),
        involved_account_ids.as_deref(),
    ).fetch_all(&state.pg_pool).await {
        HttpResponse::Ok().json(res)
    } else {
        HttpResponse::InternalServerError().finish()
    }
}

#[derive(Deserialize)]
struct NftBurnFilter {
    token_account_id: Option<String>,
    account_id: Option<String>,
}

#[get("/nft_burn")]
pub async fn nft_burn(
    state: web::Data<AppState>,
    pagination: web::Query<PaginationInfo>,
    filter: web::Query<NftBurnFilter>,
) -> impl Responder {
    if pagination.blocks > MAX_BLOCKS_PER_REQUEST {
        return HttpResponse::BadRequest().body(format!(
            "Blocks per request must be less or equal to {MAX_BLOCKS_PER_REQUEST}"
        ));
    }

    if let Ok(res) = sqlx::query_as!(NftBurnEvent,
        r#"
        WITH blocks AS (
            SELECT DISTINCT timestamp as t
            FROM nft_burn
            WHERE extract(epoch from timestamp) * 1_000_000_000 >= $1
                AND ($3::TEXT IS NULL OR contract_id = $3)
                AND ($4::TEXT IS NULL OR owner_id = $4)
            ORDER BY t
            LIMIT $2
        )
        SELECT owner_id, token_ids, memo, transaction_id, receipt_id, block_height, timestamp, contract_id
        FROM nft_burn
        INNER JOIN blocks ON timestamp = blocks.t
        WHERE ($3::TEXT IS NULL OR contract_id = $3)
            AND ($4::TEXT IS NULL OR owner_id = $4)
        ORDER BY timestamp ASC
        "#,
        pagination.start_block_timestamp_nanosec as i64,
        pagination.blocks,
        filter.token_account_id.as_deref(),
        filter.account_id.as_deref(),
    ).fetch_all(&state.pg_pool).await {
        HttpResponse::Ok().json(res)
    } else {
        HttpResponse::InternalServerError().finish()
    }
}
