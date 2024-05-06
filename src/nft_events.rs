use actix_web::{get, web, HttpResponse, Responder};
use chrono::prelude::{DateTime, Utc};
use rust_decimal::prelude::Decimal;
use serde::{Deserialize, Serialize};

use crate::{AppState, PaginationInfo, MAX_BLOCKS_PER_REQUEST};

type TransactionId = String;
type ReceiptId = String;
type AccountId = String;
type NftTokenId = String;
type BlockHeight = i64;
type Balance = Decimal;

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
    pub token_prices_near: Vec<Balance>,

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

    let res: Vec<NftMintEvent> = match(&filter.token_account_id, &filter.account_id) {
        (None, None) => sqlx::query_as!(NftMintEvent,
            r#"
            WITH blocks AS (
                SELECT DISTINCT timestamp as t
                FROM nft_mint
                WHERE extract(epoch from timestamp) * 1_000_000_000 >= $1
                ORDER BY t
                LIMIT $2
            )
            SELECT owner_id, token_ids, memo, transaction_id, receipt_id, block_height, timestamp, contract_id
            FROM nft_mint
            INNER JOIN blocks ON timestamp = blocks.t
            ORDER BY timestamp ASC
            "#,
            i64::try_from(pagination.start_block_timestamp_nanosec).unwrap() as i64,
            pagination.blocks as i64
        ).fetch_all(&state.pg_pool).await,
        (Some(token_account_id), None) => sqlx::query_as!(NftMintEvent,
            r#"
            WITH blocks AS (
                SELECT DISTINCT timestamp as t
                FROM nft_mint
                WHERE contract_id = $3 AND extract(epoch from timestamp) * 1_000_000_000 >= $1
                ORDER BY t
                LIMIT $2
            )
            SELECT owner_id, token_ids, memo, transaction_id, receipt_id, block_height, timestamp, contract_id
            FROM nft_mint
            INNER JOIN blocks ON timestamp = blocks.t
            WHERE contract_id = $3
            ORDER BY timestamp ASC
            "#,
            i64::try_from(pagination.start_block_timestamp_nanosec).unwrap() as i64,
            pagination.blocks as i64,
            token_account_id
        ).fetch_all(&state.pg_pool).await,
        (None, Some(account_id)) => sqlx::query_as!(NftMintEvent,
            r#"
            WITH blocks AS (
                SELECT DISTINCT timestamp as t
                FROM nft_mint
                WHERE owner_id = $3 AND extract(epoch from timestamp) * 1_000_000_000 >= $1
                ORDER BY t
                LIMIT $2
            )
            SELECT owner_id, token_ids, memo, transaction_id, receipt_id, block_height, timestamp, contract_id
            FROM nft_mint
            INNER JOIN blocks ON timestamp = blocks.t
            WHERE owner_id = $3
            ORDER BY timestamp ASC
            "#,
            i64::try_from(pagination.start_block_timestamp_nanosec).unwrap() as i64,
            pagination.blocks as i64,
            account_id
        ).fetch_all(&state.pg_pool).await,
        (Some(token_account_id), Some(account_id)) => sqlx::query_as!(NftMintEvent,
            r#"
            WITH blocks AS (
                SELECT DISTINCT timestamp as t
                FROM nft_mint
                WHERE contract_id = $3 AND owner_id = $4 AND extract(epoch from timestamp) * 1_000_000_000 >= $1
                ORDER BY t
                LIMIT $2
            )
            SELECT owner_id, token_ids, memo, transaction_id, receipt_id, block_height, timestamp, contract_id
            FROM nft_mint
            INNER JOIN blocks ON timestamp = blocks.t
            WHERE contract_id = $3 AND owner_id = $4
            ORDER BY timestamp ASC
            "#,
            i64::try_from(pagination.start_block_timestamp_nanosec).unwrap() as i64,
            pagination.blocks as i64,
            token_account_id,
            account_id
        ).fetch_all(&state.pg_pool).await,
    }.unwrap();

    HttpResponse::Ok().json(res)
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

    let res: Vec<NftTransferEvent> = match(&filter.token_account_id, &filter.old_owner_id, &filter.new_owner_id, &filter.involved_account_ids) {
        (None, _, _, Some(involved_account_ids)) => {
            let account_ids = involved_account_ids.split(',').map(ToOwned::to_owned).collect::<Vec<String>>();
            sqlx::query_as!(NftTransferEvent,
                r#"
                WITH blocks AS (
                    SELECT DISTINCT timestamp as t
                    FROM nft_transfer
                    WHERE ARRAY[old_owner_id, new_owner_id] @> $3 AND extract(epoch from timestamp) * 1_000_000_000 >= $1
                    ORDER BY t
                    LIMIT $2
                )
                SELECT old_owner_id, new_owner_id, token_ids, memo, token_prices_near, transaction_id, receipt_id, block_height, timestamp, contract_id
                FROM nft_transfer
                INNER JOIN blocks ON timestamp = blocks.t
                WHERE ARRAY[old_owner_id, new_owner_id] @> $3
                ORDER BY timestamp ASC
                "#,
                i64::try_from(pagination.start_block_timestamp_nanosec).unwrap() as i64,
                pagination.blocks as i64,
                &account_ids
            ).fetch_all(&state.pg_pool).await
        }
        (Some(token_account_id), _, _, Some(involved_account_ids)) => {
            let account_ids = involved_account_ids.split(',').map(ToOwned::to_owned).collect::<Vec<String>>();
            sqlx::query_as!(NftTransferEvent,
                r#"
                WITH blocks AS (
                    SELECT DISTINCT timestamp as t
                    FROM nft_transfer
                    WHERE contract_id = $3 AND ARRAY[old_owner_id, new_owner_id] @> $4 AND extract(epoch from timestamp) * 1_000_000_000 >= $1
                    ORDER BY t
                    LIMIT $2
                )
                SELECT old_owner_id, new_owner_id, token_ids, memo, token_prices_near, transaction_id, receipt_id, block_height, timestamp, contract_id
                FROM nft_transfer
                INNER JOIN blocks ON timestamp = blocks.t
                WHERE contract_id = $3 AND ARRAY[old_owner_id, new_owner_id] @> $4
                ORDER BY timestamp ASC
                "#,
                i64::try_from(pagination.start_block_timestamp_nanosec).unwrap() as i64,
                pagination.blocks as i64,
                token_account_id,
                &account_ids
            ).fetch_all(&state.pg_pool).await
        }
        (None, None, None, None) => sqlx::query_as!(NftTransferEvent,
            r#"
            WITH blocks AS (
                SELECT DISTINCT timestamp as t
                FROM nft_transfer
                WHERE extract(epoch from timestamp) * 1_000_000_000 >= $1
                ORDER BY t
                LIMIT $2
            )
            SELECT old_owner_id, new_owner_id, token_ids, memo, token_prices_near, transaction_id, receipt_id, block_height, timestamp, contract_id
            FROM nft_transfer
            INNER JOIN blocks ON timestamp = blocks.t
            ORDER BY timestamp ASC
            "#,
            i64::try_from(pagination.start_block_timestamp_nanosec).unwrap() as i64,
            pagination.blocks as i64
        ).fetch_all(&state.pg_pool).await,
        (Some(token_account_id), None, None, None) => sqlx::query_as!(NftTransferEvent,
            r#"
            WITH blocks AS (
                SELECT DISTINCT timestamp as t
                FROM nft_transfer
                WHERE contract_id = $3 AND extract(epoch from timestamp) * 1_000_000_000 >= $1
                ORDER BY t
                LIMIT $2
            )
            SELECT old_owner_id, new_owner_id, token_ids, memo, token_prices_near, transaction_id, receipt_id, block_height, timestamp, contract_id
            FROM nft_transfer
            INNER JOIN blocks ON timestamp = blocks.t
            WHERE contract_id = $3
            ORDER BY timestamp ASC
            "#,
            i64::try_from(pagination.start_block_timestamp_nanosec).unwrap() as i64,
            pagination.blocks as i64,
            token_account_id
        ).fetch_all(&state.pg_pool).await,
        (None, Some(old_owner_id), None, None) => sqlx::query_as!(NftTransferEvent,
            r#"
            WITH blocks AS (
                SELECT DISTINCT timestamp as t
                FROM nft_transfer
                WHERE old_owner_id = $3 AND extract(epoch from timestamp) * 1_000_000_000 >= $1
                ORDER BY t
                LIMIT $2
            )
            SELECT old_owner_id, new_owner_id, token_ids, memo, token_prices_near, transaction_id, receipt_id, block_height, timestamp, contract_id
            FROM nft_transfer
            INNER JOIN blocks ON timestamp = blocks.t
            WHERE old_owner_id = $3
            ORDER BY timestamp ASC
            "#,
            i64::try_from(pagination.start_block_timestamp_nanosec).unwrap() as i64,
            pagination.blocks as i64,
            old_owner_id
        ).fetch_all(&state.pg_pool).await,
        (None, None, Some(new_owner_id), None) => sqlx::query_as!(NftTransferEvent,
            r#"
            WITH blocks AS (
                SELECT DISTINCT timestamp as t
                FROM nft_transfer
                WHERE new_owner_id = $3 AND extract(epoch from timestamp) * 1_000_000_000 >= $1
                ORDER BY t
                LIMIT $2
            )
            SELECT old_owner_id, new_owner_id, token_ids, memo, token_prices_near, transaction_id, receipt_id, block_height, timestamp, contract_id
            FROM nft_transfer
            INNER JOIN blocks ON timestamp = blocks.t
            WHERE new_owner_id = $3
            ORDER BY timestamp ASC
            "#,
            i64::try_from(pagination.start_block_timestamp_nanosec).unwrap() as i64,
            pagination.blocks as i64,
            new_owner_id
        ).fetch_all(&state.pg_pool).await,
        (Some(token_account_id), Some(old_owner_id), None, None) => sqlx::query_as!(NftTransferEvent,
            r#"
            WITH blocks AS (
                SELECT DISTINCT timestamp as t
                FROM nft_transfer
                WHERE contract_id = $3 AND old_owner_id = $4 AND extract(epoch from timestamp) * 1_000_000_000 >= $1
                ORDER BY t
                LIMIT $2
            )
            SELECT old_owner_id, new_owner_id, token_ids, memo, token_prices_near, transaction_id, receipt_id, block_height, timestamp, contract_id
            FROM nft_transfer
            INNER JOIN blocks ON timestamp = blocks.t
            WHERE contract_id = $3 AND old_owner_id = $4
            ORDER BY timestamp ASC
            "#,
            i64::try_from(pagination.start_block_timestamp_nanosec).unwrap() as i64,
            pagination.blocks as i64,
            token_account_id,
            old_owner_id
        ).fetch_all(&state.pg_pool).await,
        (Some(token_account_id), None, Some(new_owner_id), None) => sqlx::query_as!(NftTransferEvent,
            r#"
            WITH blocks AS (
                SELECT DISTINCT timestamp as t
                FROM nft_transfer
                WHERE contract_id = $3 AND new_owner_id = $4 AND extract(epoch from timestamp) * 1_000_000_000 >= $1
                ORDER BY t
                LIMIT $2
            )
            SELECT old_owner_id, new_owner_id, token_ids, memo, token_prices_near, transaction_id, receipt_id, block_height, timestamp, contract_id
            FROM nft_transfer
            INNER JOIN blocks ON timestamp = blocks.t
            WHERE contract_id = $3 AND new_owner_id = $4
            ORDER BY timestamp ASC
            "#,
            i64::try_from(pagination.start_block_timestamp_nanosec).unwrap() as i64,
            pagination.blocks as i64,
            token_account_id,
            new_owner_id
        ).fetch_all(&state.pg_pool).await,
        (None, Some(old_owner_id), Some(new_owner_id), None) => sqlx::query_as!(NftTransferEvent,
            r#"
            WITH blocks AS (
                SELECT DISTINCT timestamp as t
                FROM nft_transfer
                WHERE old_owner_id = $3 AND new_owner_id = $4 AND extract(epoch from timestamp) * 1_000_000_000 >= $1
                ORDER BY t
                LIMIT $2
            )
            SELECT old_owner_id, new_owner_id, token_ids, memo, token_prices_near, transaction_id, receipt_id, block_height, timestamp, contract_id
            FROM nft_transfer
            INNER JOIN blocks ON timestamp = blocks.t
            WHERE old_owner_id = $3 AND new_owner_id = $4
            ORDER BY timestamp ASC
            "#,
            i64::try_from(pagination.start_block_timestamp_nanosec).unwrap() as i64,
            pagination.blocks as i64,
            old_owner_id,
            new_owner_id
        ).fetch_all(&state.pg_pool).await,
        (Some(token_account_id), Some(old_owner_id), Some(new_owner_id), None) => sqlx::query_as!(NftTransferEvent,
            r#"
            WITH blocks AS (
                SELECT DISTINCT timestamp as t
                FROM nft_transfer
                WHERE contract_id = $3 AND old_owner_id = $4 AND new_owner_id = $5 AND extract(epoch from timestamp) * 1_000_000_000 >= $1
                ORDER BY t
                LIMIT $2
            )
            SELECT old_owner_id, new_owner_id, token_ids, memo, token_prices_near, transaction_id, receipt_id, block_height, timestamp, contract_id
            FROM nft_transfer
            INNER JOIN blocks ON timestamp = blocks.t
            WHERE contract_id = $3 AND old_owner_id = $4 AND new_owner_id = $5
            ORDER BY timestamp ASC
            "#,
            i64::try_from(pagination.start_block_timestamp_nanosec).unwrap() as i64,
            pagination.blocks as i64,
            token_account_id,
            old_owner_id,
            new_owner_id
        ).fetch_all(&state.pg_pool).await,
    }.unwrap();

    HttpResponse::Ok().json(res)
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

    let res: Vec<NftBurnEvent> = match(&filter.token_account_id, &filter.account_id) {
        (None, None) => sqlx::query_as!(NftBurnEvent,
            r#"
            WITH blocks AS (
                SELECT DISTINCT timestamp as t
                FROM nft_burn
                WHERE extract(epoch from timestamp) * 1_000_000_000 >= $1
                ORDER BY t
                LIMIT $2
            )
            SELECT owner_id, token_ids, memo, transaction_id, receipt_id, block_height, timestamp, contract_id
            FROM nft_burn
            INNER JOIN blocks ON timestamp = blocks.t
            ORDER BY timestamp ASC
            "#,
            i64::try_from(pagination.start_block_timestamp_nanosec).unwrap() as i64,
            pagination.blocks as i64
        ).fetch_all(&state.pg_pool).await,
        (Some(token_account_id), None) => sqlx::query_as!(NftBurnEvent,
            r#"
            WITH blocks AS (
                SELECT DISTINCT timestamp as t
                FROM nft_burn
                WHERE contract_id = $3 AND extract(epoch from timestamp) * 1_000_000_000 >= $1
                ORDER BY t
                LIMIT $2
            )
            SELECT owner_id, token_ids, memo, transaction_id, receipt_id, block_height, timestamp, contract_id
            FROM nft_burn
            INNER JOIN blocks ON timestamp = blocks.t
            WHERE contract_id = $3
            ORDER BY timestamp ASC
            "#,
            i64::try_from(pagination.start_block_timestamp_nanosec).unwrap() as i64,
            pagination.blocks as i64,
            token_account_id
        ).fetch_all(&state.pg_pool).await,
        (None, Some(account_id)) => sqlx::query_as!(NftBurnEvent,
            r#"
            WITH blocks AS (
                SELECT DISTINCT timestamp as t
                FROM nft_burn
                WHERE owner_id = $3 AND extract(epoch from timestamp) * 1_000_000_000 >= $1
                ORDER BY t
                LIMIT $2
            )
            SELECT owner_id, token_ids, memo, transaction_id, receipt_id, block_height, timestamp, contract_id
            FROM nft_burn
            INNER JOIN blocks ON timestamp = blocks.t
            WHERE owner_id = $3
            ORDER BY timestamp ASC
            "#,
            i64::try_from(pagination.start_block_timestamp_nanosec).unwrap() as i64,
            pagination.blocks as i64,
            account_id
        ).fetch_all(&state.pg_pool).await,
        (Some(token_account_id), Some(account_id)) => sqlx::query_as!(NftBurnEvent,
            r#"
            WITH blocks AS (
                SELECT DISTINCT timestamp as t
                FROM nft_burn
                WHERE contract_id = $3 AND owner_id = $4 AND extract(epoch from timestamp) * 1_000_000_000 >= $1
                ORDER BY t
                LIMIT $2
            )
            SELECT owner_id, token_ids, memo, transaction_id, receipt_id, block_height, timestamp, contract_id
            FROM nft_burn
            INNER JOIN blocks ON timestamp = blocks.t
            WHERE contract_id = $3 AND owner_id = $4
            ORDER BY timestamp ASC
            "#,
            i64::try_from(pagination.start_block_timestamp_nanosec).unwrap() as i64,
            pagination.blocks as i64,
            token_account_id,
            account_id
        ).fetch_all(&state.pg_pool).await,
    }.unwrap();

    HttpResponse::Ok().json(res)
}
