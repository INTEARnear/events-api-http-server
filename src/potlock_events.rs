use actix_web::{get, web, HttpResponse, Responder};
use chrono::prelude::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    utils::{Balance, OptionalBalance},
    AppState, PaginationInfo, MAX_BLOCKS_PER_REQUEST,
};

type TransactionId = String;
type ReceiptId = String;
type AccountId = String;
type BlockHeight = i64;
type DonationId = i64;
type ProjectId = AccountId;

#[derive(Debug, Serialize, Deserialize)]
pub struct PotlockDonationEvent {
    pub transaction_id: TransactionId,
    pub receipt_id: ReceiptId,
    pub block_height: BlockHeight,
    #[serde(
        with = "chrono::serde::ts_nanoseconds",
        rename = "block_timestamp_nanosec"
    )]
    pub timestamp: DateTime<Utc>,

    pub donation_id: DonationId,
    pub donor_id: AccountId,
    pub total_amount: Balance,
    pub message: Option<String>,
    #[serde(with = "chrono::serde::ts_milliseconds")]
    pub donated_at: DateTime<Utc>,
    pub project_id: ProjectId,
    pub protocol_fee: Balance,
    pub referrer_id: Option<AccountId>,
    pub referrer_fee: OptionalBalance,
}

#[derive(Deserialize)]
struct PotlockDonationFilter {
    project_id: Option<String>,
    donor_id: Option<String>,
    referrer_id: Option<String>,
}

#[get("/potlock_donation")]
pub async fn potlock_donation(
    state: web::Data<AppState>,
    pagination: web::Query<PaginationInfo>,
    filter: web::Query<PotlockDonationFilter>,
) -> impl Responder {
    if pagination.blocks > MAX_BLOCKS_PER_REQUEST {
        return HttpResponse::BadRequest().body(format!(
            "Blocks per request must be less or equal to {MAX_BLOCKS_PER_REQUEST}"
        ));
    }

    if let Ok(res) = sqlx::query_as!(PotlockDonationEvent,
        r#"
        WITH blocks AS (
            SELECT DISTINCT timestamp as t
            FROM potlock_donation
            WHERE extract(epoch from timestamp) * 1_000_000_000 >= $1
                AND ($3::TEXT IS NULL OR project_id = $3)
                AND ($4::TEXT IS NULL OR donor_id = $4)
                AND ($5::TEXT IS NULL OR referrer_id = $5)
            ORDER BY t
            LIMIT $2
        )
        SELECT transaction_id, receipt_id, block_height, timestamp, donation_id, donor_id, total_amount, message, donated_at, project_id, protocol_fee, referrer_id, referrer_fee
        FROM potlock_donation
        INNER JOIN blocks ON timestamp = blocks.t
        WHERE ($3::TEXT IS NULL OR project_id = $3)
            AND ($4::TEXT IS NULL OR donor_id = $4)
            AND ($5::TEXT IS NULL OR referrer_id = $5)
        ORDER BY timestamp ASC
        "#,
        pagination.start_block_timestamp_nanosec as i64,
        pagination.blocks,
        filter.project_id.as_deref(),
        filter.donor_id.as_deref(),
        filter.referrer_id.as_deref(),
    ).fetch_all(&state.pg_pool).await {
        HttpResponse::Ok().json(res)
    } else {
        HttpResponse::InternalServerError().finish()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PotlockPotProjectDonationEvent {
    pub transaction_id: TransactionId,
    pub receipt_id: ReceiptId,
    pub block_height: BlockHeight,
    #[serde(
        with = "chrono::serde::ts_nanoseconds",
        rename = "block_timestamp_nanosec"
    )]
    pub timestamp: DateTime<Utc>,

    pub donation_id: DonationId,
    pub pot_id: AccountId,
    pub donor_id: AccountId,
    pub total_amount: Balance,
    pub net_amount: Balance,
    pub message: Option<String>,
    #[serde(with = "chrono::serde::ts_milliseconds")]
    pub donated_at: DateTime<Utc>,
    pub project_id: ProjectId,
    pub referrer_id: Option<AccountId>,
    pub referrer_fee: OptionalBalance,
    pub protocol_fee: Balance,
    pub chef_id: Option<AccountId>,
    pub chef_fee: OptionalBalance,
}

#[derive(Deserialize)]
struct PotlockPotProjectDonationFilter {
    pot_id: Option<String>,
    project_id: Option<String>,
    donor_id: Option<String>,
    referrer_id: Option<String>,
}

#[get("/potlock_pot_project_donation")]
pub async fn potlock_pot_project_donation(
    state: web::Data<AppState>,
    pagination: web::Query<PaginationInfo>,
    filter: web::Query<PotlockPotProjectDonationFilter>,
) -> impl Responder {
    if pagination.blocks > MAX_BLOCKS_PER_REQUEST {
        return HttpResponse::BadRequest().body(format!(
            "Blocks per request must be less or equal to {MAX_BLOCKS_PER_REQUEST}"
        ));
    }

    if let Ok(res) = sqlx::query_as!(PotlockPotProjectDonationEvent,
        r#"
        WITH blocks AS (
            SELECT DISTINCT timestamp as t
            FROM potlock_pot_project_donation
            WHERE extract(epoch from timestamp) * 1_000_000_000 >= $1
                AND ($3::TEXT IS NULL OR pot_id = $3)
                AND ($4::TEXT IS NULL OR project_id = $4)
                AND ($5::TEXT IS NULL OR donor_id = $5)
                AND ($6::TEXT IS NULL OR referrer_id = $6)
            ORDER BY t
            LIMIT $2
        )
        SELECT transaction_id, receipt_id, block_height, timestamp, donation_id, pot_id, donor_id, total_amount, net_amount, message, donated_at, project_id, referrer_id, referrer_fee, protocol_fee, chef_id, chef_fee
        FROM potlock_pot_project_donation
        INNER JOIN blocks ON timestamp = blocks.t
        WHERE ($3::TEXT IS NULL OR pot_id = $3)
            AND ($4::TEXT IS NULL OR project_id = $4)
            AND ($5::TEXT IS NULL OR donor_id = $5)
            AND ($6::TEXT IS NULL OR referrer_id = $6)
        ORDER BY timestamp ASC
        "#,
        pagination.start_block_timestamp_nanosec as i64,
        pagination.blocks,
        filter.pot_id.as_deref(),
        filter.project_id.as_deref(),
        filter.donor_id.as_deref(),
        filter.referrer_id.as_deref(),
    ).fetch_all(&state.pg_pool).await {
        HttpResponse::Ok().json(res)
    } else {
        HttpResponse::InternalServerError().finish()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PotlockPotDonationEvent {
    pub transaction_id: TransactionId,
    pub receipt_id: ReceiptId,
    pub block_height: BlockHeight,
    #[serde(
        with = "chrono::serde::ts_nanoseconds",
        rename = "block_timestamp_nanosec"
    )]
    pub timestamp: DateTime<Utc>,

    pub donation_id: DonationId,
    pub pot_id: AccountId,
    pub donor_id: AccountId,
    pub total_amount: Balance,
    pub net_amount: Balance,
    pub message: Option<String>,
    #[serde(with = "chrono::serde::ts_milliseconds")]
    pub donated_at: DateTime<Utc>,
    pub referrer_id: Option<AccountId>,
    pub referrer_fee: OptionalBalance,
    pub protocol_fee: Balance,
    pub chef_id: Option<AccountId>,
    pub chef_fee: OptionalBalance,
}

#[derive(Deserialize)]
struct PotlockPotDonationFilter {
    pot_id: Option<String>,
    donor_id: Option<String>,
    referrer_id: Option<String>,
}

#[get("/potlock_pot_donation")]
pub async fn potlock_pot_donation(
    state: web::Data<AppState>,
    pagination: web::Query<PaginationInfo>,
    filter: web::Query<PotlockPotDonationFilter>,
) -> impl Responder {
    if pagination.blocks > MAX_BLOCKS_PER_REQUEST {
        return HttpResponse::BadRequest().body(format!(
            "Blocks per request must be less or equal to {MAX_BLOCKS_PER_REQUEST}"
        ));
    }

    if let Ok(res) = sqlx::query_as!(PotlockPotDonationEvent,
        r#"
        WITH blocks AS (
            SELECT DISTINCT timestamp as t
            FROM potlock_pot_donation
            WHERE extract(epoch from timestamp) * 1_000_000_000 >= $1
                AND ($3::TEXT IS NULL OR pot_id = $3)
                AND ($4::TEXT IS NULL OR donor_id = $4)
                AND ($5::TEXT IS NULL OR referrer_id = $5)
            ORDER BY t
            LIMIT $2
        )
        SELECT transaction_id, receipt_id, block_height, timestamp, donation_id, pot_id, donor_id, total_amount, net_amount, message, donated_at, referrer_id, referrer_fee, protocol_fee, chef_id, chef_fee
        FROM potlock_pot_donation
        INNER JOIN blocks ON timestamp = blocks.t
        WHERE ($3::TEXT IS NULL OR pot_id = $3)
            AND ($4::TEXT IS NULL OR donor_id = $4)
            AND ($5::TEXT IS NULL OR referrer_id = $5)
        ORDER BY timestamp ASC
        "#,
        pagination.start_block_timestamp_nanosec as i64,
        pagination.blocks as i64,
        filter.pot_id.as_deref(),
        filter.donor_id.as_deref(),
        filter.referrer_id.as_deref(),
    ).fetch_all(&state.pg_pool).await {
        HttpResponse::Ok().json(res)
    } else {
        HttpResponse::InternalServerError().finish()
    }
}
