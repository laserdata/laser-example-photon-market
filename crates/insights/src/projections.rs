use laser_sdk::prelude::{Laser, LaserError};
use laser_sdk::query::{Projection, ProjectionBinding, RetentionPolicy};
use laser_sdk::stream::ContentType;
use photon_shared::names::{BusinessTopic, Index};
use std::time::{Duration, Instant};

const READY_TIMEOUT: Duration = Duration::from_secs(30);

pub async fn register(laser: &Laser) -> Result<(), LaserError> {
    register_projection(
        laser,
        Index::Orders,
        BusinessTopic::OrderEvents,
        &[
            ("order", "/order"),
            ("customer", "/customer"),
            ("sku", "/sku"),
            ("quantity", "/quantity"),
            ("at_micros", "/at_micros"),
            ("event_type", "/kind/type"),
            ("amount_cents", "/kind/amount_cents"),
            ("carrier", "/kind/carrier"),
        ],
        RetentionPolicy::KeepUntilSourceDeleted,
    )
    .await?;
    register_projection(
        laser,
        Index::ShopEvents,
        BusinessTopic::ShopEvents,
        &[
            ("session", "/session"),
            ("customer", "/customer"),
            ("kind", "/kind"),
            ("sku", "/sku"),
            ("price_cents", "/price_cents"),
            ("at_micros", "/at_micros"),
        ],
        RetentionPolicy::TimeToLive {
            ttl_micros: 30 * 24 * 60 * 60 * 1_000_000,
        },
    )
    .await?;
    register_projection(
        laser,
        Index::Tickets,
        BusinessTopic::SupportTickets,
        &[
            ("ticket", "/ticket"),
            ("customer", "/customer"),
            ("order", "/order"),
            ("kind", "/kind"),
            ("opened_at_micros", "/opened_at_micros"),
        ],
        RetentionPolicy::KeepUntilSourceDeleted,
    )
    .await?;
    register_projection(
        laser,
        Index::RiskCases,
        BusinessTopic::RiskEvents,
        &[
            ("order", "/order"),
            ("customer", "/customer"),
            ("device", "/device"),
            ("card_fingerprint", "/card_fingerprint"),
            ("score_bps", "/score_bps"),
            ("verdict", "/verdict"),
            ("at_micros", "/at_micros"),
        ],
        RetentionPolicy::KeepUntilSourceDeleted,
    )
    .await?;

    for index in [
        Index::Orders,
        Index::ShopEvents,
        Index::Tickets,
        Index::RiskCases,
    ] {
        wait_until_ready(laser, index).await?;
    }
    Ok(())
}

async fn register_projection(
    laser: &Laser,
    index: Index,
    topic: BusinessTopic,
    fields: &[(&str, &str)],
    retention: RetentionPolicy,
) -> Result<(), LaserError> {
    let index = index.to_string();
    let projection_id = format!("{index}.v1");
    let mut projection = Projection::builder(projection_id.clone())
        .name(&index)
        .version(1)
        .content_type(ContentType::Json);
    for (name, pointer) in fields {
        projection = projection.field_at(*name, *pointer);
    }
    laser.projections().register(projection.build()).await?;
    laser
        .bindings()
        .apply(
            ProjectionBinding::builder()
                .source(topic.stream(), topic.to_string())
                .allow(projection_id.clone())
                .default_projection(projection_id)
                .index(&index)
                .retention(retention)
                .notify()
                .build(),
        )
        .await
}

async fn wait_until_ready(laser: &Laser, index: Index) -> Result<(), LaserError> {
    let index = index.to_string();
    let deadline = Instant::now() + READY_TIMEOUT;
    loop {
        match laser.query(&index).limit(1).fetch().await {
            Ok(_) => return Ok(()),
            Err(error) if Instant::now() < deadline => {
                let _ = error;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => {
                return Err(LaserError::Invalid(format!(
                    "managed index {index} did not become ready: {error}"
                )));
            }
        }
    }
}
