use laser_sdk::prelude::{Laser, LaserError};
use laser_sdk::wire::schema::{OFFSET_FIELD_NAME, PARTITION_ID_FIELD_NAME};
use photon_shared::ShutdownWatch;
use photon_shared::names::Index;
use std::time::Duration;
use tracing::{info, warn};

const FLASH_SALE_FORK: &str = "flash-sale";

pub async fn query_loop(laser: Laser, mut shutdown: ShutdownWatch) {
    let orders = Index::Orders.to_string();
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tokio::time::sleep(Duration::from_secs(4)) => {
                match laser.query(&orders).count().group_by(["event_type"]).fetch().await {
                    Ok(result) => {
                        let groups = result.rows.len();
                        info!("Managed order-state aggregate returned {groups} group(s)");
                    }
                    Err(error) => warn!("Managed order-state query failed: {error}"),
                }
            }
        }
    }
}

pub async fn watch_loop(laser: Laser, mut shutdown: ShutdownWatch) {
    let orders = Index::Orders.to_string();
    let mut feed = match laser.watch().index(&orders).records() {
        Ok(feed) => feed,
        Err(error) => {
            warn!("Managed order watch could not start: {error}");
            return;
        }
    };
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tokio::time::sleep(Duration::from_millis(250)) => match feed.poll().await {
                Ok(records) if !records.is_empty() => {
                    let batches = records.len();
                    info!("Managed orders projection advanced by {batches} change batch(es)");
                }
                Ok(_) => {}
                Err(error) => warn!("Managed order watch failed: {error}"),
            }
        }
    }
}

pub async fn run_registry_loop(laser: Laser, mut shutdown: ShutdownWatch) {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tokio::time::sleep(Duration::from_secs(4)) => match laser.runs().list().agent("fulfillment").limit(20).fetch().await {
                Ok(page) => {
                    let runs = page.runs.len();
                    info!("Managed fulfillment registry currently lists {runs} recent run(s)");
                }
                Err(error) => warn!("Managed run registry query failed: {error}"),
            }
        }
    }
}

pub async fn flash_sale(laser: Laser, apply_plan: bool) {
    if let Err(error) = stage_flash_sale(&laser, apply_plan).await {
        warn!("Managed flash-sale fork failed: {error}");
    }
}

async fn stage_flash_sale(laser: &Laser, apply_plan: bool) -> Result<(), LaserError> {
    let orders = Index::Orders.to_string();
    let mut source_position = None;
    for _ in 0..120 {
        let result = laser
            .query(&orders)
            .filter_eq("event_type", "accepted")
            .limit(1)
            .fetch()
            .await?;
        if let Some(row) = result.rows.first() {
            let partition = result
                .value_u64(row, PARTITION_ID_FIELD_NAME)
                .and_then(|value| u32::try_from(value).ok());
            let offset = result.value_u64(row, OFFSET_FIELD_NAME);
            source_position = partition.zip(offset);
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let Some((partition, offset)) = source_position else {
        info!("No accepted order appeared in time for the flash-sale fork");
        return Ok(());
    };
    let fork = laser.fork(FLASH_SALE_FORK);
    let _ = fork.squash().await;
    fork.create().continuous().send().await?;
    fork.put_row(&orders, partition, offset)
        .field("event_type", "flash_sale_priority")
        .send()
        .await?;
    let staged = laser
        .query(&orders)
        .fork(FLASH_SALE_FORK)
        .filter_eq("event_type", "flash_sale_priority")
        .count()
        .fetch()
        .await?;
    let visible = staged.page.total.is_some_and(|total| total > 0)
        || staged
            .rows
            .first()
            .and_then(|row| staged.value_u64(row, "count"))
            .is_some_and(|count| count > 0);
    if apply_plan && visible {
        let rows = fork.promote().await?;
        info!("Promoted {rows} verified flash-sale fork row(s) to the trunk");
    } else {
        info!("Staged the flash-sale fork without changing the trunk. Verified: {visible}.");
    }
    Ok(())
}
