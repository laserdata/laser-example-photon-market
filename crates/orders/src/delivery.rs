use crate::events;
use crate::state::OrderState;
use laser_sdk::prelude::{CommitPolicy, Consumer, Laser, LaserError};
use photon_shared::ShutdownWatch;
use photon_shared::domain::Timestamp;
use photon_shared::domain::order::{OrderEvent, OrderEventKind};
use photon_shared::names::BusinessTopic;
use std::time::Duration;
use tracing::{debug, info, warn};

const MIN_DELIVERY_DELAY: Duration = Duration::from_secs(1);
const DELIVERY_JITTER_SECS: u64 = 3;

pub async fn consumer(laser: &Laser) -> Result<Consumer, LaserError> {
    BusinessTopic::OrderEvents
        .topic(laser)
        .consumer_group("orders-delivery")
        .batch_length(100)
        .commit_policy(CommitPolicy::Disabled)
        .poll_interval(Duration::from_millis(20))
        .build()
        .await
}

pub async fn run(
    laser: Laser,
    mut consumer: Consumer,
    publisher: events::Publisher,
    mut shutdown: ShutdownWatch,
) {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            next = consumer.next() => match next {
                Some(Ok(received)) => {
                    let handled = match received.json::<OrderEvent>() {
                        Ok(event) if matches!(&event.kind, OrderEventKind::Shipped { .. }) => {
                            match deliver(&laser, &publisher, event, &mut shutdown).await {
                                Ok(handled) => handled,
                                Err(error) => {
                                    let partition = received.partition_id;
                                    let offset = received.position.offset;
                                    warn!("Delivery tracker could not advance the shipment at partition {partition}, offset {offset}. {error}");
                                    false
                                }
                            }
                        }
                        Ok(_) => true,
                        Err(error) => {
                            let partition = received.partition_id;
                            let offset = received.position.offset;
                            warn!("Delivery tracker skipped an undecodable order event at partition {partition}, offset {offset}. {error}");
                            true
                        }
                    };
                    if handled && let Err(error) = consumer.commit(&received).await {
                        let partition = received.partition_id;
                        let offset = received.position.offset;
                        warn!("Delivery tracker could not commit partition {partition}, offset {offset}. {error}");
                    }
                }
                Some(Err(error)) => warn!("Delivery tracker consumer failed: {error}"),
                None => break,
            }
        }
    }
    if let Err(error) = consumer.shutdown().await {
        warn!("Delivery tracker did not leave its consumer group cleanly. {error}");
    }
}

async fn deliver(
    laser: &Laser,
    publisher: &events::Publisher,
    shipped: OrderEvent,
    shutdown: &mut ShutdownWatch,
) -> Result<bool, LaserError> {
    if publisher
        .state(shipped.order)
        .is_some_and(|state| state.state == OrderState::Delivered)
    {
        return Ok(true);
    }

    let delay = delivery_delay(shipped.order.as_u128());
    let age = Timestamp::now()
        .as_micros()
        .saturating_sub(shipped.at.as_micros());
    let remaining = delay.saturating_sub(Duration::from_micros(age));
    let recovered = remaining.is_zero();
    tokio::select! {
        _ = shutdown.cancelled() => return Ok(false),
        _ = tokio::time::sleep(remaining) => {}
    }

    if publisher
        .state(shipped.order)
        .is_some_and(|state| state.state == OrderState::Delivered)
    {
        return Ok(true);
    }
    let event = OrderEvent {
        kind: OrderEventKind::Delivered,
        at: Timestamp::now(),
        ..shipped
    };
    publisher.emit(laser, &event).await?;
    let order = event.order;
    if recovered {
        debug!("Recovered shipped order {order} and advanced it to delivered");
    } else {
        info!("Carrier delivered order {order}");
    }
    Ok(true)
}

fn delivery_delay(order: u128) -> Duration {
    MIN_DELIVERY_DELAY + Duration::from_secs((order as u64) % DELIVERY_JITTER_SECS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_an_order_when_delivery_is_scheduled_then_delay_should_be_stable_and_bounded() {
        let delay = delivery_delay(42);
        assert_eq!(delay, delivery_delay(42));
        assert!((Duration::from_secs(1)..=Duration::from_secs(3)).contains(&delay));
    }
}
