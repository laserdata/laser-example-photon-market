use crate::refunds::LocalRefundLedger;
use async_trait::async_trait;
use laser_sdk::prelude::{Consumer, Laser, LaserError};
use photon_shared::ShutdownWatch;
use photon_shared::domain::order::{OrderEvent, OrderEventKind};
use photon_shared::domain::{Money, OrderId, Sku};
use photon_shared::names::{BusinessTopic, Index};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::warn;

// The raw-Iggy order lookup: a live fold of order.events into the facts the
// support agent needs, so a reply grounds in a real status and a refund pays
// out the real charged amount rather than trusting model prose. On LaserData
// Cloud this seam is the query surface instead.
#[derive(Clone)]
pub struct OrderFacts {
    pub status: String,
    pub charged: Option<Money>,
    pub sku: Sku,
    pub quantity: u32,
}

#[async_trait]
pub trait OrderReader: Send + Sync {
    async fn status(&self, order: &OrderId) -> Result<Option<String>, LaserError>;
    async fn facts(&self, order: &OrderId) -> Result<Option<OrderFacts>, LaserError>;
}

#[derive(Default)]
pub struct LocalOrderLookup {
    orders: Mutex<HashMap<OrderId, OrderFacts>>,
}

impl LocalOrderLookup {
    pub fn new() -> Self {
        Self::default()
    }

    fn status_local(&self, order: &OrderId) -> Option<String> {
        self.orders
            .lock()
            .expect("order lookup lock is not poisoned")
            .get(order)
            .map(|facts| facts.status.clone())
    }

    fn facts_local(&self, order: &OrderId) -> Option<OrderFacts> {
        self.orders
            .lock()
            .expect("order lookup lock is not poisoned")
            .get(order)
            .cloned()
    }

    fn observe(&self, event: &OrderEvent) {
        let mut orders = self
            .orders
            .lock()
            .expect("order lookup lock is not poisoned");
        let facts = orders.entry(event.order).or_insert_with(|| OrderFacts {
            status: String::new(),
            charged: None,
            sku: event.sku.clone(),
            quantity: event.quantity,
        });
        facts.status = event.kind.to_string();
        if let OrderEventKind::Charged { amount } = &event.kind {
            facts.charged = Some(*amount);
        }
    }
}

#[async_trait]
impl OrderReader for LocalOrderLookup {
    async fn status(&self, order: &OrderId) -> Result<Option<String>, LaserError> {
        Ok(self.status_local(order))
    }

    async fn facts(&self, order: &OrderId) -> Result<Option<OrderFacts>, LaserError> {
        Ok(self.facts_local(order))
    }
}

pub struct ManagedOrderLookup {
    laser: Laser,
}

impl ManagedOrderLookup {
    pub fn new(laser: Laser) -> Self {
        Self { laser }
    }

    async fn load(&self, order: &OrderId) -> Result<Option<OrderFacts>, LaserError> {
        let index = Index::Orders.to_string();
        let events = self
            .laser
            .query(&index)
            .filter_eq("order", order.to_string())
            .order_asc("at_micros")
            .limit(100)
            .fetch_typed::<OrderEvent>()
            .await?;
        Ok(fold_order_facts(events.iter()))
    }
}

#[async_trait]
impl OrderReader for ManagedOrderLookup {
    async fn status(&self, order: &OrderId) -> Result<Option<String>, LaserError> {
        Ok(self.load(order).await?.map(|facts| facts.status))
    }

    async fn facts(&self, order: &OrderId) -> Result<Option<OrderFacts>, LaserError> {
        self.load(order).await
    }
}

fn fold_order_facts<'a>(events: impl IntoIterator<Item = &'a OrderEvent>) -> Option<OrderFacts> {
    let mut facts = None;
    for event in events {
        let current = facts.get_or_insert_with(|| OrderFacts {
            status: String::new(),
            charged: None,
            sku: event.sku.clone(),
            quantity: event.quantity,
        });
        current.status = event.kind.to_string();
        if let OrderEventKind::Charged { amount } = event.kind {
            current.charged = Some(amount);
        }
    }
    facts
}

pub async fn start_order_fold(laser: &Laser) -> Result<Consumer, LaserError> {
    BusinessTopic::OrderEvents
        .topic(laser)
        .consumer_group("desk-order-lookup")
        .batch_length(200)
        .poll_interval(Duration::from_millis(20))
        .build()
        .await
}

pub async fn rebuild_order_state(
    laser: &Laser,
    lookup: &LocalOrderLookup,
    refunds: &LocalRefundLedger,
) -> Result<(), LaserError> {
    let offsets = vec![0u64; photon_shared::topology::PARTITIONS as usize];
    let events = BusinessTopic::OrderEvents.topic(laser).json::<OrderEvent>();
    let mut records = events
        .records("desk-order-lookup-rebuild")?
        .from_offsets(offsets);
    while let Some(next) = records.next().await {
        if let Ok(record) = next {
            lookup.observe(&record.value);
            refunds.observe(&record.value);
        }
    }
    Ok(())
}

pub async fn fold_orders(
    mut consumer: Consumer,
    lookup: Arc<LocalOrderLookup>,
    refunds: Arc<LocalRefundLedger>,
    mut shutdown: ShutdownWatch,
) {
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            next = consumer.next() => match next {
                Some(Ok(received)) => {
                    if let Ok(event) = received.json::<OrderEvent>() {
                        lookup.observe(&event);
                        refunds.observe(&event);
                    }
                }
                Some(Err(error)) => warn!("Support order-lookup consumer error: {error}"),
                None => break,
            }
        }
    }
    if let Err(error) = consumer.shutdown().await {
        warn!("Support order lookup did not leave its consumer group cleanly. {error}");
    }
}
