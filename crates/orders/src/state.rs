use photon_shared::domain::order::{OrderEvent, OrderEventKind};
use photon_shared::domain::{CustomerId, OrderId, Sku};
use std::collections::HashMap;
use std::sync::Mutex;
use strum::Display;

#[derive(Clone, Copy, Debug, Default, Display, Eq, PartialEq)]
#[strum(serialize_all = "snake_case")]
pub enum OrderState {
    #[default]
    Received,
    Reserved,
    Accepted,
    Rejected,
    Charged,
    Shipped,
    Delivered,
    Refunded,
    /// The saga unwound: the charge was refunded as a compensation, so the
    /// order is settled and a restart must not resume its fulfillment.
    Compensated,
}

impl OrderState {
    fn seeded_by(kind: &OrderEventKind) -> Option<OrderState> {
        match kind {
            OrderEventKind::Received => Some(OrderState::Received),
            OrderEventKind::InventoryReserved => Some(OrderState::Reserved),
            OrderEventKind::InventoryReleased => Some(OrderState::Received),
            OrderEventKind::Accepted => Some(OrderState::Accepted),
            OrderEventKind::Rejected { .. } => Some(OrderState::Rejected),
            OrderEventKind::Charged { .. } => Some(OrderState::Charged),
            OrderEventKind::Shipped { .. } => Some(OrderState::Shipped),
            OrderEventKind::Delivered => Some(OrderState::Delivered),
            OrderEventKind::Refunded { .. } => Some(OrderState::Refunded),
            OrderEventKind::ChargeRefunded { .. } => Some(OrderState::Compensated),
            // Intermediate saga facts: the booking (or its release) does not
            // move the order between resume-relevant states on its own.
            OrderEventKind::ShipmentBooked { .. } | OrderEventKind::BookingReleased { .. } => None,
        }
    }

    fn transition(self, kind: &OrderEventKind) -> OrderState {
        match kind {
            OrderEventKind::InventoryReleased => match self {
                OrderState::Reserved | OrderState::Received => OrderState::Received,
                OrderState::Accepted | OrderState::Charged | OrderState::Compensated => {
                    OrderState::Compensated
                }
                settled => settled,
            },
            _ => OrderState::seeded_by(kind).unwrap_or(self),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConversationState {
    pub order: OrderId,
    pub customer: CustomerId,
    pub sku: Sku,
    pub quantity: u32,
    pub state: OrderState,
}

impl ConversationState {
    /// A saga was cut short: the order was accepted (or even charged) but never
    /// reached shipment or a settling compensation, so a restarted coordinator
    /// should resume its fulfillment run from the journal.
    pub fn needs_resume(&self) -> bool {
        matches!(self.state, OrderState::Accepted | OrderState::Charged)
    }

    /// Intake has already made its durable decision. A redelivered command
    /// must not reserve again or start a second workflow.
    pub fn intake_complete(&self) -> bool {
        !matches!(self.state, OrderState::Received | OrderState::Reserved)
    }

    pub fn load<I: IntoIterator<Item = OrderEvent>>(events: I) -> Option<Self> {
        let mut folded: Option<ConversationState> = None;
        for event in events {
            match folded.as_mut() {
                None => folded = Some(ConversationState::seed(event)),
                Some(state) => state.apply(&event),
            }
        }
        folded
    }

    fn seed(event: OrderEvent) -> Self {
        let state = OrderState::seeded_by(&event.kind).unwrap_or_default();
        Self {
            order: event.order,
            customer: event.customer,
            sku: event.sku,
            quantity: event.quantity,
            state,
        }
    }

    fn apply(&mut self, event: &OrderEvent) {
        self.state = self.state.transition(&event.kind);
    }
}

#[derive(Default)]
pub struct OrderStates {
    states: Mutex<HashMap<OrderId, ConversationState>>,
}

impl OrderStates {
    pub fn observe(&self, event: &OrderEvent) {
        let mut states = self
            .states
            .lock()
            .expect("order-state lock is not poisoned");
        match states.get_mut(&event.order) {
            Some(state) => state.apply(event),
            None => {
                states.insert(event.order, ConversationState::seed(event.clone()));
            }
        }
    }

    pub fn get(&self, order: OrderId) -> Option<ConversationState> {
        self.states
            .lock()
            .expect("order-state lock is not poisoned")
            .get(&order)
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use photon_shared::domain::{ContractVersion, Money, Timestamp};

    fn event(kind: OrderEventKind) -> OrderEvent {
        OrderEvent {
            version: ContractVersion::CURRENT,
            order: "01ARZ3NDEKTSV4RRFFQ69G5FAV"
                .parse()
                .expect("order id parses"),
            customer: "cust-1".parse().expect("customer id parses"),
            sku: "sku-1".parse().expect("sku parses"),
            quantity: 2,
            at: Timestamp::from_micros(1),
            kind,
        }
    }

    #[test]
    fn given_the_happy_path_events_when_folded_then_should_reach_accepted() {
        let state = ConversationState::load([
            event(OrderEventKind::Received),
            event(OrderEventKind::InventoryReserved),
            event(OrderEventKind::Accepted),
        ])
        .expect("state folds");
        assert_eq!(state.state, OrderState::Accepted);
        assert_eq!(state.quantity, 2);
    }

    #[test]
    fn given_a_rejected_command_when_folded_then_should_reach_rejected() {
        let state = ConversationState::load([
            event(OrderEventKind::Received),
            event(OrderEventKind::Rejected {
                reason: "unknown sku".to_owned(),
            }),
        ])
        .expect("state folds");
        assert_eq!(state.state, OrderState::Rejected);
    }

    #[test]
    fn given_a_replayed_event_stream_when_folded_twice_then_should_be_idempotent() {
        let events = [
            event(OrderEventKind::Received),
            event(OrderEventKind::InventoryReserved),
            event(OrderEventKind::InventoryReserved),
            event(OrderEventKind::Accepted),
        ];
        let once = ConversationState::load(events.clone()).expect("state folds");
        let twice = ConversationState::load(events.iter().chain(events.iter()).cloned())
            .expect("state folds");
        assert_eq!(once, twice);
    }

    #[test]
    fn given_no_events_when_folded_then_should_be_none() {
        assert_eq!(ConversationState::load([]), None);
    }

    #[test]
    fn given_a_charged_but_unshipped_history_when_folded_then_should_need_resume() {
        let state = ConversationState::load([
            event(OrderEventKind::Received),
            event(OrderEventKind::InventoryReserved),
            event(OrderEventKind::Accepted),
            event(OrderEventKind::Charged { amount: Money(100) }),
        ])
        .expect("state folds");
        assert!(state.needs_resume());
    }

    #[test]
    fn given_a_settled_history_when_folded_then_should_not_need_resume() {
        let shipped = ConversationState::load([
            event(OrderEventKind::Accepted),
            event(OrderEventKind::Charged { amount: Money(100) }),
            event(OrderEventKind::Shipped {
                carrier: "hermes".to_owned(),
                tracking: "trk-1".to_owned(),
            }),
        ])
        .expect("state folds");
        assert!(!shipped.needs_resume());

        let compensated = ConversationState::load([
            event(OrderEventKind::Accepted),
            event(OrderEventKind::Charged { amount: Money(100) }),
            event(OrderEventKind::BookingReleased {
                carrier: "hermes".to_owned(),
                booking: "bk-1".to_owned(),
            }),
            event(OrderEventKind::ChargeRefunded { amount: Money(100) }),
            event(OrderEventKind::InventoryReleased),
        ])
        .expect("state folds");
        assert_eq!(compensated.state, OrderState::Compensated);
        assert!(!compensated.needs_resume());
        assert!(compensated.intake_complete());
    }

    #[test]
    fn given_a_reserved_order_when_inventory_is_released_then_should_return_to_intake() {
        let state = ConversationState::load([
            event(OrderEventKind::Received),
            event(OrderEventKind::InventoryReserved),
            event(OrderEventKind::InventoryReleased),
        ])
        .expect("state folds");
        assert_eq!(state.state, OrderState::Received);
        assert!(!state.intake_complete());
    }

    #[test]
    fn given_an_accepted_order_when_inventory_is_released_then_should_be_compensated() {
        let state = ConversationState::load([
            event(OrderEventKind::Received),
            event(OrderEventKind::InventoryReserved),
            event(OrderEventKind::Accepted),
            event(OrderEventKind::InventoryReleased),
        ])
        .expect("state folds");
        assert_eq!(state.state, OrderState::Compensated);
        assert!(!state.needs_resume());
        assert!(state.intake_complete());
    }

    #[test]
    fn given_replayed_events_when_observed_then_order_states_should_fold_by_order() {
        let states = OrderStates::default();
        for event in [
            event(OrderEventKind::Received),
            event(OrderEventKind::InventoryReserved),
            event(OrderEventKind::Accepted),
        ] {
            states.observe(&event);
        }
        let order = event(OrderEventKind::Received).order;
        assert_eq!(
            states.get(order).expect("order state exists").state,
            OrderState::Accepted
        );
    }
}
