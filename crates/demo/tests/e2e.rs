use laser_sdk::iggy::prelude::*;
use laser_sdk::prelude::full::{ContextAssembler, StreamEvent};
use laser_sdk::prelude::{ConversationId, Laser, LaserError};
use laser_sdk::wire::agent::{AgentKind, CorrelationId};
use photon_shared::domain::catalog::{CatalogCommand, CatalogOp};
use photon_shared::domain::order::{OrderEvent, OrderEventKind, PlaceOrder};
use photon_shared::domain::risk::{RiskReason, RiskVerdict};
use photon_shared::domain::ticket::{Ticket, TicketKind};
use photon_shared::domain::{ContractVersion, Money, OrderId, TicketId, Timestamp};
use photon_shared::names::{
    self, AdversaryMode, AppAgent, BusinessTopic, GovernorMode, LlmProvider,
};
use photon_shared::testkit::TestIggy;
use photon_shared::trust::DemoTrust;
use photon_shared::{BusinessProvenance, publish_business, topology};
use std::collections::HashSet;
use std::time::{Duration, Instant};
use ulid::Ulid;

const ADVERSARY_SEED: u64 = 42;
const KNOWN_SKU: &str = "sku-photon-phone";

#[tokio::test]
async fn given_fabricated_support_memory_when_enforced_then_should_stream_but_not_remember_it() {
    let iggy = TestIggy::start().await;
    let factory = iggy.factory();
    let admin = factory
        .connect(names::STREAM)
        .await
        .expect("admin connects");
    topology::bootstrap_all(&admin)
        .await
        .expect("topics bootstrap");
    let desk = photon_desk::run(
        &factory,
        photon_desk::Opts {
            governor: GovernorMode::Enforce,
            refund_ceiling: Money(30_000),
            llm_provider: LlmProvider::Mock,
            skew_permille: 1_000,
            scenario_seed: 7,
        },
    )
    .await
    .expect("desk starts");

    let order = OrderId::new(Ulid::from_parts(330, 1));
    let shipped = order_event(
        order,
        OrderEventKind::Shipped {
            carrier: "hermes".to_owned(),
            tracking: "trk-skew".to_owned(),
        },
    );
    publish_order_event(&admin, &shipped, "shipped")
        .await
        .expect("shipped event publishes");
    wait_for_group_head(&admin, "desk-order-lookup", BusinessTopic::OrderEvents, 0).await;

    let first = TicketId::new(Ulid::from_parts(340, 1));
    publish_support_ticket(
        &admin,
        order,
        first,
        TicketKind::WhereIsMyOrder,
        "Remember my fabricated preference.",
    )
    .await
    .expect("first ticket publishes");
    wait_for_group_head(&admin, "support", BusinessTopic::SupportTickets, 0).await;
    let session = laser_sdk::prelude::full::SessionPolicy::PerUser.conversation_for("cust-refund");
    let first_reply = wait_for_chat(&admin, session, first).await;
    assert!(first_reply.contains("[skew:fabricate_memory]"));

    let second = TicketId::new(Ulid::from_parts(350, 2));
    publish_support_ticket(
        &admin,
        order,
        second,
        TicketKind::WhereIsMyOrder,
        "What support memory do you have?",
    )
    .await
    .expect("second ticket publishes");
    wait_for_group_head(&admin, "support", BusinessTopic::SupportTickets, 1).await;
    let second_reply = wait_for_chat(&admin, session, second).await;
    assert!(
        second_reply.contains("Relevant support memory:\nnone"),
        "the blocked fabricated memory must not appear in the next prompt"
    );

    desk.shutdown().await.expect("desk drains");
}

#[tokio::test]
async fn given_repeat_customer_tickets_when_answered_then_should_recall_and_stream_the_session() {
    let iggy = TestIggy::start().await;
    let factory = iggy.factory();
    let admin = factory
        .connect(names::STREAM)
        .await
        .expect("admin connects");
    topology::bootstrap_all(&admin)
        .await
        .expect("topics bootstrap");
    let desk = photon_desk::run(
        &factory,
        photon_desk::Opts {
            governor: GovernorMode::Enforce,
            refund_ceiling: Money(30_000),
            llm_provider: LlmProvider::Mock,
            skew_permille: 0,
            scenario_seed: 7,
        },
    )
    .await
    .expect("desk starts");

    let order = OrderId::new(Ulid::from_parts(300, 1));
    let shipped = order_event(
        order,
        OrderEventKind::Shipped {
            carrier: "atlas".to_owned(),
            tracking: "trk-session".to_owned(),
        },
    );
    publish_order_event(&admin, &shipped, "shipped")
        .await
        .expect("shipped event publishes");
    wait_for_group_head(&admin, "desk-order-lookup", BusinessTopic::OrderEvents, 0).await;

    let first = TicketId::new(Ulid::from_parts(310, 1));
    publish_support_ticket(
        &admin,
        order,
        first,
        TicketKind::WhereIsMyOrder,
        "Is atlas still carrying my first parcel?",
    )
    .await
    .expect("first ticket publishes");
    wait_for_group_head(&admin, "support", BusinessTopic::SupportTickets, 0).await;
    let session = laser_sdk::prelude::full::SessionPolicy::PerUser.conversation_for("cust-refund");
    let first_reply = wait_for_chat(&admin, session, first).await;
    assert!(first_reply.contains("status is `shipped`"));
    assert_eq!(
        first_reply.matches("[mock]").count(),
        1,
        "the replayed chat stream must contain each model chunk exactly once"
    );

    let second = TicketId::new(Ulid::from_parts(320, 2));
    publish_support_ticket(
        &admin,
        order,
        second,
        TicketKind::WhereIsMyOrder,
        "What did I ask before about this parcel?",
    )
    .await
    .expect("second ticket publishes");
    wait_for_group_head(&admin, "support", BusinessTopic::SupportTickets, 1).await;
    let second_reply = wait_for_chat(&admin, session, second).await;
    assert!(
        second_reply.contains("Is atlas still carrying my first parcel?"),
        "the second reply should include semantically recalled session memory"
    );

    desk.shutdown().await.expect("desk drains");
}

#[tokio::test]
async fn given_a_verified_over_ceiling_refund_when_reviewed_then_should_publish_once_after_approval()
 {
    let iggy = TestIggy::start().await;
    let trust = DemoTrust::generate("operator");
    let factory = iggy
        .factory()
        .with_verifier(trust.registry)
        .with_keyring(trust.keyring);
    let admin = factory
        .connect(names::STREAM)
        .await
        .expect("admin connects");
    topology::bootstrap_all(&admin)
        .await
        .expect("topics bootstrap");

    let desk = photon_desk::run(
        &factory,
        photon_desk::Opts {
            governor: GovernorMode::Enforce,
            refund_ceiling: Money(30_000),
            llm_provider: LlmProvider::Mock,
            skew_permille: 0,
            scenario_seed: 7,
        },
    )
    .await
    .expect("desk starts with the reviewer ready");

    let order = OrderId::new(Ulid::from_parts(100, 1));
    let charged = order_event(
        order,
        OrderEventKind::Charged {
            amount: Money(45_000),
        },
    );
    publish_order_event(&admin, &charged, "charged")
        .await
        .expect("charged event publishes");
    wait_for_group_head(&admin, "desk-order-lookup", BusinessTopic::OrderEvents, 0).await;

    for attempt in 0..6 {
        publish_damaged_ticket(&admin, order, attempt)
            .await
            .expect("damaged ticket publishes");
    }
    wait_for_group_head(&admin, "support", BusinessTopic::SupportTickets, 5).await;

    let events = fold_order_events(&admin, 50_000)
        .await
        .expect("order events fold");
    let refunds: Vec<&OrderEvent> = events
        .iter()
        .filter(|event| {
            event.order == order
                && matches!(
                    event.kind,
                    OrderEventKind::Refunded {
                        amount: Money(45_000)
                    }
                )
        })
        .collect();
    assert_eq!(
        refunds.len(),
        1,
        "the approval grant and refund ledger must admit one matching effect"
    );

    desk.shutdown().await.expect("desk drains");
}

#[tokio::test]
async fn given_the_spine_when_run_then_should_accept_deliver_and_fold_a_duplicate_flood_into_one() {
    let iggy = TestIggy::start().await;
    let factory = iggy.factory();
    let admin = factory
        .connect(names::STREAM)
        .await
        .expect("admin connects");
    topology::bootstrap_all(&admin)
        .await
        .expect("topics bootstrap");
    seed_catalog(&admin).await.expect("catalog seeds");

    let orders = photon_orders::run(
        &factory,
        photon_orders::Opts {
            risk_threshold_cents: 50_000,
            crash_after: None,
            zombie_charge: false,
        },
    )
    .await
    .expect("orders starts");
    let adversary = photon_adversary::run(
        &factory,
        photon_adversary::Opts {
            mode: AdversaryMode::Scripted,
            seed: ADVERSARY_SEED,
            rate: 6,
        },
    )
    .await
    .expect("adversary starts");
    let storefront = photon_storefront::run(
        &factory,
        photon_storefront::Opts {
            concurrency: 2,
            scenario_seed: 7,
            session_interval: Duration::from_millis(100),
        },
    )
    .await
    .expect("storefront starts");

    let flood_order = OrderId::new(Ulid::from_parts(1, u128::from(ADVERSARY_SEED)));
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut attempt = 0u64;
    loop {
        attempt += 1;
        let events = fold_order_events(&admin, attempt)
            .await
            .expect("order events fold");
        let accepted: Vec<&OrderEvent> = events
            .iter()
            .filter(|event| matches!(event.kind, OrderEventKind::Accepted))
            .collect();
        let flood_accepted = accepted
            .iter()
            .filter(|event| event.order == flood_order)
            .count();
        let delivered = events
            .iter()
            .filter(|event| matches!(event.kind, OrderEventKind::Delivered))
            .count();

        if accepted.len() >= 5 && delivered >= 1 && flood_accepted >= 1 {
            assert_eq!(
                flood_accepted, 1,
                "a duplicate flood must create exactly one accepted order"
            );
            let rogue = events
                .iter()
                .filter(|event| match &event.kind {
                    OrderEventKind::ShipmentBooked { carrier, .. }
                    | OrderEventKind::Shipped { carrier, .. } => carrier == "borealis",
                    _ => false,
                })
                .count();
            assert_eq!(
                rogue, 0,
                "no fabricated borealis reply may ever become a booking or shipment"
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the spine did not accept and deliver enough orders before the deadline"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    storefront.shutdown().await.expect("storefront drains");
    adversary.shutdown().await.expect("adversary drains");
    wait_for_group_drain(
        &admin,
        &AppAgent::Orders.to_string(),
        BusinessTopic::OrderCommands,
    )
    .await;
    wait_for_accepted_orders_to_settle(&admin).await;
    orders.shutdown().await.expect("orders drains");
}

#[tokio::test]
async fn given_delivery_waiting_when_orders_stops_then_should_deliver_once_after_restart() {
    let iggy = TestIggy::start().await;
    let factory = iggy.factory();
    let admin = factory
        .connect(names::STREAM)
        .await
        .expect("admin connects");
    topology::bootstrap_all(&admin)
        .await
        .expect("topics bootstrap");

    let orders = orders_service(&factory).await;
    let order = (1..=3u128)
        .map(|value| OrderId::new(Ulid::from_parts(400, value)))
        .find(|order| order.as_u128() % 3 == 2)
        .expect("a bounded iterator contains a three-second delivery id");
    let shipped = OrderEvent {
        at: Timestamp::now(),
        ..order_event(
            order,
            OrderEventKind::Shipped {
                carrier: "hermes".to_owned(),
                tracking: format!("trk-{order}"),
            },
        )
    };
    publish_order_event(&admin, &shipped, "shipped")
        .await
        .expect("shipment publishes");
    tokio::time::sleep(Duration::from_millis(300)).await;

    orders.shutdown().await.expect("orders drains");
    let before_restart = fold_order_events(&admin, 90)
        .await
        .expect("order events fold");
    assert!(
        !before_restart.iter().any(|event| {
            event.order == order && matches!(event.kind, OrderEventKind::Delivered)
        })
    );

    let restarted = orders_service(&factory).await;
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut attempt = 91;
    let delivered = loop {
        attempt += 1;
        let events = fold_order_events(&admin, attempt)
            .await
            .expect("order events fold");
        let delivered = events
            .iter()
            .filter(|event| event.order == order && matches!(event.kind, OrderEventKind::Delivered))
            .count();
        if delivered > 0 {
            break delivered;
        }
        assert!(
            Instant::now() < deadline,
            "delivery did not resume before the deadline"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(delivered, 1);
    restarted.shutdown().await.expect("restarted orders drains");
}

#[tokio::test]
async fn given_a_ring_flagged_order_when_reviewed_then_should_clear_or_reject_by_value() {
    let iggy = TestIggy::start().await;
    let factory = iggy.factory();
    let admin = factory
        .connect(names::STREAM)
        .await
        .expect("admin connects");
    topology::bootstrap_all(&admin)
        .await
        .expect("topics bootstrap");
    let desk = photon_desk::run(
        &factory,
        photon_desk::Opts {
            governor: GovernorMode::Enforce,
            refund_ceiling: Money(30_000),
            llm_provider: LlmProvider::Mock,
            skew_permille: 0,
            scenario_seed: 7,
        },
    )
    .await
    .expect("desk starts");

    // A rejected high-value case taints the shared device.
    let tainted = screen(&admin, 600, "cust-ring-a", "device-ring", "card-a", 250_000).await;
    assert_eq!(tainted.verdict, RiskVerdict::Reject);
    assert_eq!(tainted.reasons, vec![RiskReason::HighValue]);

    // A modest order on the tainted device parks in review. The reviewer
    // clears it under the manual ceiling and the verdict says who decided.
    let cleared = screen(&admin, 601, "cust-ring-b", "device-ring", "card-b", 119_700).await;
    assert_eq!(cleared.verdict, RiskVerdict::Clear);
    assert_eq!(
        cleared.reasons,
        vec![RiskReason::DeviceRing, RiskReason::ManualReview]
    );

    // A dear order on the same tainted device stays rejected after review.
    let rejected = screen(&admin, 602, "cust-ring-c", "device-ring", "card-c", 180_000).await;
    assert_eq!(rejected.verdict, RiskVerdict::Reject);
    assert_eq!(
        rejected.reasons,
        vec![RiskReason::DeviceRing, RiskReason::ManualReview]
    );

    desk.shutdown().await.expect("desk drains");
}

#[tokio::test]
async fn given_a_risk_rejected_order_when_settled_then_should_release_its_inventory() {
    let iggy = TestIggy::start().await;
    let factory = iggy.factory();
    let admin = factory
        .connect(names::STREAM)
        .await
        .expect("admin connects");
    topology::bootstrap_all(&admin)
        .await
        .expect("topics bootstrap");
    seed_catalog(&admin).await.expect("catalog seeds");
    let desk = photon_desk::run(
        &factory,
        photon_desk::Opts {
            governor: GovernorMode::Enforce,
            refund_ceiling: Money(30_000),
            llm_provider: LlmProvider::Mock,
            skew_permille: 0,
            scenario_seed: 7,
        },
    )
    .await
    .expect("desk starts");
    let orders = orders_service(&factory).await;
    let order = OrderId::new(Ulid::from_parts(700, 1));

    place(&admin, order, KNOWN_SKU, 30)
        .await
        .expect("high-value order publishes");
    let events = wait_until_rejected(&admin, order).await;
    let kinds: Vec<&OrderEventKind> = events
        .iter()
        .filter(|event| event.order == order)
        .map(|event| &event.kind)
        .collect();
    assert!(
        kinds
            .iter()
            .any(|kind| matches!(kind, OrderEventKind::InventoryReserved)),
        "risk screening happens after reservation"
    );
    assert!(
        kinds
            .iter()
            .any(|kind| matches!(kind, OrderEventKind::InventoryReleased)),
        "a rejected order must return its reservation"
    );

    orders.shutdown().await.expect("orders drains");
    desk.shutdown().await.expect("desk drains");
}

async fn screen(
    laser: &Laser,
    tail: u64,
    customer: &str,
    device: &str,
    card: &str,
    amount_cents: u64,
) -> photon_shared::domain::risk::RiskCaseEvent {
    use laser_sdk::prelude::full::InboxRoute;
    use laser_sdk::prelude::{Contract, RoutePolicy, Router};

    let request = photon_shared::domain::risk::ScreenRequest {
        version: ContractVersion::CURRENT,
        order: OrderId::new(Ulid::from_parts(tail, 1)),
        customer: customer.parse().expect("customer id parses"),
        amount_cents,
        device: device.to_owned(),
        card_fingerprint: card.to_owned(),
    };
    let payload = serde_json::to_vec(&request).expect("screen request serializes");
    let contract = laser
        .contract(Router::to_capable(
            names::Skill::ScreenOrder.to_string(),
            RoutePolicy::Any,
        ))
        .from(AppAgent::Orders.id())
        .payload(payload)
        .inbox_route(InboxRoute::Fixed(laser_sdk::prelude::AgentTopic::Commands))
        .deadline(Duration::from_secs(10))
        .send()
        .await
        .expect("screen contract sends");
    match contract {
        Contract::Completed(reply) => {
            serde_json::from_slice(reply.body()).expect("risk verdict decodes")
        }
        other => panic!("screening did not complete: {other:?}"),
    }
}

#[tokio::test]
async fn given_the_coordinator_crashed_after_charge_when_restarted_then_should_resume_without_a_second_charge()
 {
    let iggy = TestIggy::start().await;
    let factory = iggy.factory();
    let admin = factory
        .connect(names::STREAM)
        .await
        .expect("admin connects");
    topology::bootstrap_all(&admin)
        .await
        .expect("topics bootstrap");
    seed_catalog(&admin).await.expect("catalog seeds");

    let order = OrderId::new(Ulid::from_parts(500, 5));
    let crashing = photon_orders::run(
        &factory,
        photon_orders::Opts {
            risk_threshold_cents: u64::MAX,
            crash_after: Some(names::WorkflowStep::Charge),
            zombie_charge: false,
        },
    )
    .await
    .expect("orders starts with the crash boundary armed");
    place(&admin, order, KNOWN_SKU, 1)
        .await
        .expect("place the order");

    // The charge lands on the journal, then the coordinator dies mid-run:
    // no shipment, no compensation, exactly a killed process.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut attempt = 0u64;
    loop {
        attempt += 1;
        let events = fold_order_events(&admin, attempt)
            .await
            .expect("order events fold");
        if events.iter().any(|event| {
            event.order == order && matches!(event.kind, OrderEventKind::Charged { .. })
        }) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the order was not charged before the crash deadline"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    crashing.shutdown().await.expect("crashed orders drains");

    // The restart folds the log, finds the stranded run, and resumes it from
    // the journal: the recorded charge is replayed, never re-executed.
    let restarted = orders_service(&factory).await;
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut attempt = 1_000u64;
    let events = loop {
        attempt += 1;
        let events = fold_order_events(&admin, attempt)
            .await
            .expect("order events fold");
        if events.iter().any(|event| {
            event.order == order && matches!(event.kind, OrderEventKind::Shipped { .. })
        }) {
            break events;
        }
        assert!(
            Instant::now() < deadline,
            "the resumed run did not ship before the deadline"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };

    let charges = events
        .iter()
        .filter(|event| {
            event.order == order && matches!(event.kind, OrderEventKind::Charged { .. })
        })
        .count();
    assert_eq!(
        charges, 1,
        "the journal resume must replay the recorded charge, not repeat it"
    );
    restarted.shutdown().await.expect("restarted orders drains");
}

#[tokio::test]
async fn given_a_stranded_order_when_the_catalog_is_repaired_and_redriven_then_should_be_accepted()
{
    let iggy = TestIggy::start().await;
    let factory = iggy.factory();
    let admin = factory
        .connect(names::STREAM)
        .await
        .expect("admin connects");
    topology::bootstrap_all(&admin)
        .await
        .expect("topics bootstrap");
    seed_catalog(&admin).await.expect("catalog seeds");

    let orders = orders_service(&factory).await;
    let adversary = photon_adversary::run(
        &factory,
        photon_adversary::Opts {
            mode: AdversaryMode::Scripted,
            seed: ADVERSARY_SEED,
            rate: 6,
        },
    )
    .await
    .expect("adversary starts");

    // The scripted acts strand one valid order on a missing sku and inject one
    // undecodable poison. Both must be quarantined before the operator acts.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let capsules = photon_shared::deadletter::scan(&admin)
            .await
            .expect("dead-letter scan succeeds");
        if capsules.len() >= 2 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the adversary acts did not dead-letter before the deadline"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let redriven = photon_demo::operator::repair_and_redrive(&admin)
        .await
        .expect("operator repair and redrive succeeds");
    let stranded = OrderId::new(Ulid::from_parts(2, u128::from(ADVERSARY_SEED)));
    assert_eq!(
        redriven,
        vec![stranded],
        "exactly the stranded order is redriven, poison stays quarantined"
    );

    wait_until_accepted(&admin, stranded).await;
    let events = fold_order_events(&admin, u64::MAX)
        .await
        .expect("order events fold");
    assert_eq!(
        accepted_for(&events, stranded),
        1,
        "the repaired and redriven order is accepted exactly once"
    );

    adversary.shutdown().await.expect("adversary drains");
    orders.shutdown().await.expect("orders drains");
}

#[tokio::test]
async fn given_stranded_orders_when_the_repair_loop_ticks_then_should_self_heal_without_manual_repair()
 {
    let iggy = TestIggy::start().await;
    let factory = iggy.factory();
    let admin = factory
        .connect(names::STREAM)
        .await
        .expect("admin connects");
    topology::bootstrap_all(&admin)
        .await
        .expect("topics bootstrap");
    seed_catalog(&admin).await.expect("catalog seeds");

    let orders = orders_service(&factory).await;

    // Two orders stranded on the same never-seeded sku. The repair loop
    // must discover and redrive both on its own, with no call to
    // `repair_and_redrive`, proving the loop actually ticks and self-heals.
    let sku = "sku-repair-loop-missing";
    let first = OrderId::new(Ulid::from_parts(400, 1));
    let second = OrderId::new(Ulid::from_parts(400, 2));
    place(&admin, first, sku, 1)
        .await
        .expect("place first stranded order");
    place(&admin, second, sku, 1)
        .await
        .expect("place second stranded order");

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let capsules = photon_shared::deadletter::scan(&admin)
            .await
            .expect("dead-letter scan succeeds");
        if capsules.len() >= 2 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "both orders did not dead-letter before the deadline"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let mut repair = photon_shared::ServiceHandle::new("test-repair-loop");
    repair.track(tokio::spawn(photon_demo::operator::repair_loop(
        admin.clone(),
        Duration::from_millis(50),
        repair.watch(),
    )));

    wait_until_accepted(&admin, first).await;
    wait_until_accepted(&admin, second).await;

    repair.shutdown().await.expect("repair loop drains cleanly");
    orders.shutdown().await.expect("orders drains");
}

#[tokio::test]
async fn given_orders_restarted_when_an_order_is_redelivered_then_should_recover_and_not_double_reserve()
 {
    let iggy = TestIggy::start().await;
    let factory = iggy.factory();
    let admin = factory
        .connect(names::STREAM)
        .await
        .expect("admin connects");
    topology::bootstrap_all(&admin)
        .await
        .expect("topics bootstrap");
    seed_catalog(&admin).await.expect("catalog seeds");

    let first = OrderId::new(Ulid::from_parts(10, 1));
    let second = OrderId::new(Ulid::from_parts(20, 2));

    let orders = orders_service(&factory).await;
    place(&admin, first, KNOWN_SKU, 1)
        .await
        .expect("place first order");
    wait_until_order_event(&admin, first, |kind| {
        matches!(kind, OrderEventKind::Shipped { .. })
    })
    .await;
    orders
        .shutdown()
        .await
        .expect("orders drains before restart");

    // Restart: a fresh orders process rebuilds inventory from the log, then a
    // redelivery of the first order and a brand new order both arrive.
    let restarted = orders_service(&factory).await;
    place(&admin, second, KNOWN_SKU, 1)
        .await
        .expect("place second order");
    place(&admin, first, KNOWN_SKU, 1)
        .await
        .expect("redeliver first order");
    wait_until_accepted(&admin, second).await;
    restarted.shutdown().await.expect("restarted orders drains");

    let events = fold_order_events(&admin, u64::MAX)
        .await
        .expect("order events fold");
    let accepted_first = accepted_for(&events, first);
    let accepted_second = accepted_for(&events, second);
    assert_eq!(
        accepted_first, 1,
        "a redelivered order after restart must not create a second acceptance"
    );
    assert_eq!(
        accepted_second, 1,
        "a new order after restart must be accepted, proving inventory recovered"
    );
}

async fn orders_service(factory: &photon_shared::LaserFactory) -> photon_shared::ServiceHandle {
    photon_orders::run(
        factory,
        photon_orders::Opts {
            risk_threshold_cents: 50_000,
            crash_after: None,
            zombie_charge: false,
        },
    )
    .await
    .expect("orders starts")
}

async fn place(laser: &Laser, order: OrderId, sku: &str, quantity: u32) -> Result<(), LaserError> {
    let command = PlaceOrder {
        version: ContractVersion::CURRENT,
        order,
        customer: "cust-test".parse().expect("customer id"),
        sku: sku.parse().expect("sku"),
        quantity,
        unit_price: Money(9_900),
        device: "device-test".to_owned(),
        ship_to: "addr-test".to_owned(),
        card_fingerprint: "card-test".to_owned(),
        placed_at: Timestamp::now(),
    };
    publish_business(
        laser,
        BusinessTopic::OrderCommands,
        &command,
        BusinessProvenance {
            conversation: order.to_string().parse().expect("conversation id"),
            source: AppAgent::Operator,
            causal_parent: None,
            idempotency_key: format!("place-order/{order}"),
        },
    )
    .await
}

async fn wait_until_accepted(laser: &Laser, order: OrderId) {
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut attempt = 1_000u64;
    loop {
        attempt += 1;
        let events = fold_order_events(laser, attempt)
            .await
            .expect("order events fold");
        if accepted_for(&events, order) >= 1 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "order {order} was not accepted before the deadline"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

async fn wait_until_order_event(
    laser: &Laser,
    order: OrderId,
    expected: impl Fn(&OrderEventKind) -> bool,
) {
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut attempt = 1_500u64;
    loop {
        attempt += 1;
        let events = fold_order_events(laser, attempt)
            .await
            .expect("order events fold");
        if events
            .iter()
            .any(|event| event.order == order && expected(&event.kind))
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "order {order} did not reach the expected event before the deadline"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn accepted_for(events: &[OrderEvent], order: OrderId) -> usize {
    events
        .iter()
        .filter(|event| event.order == order && matches!(event.kind, OrderEventKind::Accepted))
        .count()
}

async fn wait_until_rejected(laser: &Laser, order: OrderId) -> Vec<OrderEvent> {
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut attempt = 2_000u64;
    loop {
        attempt += 1;
        let events = fold_order_events(laser, attempt)
            .await
            .expect("order events fold");
        if events.iter().any(|event| {
            event.order == order && matches!(event.kind, OrderEventKind::Rejected { .. })
        }) {
            return events;
        }
        assert!(
            Instant::now() < deadline,
            "order {order} was not rejected before the deadline"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn seed_catalog(laser: &Laser) -> Result<(), LaserError> {
    for (sku, available) in photon_storefront::catalog() {
        let command = CatalogCommand {
            version: ContractVersion::CURRENT,
            op: CatalogOp::UpsertSku {
                sku: sku.clone(),
                available,
            },
        };
        publish_business(
            laser,
            BusinessTopic::CatalogCommands,
            &command,
            BusinessProvenance {
                conversation: ConversationId::derive(sku.as_ref()),
                source: AppAgent::Operator,
                causal_parent: None,
                idempotency_key: format!("catalog-upsert/{sku}"),
            },
        )
        .await?;
    }
    Ok(())
}

fn order_event(order: OrderId, kind: OrderEventKind) -> OrderEvent {
    OrderEvent {
        version: ContractVersion::CURRENT,
        order,
        customer: "cust-refund".parse().expect("customer id parses"),
        sku: KNOWN_SKU.parse().expect("sku parses"),
        quantity: 1,
        at: Timestamp::from_micros(100),
        kind,
    }
}

async fn publish_order_event(
    laser: &Laser,
    event: &OrderEvent,
    suffix: &str,
) -> Result<(), LaserError> {
    publish_business(
        laser,
        BusinessTopic::OrderEvents,
        event,
        BusinessProvenance {
            conversation: event.order.to_string().parse().expect("conversation id"),
            source: AppAgent::Orders,
            causal_parent: None,
            idempotency_key: format!("order-event/{}/{suffix}", event.order),
        },
    )
    .await
}

async fn publish_damaged_ticket(
    laser: &Laser,
    order: OrderId,
    attempt: u64,
) -> Result<(), LaserError> {
    let ticket_id = TicketId::new(Ulid::from_parts(200 + attempt, u128::from(attempt)));
    publish_support_ticket(
        laser,
        order,
        ticket_id,
        TicketKind::Damaged,
        "The parcel arrived damaged. Please refund it.",
    )
    .await
}

async fn publish_support_ticket(
    laser: &Laser,
    order: OrderId,
    ticket_id: TicketId,
    kind: TicketKind,
    body: &str,
) -> Result<(), LaserError> {
    let customer = "cust-refund".parse().expect("customer id parses");
    let ticket = Ticket {
        version: ContractVersion::CURRENT,
        ticket: ticket_id,
        customer,
        order,
        kind,
        body: body.to_owned(),
        opened_at: Timestamp::from_micros(ticket_id.as_u128() as u64),
    };
    publish_business(
        laser,
        BusinessTopic::SupportTickets,
        &ticket,
        BusinessProvenance {
            conversation: ConversationId::derive(ticket.customer.as_ref()),
            source: AppAgent::Storefront,
            causal_parent: None,
            idempotency_key: format!("ticket/{ticket_id}"),
        },
    )
    .await
}

async fn wait_for_chat(laser: &Laser, session: ConversationId, ticket: TicketId) -> String {
    let deadline = Instant::now() + Duration::from_secs(30);
    let correlation = CorrelationId::from_u128(ticket.as_u128());
    loop {
        let messages = ContextAssembler::builder()
            .conversation_id(session)
            .topics(vec![laser_sdk::prelude::AgentTopic::LlmIo])
            .build()
            .assemble(laser)
            .await
            .expect("LLM stream context assembles");
        if let Some(channel) = messages.iter().find_map(|message| {
            message.envelope.as_ref().and_then(|envelope| {
                (envelope.kind == AgentKind::Chunk && envelope.correlation == Some(correlation))
                    .then_some(envelope.channel)
                    .flatten()
            })
        }) {
            let events = laser
                .reassemble_channel(session, laser_sdk::prelude::AgentTopic::LlmIo, channel)
                .await
                .expect("chat stream reassembles");
            if events.iter().any(|event| {
                matches!(
                    event,
                    StreamEvent::Finished {
                        finish_reason: Some(reason),
                        synthetic: false,
                        ..
                    } if reason == "stop"
                )
            }) {
                let body = events
                    .iter()
                    .filter_map(|event| match event {
                        StreamEvent::Body { payload, .. } => Some(payload.as_slice()),
                        _ => None,
                    })
                    .flatten()
                    .copied()
                    .collect();
                return String::from_utf8(body).expect("support reply is utf8");
            }
        }
        assert!(
            Instant::now() < deadline,
            "support did not finish chat stream for ticket {ticket}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for_group_head(laser: &Laser, group: &str, topic: BusinessTopic, minimum_head: u64) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let consumer = Consumer::group(Identifier::named(group).expect("consumer group identifier"));
    let stream = Identifier::named(names::STREAM).expect("stream identifier");
    let topic = Identifier::named(&topic.to_string()).expect("topic identifier");
    loop {
        for partition in 0..topology::PARTITIONS {
            let offset = laser
                .client()
                .get_consumer_offset(&consumer, &stream, &topic, Some(partition))
                .await
                .expect("consumer offset reads");
            if offset.is_some_and(|offset| {
                offset.current_offset >= minimum_head
                    && offset.stored_offset >= offset.current_offset
            }) {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "consumer group {group} did not reach the head of {topic}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for_group_drain(laser: &Laser, group: &str, topic: BusinessTopic) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let consumer = Consumer::group(Identifier::named(group).expect("consumer group identifier"));
    let stream = Identifier::named(names::STREAM).expect("stream identifier");
    let topic = Identifier::named(&topic.to_string()).expect("topic identifier");
    loop {
        let mut drained = true;
        for partition in 0..topology::PARTITIONS {
            let offset = laser
                .client()
                .get_consumer_offset(&consumer, &stream, &topic, Some(partition))
                .await
                .expect("consumer offset reads");
            if !offset.is_some_and(|offset| offset.stored_offset >= offset.current_offset) {
                drained = false;
                break;
            }
        }
        if drained {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "consumer group {group} did not drain {topic}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for_accepted_orders_to_settle(laser: &Laser) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut attempt = 60_000u64;
    loop {
        attempt += 1;
        let events = fold_order_events(laser, attempt)
            .await
            .expect("order events fold");
        let accepted: HashSet<OrderId> = events
            .iter()
            .filter_map(|event| {
                matches!(event.kind, OrderEventKind::Accepted).then_some(event.order)
            })
            .collect();
        let terminal: HashSet<OrderId> = events
            .iter()
            .filter_map(|event| {
                matches!(
                    event.kind,
                    OrderEventKind::Shipped { .. }
                        | OrderEventKind::Rejected { .. }
                        | OrderEventKind::ChargeRefunded { .. }
                )
                .then_some(event.order)
            })
            .collect();
        if !accepted.is_empty() && accepted.is_subset(&terminal) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "accepted orders did not settle before shutdown"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn fold_order_events(laser: &Laser, attempt: u64) -> Result<Vec<OrderEvent>, LaserError> {
    let typed = BusinessTopic::OrderEvents.topic(laser).json::<OrderEvent>();
    let mut records = typed.records(&format!("e2e-order-fold-{attempt}"))?;
    let mut events = Vec::new();
    while let Some(next) = records.next().await {
        if let Ok(record) = next {
            events.push(record.value);
        }
    }
    Ok(events)
}
