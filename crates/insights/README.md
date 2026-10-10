# Photon insights

Insights is the read side and owns no truth: it derives everything from the log. On Laser Stack and LaserData Cloud it registers queryable projections and adds watch feeds, a session index listing of fulfillment runs, and a fork-based what-if. It also folds live dashboards in process and audits the governor's policy-evidence chains for tamper.

## Owned topics and contracts

| surface | type | role |
| --- | --- | --- |
| `shop.events` | `ShopEvent` | folded into the funnel dashboard |
| `order.events` | `OrderEvent` | folded into the order-state dashboard, projected when managed |
| dead-letter topic | `AgentDeadLetter` | tailed and counted, each capsule narrated |
| audit topic | `PolicyEvidence` | replay-deduplicated chain audit and per-agent activity |
| `orders`, `shop_events`, `tickets`, `risk_cases` indexes | projections | managed query, watch, and fork surface |

## Start here

- `Dashboards` and `EvidenceChains::fold` (`src/dashboards.rs`): replay-safe evidence chains plus the SDK's `SwarmActivity` supervisor view.
- `projections::register` (`src/projections.rs`): the projection and binding declarations with retention policies.
- `flash_sale` (`src/managed.rs`): stage a change on a fork, verify it there, and only then promote or discard.

## Laser Stack versus LaserData Cloud

| concern | local Laser Stack | LaserData Cloud |
| --- | --- | --- |
| dashboards, order lookup | materialized indexes, query DSL | materialized indexes, query DSL |
| change feeds | projection watch feed | projection watch feed |
| session index, forks | live | live |
| evidence chain audit | runs | runs |

## Focused tests

Unit tests in `src/dashboards.rs` cover linked evidence, restart boundaries, digest mismatches, replay, and live totals. Run `cargo test -p photon-insights`. Managed surfaces require Laser Stack or a LaserData Cloud connection.
