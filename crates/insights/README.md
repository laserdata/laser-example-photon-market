# Photon insights

Insights is the read side and owns no truth: it derives everything from the log. On raw Apache Iggy it folds live dashboards in process. On LaserData Cloud it registers queryable projections and adds watch feeds, the workflow run registry, and a fork-based what-if. It also audits the governor's policy-evidence chains for tamper.

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

## Raw Apache Iggy versus LaserData Cloud

| concern | raw Apache Iggy | LaserData Cloud |
| --- | --- | --- |
| dashboards, order lookup | consumer-group folds in process | materialized indexes, query DSL |
| change feeds | none, folds tail the log directly | projection watch feed |
| run registry, forks | skipped with a pointer | live |
| evidence chain audit | runs | runs |

## Focused tests

Unit tests in `src/dashboards.rs` cover linked evidence, restart boundaries, digest mismatches, replay, and live totals. Run `cargo test -p photon-insights`. Managed surfaces require a LaserData Cloud connection.
