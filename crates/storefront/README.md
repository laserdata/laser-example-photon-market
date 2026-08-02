# Photon storefront

The storefront is Photon Market's world: the only module that fabricates data. It walks seeded shoppers through browse, cart, and checkout, places the resulting orders, and opens support tickets for a share of shipped orders. Everything downstream reacts to what it publishes.

`LASER_CONCURRENCY` controls independent shopper streams. `LASER_SESSION_INTERVAL_MS` controls their approximate pace. Each stream applies seeded jitter around that interval.

## Owned topics and contracts

| surface | type | role |
| --- | --- | --- |
| `shop.events` (photon-traffic) | `ShopEvent` | clickstream batches, partitioned by session |
| `order.commands` | `PlaceOrder` | one checkout, conversation id minted from the order id |
| `support.tickets` | `Ticket` | damaged and where-is-my-order tickets, per customer session |
| `order.events` | `OrderEvent` | consumed only, to open tickets after a shipment |

## Start here

- `ShopperWalk::next_session` (`src/shoppers.rs`): the seeded world model, replayable behavior with per-run identity epochs.
- `publish_order` (`src/publisher.rs`): where the order's `ConversationId` and idempotency key are minted, the start of every provenance chain.
- `tickets::run` (`src/tickets.rs`): tails `order.events` and turns a share of shipments into support tickets.

## Laser Stack versus LaserData Cloud

| concern | local Laser Stack | LaserData Cloud |
| --- | --- | --- |
| clickstream, orders, tickets | runs | runs |
| managed surfaces | none used: the storefront only publishes and tails | identical, no code change |

## Focused tests

Unit tests live in `src/shoppers.rs`: same-seed reproducibility, the checkout ratio staying within bounds over a long walk, and checkouts drawing from the shared device, address, and card pools that let the fraud graph find rings. Run `cargo test -p photon-storefront`. The publish path is exercised end to end by the spine test in `crates/demo/tests/e2e.rs` (behind the `e2e` feature).
