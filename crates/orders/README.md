# Photon orders

Orders owns the order lifecycle: intake with deduplication, inventory truth, risk screening as a contract, the journaled fulfillment saga, and the honest carriers it hires. A delivery tracker advances shipped orders only after a short delay and commits its input only after `Delivered` is durable. Every state change is an `OrderEvent` on the log, which is also how the service recovers after a kill.

## Owned topics and contracts

| surface | type | role |
| --- | --- | --- |
| `order.commands` | `PlaceOrder` | consumed by `OrdersHandler` behind a `SlidingWindow` dedup, with commit and dead-letter failures propagated to service shutdown |
| `catalog.commands` | `CatalogCommand` | consumed by `CatalogHandler`: `UpsertSku` sets stock absolutely (seeding), `Restock` adds to it (repair) |
| `order.events` | `OrderEvent` | published for every lifecycle fact, schema guarded when managed |
| `screen_order` contract | `ScreenRequest` | sent by capability with a deadline, no reply fails closed |
| `quote_shipment`, `book_shipment` | `CarrierRequest` | quorum fan-out, then a directed booking with runner-up fallback |
| `fulfillment` workflow | `FulfillmentTask` | charge, quote, book, dispatch with reverse compensations |
| `orders-delivery` group | `OrderEvent` | tails shipments, publishes delivery, then commits the shipment offset |

## Start here

- `run` (`src/lib.rs`): service composition, with the carrier agents tracked as a shutdown dependency so they outlive the main drain and an in-flight fulfillment handler never waits out its carrier deadlines.
- `Runtime::prepare` (`src/runtime.rs`): capability-driven selection of local or managed state backends, performed once before handlers start.
- `OrdersHandler::handle` (`src/handler.rs`): log-derived intake state, reservation idempotency, rejection cleanup, and the screening gate.
- `run_saga` (`src/saga.rs`): the journaled workflow, its budget, compensations, cancellation ownership, and the simulated crash boundary.
- `delivery::run` (`src/delivery.rs`): delayed lifecycle progression with explicit commit-after-publish semantics.
- `ConversationState` and `OrderStates` (`src/state.rs`): the pure lifecycle transition and its replay-safe in-process read model.

## Laser Stack versus LaserData Cloud

| concern | local Laser Stack | LaserData Cloud |
| --- | --- | --- |
| inventory | `ManagedInventory` KV compare-and-swap, cross process | `ManagedInventory` KV compare-and-swap, cross process |
| charge idempotency | `ManagedChargeLedger` fenced CAS, stale holders rejected | `ManagedChargeLedger` fenced CAS, stale holders rejected |
| `order.events` schema guard | JSON Schema enforced on publish | JSON Schema enforced on publish |
| run registry | registered through the runs API | registered through the runs API |

## Focused tests

Unit tests sit beside the code. End-to-end tests prove the open streaming path over Apache Iggy, cancellation before delivery commit, reservation cleanup, redelivery idempotency, and crash recovery without a second charge. Run `cargo test -p photon-orders`, then `just e2e`.
