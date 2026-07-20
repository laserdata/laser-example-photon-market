# Architecture

## Boundaries

Photon Market is four services, one adversary simulator, one shared library, and one demo runner. Every service is its own binary and connects through the same `LaserFactory`. Services never call each other: a record on a topic is the only interface, so the log is the single source of truth for messaging, ordering, replay, and audit.

One order equals one conversation. The `ConversationId` minted at checkout is the partition key for everything the order touches, so per order ordering is total and the whole journey rebuilds from one conversation.

No Photon service holds a direct database or object store client. External stores attach only downstream of the log.

## Streams and topics

| stream | topics | why |
| --- | --- | --- |
| `photon` | `catalog.commands`, `order.commands`, `order.events`, `risk.events`, `support.tickets`, agent and memory topics | the application boundary: coordination and business events share one stream so routing and replay stay coherent |
| `photon-traffic` | `shop.events` | high volume clickstream, decoupled retention |

## Contracts

Bodies are JSON with `agdx.ct=json`, decoded into versioned domain types. Every command that may cause an effect carries a stable business key. Money is integer cents (`Money`), except an untrusted carrier quote price, which is a signed integer so a verifier can reject a negative wire value before converting it. Timestamps are epoch microseconds (`Timestamp`). Contract versions ride a `ContractVersion` field spelled `v` on the wire.

| topic | type | partition key | producer to consumers |
| --- | --- | --- | --- |
| `catalog.commands` | `CatalogCommand` (flat, internally tagged `{v, op, ...}`) | SKU | operator to orders |
| `shop.events` | `ShopEvent` | session | storefront to insights |
| `order.commands` | `PlaceOrder` | order conversation | storefront, adversary to orders |
| `order.events` | `OrderEvent` | order conversation | orders to insights, desk, storefront |
| `risk.events` | `RiskCaseEvent` | order conversation | desk to insights |
| `support.tickets` | `Ticket` | customer session | storefront to desk |

## Delivery semantics

Streaming and handlers are at least once. Ordering is per partition. Replay-safe folds rebuild service state before intake opens, and handlers consult that durable history before repeating work. Local ledgers give one process conflict control. Key value compare and swap gives cross process conflict control. Fenced compare and swap gives stale holder rejection. These are never compressed into "exactly once".

## Shutdown and process death

Ctrl+C and SIGTERM start a graceful shutdown. The demo stops new ingress before dependent services. SDK agents stop polling, finish in-flight handlers, and commit handled offsets. Raw consumers leave their groups explicitly. Handlers receive 10 seconds to drain and each service receives 15 seconds overall.

The orders service owns every fulfillment task. At shutdown it pauses active workflows without compensation, then resumes their journals on restart. Delivery uses manual commit and stores the `Shipped` offset only after `Delivered` is durable. If a deadline expires, the task is aborted and shutdown returns an error. The uncommitted input is replayed after restart.

SIGKILL and machine loss cannot run cleanup. Recovery comes from the log, idempotent effects, workflow journals, and explicit commit boundaries. The application does not claim graceful behavior for forced process death.

## Local versus managed

| concern | raw Apache Iggy | LaserData Cloud |
| --- | --- | --- |
| order pipeline, agents, contracts, workflow | runs | runs |
| inventory, charge, refund idempotency | in process ledger folded from the log | key value compare and swap, cross process |
| dashboards, order lookup | consumer group fold | materialized indexes, query DSL, watch |
| memory | explicit vector backend | durable memory plus reranker |
| graph, forks, run registry, schema guard, fenced exclusivity | skipped with a pointer | live |

Every managed surface is behind a capability check made once at startup. A managed operation returning unsupported after startup is an error, never a silent downgrade. The raw Iggy column is not the SDK degrading: the in-process ledger and the dashboard folds are this example's own application code, and each states its scope honestly.
