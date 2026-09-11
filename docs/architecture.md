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

Ctrl+C and SIGTERM start a graceful shutdown. The demo stops new ingress before dependent services. SDK agents stop polling, drain their bounded partition lanes, and commit each successfully handled offset. A commit or dead-letter publication failure is surfaced by service shutdown. Raw consumers leave their groups explicitly. Handlers receive 10 seconds to drain and each service receives 15 seconds overall.

The orders service owns every fulfillment task. At shutdown it pauses active workflows without compensation, then resumes their journals on restart. Its carrier agents stop only after the main drain, so a fulfillment handler caught in flight completes its quote or booking round-trip instead of waiting out carrier deadlines. Delivery uses manual commit and stores the `Shipped` offset only after `Delivered` is durable. If a deadline expires, the task is aborted and shutdown returns an error. The uncommitted input is replayed after restart.

SIGKILL and machine loss cannot run cleanup. Recovery comes from the log, idempotent effects, workflow journals, and explicit commit boundaries. The application does not claim graceful behavior for forced process death.

## Deployment targets

| concern | local Laser Stack | LaserData Cloud |
| --- | --- | --- |
| order pipeline, agents, contracts, workflow | runs over the local log | runs over the hosted log |
| inventory, charge, refund idempotency | key value compare and swap, cross process | key value compare and swap, cross process |
| dashboards, order lookup | materialized indexes, query DSL, watch | materialized indexes, query DSL, watch |
| memory | durable memory plus deterministic reranking | durable memory plus deterministic reranking |
| graph, forks, run registry, schema guard, fenced exclusivity | live through the local plane | live through the hosted plane |
| connection | `iggy:laser@127.0.0.1:8090`, plaintext on loopback | deployment credentials, TLS CA attached automatically |
| operations | two current Docker images and local volumes | hosted deployment and the full cloud experience |

Every managed surface is behind a capability check made once at startup. A managed operation returning unsupported after startup is an error, never a silent downgrade. The open Apache Iggy fallbacks remain explicit application implementations for baseline tests, but the default `just up` runtime uses Laser Stack and selects the managed paths.
