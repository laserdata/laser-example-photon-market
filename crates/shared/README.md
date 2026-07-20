# Photon shared

The shared crate is the vocabulary every service speaks: versioned domain contracts, topic and agent names as enums, the connection factory, topology bootstrap, provenance-stamped publishing, lifecycle plumbing, configuration knobs, the LLM seam, and the Docker test harness. No business logic lives here.

## Owned topics and contracts

| surface | type | role |
| --- | --- | --- |
| `domain::*` | `PlaceOrder`, `OrderEvent`, `RiskCaseEvent`, `Ticket`, ... | the wire contracts, versioned with `ContractVersion` and built on newtypes (`Money`, `Timestamp`, id types) |
| `names` | `BusinessTopic`, `AppAgent`, `Skill`, `Index`, `KvSpace` | every name an enum, never a magic string |
| `publish` | `publish_business`, `BusinessProvenance` | conversation, source, and idempotency stamped on every business record |
| `knobs` | `LASER_*` consts | configuration keys as constants with structured errors |
| `output`, `telemetry` | terminal narration and tracing filters | one stdout stream, one ANSI decision, clean trusted sections, structured application logs, and compact live status |

## Start here

- `BusinessTopic` and `AppAgent` (`src/names.rs`): the whole system's surface area in two enums.
- `publish_business` and `BusinessProvenance` (`src/publish.rs`): how every hop carries its lineage.
- `LaserFactory` (`src/connect.rs`): one connection story for local Iggy and LaserData Cloud, with the demo trust attach points.
- `ServiceHandle` (`src/lifecycle.rs`): readiness, cooperative cancellation, a global drain deadline, and abort-on-timeout fallback.
- `output` and `telemetry` (`src/output.rs`, `src/telemetry.rs`): readable narration without exposing application log fields to terminal control sequences.

## Raw Apache Iggy versus LaserData Cloud

| concern | raw Apache Iggy | LaserData Cloud |
| --- | --- | --- |
| connection | local default connection string | TLS CA auto attached for `*.laserdata.cloud` hosts |
| semantic seam | `DeterministicEmbedder` for explicit vector memory | `DeterministicReranker` over log-backed durable memory |
| skips | the demo capability report prints one precise line per absent surface, service-local gates stay at debug | surfaces live |

## Focused tests

Unit tests sit beside each module. `src/lifecycle.rs` proves cooperative cleanup and bounded abort of stuck work. `tests/integration.rs` proves topology bootstrap against Apache Iggy through the shared `TestIggy` harness. Run `cargo test -p photon-shared`, then `just test-it`.
