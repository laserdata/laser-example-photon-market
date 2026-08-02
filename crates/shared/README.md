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
- `LaserFactory` (`src/connect.rs`): one connection story for local Laser Stack and LaserData Cloud, with the demo trust attach points.
- `ServiceHandle` (`src/lifecycle.rs`): readiness, cooperative cancellation, propagated background failures, a global drain deadline, abort-on-timeout fallback, and dependency ordering so an agent that draining handlers still call stops only after they finish.
- `output` and `telemetry` (`src/output.rs`, `src/telemetry.rs`): readable narration without exposing application log fields to terminal control sequences.

## Laser Stack versus LaserData Cloud

| concern | local Laser Stack | LaserData Cloud |
| --- | --- | --- |
| connection | `iggy:laser@127.0.0.1:8090` over VSR | deployment credentials over VSR, with the TLS CA attached automatically |
| semantic seam | `DeterministicReranker` over log-backed durable memory | `DeterministicReranker` over log-backed durable memory |
| managed surfaces | advertised by the local plane | advertised by the hosted plane |

## Focused tests

Unit tests sit beside each module. `src/lifecycle.rs` proves cooperative cleanup and bounded abort of stuck work. `tests/integration.rs` proves the open streaming topology against the LaserData Iggy fork, without the plane, through the shared `TestIggy` harness. Run `cargo test -p photon-shared`, then `just test-it`.
