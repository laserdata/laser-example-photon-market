# Photon Market: contributor guide

Photon Market is a fictional online store built as a distributed system on the Laser SDK over Apache Iggy. It is four small services (`storefront`, `orders`, `desk`, `insights`), an `adversary` simulator, and a `demo` composition root, each a Laser SDK client that coordinates only through the log. There are no direct service to service calls: a message on a topic is the only interface.

The same binaries run two ways. Against a local Apache Iggy the core order story runs green with no model key and no cloud account, and every managed surface prints one precise skip line. Against a LaserData Cloud connection string the managed surfaces (queryable projections, key value coordination, forks, graphs, durable memory, agent operations) light up with no code change.

## Structure

```
crates/
  shared/       contracts, names, connect, topology, lifecycle, config, llm seam, output, tracing
  storefront/   traffic generator: clickstream, orders, tickets
  orders/       order lifecycle, inventory, fulfillment workflow, carriers
  desk/         risk and support agents, reviewer, action governor
  insights/     projections, dashboards, forks, audit, dead letters
  adversary/    injected misbehavior: rogue carrier, impersonator, poison, floods
  demo/         finite and living composition root over the service libraries
docs/           architecture notes and the end to end walkthrough
```

TLS for a `*.laserdata.cloud`/`*.laserdata.com` host is auto attached by `laser-sdk` itself (its own bundled CA), no code or cert in this repo.

Each crate owns its tests. Unit tests live next to the code. Integration and end to end tests live under the crate's own `tests/` directory behind `integration` and `e2e` cargo features, so `cargo test --workspace` stays Docker free. The shared `TestIggy` harness lives in `photon-shared` behind the `testkit` feature.

## Conventions

- No em dashes anywhere.
- Terse, self documenting code. Comments only for a genuinely non obvious decision.
- One sorted import block per file.
- Order items for top-down reading: constants and types first, then public entry points, then private helpers in first-use order. A caller must never require jumping upward in the same scope. Test cases come before their local fixture helpers.
- Enums over magic strings, with `strum` `Display`/`EnumString` and the richer derives (`EnumIter` and friends) where they remove duplication.
- Domain newtypes over raw primitives (`Money`, `Timestamp`, `ContractVersion`, id types), with the wire spelling kept through `#[serde(rename = ...)]`.
- `FromStr::Err` and every crate error are structured `thiserror` enums with meaningful variants.
- `.expect("meaningful message")`, never a bare `.unwrap()`, in code and tests alike.
- `bon` builders where a builder earns it.
- `#![forbid(unsafe_code)]` in every crate.
- Runtime output goes through `tracing` with well written, inline interpolated messages, never `println!` (except the one version banner before tracing is initialized). Everything rides stdout: the banner, the demo narration, and the tracing formatter share one stream, and ANSI is resolved once for all of them (NO_COLOR wins, a nonzero CLICOLOR_FORCE forces color, otherwise color follows the terminal). An explicit RUST_LOG takes the pipeline verbatim with no benign-noise suppression.
- Tests assert typed structs or exact `serde_json::json!` values, never poke `json["field"]` or compare against raw JSON string literals.
- BDD test names: `given_<state>_when_<action>_then_should_<outcome>`.
- Cite code by symbol, not line number.
- Docs are part of every change: a crate change updates its README section and the root README tables in the same change.

## Verification order

```
cargo fmt --all
cargo sort --workspace
cargo machete --with-metadata
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace                        # unit, Docker free
cargo test --workspace --all-features --doc   # feature gated doctests
just test-it                                  # focused Docker integration
just e2e                                       # full Docker scenario
```

`just ci` runs the whole sequence.

## What runs where

The local versus managed matrix lives in [docs/architecture.md](docs/architecture.md), the single source for it. Per-crate READMEs restate only their own slice.
