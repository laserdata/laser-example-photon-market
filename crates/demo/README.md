# Photon demo

The demo is the composition root. It bootstraps topology, creates temporary trust keys, and starts every service in one process. `just demo` runs until Ctrl+C. `just demo-once` runs the finite scenario. The composition root also quarantines the rogue carrier, repairs valid dead letters, and lifts the quarantine.

Long-running mode is the default. It uses paced traffic, seeded fault injection, model skew, enforced governance, continuous repair, and a compact dashboard. `just run-calm` disables injected faults. Finite mode is explicit (`photon-demo finite` or `just demo-once`) and exits after the deterministic walkthrough.

On Ctrl+C or SIGTERM, the demo stops storefront and adversary ingress first. It then drains orders, desk, and insights in dependency order. Every service has a fixed deadline. Forced termination is covered by replay and recovery, not by cleanup hooks.

## Owned topics and contracts

| surface | type | role |
| --- | --- | --- |
| all business and agent topics | bootstrap | `topology::bootstrap_all` ensures them before any service starts |
| `catalog.commands` | `CatalogCommand` | seeded (`UpsertSku`, absolute) by the operator, restocked (`Restock`, additive) on repair |
| dead-letter topic | `AgentDeadLetter` | scanned, repaired, and redriven once in finite mode and continuously in long-running mode |
| agent registry | signed facts | quarantine and unquarantine of the rogue carrier |

## Start here

- `scenario::run` (`src/scenario.rs`): the whole arc, bootstrap through shutdown, in one readable function.
- `repair_and_redrive` and `repair_loop` (`src/operator.rs`): poison stays quarantined. A valid order with an unknown SKU gets a catalog restock and redrive. Finite mode performs one repair pass. Long-running mode repeats it on an interval.
- `probe` (`src/probe.rs`): the settle observers. Finite mode stops ingress after the first shipped proof, then waits for acts contained, repair applied, unquarantine folded, and every started order terminal with bounded deadlines.

## Laser Stack versus LaserData Cloud

| concern | local Laser Stack | LaserData Cloud |
| --- | --- | --- |
| finite and long-running modes | run | run |
| managed surfaces | light up through the local plane | light up through the hosted plane |
| trust | ephemeral demo keys, signed operator facts, managed backends | same, with the full hosted experience |

## Focused tests

`tests/e2e.rs` (behind the `e2e` feature, Docker) proves the composed system: the spine accepts orders while a duplicate flood folds to one and drains committed ingress before shutdown, screening clears or rejects ring-flagged orders by value, a verifier accepts the reviewer's signed approval and an over-ceiling refund publishes exactly once, a crashed coordinator resumes without a second charge, a repaired stranded order is accepted exactly once, and two orders stranded on the same sku both self-heal once `repair_loop` starts ticking, with no manual repair call. Run `just e2e`.
