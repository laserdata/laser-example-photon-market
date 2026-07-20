# Photon adversary

The adversary joins the fabric from outside and misbehaves through the same public surface every honest service uses: no privileged hooks, no test seams. It runs Borealis, a rogue carrier that lies in contracts, and fires poison, floods, forged replies, and impersonation at the running system so the defenses can be watched working.

## Owned topics and contracts

| surface | act | expected containment |
| --- | --- | --- |
| `order.commands` | `poison`, `duplicate_flood`, `unprocessable` | dead-lettered, deduplicated to one order, stranded for repair |
| agent responses | `unsolicited_replies` | forged bookings ignored, no live correlation matches |
| agent registry | `impersonate` | claim-only card, the forged unsigned quarantine never folds |
| `quote_shipment`, `book_shipment` | `Borealis` replies | absurd quotes excluded by the panel verifier, bookings never win |

## Start here

- `acts` (`src/acts.rs`): every attack as one small function with its expected containment.
- `Borealis` (`src/borealis.rs`): the rogue carrier's negative quotes and fabricated bookings.
- `run_script` (`src/lib.rs`): the scripted sequence `LASER_ADVERSARY=scripted` fires, and the seeded `random` loop next to it.

## Raw Apache Iggy versus LaserData Cloud

| concern | raw Apache Iggy | LaserData Cloud |
| --- | --- | --- |
| every act and its containment | runs | runs |
| managed surfaces | none used: the defenses live in the other crates | identical, no code change |

## Focused tests

The adversary carries no unit tests of its own: its acts only mean something against the running system, so the proofs live in `crates/demo/tests/e2e.rs` (behind the `e2e` feature). The spine test asserts a duplicate flood becomes exactly one accepted order and no fabricated Borealis reply ever becomes a booking. The repair test asserts poison stays quarantined while the stranded order is redriven and accepted exactly once.
