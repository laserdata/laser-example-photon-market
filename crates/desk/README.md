# Photon desk

The desk is Photon Market's agentic safety boundary. It screens risky orders, answers grounded support tickets, and places refund effects behind an action governor and a visible reviewer.

## What it does

1. The risk agent advertises `screen_order`, recalls customer precedent, records customer-device-card links, and applies a deterministic policy: an outsized amount rejects, a ring link or repeat-rejection precedent parks the order in review for the human gate (digest-bound, fails closed on timeout), and everything else clears. Raw Iggy uses vector memory and a replayed link fold. Managed mode uses durable memory and graph traversal. Every terminal verdict lands on `risk.events` and answers the screening contract.
2. The support agent consumes `support.tickets`, enforces a stable `SessionPolicy::PerUser` conversation, combines bounded ticket context with semantically recalled support memory and grounded order facts, and uses the configured `LlmClient` only to phrase status answers. Raw Iggy replays `order.events`. Managed mode queries the orders projection.
3. Damaged-order refunds use the charged amount from the log. Raw Iggy uses a process-local replayed ledger. Managed mode claims the order through KV CAS for cross-process idempotency. In enforce mode, an over-ceiling publish steps up before the effect occurs.
4. The reviewer receives a typed request on `agent.human_input`. It approves only when the proposed refund matches the grounded order value and returns the exact proposed-effect digest.
5. An approval becomes a one-use grant keyed by conversation, scope, and digest. The same governed publish is retried, while altered payloads and repeated tickets cannot reuse the grant.
6. Every status answer is emitted as a buffered AGDX `chat` stream on `agent.llm_io`, with model token usage on the terminal chunk. Model failures produce a bounded grounded fallback.

The over-ceiling refund handshake, end to end:

```mermaid
sequenceDiagram
    participant S as Support agent
    participant G as Governor
    participant R as Reviewer
    participant L as Refund ledger

    S->>G: propose refund with effect digest
    G-->>S: step up required
    S->>R: request approval for the digest
    R-->>S: one-use grant (digest must match)
    S->>G: retry the exact same effect
    G-->>S: allow once
    S->>L: claim the order, publish Refunded
```

## Run it

```sh
just up
LASER_GOVERNOR=enforce LASER_REFUND_CEILING=30000 cargo run -p photon-desk
```

The default `mock` model needs no API key. Use `LASER_LLM_PROVIDER=anthropic` or `openai` only with the matching compiled feature and key.

## Where to look

On raw Apache Iggy, inspect `support.tickets`, `order.events`, `risk.events`, `agent.human_input`, and `agent.audit` in the `photon` stream. The `support`, `risk`, and `reviewer` consumer groups show the three independent agents. On LaserData Cloud, inspect the `orders` projection, the `fraud` graph, the `risk` and `support` memory views, the refund KV namespace, and the same policy-evidence topic.

## Highlights

- `runtime` selects local folds or managed memory, query, graph, and KV at startup. Handlers receive narrow traits and remain deployment-independent.
- `Agent::builder` runs the ticket and human-input handlers through reliable consumer groups.
- `MemoryBackend::Vector` makes local support and risk memory explicit. Managed query and KV select log-backed memory wrapped by `DeterministicReranker`.
- `Laser::with_governor` covers both agent effects and typed topic publishes (`risk.events`, `order.events`).
- `AgentCtx::approval_gate` and `respond_input` park and resume the refund handler through the log.
- `ApprovalGrants` binds a decision to one conversation, scope, and SHA-256 payload digest.

Focused e2e tests prove per-user recall across tickets, deterministic chunk reassembly, fabricated-memory containment, and exactly one `Refunded` event after six damaged-ticket deliveries.
