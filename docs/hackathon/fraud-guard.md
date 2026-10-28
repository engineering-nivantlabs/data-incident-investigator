# Fraud Guard MVP

Open **http://127.0.0.1:8080/fraud**. Select **Start guided fraud demo** for the complete merchant-side story. Autoplay advances through nine steps and takes about a minute. Pause, Previous step and Next step support discussion. Run URLs restore the latest saved step in paused mode. New runs preserve existing experiments.

## What executes

The Flask control surface calls a real Python state machine. Synthetic transactions pass a strict contract, rolling attempt counts and known-device checks. Versioned deterministic policy returns ALLOW, CHALLENGE, REVIEW, BLOCK or UNAVAILABLE. A gateway adapter calls an issuer adapter only when a transaction is eligible. SQLite atomically persists the session, local issuer ledger, audit evidence and Signal investigation projection. Historical demo steps capture their own evidence and lineage.

Fraud clearance is separate from payment authorization. An issuer decline remains a decline. Analyst release applies only to REVIEW transactions before authorization. It never overrides BLOCK, failed authentication, UNAVAILABLE or an issuer decision. Human review happens as an asynchronous hold/release concept, not an indefinitely waiting gateway request.

## Guided story

1. Receive a normal purchase and a large purchase on a previously seen device.
2. Compute rules and features. The normal purchase gets ALLOW. The large purchase gets REVIEW.
3. Authorize the normal purchase while the flagged purchase remains held.
4. Open the investigation context with supporting rules and payment-flow lineage.
5. Request release as `analyst-demo`, binding the current evidence fingerprint and expiry.
6. Approve as distinct `supervisor-demo`.
7. Execute release without changing the original fraud finding.
8. Authorize the released purchase. The synthetic issuer accepts it, but an injected lost acknowledgment leaves gateway state UNKNOWN.
9. Reconcile the matching issuer receipt. Two requests have two receipts, without a third authorization.

## Manual scenarios

| Scenario | Action to demonstrate | Expected evidence |
|---|---|---|
| Normal purchase | Evaluate, authorize | ALLOW followed by APPROVED |
| Issuer decline | Evaluate, authorize | ALLOW followed by DECLINED |
| Velocity | Evaluate, attempt authorize | Four attempts in 60 seconds, BLOCK, zero receipts |
| New device and large amount | Evaluate, pass authentication, authorize | CHALLENGE gates the issuer call |
| Authentication failure | Evaluate, fail authentication, attempt authorize | FAILED challenge, zero issuer calls |
| Analyst review and timeout | Evaluate, request, approve, release, authorize, reconcile | Evidence-bound release, UNKNOWN resolved by receipt |
| Risk service unavailable | Evaluate, attempt authorize | UNAVAILABLE with explicit HOLD policy, zero receipts |
| Authorization timeout | Evaluate, authorize, reconcile | UNKNOWN, no blind retry, one issuer receipt |

## Contracts and safety boundaries

Transactions use synthetic SYN-prefixed identifiers, integer USD minor units and a bounded country vocabulary. Extra fields, including PAN/CVV/PIN fields, fail validation. This is a demo contract restriction, not a PCI certification or a complete data-loss prevention system. Never submit real card or personal data.

Idempotency binds both transaction identity and request key to a payload within a session. Identical duplicate submissions reuse the existing transaction. Conflicts fail. Repeated final authorization requests reuse the result. UNKNOWN requires reconciliation before retry. Session isolation means no claim of cross-session or global payment deduplication.

Velocity uses the trusted virtual receipt clock rather than caller-supplied event time, and counts attempts across the session's history. Out-of-order timestamps cannot bypass the window. Each evaluated transaction freezes its feature snapshot and policy result. Values and thresholds are illustrative. There is no trained fraud model, calibrated probability or claimed fraud accuracy.

Authentication challenges expire after 120 virtual seconds. Passing authentication is a simulation, not EMV 3DS. Analyst proposals also expire after 120 seconds. Request, approval and execution use named simulated roles. State changes or clock advancement invalidate the evidence fingerprint. An executed release must still be current and unexpired when authorization begins. These local actor names do not implement enterprise authentication or production separation of duties.

The default risk-service outage policy is HOLD. Real deployment must agree fallback behavior with risk and payments owners. No live fallback policy or latency SLO is established by the local demo. Authorize, evaluate and reconcile are explicit synchronous local operations; no background Kafka consumer or real-time networking is implied.

Signal's graph represents the declared payment processing path, not Unity Catalog table lineage. Green means the session reached safely handled demo outcomes, including a policy block or issuer decline. It does not establish that a payment is legitimate or fraud-free. An unresolved service outage or unknown authorization keeps completion blocked. Investigations preserve their original risk reasons in the audit even after completion.

## Architecture and Databricks boundary

Synthetic generator → validation and feature computation → policy → authentication / analyst hold → gateway simulator → issuer simulator → persisted evidence → Signal investigation and guided UI.

`investigator/fraud.py` contains rule evaluation, simulator adapters and the transactional execution service. `static/fraud.js` drives the presentation and invokes backend commands. The existing platform shares evidence and navigation, while payment state remains separate from Delivery Lab transfer recovery.

Fraud Guard runs only in local synthetic mode. The current Databricks App path does not enable this module or migrate its SQLite state. A later pilot needs authenticated merchant identity, tenant-scoped idempotency, an operational claim/decision store, feature freshness and an actual serving latency evaluation. Databricks can host governed historical events, feature pipelines and evaluation jobs. Integration of Fraud Guard with the Workflow Builder's exported Databricks sources remains future work.

No real gateway, issuer, payment network, Kafka, MFTS, EMV 3DS, LLM or external model service receives a call. The issuer receipt ledger makes acceptance observable locally. A real gateway requires provider-specific lookup/reconciliation APIs and explicit authorization/capture/reversal semantics, crash-window testing, status retention and callback validation. Capture, refund, reversal, settlement and chargeback handling are outside this authorization-only MVP.

## Pilot acceptance

- Replay identical and conflicting requests and verify expected idempotency behavior.
- Demonstrate ALLOW with issuer approval and independently with issuer decline.
- Confirm blocks, failed authentication and service outages never reach the issuer.
- Reject missing, wrong-role, stale and expired approvals.
- Enforce authentication and release expiry before authorization.
- Reconcile ambiguous acceptance without a second issuer receipt.
- Preserve state after restart and serialize competing authorization attempts.
- Roll back local issuer state and Signal evidence together on persistence failure.
- Keep historical demo snapshots unchanged after later recovery.
- Validate real adapter semantics separately before any live endpoint pilot.

The local transactional model cannot establish atomicity between an external issuer and the operational store. Synthetic test success does not establish production safety, legal compliance, fraud detection accuracy or business value.

## Local validation record

The implementation suite passed 59 Python tests, with JavaScript syntax and lineage-model checks also passing. `scripts/evaluate_fraud.py` independently exercises all eight scenario families and writes [fraud-evaluation.json](fraud-evaluation.json). These are local checks, not fraud accuracy or cloud performance measurements.
