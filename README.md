# Signal — Data Incident Investigator

An internal pilot for Databricks incident investigation, with Jobs API collection, Unity Catalog lineage, actual table freshness checks, evidence timelines, assignment and recovery verification.

## Architect-ready hackathon demo

Open **http://127.0.0.1:8080/architecture** for the review narrative, then **http://127.0.0.1:8080/delivery** for DeliveryGuard.

This is a real local execution path consuming synthetic JSON records. Python performs ingestion, validation, routing, identity generation, local broker/transfer writes, manifest tracking, deterministic risk evaluation, evidence-bound approval and recovery, and consumer-content verification. SQLite commits execution state, audit and linked Signal projection together. External systems are local adapters: no live Kafka, MFTS, Spark, Databricks or model is invoked by Delivery Lab.

Golden flow: Start Treasury deadline / MFTS-only recovery → Run synthetic pipeline → Request approval → Approve with a separate simulated identity → Execute → Refresh and verify consumers. Five Kafka writes remain five while transfer artifacts increase from zero to five. Replay skips acknowledged deliveries. Snapshots preserve failure, channel recovery and consumer verification.

Nine executable scenarios cover success, MFTS-only retry, ambiguous Kafka acknowledgment, exhausted retries, stalled processing, optional transfer, quarantine, duplicate replay and deferred Kafka. The lab is disabled in live Databricks mode; its local approval identities are not enterprise authentication.

- [Architecture, seven-minute script and acceptance gates](docs/hackathon/architecture-and-demo.md)
- [Generated nine-scenario evaluation](docs/hackathon/evaluation.json)
- Regenerate evaluation: `.venv/bin/python scripts/evaluate_delivery.py`

## Run locally

Use Python 3.11+ for the pilot (the code is also tested with the available Python 3.9 local runtime).

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m investigator.server
```

Open http://127.0.0.1:8080. Demo mode opens eight preloaded scenario investigations backed by a separate demo-scenarios.sqlite3 database. Explore failed ingestion, successful-but-stale data, event-time lag, denied freshness access, duplicate IDs, expired evidence, missing lineage, and a recovered pipeline. Each case includes a visual lineage graph from the incident table through curated metrics to downstream reports. Red/amber marks an incident and potential exposure; green marks source recovery while reports remain pending verification. Use Before recovery / Current status after simulation. Filter cases, inspect evidence, click Simulate recovery, then change status to resolved. Replay scenarios refreshes baseline signals for open cases; resolved snapshots remain preserved. All fixtures are synthetic and simulation endpoints are blocked in live mode.

## Connect a workspace locally

Configure approved Databricks SDK unified authentication (for example CLI OAuth profile), then edit config/pilot.json with real table and job identities.

```sh
export DATABRICKS_CONFIG_PROFILE=YOUR_PROFILE
export DATABRICKS_WAREHOUSE_ID=YOUR_WAREHOUSE_ID
DII_MODE=databricks DII_CONFIG=config/pilot.json python -m investigator.server
```

Click Sync telemetry, then Investigate. Credentials stay server-side. Collection reads completed job runs from seven days, table freshness, and 30-day observed lineage. Each asset's optional `freshness_column` selects max event time; otherwise latest Delta commit is used. Sources/errors appear in the evidence timeline. Declare every table you want in observed dependency traversal; both endpoints must be allowlisted.

Table-based recovery requires recent table evidence within its configured age threshold; job success alone cannot clear a stale table. Observation TTL defaults to 15 minutes. Timestamp freshness does not verify business completeness. Unknown/expired observations block resolution. Assets without tables retain a clearly labeled run-completion proxy.

## Databricks App deployment

See [internal pilot deployment guide](docs/databricks-pilot.md), `app.yaml`, `databricks.yml` and `config/bootstrap.sql`.

The app runs Gunicorn behind the Databricks authentication proxy, using managed app identity and a SQL warehouse resource. In Apps mode, state must be persisted in a pre-provisioned Delta table (`DII_STATE_TABLE`); it never falls back to ephemeral SQLite. The app does not grant permissions or modify monitored tables. All pilot users share app-identity evidence visibility; restrict app access accordingly.

Configuration: DII_MODE (`demo`/`databricks`), DII_CONFIG (JSON), DII_DB (local SQLite), DII_PORT (local port), DATABRICKS_WAREHOUSE_ID, DII_STATE_TABLE. Apps supplies DATABRICKS_APP_PORT and managed authentication.

## Verification

```sh
.venv/bin/python -m unittest discover -s tests -v
node --check static/app.js
node tests/test_lineage.cjs
```

Tests cover freshness/TTL, empty/future/missing timestamps, failed observations, lineage allowlisting, dependency cycles, SQL parameterization/chunks, HTTP assignment/resolution, and persistence. Live SQL/API execution and workspace deployment require a real workspace and remain unverified here.

## Pilot limits

- Single instance/worker with serialized mutations. Delta JSON state is durable but intentionally simple and inefficient at large volumes.
- Live Databricks mode still uses manual telemetry sync and incident creation; no live notifications, AI diagnosis or repair execution. Delivery Lab has local execution only.
- Observed lineage can be delayed/incomplete; only configured table-to-table edges are collected. No column lineage or BI connector yet.
- Latest commit may reflect maintenance. MAX timestamp queries can scan data. Use appropriate event-time semantics and warehouse budgets.
- App-wide shared visibility; no per-user evidence authorization or multi-tenancy. Local development server stays loopback-only.
- Quality-rule interpretation exists, but no live quality provider is wired. No schema diff adapter.
- Source messages and incident metadata can contain sensitive information; treat app state as confidential.

Collector, engine, persistence and UI remain separate for future enterprise hosting and storage evolution. See [competitor research](docs/competitive-landscape.md). No third-party project code was copied and no product license has been selected.

## Simulated end-to-end recovery

In demo mode, click **Simulate recovery**, then **Simulate downstream refresh**. Source recovery validates the source; downstream refresh records synthetic checks for curated metrics and both reports, turning all graph nodes green. Compare the numbered Incident / Source recovered / Downstream refreshed snapshots. Replay clears open cases’ progression; a new source recovery invalidates previous downstream checks. No real jobs or reports execute. Simulation endpoints are disabled in live mode.

### Databricks Workflow Builder

Open `/builder` to generate and export a synthetic batch job, Lakeflow pipeline, or pipeline-plus-verification workflow. This offline template compiler infers hourly scheduling and uses structured fields for resource names and catalog/schema. It previews infrastructure prerequisites and generated files; schedules are paused and no deployment occurs. See [workflow builder](docs/hackathon/workflow-builder.md) for the demo and reusable implementation prompt.

### One-click manager demo

Open `/demo` and select **Start guided demo** for the eight-step execution → failure detection → investigation → approval → recovery walkthrough. Pause or revisit earlier steps to inspect their preserved lineage and audit evidence. Refreshing the run URL restores the latest saved step in paused mode. This local-only tour uses simulated approval actors and never connects to live services.

### Fraud Guard MVP

Open `/fraud` for merchant-side fraud screening with synthetic transactions, local gateway/issuer simulators, versioned rules, expiring authentication and analyst release gates, Signal investigations and a one-click guided demo. Fraud clearance remains separate from issuer authorization. The local module never moves real money or connects to external payment services. See [Fraud Guard walkthrough and boundaries](docs/hackathon/fraud-guard.md).
