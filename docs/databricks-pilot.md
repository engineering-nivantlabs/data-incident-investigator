# Databricks Apps internal pilot

## Deployment model

One app, one running instance, one Gunicorn worker (four HTTP threads), one shared evidence audience. A process lock serializes mutations. The app uses its own service principal through SDK unified authentication. Databricks authenticates visitors and controls app access; this version does not implement per-user Unity Catalog impersonation. Grant app access only to the pilot group entitled to every configured dataset's evidence. Never publicly expose the server outside the Apps proxy.

State is persisted in a dedicated Unity Catalog Delta table, not the app's ephemeral local disk. SQLite remains the local development backend. Delta persistence is intentionally a low-volume pilot approach: individual event writes and full event reads are inefficient at scale. Do not run multiple replicas/workers or independent writers. For broader production use introduce transactional state storage, stronger concurrency control, incremental collection and retention.

## Configure

1. Edit `config/pilot.json` with real three-part table names and optional job IDs. Set positive `max_age_minutes` and `check_ttl_minutes` (15 by default). Use simple SQL identifiers. Every table must be explicitly configured; observed edges only appear when both endpoints are configured. Add relevant intermediate tables for multi-hop coverage.
2. Choose freshness semantics per table:
   - Without `freshness_column`: `DESCRIBE HISTORY ... LIMIT 1` reads latest Delta commit. Maintenance commits can advance this timestamp; this does not prove new business records arrived. Non-Delta/unsupported tables produce UNKNOWN.
   - With `freshness_column`: the app queries MAX(unix_timestamp(column)). Use a TIMESTAMP ingestion/event column (not free-form strings). Empty/all-null data and query failures produce UNKNOWN. Future timestamps more than five minutes ahead produce UNKNOWN. MAX queries may scan data and incur warehouse cost.
3. Edit `app.yaml` DII_STATE_TABLE to match the dedicated state table you will provision. Keep DII_MODE=databricks. No PATs or credentials belong in source files.
4. Create an app in Databricks Apps without starting a deployment yet. Record its service principal application ID. Attach a SQL warehouse resource with key `sql-warehouse` and CAN USE.
5. Adapt and execute `config/bootstrap.sql` as an administrator to provision the state table and grant the app access. Grant CAN VIEW on each configured job. The app itself never creates schemas, grants permissions, starts jobs or modifies monitored tables.
6. Enable/access the `system.access.table_lineage` system table. The example grants direct read access; for stricter environments expose a curated view and adapt the collector source before deploying. Stored incident evidence inherits the shared pilot audience, not original per-user policies.

## Deploy from workspace (recommended first pilot)

Run `python scripts/package_pilot.py` to create a clean `pilot-source/` upload directory (it refuses to overwrite an existing directory). It contains only `app.yaml`, `requirements.txt`, `investigator/`, `static/`, and `config/pilot.json`. Upload it to an app source workspace folder. This avoids uploading local databases, virtual environments, screenshots or test evidence. Give the app service principal read access to its source folder.

In Databricks Apps, select Deploy, choose that workspace folder, and deploy. Dependencies install from requirements.txt. The launcher uses Gunicorn on 0.0.0.0 and DATABRICKS_APP_PORT. App service credentials are supplied by Databricks.

Alternatively, with an authenticated current Databricks CLI:

```sh
databricks sync ./pilot-source /Workspace/Users/YOUR_USER/incident-investigator
databricks apps deploy incident-investigator --source-code-path /Workspace/Users/YOUR_USER/incident-investigator
```

The app name and workspace path are examples; use your own existing app and approved workspace location.

## Optional bundle path

`databricks.yml` declares the app and warehouse resource binding. Supply a warehouse ID and unique app name:

```sh
databricks bundle validate -t pilot --var="warehouse_id=YOUR_WAREHOUSE_ID,app_name=incident-investigator"
databricks bundle deploy -t pilot --var="warehouse_id=YOUR_WAREHOUSE_ID,app_name=incident-investigator"
databricks bundle run -t pilot investigator --var="warehouse_id=YOUR_WAREHOUSE_ID,app_name=incident-investigator"
```

For an existing app, use the CLI's documented bundle resource binding workflow rather than create a duplicate. Bootstrap the service principal's table/job grants before starting the app. Validate with your installed CLI version; no CLI/workspace deployment has been performed from this repo. Review the synchronized file list before upload; local state files and virtual environments are ignored.

## Acceptance checks in the workspace

- GET /healthz succeeds; UI identifies Databricks mode.
- Sync reads real jobs, emits freshness observations and observed dependencies.
- Test a successful job with a stale table: recovery remains blocked.
- Verify table-only assets can be investigated.
- Revoke test SELECT access: new observation becomes UNKNOWN, without exposing raw errors or reusing previous healthy evidence.
- Check downstream edges against Catalog Explorer; account for its capture gaps and the 30-day observation window. Coverage error events are displayed in the timeline.
- Change source data in an approved sandbox and sync: fresh timestamps can pass recovery, subject to job and quality findings.
- Wait beyond observation TTL: resolution is blocked until telemetry is refreshed.
- Restart/redeploy: incidents and evidence remain in the Delta state table.
- Restrict CAN USE on the app to the pilot group; verify an unauthorized user cannot open it.

Telemetry collection is manually triggered in this version. Observed dependencies are a replaceable snapshot of the last sync (30-day window), not perpetual lineage. Partial permission failures produce coverage events and remove unavailable observed edges; declared edges remain. No claim is made that an absent edge proves absence of dependency. Every incident preserves the dependency snapshot used for that investigation.

Freshness verifies only its configured timestamp rule. Business completeness, correctness and external report content remain unverified. Quality adapters and automatic polling are separate future work.

## References

- [Apps runtime and app.yaml](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/app-runtime)
- [SQL warehouse resources](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/sql-warehouse)
- [Deploy an app](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/deploy)
- [App and user authorization](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/auth)
- [Lineage capture limitations](https://docs.databricks.com/aws/en/admin/system-tables/lineage)
- [Delta history](https://docs.databricks.com/gcp/en/tables/history)
