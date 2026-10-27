-- Run as an administrator. Replace catalog/schema and APP_SERVICE_PRINCIPAL_ID.
-- Match app.yaml DII_STATE_TABLE. Do not use this schema for monitored business data.
CREATE SCHEMA IF NOT EXISTS main.incident_investigator;
CREATE TABLE IF NOT EXISTS main.incident_investigator.app_state (
  kind STRING, id STRING, body STRING
) USING DELTA;
GRANT USE CATALOG ON CATALOG main TO `APP_SERVICE_PRINCIPAL_ID`;
GRANT USE SCHEMA ON SCHEMA main.incident_investigator TO `APP_SERVICE_PRINCIPAL_ID`;
GRANT SELECT, MODIFY ON TABLE main.incident_investigator.app_state TO `APP_SERVICE_PRINCIPAL_ID`;
-- Monitored tables from config/pilot.json:
GRANT USE SCHEMA ON SCHEMA main.payments TO `APP_SERVICE_PRINCIPAL_ID`;
GRANT SELECT ON TABLE main.payments.settlements TO `APP_SERVICE_PRINCIPAL_ID`;
GRANT SELECT ON TABLE main.payments.cash_position TO `APP_SERVICE_PRINCIPAL_ID`;
-- Administrator must enable system.access schema first if unavailable.
-- This grants raw lineage visibility to the app identity. UI emits allowlisted edges only.
GRANT USE CATALOG ON CATALOG system TO `APP_SERVICE_PRINCIPAL_ID`;
GRANT USE SCHEMA ON SCHEMA system.access TO `APP_SERVICE_PRINCIPAL_ID`;
GRANT SELECT ON TABLE system.access.table_lineage TO `APP_SERVICE_PRINCIPAL_ID`;
-- Also grant CAN VIEW on configured jobs and CAN USE on the warehouse via Apps resources/UI.
