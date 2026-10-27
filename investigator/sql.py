"""Bounded SQL Statement Execution with parameter binding and chunk handling."""
import re
import time


def identifier(name, parts=3):
    tokens = name.split('.')
    if len(tokens) != parts or any(not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', t) for t in tokens):
        raise ValueError('Expected a qualified identifier with simple catalog/schema/table names')
    return '.'.join('`'+t+'`' for t in tokens)


class SQL:
    def __init__(self, client, warehouse_id):
        if not warehouse_id:
            raise ValueError('DATABRICKS_WAREHOUSE_ID is required')
        self.client, self.warehouse_id = client, warehouse_id

    def query(self, statement, parameters=None):
        from databricks.sdk.service.sql import StatementParameterListItem
        response = self.client.statement_execution.execute_statement(
            warehouse_id=self.warehouse_id, statement=statement, wait_timeout='10s',
            parameters=[StatementParameterListItem(name=k, value=str(v)) for k, v in (parameters or {}).items()])
        deadline = time.monotonic()+120
        while getattr(response.status.state, 'value', response.status.state) in {'PENDING', 'RUNNING'}:
            if time.monotonic() > deadline:
                self.client.statement_execution.cancel_execution(response.statement_id)
                raise TimeoutError('SQL collection exceeded 120 seconds')
            time.sleep(1)
            response = self.client.statement_execution.get_statement(response.statement_id)
        if getattr(response.status.state, 'value', response.status.state) != 'SUCCEEDED':
            raise RuntimeError('SQL statement failed; check grants and warehouse query history')
        if response.manifest and response.manifest.truncated:
            raise RuntimeError('SQL results truncated; collection is incomplete')
        columns = [c.name for c in response.manifest.schema.columns] if response.manifest and response.manifest.schema else []
        rows, chunk = [], response.result
        while chunk:
            rows.extend(dict(zip(columns, r)) for r in (chunk.data_array or []))
            if chunk.next_chunk_index is None:
                break
            chunk = self.client.statement_execution.get_statement_result_chunk_n(response.statement_id, chunk.next_chunk_index)
        return rows
