"""Offline, constrained prompt compiler. Generates files; never deploys or executes prompt text."""
import io
import json
import re
from zipfile import ZipFile, ZIP_DEFLATED


def build(data):
    prompt = data.get('prompt', '')
    if not isinstance(prompt, str) or not 10 <= len(prompt) <= 4000:
        raise ValueError('Describe the workflow in 10–4000 characters')
    kind = data.get('kind', 'job')
    if kind not in {'job', 'pipeline', 'workflow'}:
        raise ValueError('Choose job, pipeline or workflow')
    values = {key: data.get(key, default) for key, default in [('name','synthetic_delivery'),('catalog','main'),('schema','delivery_demo')]}
    for key, value in values.items():
        if not isinstance(value, str) or not re.fullmatch(r'[a-z][a-z0-9_]{0,62}', value):
            raise ValueError(key+' must use lowercase letters, digits and underscores, starting with a letter')
    # Prompt text is intent data, never interpolated into executable code or resource names.
    blocked = bool(re.search(r'\b(drop|delete|destroy|production|prod|unpause|secret|password)\b', prompt, re.I))
    hourly = bool(re.search(r'\bhourly\b|every hour', prompt, re.I))
    pipeline = kind in {'pipeline', 'workflow'}
    name = values['name']
    resources = {}
    if pipeline:
        resources['pipelines'] = {'synthetic_pipeline': {
            'name': name+'_pipeline', 'catalog': '${var.catalog}', 'schema': '${var.schema}',
            'serverless': True, 'continuous': False, 'development': True,
            'root_path': '.', 'libraries': [{'glob': {'include': 'transformations/**'}}]}}
    tasks = []
    if kind == 'job':
        tasks = [{'task_key': 'synthetic_batch', 'spark_python_task': {'python_file': 'src/batch.py'}, 'environment_key': 'default'}]
    elif kind == 'workflow':
        tasks = [{'task_key': 'refresh_pipeline', 'pipeline_task': {'pipeline_id': '${resources.pipelines.synthetic_pipeline.id}'}},
                 {'task_key': 'verify_counts', 'depends_on': [{'task_key':'refresh_pipeline'}],
                  'spark_python_task': {'python_file': 'src/verify.py', 'parameters': ['${var.catalog}', '${var.schema}']}, 'environment_key':'default'}]
    if tasks:
        job = {'name': name, 'max_concurrent_runs': 1, 'tasks': tasks,
               'environments': [{'environment_key': 'default', 'spec': {'environment_version': '2'}}]}
        if hourly:
            job['schedule'] = {'quartz_cron_expression': '0 0 * * * ?', 'timezone_id': 'UTC', 'pause_status':'PAUSED'}
        resources['jobs'] = {'synthetic_workflow': job}
    bundle = {'bundle': {'name': name}, 'variables': {'catalog': {'default': values['catalog']}, 'schema': {'default': values['schema']}},
              'resources': resources, 'targets': {'dev': {'default': True, 'mode':'development'}}}
    batch = '''from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
# Real Spark transformations over synthetic input. No external source connection.
rows = [("SYN-001", "TREASURY", 120.0), ("SYN-002", "OPERATIONS", 80.0)]
frame = spark.createDataFrame(rows, "record_id string, route string, amount double")
result = frame.filter("amount > 0").groupBy("route").sum("amount")
assert frame.count() == 2
result.show()
'''
    transform = '''from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(name="synthetic_bronze")
def synthetic_bronze():
    return spark.range(10).select(F.concat(F.lit("SYN-"), F.col("id")).alias("record_id"), F.lit(100).alias("amount"))

@dp.table(name="synthetic_silver")
@dp.expect_or_fail("positive_amount", "amount > 0")
def synthetic_silver():
    return spark.read.table("synthetic_bronze").filter("amount > 0")
'''
    verify = '''import sys
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
catalog, schema = sys.argv[1:3]
# Identifiers are validated by the generator before export.
count = spark.table(f"{catalog}.{schema}.synthetic_silver").count()
assert count == 10, f"Expected 10 synthetic rows, got {count}"
print("Synthetic pipeline verified", count)
'''
    files = {'databricks.yml': json.dumps(bundle, indent=2)+'\n'}  # JSON is valid YAML 1.2
    if kind == 'job': files['src/batch.py'] = batch
    if pipeline: files['transformations/synthetic.py'] = transform
    if kind == 'workflow': files['src/verify.py'] = verify
    local_workflow = {'version': 1, 'name': name, 'template': kind,
        'runtime': 'Delivery Lab Python executor (not Spark)',
        'tasks': [{'key':'ingest', 'operation':'ingest', 'depends_on':[]},
                  {'key':'distribute', 'operation':'dispatch', 'depends_on':['ingest']},
                  {'key':'verify_consumers', 'operation':'refresh_consumers', 'depends_on':['distribute']}]}
    files['local-workflow.json'] = json.dumps(local_workflow, indent=2)+'\n'
    checks = ['Resource names and catalog identifiers validated', 'Only generated synthetic sources included', 'Development target selected', 'Schedules paused; no deployment performed', 'Task dependencies reference generated tasks']
    for path, source in files.items():
        if path.endswith('.py'): compile(source,path,'exec')
    files['README.md'] = f'''# {name}
Generated by an offline constrained template compiler, not an LLM. Prompt intent is retained in plan.json; structured fields control resource type and identifiers. Supported prompt inference: hourly schedule only. All other behavior follows the selected template.

## Review and deploy to an approved sandbox
1. Review databricks.yml and source files; select an isolated catalog/schema.
2. Configure Databricks CLI authentication for your sandbox. Never put credentials in this folder.
3. Run `databricks bundle validate -t dev` (requires workspace connectivity).
4. Review resolved resources, grants and serverless availability before `databricks bundle deploy -t dev`.
5. Start the generated job or pipeline manually in the workspace. Any generated schedule stays paused.

Requires existing workspace, Unity Catalog catalog/schema and serverless entitlement. Pipeline execution needs USE CATALOG, USE SCHEMA and CREATE TABLE on the sandbox schema. Job execution needs workspace job creation/run permissions and serverless access. Verify permissions with your administrator. No cloud infrastructure or IAM is provisioned. Job-only template shows results without writing tables. Pipeline creates synthetic_bronze and synthetic_silver in the chosen schema. Reusing a name updates bundle-managed resources; use a fresh name/schema for isolation.

Local checks validate identifiers, generated Python syntax and template structure only. Databricks CLI/workspace validation and Spark execution have not occurred locally. The exported local-workflow.json runs an explicit Delivery Lab execution profile: real file validation, local channel distribution and consumer verification. It is linked to this generated plan but does not execute the exported Spark source or emulate Databricks runtime semantics.
'''
    result = {'compiler':'deterministic templates', 'kind':kind, 'intent':prompt, 'configuration':values,
              'local_workflow':local_workflow,
              'status':'REVIEW_REQUIRED' if not blocked else 'BLOCKED',
              'blockers':['Sensitive or destructive intent detected. Use a synthetic sandbox description.'] if blocked else [],
              'checks':checks, 'infrastructure':{'compute':'serverless (workspace prerequisite)', 'storage':'existing Unity Catalog schema' if pipeline else 'no persistent table writes', 'authentication':'Databricks CLI profile; no embedded credentials'},
              'limitations':['No arbitrary prompt interpretation or LLM call', 'No Terraform apply, cloud provisioning or workspace deployment', 'Workspace grants and runtime compatibility require connected validation'],
              'files':files if not blocked else {}}
    return result


def archive(plan):
    if plan['status'] == 'BLOCKED': raise ValueError('Blocked plans cannot be exported')
    output=io.BytesIO()
    with ZipFile(output,'w',ZIP_DEFLATED) as zipped:
        for path, content in plan['files'].items(): zipped.writestr(path,content)
        zipped.writestr('plan.json',json.dumps({k:v for k,v in plan.items() if k!='files'},indent=2))
    output.seek(0)
    return output
