import yaml
import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from metadata.config.common import load_config_file
from metadata.workflow.metadata import MetadataWorkflow


config = """

"""

def metadata_ingestion_workflow():
    workflow_config = yaml.safe_load(config)
    workflow = MetadataWorkflow.create(workflow_config)
    workflow.execute()
    workflow.raise_from_status()
    workflow.print_status()
    workflow.stop()


with DAG(
    "ingest_metadata",
    start_date=datetime.datetime(2024, 12, 1),
    is_paused_upon_creation=False,
    schedule='@monthly',
    catchup=False,
) as dag:
    ingest_task = PythonOperator(
        task_id="ingest_metadata_task",
        python_callable=metadata_ingestion_workflow,
    )
