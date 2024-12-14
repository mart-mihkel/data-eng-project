import yaml
import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from metadata.config.common import load_config_file
from metadata.workflow.metadata import MetadataWorkflow


config = """
source:
  type: mongodb
  serviceName: mongo
  serviceConnection:
    config:
      type: MongoDB
      username: admin
      password: admin
      hostPort: mongo:27017
      # connectionOptions:
      #   key: value
      databaseName: dataeng_project
  sourceConfig:
    config:
      type: DatabaseMetadata
      markDeletedTables: true
      markDeletedStoredProcedures: true
      includeTables: true
      includeViews: true
      # includeTags: true
      # includeOwners: false
      # includeStoredProcedures: true
      # includeDDL: true
      # queryLogDuration: 1
      # queryParsingTimeoutLimit: 300
      # useFqnForFiltering: false
      # threads: 4
      # incremental:
      #   enabled: true
      #   lookbackDays: 7
      #   safetyMarginDays: 1
      # databaseFilterPattern:
      #   includes:
      #     - database1
      #     - database2
      #   excludes:
      #     - database3
      #     - database4
      # schemaFilterPattern:
      #   includes:
      #     - schema1
      #     - schema2
      #   excludes:
      #     - schema3
      #     - schema4
      # tableFilterPattern:
      #   includes:
      #     - users
      #     - type_test
      #   excludes:
      #     - table3
      #     - table4
sink:
  type: metadata-rest
  config: {}
workflowConfig:
  loggerLevel: INFO  # DEBUG, INFO, WARNING or ERROR
  openMetadataServerConfig:
    hostPort: "http://openmetadata-server:8585/api"
    authProvider: openmetadata
    securityConfig:
      jwtToken: "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImluZ2VzdGlvbi1ib3QiLCJyb2xlcyI6WyJJbmdlc3Rpb25Cb3RSb2xlIl0sImVtYWlsIjoiaW5nZXN0aW9uLWJvdEBvcGVuLW1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3MzQxODk3ODgsImV4cCI6bnVsbH0.mAfU8V5szSotMvLPfqE8l92h0ptNBzwXsfwyAVewxUKY4z6a8d4CYu3qUe951HTTDdWZd0GS2BdXxIT9B2DdxL33x6Nc4fpa8ZczLycESqtHpdzS_0-pjnnQ4_d1UKnJqtoMJqtju_QsT4lVEooUdQ922GOTgA5tpot0Q2Dop6XEZLXX8-0xzSaNqF3iCgPFhnTkGlhIoXnDuSSsBN8_Schh06a4_xgt-q0J0XqkHlXcwHJz5FgHcio0hqE6Hnmaqxcs9kU9wmqg_6SMTqC4iGK0OrqGXWkJao_GdYYSBtK7t5Ezax-mX4YDk6IEDakxts5HSek2hGPr4D-K8vS8Mw"
    ## Store the service Connection information
    storeServiceConnection: true  # false
    ## Secrets Manager Configuration
    # secretsManagerProvider: aws, azure or noop
    # secretsManagerLoader: airflow or env
    ## If SSL, fill the following
    # verifySSL: validate  # or ignore
    # sslConfig:
    #   caCertificate: /local/path/to/certificate
"""

def metadata_ingestion_workflow():
    workflow_config = yaml.safe_load(config)
    workflow = MetadataWorkflow.create(workflow_config)
    workflow.execute()
    workflow.raise_from_status()
    workflow.print_status()
    workflow.stop()


with DAG(
    "ingest_mongodb_metadata",
    start_date=datetime.datetime(2024, 12, 1),
    is_paused_upon_creation=False,
    schedule='@monthly',
    catchup=False,
) as dag:
    ingest_task = PythonOperator(
        task_id="ingest_metadata",
        python_callable=metadata_ingestion_workflow,
    )
