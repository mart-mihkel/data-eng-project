import os
import yaml
import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from metadata.config.common import load_config_file
from metadata.workflow.metadata import MetadataWorkflow

BOT_JWT = "<bot_token_here>"

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
  config: {{}}
workflowConfig:
  loggerLevel: INFO  # DEBUG, INFO, WARNING or ERROR
  openMetadataServerConfig:
    hostPort: "http://openmetadata-server:8585/api"
    authProvider: openmetadata
    securityConfig:
      jwtToken: {bot_jwt}
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
    tokened = config.format(bot_jwt=BOT_JWT)
    workflow_config = yaml.safe_load(tokened)
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
