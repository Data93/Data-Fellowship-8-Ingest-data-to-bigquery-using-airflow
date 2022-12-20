import os
import logging
import requests
import json

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from google.oauth2 import service_account
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.json as jsw
import pyarrow.parquet as pq
import pandas as pd

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "tugas-data-fellowship-8")
BUCKET_NAME = os.environ.get("GCP_GCS_BUCKET", "data-fellowship-8-bucket")
CREDS_FILE = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ".google/tugas-data-fellowship-8-0c30f4ea4494.json")

URL_API = 'https://datausa.io/api/data?drilldowns=Nation&measures=Population'
API_RESULT = 'us_sensus.json'
LOCAL_HOME_PATH = os.environ.get('AIRFLOW_HOME', '/opt/airflow/')
CSV_SAVED = API_RESULT.replace('.json', '.csv')
PARQUET_FILE = CSV_SAVED.replace('.csv', '.parquet')
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', 'datausa')

def api_call_data(url_json: str, local_json: str):
    """
    Call for the api to get json file

    * url_json: retrieve data as json from url
    * local_json: local path to store the file
    """
    r = requests.get(url = url_json)

    data = r.json()
    with open(local_json, 'w') as outfile:
        json.dump(data, outfile)

def csv_saver(json_file: str):
    """Pass json file path url and convert to csv filetype"""
    if not json_file.endswith('.json'):
        logging.error('Can only accept source files in JSON format, for the moment')
        return

    table = jsw.read_json(json_file)
    table_arr = table['data'].to_numpy()

    list_tbl = list(i for i in table_arr[0])
    # pd_tbl = pa.RecordBatch.from_pylist(list_tbl)
    df = pd.DataFrame.from_dict(list_tbl)
    df_tbl = pa.Table.from_pandas(df)

    pv.write_csv(df_tbl, f"{LOCAL_HOME_PATH}/{CSV_SAVED}")

def format_to_parquet(src_file: str):
    """Convert CSV file to PARQUET file format"""
    if not src_file.endswith('.csv'):
        logging.error('Can only accept source files in CSV format, for the moment')
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))

def upload_to_gcs(bucket: str, object_name: str, local_file: str):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    * bucket: GCS bucket name (existed)
    * object_name: target path & file-name
    * local_file: source path & file-name\n
    -> return log
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # # End of Workaround

    # CREDS_PATH = f"{LOCAL_HOME_PATH}/{CREDS_FILE}"
    # AUTH_CREDS = service_account.Credentials.from_service_account_file(CREDS_FILE)

    # scoped_credentials = AUTH_CREDS.with_scopes(
    #     ['https://www.googleapis.com/auth/cloud-platform'])
    # credentials, project_id = google.auth.default()

    # Setting Credentials using SERVICE ACCOUNT CREDENTIALS if use ADC just remove the 'credentials param'
    # storage_client = storage.Client(credentials=AUTH_CREDS)
    storage_client = storage.Client()

    buckt = storage_client.bucket(bucket)

    blob = buckt.blob(object_name)
    blob.upload_from_filename(local_file)

# Airflow -------------------
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="api-ingest-dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['df8'],
) as dag:

    # call_dataset_task = BashOperator(
    #     task_id="call_dataset_task",
    #     bash_command=f"wget {URL_API} -O {LOCAL_HOME_PATH}/{API_RESULT}"
    # )

    call_dataset_task = PythonOperator(
        task_id="call_dataset_task",
        python_callable=api_call_data,
        op_kwargs={ 
            "url_json": URL_API,
            "local_json": f"{LOCAL_HOME_PATH}/{API_RESULT}"
        }
    )

    save_as_csv = PythonOperator(
        task_id="save_as_csv",
        python_callable=csv_saver,
        op_kwargs={ 
            "json_file": f"{LOCAL_HOME_PATH}/{API_RESULT}",
        }
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{LOCAL_HOME_PATH}/{CSV_SAVED}",
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET_NAME,
            "object_name": f"raw/{PARQUET_FILE}",
            "local_file": f"{LOCAL_HOME_PATH}/{PARQUET_FILE}",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET_NAME}/raw/{PARQUET_FILE}"],
            },
        },
    )

    call_dataset_task >> save_as_csv >> format_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task