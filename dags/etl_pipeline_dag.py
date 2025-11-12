# dags/etl_pipeline_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.email import send_email

PROJECT_ID = "your-gcp-project"
REGION = "us-central1"
GCS_RAW_BUCKET = "gs://your_raw_bucket"
DATAFLOW_STAGING = "gs://your_staging_bucket/dataflow/staging"
DATAFLOW_TEMP = "gs://your_staging_bucket/dataflow/temp"
BQ_DATASET = "your_dataset"
BQ_TABLE = "your_table"
BEAM_PY_FILE = "gs://your_code_bucket/beam_transform.py"  # upload this before DAG runs
RAW_GCS_PATH_TEMPLATE = f"{GCS_RAW_BUCKET}/raw/{{{{ ds }}}}/data.json"

default_args = {
    "owner": "durgesh",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email": ["you@example.com"],
    "email_on_failure": False,
    "email_on_retry": False
}

def on_failure_callback(context):
    subject = f"Airflow DAG Failed: {context['dag'].dag_id}"
    body = f"DAG: {context['dag'].dag_id}\nTask: {context['task_instance'].task_id}\nException: {context.get('exception')}"
    send_email(to=["you@example.com"], subject=subject, html_content=body)

def fetch_and_upload(ds, **kwargs):
    from scripts.fetch_api_and_upload import fetch_and_upload_to_gcs
    # ds is execution date string YYYY-MM-DD
    dest_path = RAW_GCS_PATH_TEMPLATE.format(ds=ds)
    fetch_and_upload_to_gcs(dest_path)

def run_ge_validation(**kwargs):
    from scripts.ge_validate import run_ge_suite
    run_ge_suite(project_id=PROJECT_ID, dataset=BQ_DATASET, table=BQ_TABLE)

with DAG(
    dag_id="etl_gcp_dataflow_ge",
    default_args=default_args,
    description="Daily ETL: API -> GCS -> Dataflow -> BigQuery -> GE checks",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    on_failure_callback=on_failure_callback,
) as dag:

    t1_fetch = PythonOperator(
        task_id="fetch_api_upload_gcs",
        python_callable=fetch_and_upload,
        provide_context=True,
    )

    t2_dataflow = DataflowCreatePythonJobOperator(
        task_id="run_dataflow_transform",
        py_file=BEAM_PY_FILE,
        job_name="beam-transform-{{ ds_nodash }}",
        options={
            "project": PROJECT_ID,
            "region": REGION,
            "staging_location": DATAFLOW_STAGING,
            "temp_location": DATAFLOW_TEMP,
            "input": RAW_GCS_PATH_TEMPLATE,
            "output_table": f"{PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE}",
            "runner": "DataflowRunner"
        },
        py_requirements=["apache-beam[gcp]==2.49.0"],  # pin as required
    )

    t3_bq_sql_check = BigQueryInsertJobOperator(
        task_id="bq_sql_check",
        configuration={
            "query": {
                "query": f"SELECT COUNT(*) as cnt FROM `{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`",
                "useLegacySql": False
            }
        }
    )

    t4_ge = PythonOperator(
        task_id="great_expectations_validate",
        python_callable=run_ge_validation
    )

    t5_notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=lambda: print("Pipeline succeeded")
    )

    t1_fetch >> t2_dataflow >> t3_bq_sql_check >> t4_ge >> t5_notify_success
