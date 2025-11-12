# scripts/ge_validate.py
import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext

def run_ge_suite(project_id, dataset, table):
    context = BaseDataContext(project_root_dir="/home/airflow/gcs/data/great_expectations") 
    # In Composer, place GE project in a path accessible by the worker
    datasource_name = "bigquery_ge"

    batch_request = RuntimeBatchRequest(
        datasource_name=datasource_name,
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name=f"{dataset}.{table}",
        runtime_parameters={
            "query": f"SELECT * FROM `{project_id}.{dataset}.{table}` LIMIT 1000000"
        },
        batch_identifiers={"default_identifier_name": "daily_batch"},
    )

    # Expectation suite name (pre-created via CLI or code)
    suite_name = "daily_price_checks"
    validator = context.get_validator(batch_request=batch_request, expectation_suite_name=suite_name)

    results = validator.validate()
    if not results.success:
        raise Exception(f"Great Expectations validation failed: {results.statistics}")
    print("GE checks passed.")
