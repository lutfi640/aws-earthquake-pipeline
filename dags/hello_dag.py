from airflow import DAG
airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator as LambdaInvokeOperator
from datetime import datetime
import json

default_args = {
    'owner': 'imam',
    'start_date': datetime(2026, 1, 1),
    'retries': 0,
}

with DAG(
    dag_id='test_hello_world_lambda',
    default_args=default_args,
    schedule=None, # None artinya cuma jalan kalau kita klik "Trigger" manual di UI
    catchup=False,
    tags=['hello', 'lambda', 'test']
) as dag:

    # Payload buat dikirim ke Lambda event
    lambda_payload = {
        "bucket": "learn-aws-imam",
        "key": "BRONZE/dummy_hello.json"
    }

    # Task: Panggil Lambda buat masak jadi Silver
    test_hello_lambda = LambdaInvokeOperator(
        task_id='trigger_hello_lambda',
        function_name='hello_world_docker_lambda',
        payload=json.dumps(lambda_payload),
        aws_conn_id='aws_default' # Ganti kalau nama koneksi AWS di UI Airflow lo beda
    )