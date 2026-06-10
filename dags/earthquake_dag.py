from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator as LambdaInvokeOperator
from datetime import datetime
import json

with DAG(
    dag_id='earthquake_hello_world_pipeline',
    start_date=datetime(2026, 1, 1),
    schedule=None, # None artinya cuma jalan kalau kita klik "Trigger" manual di UI
    catchup=False
) as dag:

    # Task 1: Jalankan extractor di EC2 buat bikin file Bronze
    extract_bronze = BashOperator(
        task_id='extract_dummy_to_bronze',
        bash_command='python3 /home/ec2-user/airflow/scripts/extract_earthquake.py' 
        # ^ Pastikan path di atas sesuai dengan folder di EC2 lo ya!
    )

    # Payload buat dikirim ke Lambda event
    lambda_payload = {
        "bucket": "learn-aws-imam",
        "key": "BRONZE/dummy_hello.json"
    }

    # Task 2: Panggil Lambda buat masak jadi Silver
    transform_silver = LambdaInvokeOperator(
        task_id='trigger_lambda_silver',
        function_name='earthquake-transformer',
        payload=json.dumps(lambda_payload),
        aws_conn_id='aws_default' # Ganti kalau nama koneksi AWS di UI Airflow lo beda
    )

    extract_bronze >> transform_silver