from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator as LambdaInvokeOperator
from datetime import datetime, timedelta
import json

default_args = {
    'owner': 'imam',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

today_str = datetime.now().strftime('%Y-%m-%d')
s3_key_bronze = f"BRONZE/earthquake_data_{today_str}.json"

with DAG(
    'earthquake_pipeline_v2',
    default_args=default_args,
    description='Pipeline Gempa Dinamis Tanpa Rebuild Docker',
    schedule=None, # Gak jalan otomatis, cuma manual trigger
    catchup=False,
    tags=['aws', 'earthquake']
) as dag:

    # TASK 1: EXTRACT (Tetap seperti biasa)
    extract_task = BashOperator(
        task_id='extract_api_to_bronze',
        bash_command='python /opt/airflow/scripts/extractors/extract_earthquake.py',
    )

    # -------------------------------------------------------------
    # TRIK RAHASIA: Membaca file script transform lokal EC2 lo sebagai string
    # -------------------------------------------------------------
    try:
        with open('/opt/airflow/lambda/transformers/dim_earthquake.py', 'r') as file:
            script_code_string = file.read()
    except Exception as e:
        # Cadangan kalau filenya belum ke-copy ke EC2 agar DAG gak rusak/broken
        script_code_string = f"print('Gagal membaca file script lokal: {str(e)}')"
        raise e

    # -------------------------------------------------------------
    # TASK 2: GENERATE DIM 
    # -------------------------------------------------------------
    generate_dim = LambdaInvokeOperator(
        task_id='generate_dim_earthquake',
        function_name='earthquake-transformer-docker', # Nama fungsi Lambda Docker lo
        payload=json.dumps({
            "code": script_code_string, # KODINGAN LO DISUNTIK DI SINI SEBAGAI TEKS!
            "bucket": "learn-aws-imam", # Parameter tambahan buat dibaca script lo
            "key": s3_key_bronze
        }),
        aws_conn_id='aws_default',
        log_type='Tail'
    )


    try:
        with open('/opt/airflow/lambda/transformers/transform_earthquake.py', 'r') as file:
            script_code_string = file.read()
    except Exception as e:
        # Cadangan kalau filenya belum ke-copy ke EC2 agar DAG gak rusak/broken
        script_code_string = f"print('Gagal membaca file script lokal: {str(e)}')"
        raise e
    # -------------------------------------------------------------
    # TASK 3: TRANSFORM (Kirim kodenya lewat payload parameter 'code')
    # -------------------------------------------------------------
    transform_task = LambdaInvokeOperator(
        task_id='transform_bronze_to_silver',
        function_name='earthquake-transformer-docker', # Nama fungsi Lambda Docker lo
        payload=json.dumps({
            "code": script_code_string, # KODINGAN LO DISUNTIK DI SINI SEBAGAI TEKS!
            "bucket": "learn-aws-imam", # Parameter tambahan buat dibaca script lo
            "key": s3_key_bronze
        }),
        aws_conn_id='aws_default',
        log_type='Tail'
    )

    

    extract_task >> generate_dim >> transform_task