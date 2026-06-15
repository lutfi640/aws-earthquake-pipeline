import pandas as pd
import boto3
import json
from datetime import datetime
import io
import sys
import os

# Setup path agar bisa baca dari utils
# current_dir = os.path.dirname(os.path.abspath(__file__))
# project_root = os.path.dirname(current_dir)
# if project_root not in sys.path:
#     sys.path.append(project_root)

from utils.aws_helper import read_from_s3, upload_to_s3

def lambda_handler(event, context):
    print("Menerima event dari Airflow:", event)
    
    # 1. Ambil bucket dan key dari event (Default ke hari ini kalau gak ada)
    bucket_name = event.get('bucket', 'learn-aws-imam')
    default_key = f"BRONZE/earthquake_data_{datetime.now().strftime('%Y-%m-%d')}.json"
    file_key = event.get('key', default_key)
    
    print(f"Mulai memproses file: {file_key} dari bucket: {bucket_name}")

    try:
        # 2. Baca JSON dari S3
        content = read_from_s3(bucket_name, file_key)
        data = json.loads(content)

        # 3. Flatten JSON menggunakan Pandas
        df = pd.json_normalize(data['features'])
        df['properties.time'] = pd.to_datetime(df['properties.time'], unit='ms')
        df['properties.updated'] = pd.to_datetime(df['properties.updated'], unit='ms')
        df = df.loc[:, ['properties.mag', 'properties.place', 'properties.time',
               'properties.updated', 'properties.alert', 'properties.tsunami', 'properties.sig', 
                'properties.type', 'geometry.coordinates']]

        # 4. Create memory buffer & Convert ke Parquet
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow')

        # 5. Upload ke S3 (Silver Layer)
        silver_path = file_key.replace('BRONZE', 'SILVER').replace('.json', '.parquet')
        upload_to_s3(bucket_name, silver_path, parquet_buffer.getvalue())
        
        pesan_sukses = f"Sukses! Data diproses ke Silver layer: {silver_path}"
        print(pesan_sukses)

        return {
            'statusCode': 200,
            'body': json.dumps(pesan_sukses)
        }
        
    except Exception as e:
        print(f"Error terjadi: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error saat memproses data: {str(e)}")
        }