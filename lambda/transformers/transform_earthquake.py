import json
import pandas as pd
import io
import boto3
from datetime import datetime

# ==========================================
# 1. FUNGSI UTILS (LANGSUNG DITEMPEL DI SINI)
# ==========================================
def read_from_s3(bucket, key):
    # s3_client sudah otomatis pakai permission IAM Role Lambda lo
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return obj['Body'].read().decode('utf-8')

def upload_to_s3(bucket, key, data):
    s3_client = boto3.client('s3')
    s3_client.put_object(Bucket=bucket, Key=key, Body=data)
    print(f"Sukses upload ke s3://{bucket}/{key}")

# ==========================================
# 2. LOGIC TRANSFORMATION UTAMA (DINAMIS)
# ==========================================
def lambda_handler(event, context):
    print("Menerima event dinamis dari Airflow:", event)

    # Ambil bucket dan key dari event (Default ke hari ini kalau gak ada)
    bucket_name = event.get('bucket', 'learn-aws-imam')
    
    # Ambil tanggal hari ini untuk penamaan default dan partisi
    today_date = datetime.now().strftime('%Y-%m-%d')
    default_key = f"bronze/earthquake_data_{today_date}.json" # [REVISI] Biasakan folder huruf kecil
    file_key = event.get('key', default_key)

    print(f"Mulai memproses file: {file_key} dari bucket: {bucket_name}")

    try:
        # Baca JSON dari S3
        content = read_from_s3(bucket_name, file_key)
        data = json.loads(content)

        # Flatten JSON menggunakan Pandas
        df = pd.json_normalize(data['features'])
        df['properties.time'] = pd.to_datetime(df['properties.time'], unit='ms')
        df['properties.updated'] = pd.to_datetime(df['properties.updated'], unit='ms')
        df = df.loc[:, ['properties.mag', 'properties.place', 'properties.time',
                        'properties.updated', 'properties.alert', 'properties.tsunami', 'properties.sig', 
                        'properties.type', 'geometry.coordinates']]
        
        # pecah kolom geometry.coordinates menjadi tiga kolom baru: longitude, latitude, depth
        df['longitude'] = df['geometry.coordinates'].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None)
        df['latitude'] = df['geometry.coordinates'].apply(lambda x: x[1] if isinstance(x, list) and len(x) > 1 else None)
        df['depth'] = df['geometry.coordinates'].apply(lambda x: x[2] if isinstance(x, list) and len(x) > 2 else None)

        # delete kolom geometry.coordinates
        df.drop(columns=['geometry.coordinates'], inplace=True)

        # rename kolom biar tidak ada titik di nama kolom
        df.rename(columns={
            'properties.mag': 'magnitude',
            'properties.place': 'place',
            'properties.time': 'time',
            'properties.updated': 'updated',
            'properties.alert': 'alert',
            'properties.tsunami': 'tsunami',
            'properties.sig': 'sig',
            'properties.type': 'type'
        }, inplace=True)

        # Create memory buffer & Convert ke Parquet
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow')

        # ==========================================
        # [REVISI] Upload ke S3 (Silver Layer - DW Fact Table)
        # ==========================================
        # 1. Kita bikin variabel partition_date (mengambil dari variable today_date di atas)
        partition_date = today_date 
        
        # 2. Bikin path dinamis menggunakan Hive Partitioning (dt=YYYY-MM-DD)
        # Struktur: silver/earthquake/fact_earthquake/dt=2026-07-17/data.parquet
        silver_path = f"silver/earthquake/fact_earthquake/dt={partition_date}/data.parquet"
        
        # Eksekusi upload
        upload_to_s3(bucket_name, silver_path, parquet_buffer.getvalue())
        
        print(f"Sukses! Data diproses ke Silver layer: {silver_path}")
        
        # Return success (buat konfirmasi ke Airflow)
        return {
            'statusCode': 200,
            'body': f"Success saving to {silver_path}"
        }

    except Exception as e:
        print(f"Error terjadi di dalam Lambda Executor: {str(e)}")
        raise e # Kita raise error-nya biar Airflow tahu kalau task ini gagal