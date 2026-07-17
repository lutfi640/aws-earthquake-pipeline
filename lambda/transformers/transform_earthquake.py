import json
import pandas as pd
import io
import boto3
from datetime import datetime

# ==========================================
# 1. FUNGSI UTILS
# ==========================================
def read_from_s3(bucket, key):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return obj['Body'].read().decode('utf-8')

def upload_to_s3(bucket, key, data):
    s3_client = boto3.client('s3')
    s3_client.put_object(Bucket=bucket, Key=key, Body=data)
    print(f"Sukses upload ke s3://{bucket}/{key}")

# ==========================================
# 2. LOGIC TRANSFORMATION UTAMA (DINAMIS & PARTITIONED)
# ==========================================
def lambda_handler(event, context):
    print("Menerima event dinamis dari Airflow:", event)

    # Ambil bucket dan key dari event (Default ke hari ini kalau gak ada)
    bucket_name = event.get('bucket', 'learn-aws-imam')
    
    # Ambil tanggal hari ini untuk penamaan default fallback file mentah
    today_date = datetime.now().strftime('%Y-%m-%d')
    default_key = f"bronze/earthquake_data_{today_date}.json" 
    file_key = event.get('key', default_key)

    print(f"Mulai memproses file: {file_key} dari bucket: {bucket_name}")

    try:
        # Baca JSON dari S3 Bronze
        content = read_from_s3(bucket_name, file_key)
        data = json.loads(content)

        # Flatten JSON menggunakan Pandas
        df = pd.json_normalize(data['features'])
        df['properties.time'] = pd.to_datetime(df['properties.time'], unit='ms')
        df['properties.updated'] = pd.to_datetime(df['properties.updated'], unit='ms')
        
        # Filter kolom yang dibutuhkan
        df = df.loc[:, ['properties.mag', 'properties.place', 'properties.time',
                        'properties.updated', 'properties.alert', 'properties.tsunami', 'properties.sig', 
                        'properties.type', 'geometry.coordinates']]
        
        # Pecah kolom geometry.coordinates menjadi tiga kolom baru: longitude, latitude, depth
        df['longitude'] = df['geometry.coordinates'].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None)
        df['latitude'] = df['geometry.coordinates'].apply(lambda x: x[1] if isinstance(x, list) and len(x) > 1 else None)
        df['depth'] = df['geometry.coordinates'].apply(lambda x: x[2] if isinstance(x, list) and len(x) > 2 else None)

        # Delete kolom array bawaan
        df.drop(columns=['geometry.coordinates'], inplace=True)

        # Rename kolom biar rapi dan clean
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

        # ==========================================
        # 3. DYNAMIC HIVE PARTITIONING BY EVENT DATE
        # ==========================================
        print("Mulai proses pemecahan partisi data...")
        
        # Bikin kolom sementara untuk menampung format YYYY-MM-DD dari waktu kejadian
        df['event_date'] = df['time'].dt.strftime('%Y-%m-%d')
        
        # Looping untuk setiap tanggal yang ada di dalam dataframe
        for event_date, group_df in df.groupby('event_date'):
            
            # Kita drop/buang kolom event_date dari Parquet biar gak redundan
            # (Karena informasi tanggalnya udah ada di nama foldernya nanti)
            group_df = group_df.drop(columns=['event_date'])
            
            # Create memory buffer & Convert ke Parquet untuk pecahan data ini
            parquet_buffer = io.BytesIO()
            group_df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
            
            # Tentukan path s3 tujuan dengan format dt=YYYY-MM-DD
            silver_path = f"silver/earthquake/fact_earthquake/dt={event_date}/data.parquet"
            
            # Upload data yang sudah di-group ke folder partisinya masing-masing
            upload_to_s3(bucket_name, silver_path, parquet_buffer.getvalue())
            
        print("Sukses! Semua partisi dinamis berhasil didorong ke S3 Silver layer.")
        
        # Return success (buat log/konfirmasi ke Airflow)
        return {
            'statusCode': 200,
            'body': "Successfully processed and dynamically partitioned data to Silver layer."
        }

    except Exception as e:
        print(f"Error terjadi di dalam Lambda Executor: {str(e)}")
        raise e