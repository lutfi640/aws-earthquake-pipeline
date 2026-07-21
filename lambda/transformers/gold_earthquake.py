import boto3
import json
import pandas as pd
import io
from datetime import datetime, timedelta

# ==========================================
# 1. FUNGSI UTILS (TIDAK DIUBAH SAMA SEKALI)
# ==========================================
def read_from_s3(bucket, key):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return obj['Body'].read().decode('utf-8')

def read_parquet_from_s3(bucket, key):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(io.BytesIO(obj['Body'].read()))

def upload_to_s3(bucket, key, data):
    s3_client = boto3.client('s3')
    s3_client.put_object(Bucket=bucket, Key=key, Body=data)
    print(f"Sukses upload ke s3://{bucket}/{key}")

# ==========================================
# 2. LOGIC TRANSFORMATION UTAMA (GOLD LAYER)
# ==========================================
print("Menerima event dinamis dari Airflow:", event)

# Ambil bucket dari event
bucket_name = event.get('bucket', 'learn-aws-imam')

try:
    print("Membaca data tabel dimensi dari layer Silver...")
    # NOTE: Pastikan path ini sesuai dengan path saat lo nyimpen dimensi kemarin (lowercase/uppercase)
    df_dim_place = read_parquet_from_s3(bucket_name, "SILVER/earthquake/dim_place/dim_place.parquet")
    df_dim_alert = read_parquet_from_s3(bucket_name, "SILVER/earthquake/dim_alert/dim_alert.parquet")
    df_dim_type = read_parquet_from_s3(bucket_name, "SILVER/earthquake/dim_type/dim_type.parquet")

    # Hitung batasan waktu: Hari ini sampai 30 hari ke belakang (Cutoff)
    print('king')
    today = datetime.now()
    thirty_days_ago = today - timedelta(days=30)
    
    print(f"Mengambil data Fact dari partisi tanggal: {thirty_days_ago.strftime('%Y-%m-%d')} sampai {today.strftime('%Y-%m-%d')}")
    
    # PERBAIKAN: Mengganti List Comprehension dengan For Loop Tradisional
    # Biar gak kena bug scope exec() di Airflow
    date_list = []
    for x in range(31):
        tanggal_mundur = (today - timedelta(days=x)).strftime('%Y-%m-%d')
        date_list.append(tanggal_mundur)
    
    fact_dfs = []
    
    # Looping partisi S3 (Hanya ambil yang 30 hari terakhir biar gak boros memory)
    for dt_str in date_list:
        silver_fact_path = f"SILVER/earthquake/fact_earthquake/dt={dt_str}/data.parquet"
        try:
            # Coba baca parquet harian
            df_day = read_parquet_from_s3(bucket_name, silver_fact_path)
            fact_dfs.append(df_day)
        except Exception:
            # Kalau misal di tanggal tersebut nggak ada data/file (ex: S3 NoSuchKey), kita skip aja
            pass

    if not fact_dfs:
        raise ValueError("Tidak ada data di Fact Table pada rentang 30 hari terakhir!")

    # Gabungin semua DataFrame harian jadi satu
    df = pd.concat(fact_dfs, ignore_index=True)

    print("Melakukan Join balik (Denormalisasi) dari ID ke Nama/String...")
    
    # 1. Join Place
    df = df.merge(df_dim_place[['place_id', 'place']], on='place_id', how='left')
    
    # 2. Join Alert
    df = df.merge(df_dim_alert[['alert_id', 'alert']], on='alert_id', how='left')
    
    # 3. Join Type (Inget kemarin nama kolomnya 'event_type' di dimensi)
    df = df.merge(df_dim_type[['type_id', 'event_type']], on='type_id', how='left')
    
    # Buang kolom ID karena di Gold Layer analis cuma butuh String/Namanya aja
    df.drop(columns=['place_id', 'alert_id', 'type_id'], inplace=True)
    
    # Rename event_type jadi type lagi (biar downstream / dashboard lo gak kaget)
    df.rename(columns={'event_type': 'type'}, inplace=True)

    # Pastikan kolom time berformat datetime untuk filter presisi jam/menit/detik
    df['time'] = pd.to_datetime(df['time'])
    
    # FINAL FILTER: Pastikan data Bener-bener cutoff sysdate-30 sampai sysdate
    df = df[(df['time'] >= thirty_days_ago) & (df['time'] <= today)]

    # ==========================================
    # UPLOAD KE GOLD LAYER
    # ==========================================
    print(f"Menyimpan data Gold (Total: {len(df)} baris) ke S3...")
    
    # Simpan sebagai satu file terpusat untuk 30 hari terakhir (OVERWRITE)
    gold_path = "GOLD/earthquake/dm_earthquake/data.parquet"
    
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    
    upload_to_s3(bucket_name, gold_path, parquet_buffer.getvalue())
    
    print("Sukses! Data berhasil di-denormalisasi dan di-save ke layer GOLD.")
    
except Exception as e:
    print(f"Error terjadi di dalam Lambda Executor: {str(e)}")
    raise e