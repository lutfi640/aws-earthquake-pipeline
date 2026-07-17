import boto3
import json
import pandas as pd
import io
from datetime import datetime

# ==========================================
# 1. FUNGSI UTILS (UDAH DI-MERGE BIAR AMAN DARI EXEC)
# ==========================================
def read_from_s3(bucket, key):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return obj['Body'].read().decode('utf-8')

def upload_df_to_parquet_s3(df, bucket, key):
    # Logic upload S3 langsung digabung ke sini
    s3_client = boto3.client('s3')
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    s3_client.put_object(Bucket=bucket, Key=key, Body=parquet_buffer.getvalue())
    print(f"Sukses upload parquet ke s3://{bucket}/{key}")

# ==========================================
# 2. LOGIC TASK: BUILD DIMENSIONS
# ==========================================
print("Menerima event dinamis dari Airflow:", event)

bucket_name = event.get('bucket', 'learn-aws-imam')
date_str = datetime.now().strftime('%Y-%m-%d')
default_key = f"BRONZE/earthquake_data_{date_str}.json"
file_key = event.get('key', default_key)

try:
    print(f"[TASK: DIM] Mulai memproses file: {file_key} dari bucket: {bucket_name}")
    content = read_from_s3(bucket_name, file_key)
    data = json.loads(content)
    df_raw = pd.json_normalize(data['features'])

    # 1. DIM_PLACE
    df_place = df_raw[['properties.place']].drop_duplicates().dropna().reset_index(drop=True)
    df_place.rename(columns={'properties.place': 'place'}, inplace=True)
    df_place['place_id'] = 'PLC-' + (df_place.index + 1).astype(str).str.zfill(4)
    df_place['country'] = df_place['place'].apply(lambda x: str(x).split(',')[-1].strip() if ',' in str(x) else str(x).strip())
    df_place = df_place[['place_id', 'place', 'country']]

    # 2. DIM_ALERT
    df_raw['properties.alert'] = df_raw['properties.alert'].fillna('unknown')
    df_alert = pd.DataFrame({'alert': df_raw['properties.alert'].unique()})
    df_alert['alert_id'] = 'ALT-' + (df_alert.index + 1).astype(str).str.zfill(2)
    df_alert = df_alert[['alert_id', 'alert']]

    # 3. DIM_TYPE
    df_type = df_raw[['properties.type']].drop_duplicates().dropna().reset_index(drop=True)
    df_type.rename(columns={'properties.type': 'event_type'}, inplace=True)
    df_type['type_id'] = 'TYP-' + (df_type.index + 1).astype(str).str.zfill(2)
    df_type = df_type[['type_id', 'event_type']]

    # UPLOAD DIM KE SILVER
    file_basename = file_key.split('/')[-1].replace('.json', '')
    date_suffix = file_basename.replace('earthquake_data_', '')

    upload_df_to_parquet_s3(df_place, bucket_name, f"SILVER/dim_place/dim_place_{date_suffix}.parquet")
    upload_df_to_parquet_s3(df_alert, bucket_name, f"SILVER/dim_alert/dim_alert_{date_suffix}.parquet")
    upload_df_to_parquet_s3(df_type, bucket_name, f"SILVER/dim_type/dim_type_{date_suffix}.parquet")

    print(f"🎉 SUKSES! 3 Tabel Dimensi disimpan ke Silver.")

except Exception as e:
    print(f"Error di Task DIM: {str(e)}")
    raise e