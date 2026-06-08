import pandas as pd
import boto3
import json
from datetime import datetime
import io

import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.append(project_root)

from utils.aws_helper import read_from_s3, upload_to_s3


s3 = boto3.client('s3')
bucket_name = 'learn-aws-imam'
file_key = f"BRONZE/earthquake_data_{datetime.now().strftime('%Y-%m-%d')}.json"

#baca json dari S3
content = read_from_s3(bucket_name, file_key)
data = json.loads(content)

#flatten json
df = pd.json_normalize(data['features'])
df['properties.time'] = pd.to_datetime(df['properties.time'], unit='ms')
df['properties.updated'] = pd.to_datetime(df['properties.updated'], unit='ms')
df = df.loc[:, ['properties.mag', 'properties.place', 'properties.time',
       'properties.updated', 'properties.alert', 'properties.tsunami', 'properties.sig', 
        'properties.type', 'geometry.coordinates']]

#Create memory buffer
parquet_buffer = io.BytesIO()
df.to_parquet(parquet_buffer, index=False, engine='pyarrow')

#Upload to S3
silver_path = file_key.replace('BRONZE', 'SILVER').replace('.json', '.parquet')
upload_to_s3(bucket_name, silver_path, parquet_buffer.getvalue())




