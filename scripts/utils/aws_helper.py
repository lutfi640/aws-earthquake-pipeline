import boto3
import requests

def get_s3_client():
    return boto3.client('s3')
    
def upload_to_s3(bucket_name, key, data):
    s3_client = get_s3_client()
    s3_client.put_object(Bucket=bucket_name, Key=key, Body=data)
    print(f"Data berhasil disimpan di S3 dengan path: {key}")

def read_from_s3(bucket_name, key):
    s3_client = get_s3_client()
    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    data = response['Body'].read().decode('utf-8')
    return data