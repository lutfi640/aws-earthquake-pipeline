import requests
import json
from datetime import datetime
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.append(project_root)

from utils.aws_helper import upload_to_s3

#url api
url = 'https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&minmagnitude=5'
response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    path = f"BRONZE/earthquake_data_{datetime.now().strftime('%Y-%m-%d')}.json"

    #upload to S3
    data_string = json.dumps(data)
    upload_to_s3('learn-aws-imam', path, data_string)
else:
    print(f"Gagal mengambil data, status code: {response.status_code}")