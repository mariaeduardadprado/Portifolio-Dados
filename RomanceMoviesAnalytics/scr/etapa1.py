import boto3
import os
from datetime import datetime

AWS_ACCESS_KEY = ''
AWS_SECRET_KEY = ''
AWS_SESSION_TOKEN = ''
REGIAO = 'us-east-1'
BUCKET_NAME = 'sprint5-bucket'

files = {
    "movies.csv": "Raw/Local/CSV/Movies",
    "series.csv": "Raw/Local/CSV/Series"
}


hoje = datetime.now().strftime("%Y/%m/%d")

s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    aws_session_token=AWS_SESSION_TOKEN,
    region_name=REGIAO
)

for local_path, s3_prefix in files.items():
    filename = os.path.basename(local_path)
    key = f"{s3_prefix}/{hoje}/{filename}"

    if os.path.exists(local_path):
       s3.upload_file(local_path, BUCKET_NAME, key)

