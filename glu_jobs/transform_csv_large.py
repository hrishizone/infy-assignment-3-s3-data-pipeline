import sys
import boto3
import pandas as pd
import os
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['bucket_name', 'object_key', 'crawler_bucket'])

bucket_name = args['bucket_name']
object_key = args['object_key']
crawler_bucket = args['crawler_bucket']

s3 = boto3.client('s3')
country_code_map = {"India": "IN", "USA": "US", "Canada": "CA"}

input_path = '/tmp/input.csv'
output_path = '/tmp/output.csv'

s3.download_file(bucket_name, object_key, input_path)

df = pd.read_csv(input_path)

if 'country' not in df.columns:
    df['country'] = ''

df['country_code'] = df['country'].map(country_code_map).fillna('NA')

df.to_csv(output_path, index=False)

s3.upload_file(output_path, crawler_bucket, f'output/csv/large/{os.path.basename(object_key)}')

