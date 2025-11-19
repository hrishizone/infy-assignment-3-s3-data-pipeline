import sys
import boto3
import json
import os
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['bucket_name', 'object_key', 'crawler_bucket'])

bucket_name = args['bucket_name']
object_key = args['object_key']
crawler_bucket = args['crawler_bucket']

s3 = boto3.client('s3')
country_codes = {"India": "IN", "USA": "US", "Canada": "CA"}

input_path = '/tmp/input.json'
output_path = '/tmp/output.json'

s3.download_file(bucket_name, object_key, input_path)

with open(input_path, 'r') as f:
    data = json.load(f)

for record in data:
    record['country_code'] = country_codes.get(record.get('country', ''), 'NA')

with open(output_path, 'w') as f:
    for record in data:
        json.dump(record, f)
        f.write('\n')

s3.upload_file(output_path, crawler_bucket, f'output/json/large/{os.path.basename(object_key)}')
