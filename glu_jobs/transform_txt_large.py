import sys
import boto3
import os
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['bucket_name', 'object_key', 'crawler_bucket'])

bucket_name = args['bucket_name']
object_key = args['object_key']
crawler_bucket = args['crawler_bucket']

s3 = boto3.client('s3')
country_codes = {"India": "IN", "USA": "US", "Canada": "CA"}

input_path = '/tmp/input.txt'
output_path = '/tmp/output.txt'


s3.download_file(bucket_name, object_key, input_path)

with open(input_path, 'r') as f:
    lines = f.read().strip().split('\n')

output_lines = []
for line in lines:
    parts = line.strip().split(',')
    if len(parts) >= 3:
        name, age, country = parts[:3]
        code = country_codes.get(country.strip(), 'NA')
        output_lines.append(f"{name},{age},{country},{code}") 

with open(output_path, 'w') as f:
    f.write('name,age,country,country_code\n')
    f.write('\n'.join(output_lines))


s3.upload_file(output_path, crawler_bucket, f'output/txt/large/{os.path.basename(object_key)}')


