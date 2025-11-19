import boto3
import json
import urllib.parse
import time
import logging
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('S3ObjectMetadata')
glue = boto3.client('glue')

GLUE_JOB_MAP = {
    ('csv', 'Small'): 'csv-0-5kb-job',
    ('csv', 'Medium'): 'csv-5-10kb-job',
    ('csv', 'Large'): 'csv-above-10kb-job',
    ('json', 'Small'): 'json-0-5kb-job',
    ('json', 'Medium'): 'json-5-10kb-job',
    ('json', 'Large'): 'json-above-10kb-job',
    ('txt', 'Small'): 'txt-0-5kb-job',
    ('txt', 'Medium'): 'txt-5-10kb-job',
    ('txt', 'Large'): 'txt-above-10kb-job',
}

def lambda_handler(event, context):
    sns_message = event['Records'][0]['Sns']['Message']
    logger.info("Received SNS event: %s", sns_message)

    s3_event = json.loads(sns_message)

    if "Records" not in s3_event:
        logger.warning("SNS test event received, skipping...")
        return True

    for record in s3_event['Records']:
        bucket_name = record['s3']['bucket']['name']
        object_key = urllib.parse.unquote_plus(record['s3']['object']['key'])
        logger.info("Processing file: %s", object_key)

        response = table.get_item(Key={'object_key': object_key})
        metadata = response.get('Item')

        if not metadata:
            logger.warning("Metadata for %s not found. Retrying.", object_key)
            time.sleep(6)
            metadata = table.get_item(Key={'object_key': object_key}).get('Item')

        if not metadata:
            logger.error("Metadata for %s not found.", object_key)
            continue

        file_type = metadata.get('file_type', 'unknown').lower()
        file_size_mb = Decimal(str(metadata.get('file_size', 0)))
        file_size_kb = file_size_mb * 1024

        if file_size_kb <= 5:
            size_category = 'Small'
        elif 5 < file_size_kb <= 10:
            size_category = 'Medium'
        else:
            size_category = 'Large'

        glue_job_name = GLUE_JOB_MAP.get((file_type, size_category))

        if not glue_job_name:
            logger.error("No Glue job mapping found.")
            continue

        logger.info("Selected Glue job: %s", glue_job_name)

        try:
            response = glue.start_job_run(
                JobName=glue_job_name,
                Arguments={
                    '--bucket_name': bucket_name,
                    '--object_key': object_key,
                    '--file_type': file_type,
                    '--size_category': size_category
                }
            )
            logger.info("Glue job started successfully! JobRunId: %s", response['JobRunId'])
        except glue.exceptions.EntityNotFoundException:
            logger.error("Glue job %s not found in AWS Glue.", glue_job_name)
        except Exception as e:
            logger.exception("Unexpected error while starting Glue job: %s", str(e))

    return True
