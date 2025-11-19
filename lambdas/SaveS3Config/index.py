import boto3
import urllib.parse
import json
import logging
from datetime import datetime, UTC
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('S3ObjectMetadata')

def lambda_handler(event, context):
    try:
        sns_message = event['Records'][0]['Sns']['Message']
        s3_event = json.loads(sns_message)

        for record in s3_event['Records']:
            bucket_name = record['s3']['bucket']['name']
            object_key = urllib.parse.unquote_plus(record['s3']['object']['key'])
            
            logger.info("Processing object: Bucket='%s', Key='%s'", bucket_name, object_key)

            object_metadata = s3_client.head_object(Bucket=bucket_name, Key=object_key)
            file_size = Decimal(str(round(object_metadata['ContentLength'] / (1024 * 1024), 2)))
            content_type = object_metadata.get('ContentType', 'unknown')
            last_modified = object_metadata['LastModified'].isoformat()

            if object_key.lower().endswith('.csv'):
                file_type = 'csv'
            elif object_key.lower().endswith('.json'):
                file_type = 'json'
            elif object_key.lower().endswith('.txt'):
                file_type = 'txt'
            else:
                file_type = 'unknown'
            
            logger.debug("File details are Type: %s, Size: %s KB, ContentType: %s", file_type, file_size, content_type)

            item = {
                'object_key': object_key,
                'bucket_name': bucket_name,
                'file_type': file_type,
                'file_size': file_size,
                'content_type': content_type,
                'last_modified': last_modified,
                'inserted_at': datetime.now(UTC).isoformat()
            }

            response = table.put_item(Item=item)
            logger.info("Details inserted into DynamoDB. Response: %s", json.dumps(response, default=str))

        logger.info("All records processed successfully.")
        return True
    except Exception as e:
        logger.exception("Error processing event: %s", str(e))
        return False