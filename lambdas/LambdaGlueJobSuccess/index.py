import boto3
import time
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue = boto3.client('glue')
athena = boto3.client('athena')

def lambda_handler(event, context):
    logger.info("Received event: %s", event)

    detail = event.get('detail', {})
    job_name = detail.get('jobName', 'unknown')
    logger.info("Triggered by Glue job: %s", job_name)

    if "txt" in job_name:
        crawler_name = os.environ['TXT_CRAWLER']
    elif "csv" in job_name:
        crawler_name = os.environ['CSV_CRAWLER']
    elif "json" in job_name:
        crawler_name = os.environ['JSON_CRAWLER']
    else:
        logger.warning("No crawler found for job name: %s", job_name)
        return False

    logger.info("Starting crawler: %s", crawler_name)
    glue.start_crawler(Name=crawler_name)

    while True:
        status = glue.get_crawler(Name=crawler_name)['Crawler']['State']
        if status == 'READY':
            logger.info("Crawler completed successfully.")
            break
        time.sleep(10)

   