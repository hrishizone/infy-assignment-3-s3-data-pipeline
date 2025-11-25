import boto3
import time
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue = boto3.client('glue')
athena = boto3.client('athena')

ATHENA_OUTPUT_BUCKET = os.environ['ATHENA_OUTPUT_BUCKET']
GLUE_DATABASE = os.environ['GLUE_DATABASE']

def lambda_handler(event, context):
    logger.info("Received event: %s", event)

    detail = event.get('detail', {})
    job_name = detail.get('jobName', 'unknown')
    logger.info("Triggered by Glue job: %s", job_name)

    # Decide which crawler and table prefix to use
    if "txt" in job_name:
        crawler_name = os.environ['TXT_CRAWLER']
        table_prefix = "txt_"
    elif "csv" in job_name:
        crawler_name = os.environ['CSV_CRAWLER']
        table_prefix = "csv_"
    elif "json" in job_name:
        crawler_name = os.environ['JSON_CRAWLER']
        table_prefix = "json_"
    else:
        logger.warning("No crawler found for job name: %s", job_name)
        return False

    logger.info("Starting crawler: %s", crawler_name)
    glue.start_crawler(Name=crawler_name)

    # Wait for crawler to complete
    max_wait_seconds = 90
    waited = 0

    while True:
        status = glue.get_crawler(Name=crawler_name)['Crawler']['State']
        logger.info("Crawler state: %s", status)

        if status in ('READY', 'STOPPING'):
            logger.info("Crawler finished with state: %s", status)
            break

        time.sleep(5)
        waited += 5

        if waited >= max_wait_seconds:
            logger.warning("Crawler did not reach READY in time, continuing anyway.")
            break

    # Get one table from Glue Catalog matching the prefix (e.g. txt_, csv_, json_)
    logger.info("Getting table for prefix: %s in database: %s", table_prefix, GLUE_DATABASE)
    tables_resp = glue.get_tables(
        DatabaseName=GLUE_DATABASE,
        Expression=f"{table_prefix}*"
    )
    tables = tables_resp.get("TableList", [])
    if not tables:
        logger.warning("No tables found with prefix %s in DB %s", table_prefix, GLUE_DATABASE)
        return False

    table_name = tables[0]["Name"]
    logger.info("Using table: %s", table_name)

    # Run Athena query: SELECT * FROM table
    query = f"SELECT * FROM {table_name}"
    output_location = f"s3://{ATHENA_OUTPUT_BUCKET}/results/"

    logger.info("Starting Athena query: %s", query)
    resp = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': GLUE_DATABASE},
        ResultConfiguration={'OutputLocation': output_location}
    )

    qid = resp['QueryExecutionId']
    logger.info("Athena query started. QueryExecutionId: %s", qid)
    logger.info("Results will be written under: %s", output_location)

    # 🔹 NEW: wait for Athena to finish
    while True:
        q = athena.get_query_execution(QueryExecutionId=qid)
        state = q['QueryExecution']['Status']['State']
        logger.info("Athena query state: %s", state)

        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break

        time.sleep(5)

    if state != "SUCCEEDED":
        reason = q["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
        logger.error("Athena query did NOT succeed. State: %s, Reason: %s", state, reason)
        return False

    logger.info("Athena query SUCCEEDED. Results should be in: s3://%s/results/", ATHENA_OUTPUT_BUCKET)
    return True
