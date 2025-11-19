import boto3
import os
import json

sns = boto3.client('sns')
topic_arn = os.environ['SNS_TOPIC_ARN']

def lambda_handler(event, context):
    detail = event.get('detail', {})
    job_name = detail.get('jobName', 'unknown')

    message = f"Glue job failed: {job_name}\n\nDetails:\n{json.dumps(detail, indent=2)}"

    sns.publish(
        TopicArn=topic_arn,
        Subject=f"Glue Job Failed: {job_name}",
        Message=message
    )

    return {"status": "notification_sent", "job": job_name}
