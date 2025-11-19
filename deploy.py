import os
import zipfile
import boto3
import botocore
import logging

STACK_NAME = "assignment-3-data-pipeline"
REGION = "ap-south-1"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger()

def zip_lambda(src_folder, zip_path):
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(src_folder):
            for file in files:
                full_path = os.path.join(root, file)
                arcname = os.path.relpath(full_path, src_folder)
                zipf.write(full_path, arcname)

    logger.info("Created ZIP: %s", zip_path)

def ensure_bucket(bucket_name, s3):
    try:
        s3.head_bucket(Bucket=bucket_name)
        logger.info("Bucket %s already exists", bucket_name)
    except botocore.exceptions.ClientError:
        logger.info("Creating bucket: %s", bucket_name)
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": REGION}
        )
        logger.info("Bucket %s created", bucket_name)

    logger.info("Enabling Versioning on bucket %s", bucket_name)
    s3.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={"Status": "Enabled"}
    )

def upload_and_get_version(bucket, key, file_path, s3):
    logger.info("Uploading: %s -> s3://%s/%s", file_path, bucket, key)
    s3.upload_file(file_path, bucket, key)

    head = s3.head_object(Bucket=bucket, Key=key)
    version = head["VersionId"]

    logger.info("Uploaded %s (VersionId=%s)", key, version)
    return version


def deploy_stack(cfn, template_body, params):
    try:
        cfn.describe_stacks(StackName=STACK_NAME)
        exists = True
    except botocore.exceptions.ClientError:
        exists = False

    if exists:
        logger.info("Updating stack...")

        try:
            resp = cfn.update_stack(
                StackName=STACK_NAME,
                TemplateBody=template_body,
                Parameters=params,
                Capabilities=["CAPABILITY_NAMED_IAM"]
            )

            logger.info("Update Started: %s", resp["StackId"])
            waiter = cfn.get_waiter("stack_update_complete")
            waiter.wait(StackName=STACK_NAME)

            logger.info("Stack Updated Successfully!")
        except botocore.exceptions.ClientError as e:
            if "No updates are to be performed" in str(e):
                logger.info("No updates — stack already up to date.")
                return
            raise

    else:
        logger.info("Creating stack...")
        resp = cfn.create_stack(
            StackName=STACK_NAME,
            TemplateBody=template_body,
            Parameters=params,
            Capabilities=["CAPABILITY_NAMED_IAM"]
        )
        logger.info("Create Started: %s", resp["StackId"])

        waiter = cfn.get_waiter("stack_create_complete")
        waiter.wait(StackName=STACK_NAME)

        logger.info("Stack Created Successfully!")


def main():
    s3 = boto3.client("s3", region_name=REGION)
    cfn = boto3.client("cloudformation", region_name=REGION)

    deployment_bucket = f"{STACK_NAME}-deployment"

    ensure_bucket(deployment_bucket, s3)

    lambda_sources = {
        "lambdas/SaveS3Config": "SaveS3Config.zip",
        "lambdas/TriggerGlueJob": "TriggerGlueJob.zip",
        "lambdas/LambdaGlueJobSuccess": "LambdaGlueJobSuccess.zip",
        "lambdas/LambdaGlueJobFailure": "LambdaGlueJobFailure.zip",
    }

    lambda_versions = {}

    for src_folder, zip_name in lambda_sources.items():
        zip_path = zip_name
        zip_lambda(src_folder, zip_path)

        key = f"lambdas/{zip_name}"
        version = upload_and_get_version(deployment_bucket, key, zip_path, s3)

        lambda_versions[zip_name.replace(".zip", "")] = version

    script_folder = "glu_jobs"
    for root, _, files in os.walk(script_folder):
        for f in files:
            full_path = os.path.join(root, f)
            key = f"scripts/{f}"
            logger.info("Uploading Glue script: %s", key)
            s3.upload_file(full_path, deployment_bucket, key)

    with open("template.yaml") as f:
        template_body = f.read()

    params = [
        {"ParameterKey": "DeploymentBucketName", "ParameterValue": deployment_bucket},
        {"ParameterKey": "EmailSubscription", "ParameterValue": "prime.hrishi@gmail.com"},
        {"ParameterKey": "SaveS3ConfigVersion", "ParameterValue": lambda_versions["SaveS3Config"]},
        {"ParameterKey": "TriggerGlueJobVersion", "ParameterValue": lambda_versions["TriggerGlueJob"]},
        {"ParameterKey": "LambdaGlueJobSuccessVersion", "ParameterValue": lambda_versions["LambdaGlueJobSuccess"]},
        {"ParameterKey": "LambdaGlueJobFailureVersion", "ParameterValue": lambda_versions["LambdaGlueJobFailure"]},
    ]

    deploy_stack(cfn, template_body, params)


if __name__ == "__main__":
    main()
