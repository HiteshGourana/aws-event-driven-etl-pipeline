import boto3
import json


# Create AWS Glue client
glue_client = boto3.client("glue")


# Name of the AWS Glue ETL Job
GLUE_JOB_NAME = "employee-data-cleaning-job"


def lambda_handler(event, context):
    """
    AWS Lambda handler function.

    This function is automatically triggered when a CSV file
    is uploaded to the configured Amazon S3 bucket.

    It starts the AWS Glue ETL job using boto3.
    """

    print("Received S3 Event:")
    print(json.dumps(event))

    try:

        # Start AWS Glue ETL Job
        response = glue_client.start_job_run(
            JobName=GLUE_JOB_NAME
        )

        # Get Glue Job Run ID
        job_run_id = response["JobRunId"]

        print("Glue job started successfully")
        print(f"Glue Job Name: {GLUE_JOB_NAME}")
        print(f"Glue Job Run ID: {job_run_id}")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Glue job started successfully",
                "jobRunId": job_run_id
            })
        }

    except Exception as error:

        print(f"Error starting Glue job: {str(error)}")

        raise error