import json
import boto3


def get_s3_client(aws_access_key: str, aws_secret_access_key: str) -> boto3.client:
    """Gets AWS client for connecting to s3.

    Parameters
    ----------
    aws_access_key : str
        Obtained from your IAM credentials page in the AWS console
    aws_secret_access_key : str
        Obtained from your IAM credentials page in the AWS console

    """
    s3_client = boto3.client(
        service_name="s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_access_key,
    )
    return s3_client


def save_json_to_s3(
    s3_con: boto3.client, bucket_name: str, json_object: str, timestamp: str
) -> None:
    """Saves json object to AWS s3 bucket.

    Parameters
    ----------
    s3_con : boto3.client
        AWS client for connecting to s3.
    bucket_name : str
        Name of the target s3 bucket
    json_object : str
        json data to upload
    timestamp : str
        current timestamp
    """
    s3_con.put_object(
        Body=json.dumps(json_object), Bucket=bucket_name, Key=f"jobs-{timestamp}"
    )
