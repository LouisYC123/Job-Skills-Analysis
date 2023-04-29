""" Module to provide s3 interaction tools"""

import os
import boto3
from botocore.exceptions import ClientError
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json
import logging
from airflow import settings
from airflow.models import Connection


# ---- 'Get' Functions


def get_s3_client(aws_conn_id: str = "aws_default"):
    """Returns a session object to connect to S3"""
    return S3Hook(aws_conn_id=aws_conn_id).get_client()


def get_s3_session(aws_conn_id: str = "aws_default"):
    """Returns a session object to connect to S3"""
    return S3Hook(aws_conn_id=aws_conn_id).get_session()


def get_s3_hook(aws_conn_id: str = "aws_default"):
    """Returns a Hook object to connect to S3"""
    return S3Hook(aws_conn_id=aws_conn_id)


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


# ---- 'Create' Functions


def create_s3_bucket(bucket_name: str, aws_region: str, s3_client):
    """Creates s3 buckets using the provided bucket_name

    Parameters
    ----------
    aws_region : str
        s3 bucket target region (ap-southeast-2)
    bucket_name : str
        the name you want to give the created bucket
    """
    s3_client.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={
            "LocationConstraint": aws_region,
        },
    )
    print(f"create_s3_bucket: Succesfully created s3 bucket: {bucket_name}")


def create_s3_folder(bucket_name: str, folder_name: str, s3_client):
    """Creates a sub-folder within an s3 bucket.

    Parameters
    ----------
    bucket_name : str
        Name of the s3 bucket you want to create a sub-folder in
    folder_name : str
        name for the sub-folder
    """
    s3_client.put_object(Bucket=bucket_name, Key=(folder_name + "/"))


def create_aws_conn_id(
    aws_access_key: str, aws_secret_access_key: str, conn_id: str = "aws_default"
):
    conn = Connection(
        conn_id=conn_id,
        conn_type="aws",
        login=aws_access_key,
        password=aws_secret_access_key,
    )
    session = settings.Session()
    conn_name = (
        session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()
    )
    # Check if connection already exists
    if str(conn_name) == str(conn.conn_id):
        logging.warning(f"Connection {conn.conn_id} already exists")
        return None

    # create a connection object

    session.add(conn)
    session.commit()
    logging.info(f"Connection {conn_id} is created")


def create_snowflake_conn_id(
    account: str,
    snow_user: str,
    password: str,
    database: str,
    db_schema: str,
    region: str = "ap-southeast-2",
    warehouse: str = "COMPUTE_WH",
    conn_id: str = "snowflake_default",
):
    conn = Connection(
        conn_id=conn_id,
        conn_type="snowflake",
        login=snow_user,
        password=password,
        schema=db_schema,
        extra={
            "account": account,
            "database": database,
            "region": region,
            "warehouse": warehouse,
        },
    )
    session = settings.Session()
    conn_name = (
        session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()
    )
    # Check if connection already exists
    if str(conn_name) == str(conn.conn_id):
        logging.warning(f"Connection {conn.conn_id} already exists")
        return None

    # create a connection object

    session.add(conn)
    session.commit()
    logging.info(f"Connection {conn_id} is created")


# ---- 'Check' Functions


def check_s3_bucket_exists(bucket_name: str, s3_session: boto3.Session):
    """Checks whether s3 bucket exists or not"""
    try:
        s3_resource = s3_session.resource("s3")
        # If bucket exists, return exists=True
        s3_resource.meta.client.head_bucket(Bucket=bucket_name)
        print("Bucket already exists:", bucket_name)
        exists = True
    except ClientError as error:
        # If bucket does not exist or you do not have access, return exists=False
        error_code = int(error.response["Error"]["Code"])
        if error_code == 403:
            print("Private Bucket. Forbidden Access! ", bucket_name)
        elif error_code == 404:
            print("Bucket Does Not Exist", bucket_name)
        exists = False
    return exists


# ---- 'Copy' Functions


def copy_s3_object(
    source_s3_key: str,
    source_folder: str,
    destination_folder: str,
    destination_suffix: str | None = None,
    aws_conn_id: str = "aws_default",
):
    """Copies an s3 object from source folder to destination folder.

    Parameters
    ----------
    source_s3_key : str
        s3_key of object to be copied
    source_folder : str
        sub-folder where source_s3_key is located
    destination_folder : str
        sub-folder where object will be copied to
    aws_conn_id : str, optional
        Airflow conn_id used to create S3Hook, by default "aws_default"
    """
    # Get S3 Hook
    s3_hook = get_s3_hook(aws_conn_id=aws_conn_id)
    # Extract the filename and extension from the source_s3_key
    filename, file_extension = os.path.splitext(source_s3_key)
    # Create new s3 key for the destination
    if not destination_suffix:
        destination_suffix = ""
    destination_filename = (
        filename.replace(source_folder, destination_folder)
        + f"{destination_suffix}"
        + file_extension
    )
    # Copy object from source to destination
    s3_hook.copy_object(
        source_bucket_key=source_s3_key,
        dest_bucket_key=destination_filename,
    )


# ---- 'Save' Functions


def save_json_to_s3(
    data: dict,
    timestamp: str,
    context,
) -> None:
    """Saves json object to AWS s3 bucket.

    Parameters
    ----------
    s3_con : boto3.client
        AWS client for connecting to s3.
    bucket_name : str
        Name of the target s3 bucket
    data : str
        data to upload
    timestamp : str
        current timestamp
    """
    bucket_name = context["ti"].xcom_pull(key="bucket_name")
    s3_hook = get_s3_hook()
    filename = f"jobs-{timestamp}.json"
    s3_key = f"raw_jobs_data/{filename}"
    s3_hook.load_string(
        string_data=json.dumps(data),
        bucket_name=bucket_name,
        key=s3_key,
    )
    context["ti"].xcom_push(
        key=f"saved_filename_for_{context['run_id']}", value=filename
    )
    return s3_key


# ---- 'Delete' Functions


def delete_s3_object(
    bucket: str,
    s3_key: str,
    aws_conn_id: str = "aws_default",
):
    """Deletes an object from s3

    Parameters
    ----------
    bucket : str
        s3 bucket name
    s3_key : str
        s3_key of object to be deleted
    aws_conn_id : str, optional
        Airflow conn_id used to create S3Hook
        by default "aws_default"
    """
    # Get S3Hook
    s3_session = get_s3_hook(
        aws_conn_id=aws_conn_id,
    ).get_session()
    # Create S3 resource from session
    s3 = s3_session.resource("s3")
    # delete target object
    s3.Object(bucket, s3_key).delete()


# ---- 'Archive' Functions


def archive_raw_data(context):
    """Archives raw data in raw_jobs_data/ by moving it to the
    archived subfolder
    """
    bucket_name = context["ti"].xcom_pull(key="bucket_name")
    target_file = context["ti"].xcom_pull(key=f"saved_s3_key_for_{context['run_id']}")
    target_key = f"s3://{bucket_name}/{target_file}"
    # Copy object to the _archived folder
    logging.info(f"Archiving {target_key}")
    print(target_key)
    copy_s3_object(
        source_s3_key=target_key,
        source_folder="raw_jobs_data",
        destination_folder="archived",
        destination_suffix="_archived",
    )
    # Delete object from the "drop-zone"
    delete_s3_object(bucket_name, target_file)
