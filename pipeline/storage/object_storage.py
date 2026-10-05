import os

import boto3
from botocore.config import Config


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        config=Config(
            s3={
                "addressing_style": "path",
            }
        ),
    )


def upload_bytes(
    *,
    bucket: str,
    key: str,
    data: bytes,
    content_type: str = "application/octet-stream",
):
    client = get_s3_client()

    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=data,
        ContentType=content_type,
    )

    return {
        "bucket": bucket,
        "key": key,
    }


def download_bytes(
    *,
    bucket: str,
    key: str,
) -> bytes:
    client = get_s3_client()

    response = client.get_object(
        Bucket=bucket,
        Key=key,
    )

    return response["Body"].read()