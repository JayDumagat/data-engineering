"""S3-compatible object storage adapter and default store factory."""

import os
from functools import lru_cache

import boto3
from botocore.config import Config

from pipeline.core.contracts import ObjectStore


@lru_cache(maxsize=4)
def _create_s3_client(endpoint_url: str, access_key: str, secret_key: str, region: str):
    """Reuse a connection-pooled S3 client for repeated lake operations."""
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(
            connect_timeout=10,
            read_timeout=60,
            max_pool_connections=16,
            retries={"mode": "standard", "max_attempts": 4},
            s3={"addressing_style": "path"},
        ),
    )


def get_s3_client():
    return _create_s3_client(
        os.environ["S3_ENDPOINT_URL"],
        os.environ["AWS_ACCESS_KEY_ID"],
        os.environ["AWS_SECRET_ACCESS_KEY"],
        os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
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

    body = response["Body"]
    try:
        return body.read()
    finally:
        body.close()


class S3ObjectStore:
    """S3-compatible raw object adapter implementing the shared storage contract."""

    def read(self, bucket: str, key: str) -> bytes:
        return download_bytes(bucket=bucket, key=key)

    def write(
        self,
        bucket: str,
        key: str,
        body: bytes,
        content_type: str = "application/octet-stream",
    ) -> dict[str, str]:
        return upload_bytes(bucket=bucket, key=key, data=body, content_type=content_type)


def get_object_store() -> ObjectStore:
    """Create the default S3-compatible adapter for pipeline flows."""
    return S3ObjectStore()
