import os
import json
import boto3
from datetime import date

def get_client():
    return boto3.client(
        "s3",
        region_name=os.environ.get("AWS_REGION", "us-east-1")
    )

def write_bronze(source: str, data: list | dict, obs_date: date = None, filename: str = "raw.json") -> str:
    if obs_date is None:
        obs_date = date.today()

    bucket = os.environ["S3_BRONZE_BUCKET"]
    key = f"{source}/{obs_date.strftime('%Y/%m/%d')}/{filename}"

    get_client().put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data, default=str),
        ContentType="application/json"
    )
    return f"s3://{bucket}/{key}"