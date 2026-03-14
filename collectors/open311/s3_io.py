from __future__ import annotations

import json
from pathlib import Path
import boto3


class S3Writer:
    def __init__(self, bucket: str) -> None:
        self.bucket = bucket
        self.client = boto3.client("s3")

    def upload_file(self, local_path: Path, s3_key: str, content_type: str = "application/json") -> None:
        with open(local_path, "rb") as f:
            self.client.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=f.read(),
                ContentType=content_type,
            )

    def upload_json(self, payload: dict | list, s3_key: str) -> None:
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.client.put_object(
            Bucket=self.bucket,
            Key=s3_key,
            Body=body,
            ContentType="application/json",
        )
