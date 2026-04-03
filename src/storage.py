import io
import os

import boto3
import pandas as pd

BUCKET = os.getenv("MINIO_BUCKET", "brokerage")


def _client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    )


def ensure_bucket():
    c = _client()
    existing = [b["Name"] for b in c.list_buckets().get("Buckets", [])]
    if BUCKET not in existing:
        c.create_bucket(Bucket=BUCKET)
        print(f"Created MinIO bucket: {BUCKET}")


def upload_df(df, key):
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    _client().put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue().encode())
    print(f"Uploaded {len(df)} rows -> s3://{BUCKET}/{key}")


def download_df(key):
    obj = _client().get_object(Bucket=BUCKET, Key=key)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))
    print(f"Downloaded {len(df)} rows <- s3://{BUCKET}/{key}")
    return df
