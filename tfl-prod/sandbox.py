import boto3, os

from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["R2_ENDPOINT"],
    aws_access_key_id=os.environ["R2_ACCESS_KEY"],
    aws_secret_access_key=os.environ["R2_SECRET_KEY"],
)

bucket = os.environ["R2_BUCKET"]
prefix = "checkpoints/silver/"

paginator = s3.get_paginator("list_objects_v2")
for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
    for obj in page.get("Contents", []):
        s3.delete_object(Bucket=bucket, Key=obj["Key"])
        print(f"deleted {obj['Key']}")

print("checkpoints cleared")