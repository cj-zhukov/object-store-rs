import os
import datafusion
from datafusion.object_store import AmazonS3
import boto3

region = os.getenv("REGION")
bucket_name = os.getenv("BUCKET")
prefix = os.getenv("PREFIX")
session = boto3.Session(profile_name="default")
creds = session.get_credentials().get_frozen_credentials()

os.environ["AWS_ACCESS_KEY_ID"] = creds.access_key
os.environ["AWS_SECRET_ACCESS_KEY"] = creds.secret_key
os.environ["AWS_SESSION_TOKEN"] = creds.token

s3 = AmazonS3(
    bucket_name=bucket_name,
    region=region,
)

ctx = datafusion.SessionContext()
ctx.register_object_store("s3://", s3, None)
path = f"s3://{bucket_name}/{prefix}"
ctx.register_parquet("t", path)
df = ctx.sql("select * from t")
df.show()

# write to file
# df.write_parquet("index-data.parquet")
