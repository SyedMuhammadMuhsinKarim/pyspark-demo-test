from src.spark_jobs import SparkDataProcessor
import os


# .env file should contain the following variables:
access_key = os.getenv("AWS_ACCESS_KEY_ID")
secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
endpoint_url = os.getenv("AWS_S3_ENDPOINT_URL")
bucket_name = os.getenv("AWS_S3_BUCKET_NAME")

processor = SparkDataProcessor()

path_to_the_data = "./data/bulk_data.parquet"
output_path = "output_data/"  # Local path
output_path = f"s3a://{bucket_name}/{output_path}"  # S3 path

with processor.get_spark_session() as spark:
    df = processor.load_parquet_data(spark, path_to_the_data)
    df_with_count = processor.add_social_post_count(df)
    processor.partition_and_bucket_data(df_with_count, output_path)
    