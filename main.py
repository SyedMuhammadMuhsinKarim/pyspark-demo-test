from src.spark_jobs import SparkDataProcessor
# from dotenv import load_dotenv
import os

# Load environment variables
# load_dotenv()

access_key = os.getenv("AWS_ACCESS_KEY_ID")
secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
endpoint_url = os.getenv("AWS_S3_ENDPOINT_URL", "https://s3.wasabisys.com")  # Default to Wasabi endpoint if not provided
bucket_name = os.getenv("AWS_S3_BUCKET_NAME", 'testbucket34567')  # Default bucket name

# Initialize the SparkDataProcessor
processor = SparkDataProcessor()

# Path to input data (Parquet file)
path_to_the_data = "./data/bulk_data.parquet"
output_path = "output_data/"  # Local path
output_path = f"s3a://{bucket_name}/{output_path}"  # S3 path (Wasabi)

print(f"Bucked name: {bucket_name}")
print(f"Output path: {output_path}")

# Using context manager to handle the Spark session
with processor.get_spark_session() as spark:
    df = processor.load_parquet_data(spark, path_to_the_data)

    # Add post count column
    df_with_count = processor.add_social_post_count(df)
    df_with_count.show(5)  # Display first 5 rows

    # Partition and bucket the data, then save it to Wasabi
    processor.partition_and_bucket_data(df_with_count, output_path)
