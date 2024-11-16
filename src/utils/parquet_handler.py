import random
import uuid
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


class DynamicParquetWriter:
    def __init__(self, app_name="DynamicParquetWriter"):
        """
        Initialize the Spark session.
        """
        self.spark = SparkSession.builder.appName(app_name).getOrCreate()

    def generate_bulk_data(self, num_rows, num_profiles, start_date, end_date):
        """Generate a bulk dataset dynamically.
        
        Args:
            num_rows: Number of rows to generate.
            num_profiles: Number of unique profiles.
            start_date: Start date for created_time.
            end_date: End date for created_time.
        
        Returns:
            List of dictionaries representing the dataset.
        """
        profiles = [f"profile_{i}" for i in range(1, num_profiles + 1)]
        posts = []

        for _ in range(num_rows):
            profile_id = random.choice(profiles)
            post_id = str(uuid.uuid4())  # Unique post ID
            created_time = start_date + timedelta(
                seconds=random.randint(0, int((end_date - start_date).total_seconds()))
            )
            col1 = random.randint(1, 1000)
            col2 = random.randint(50, 500)
            col3 = random.randint(10, 300)

            posts.append({
                "post_id": post_id,
                "profile_id": profile_id,
                "created_time": created_time.strftime("%Y-%m-%d %H:%M:%S"),
                "col1": col1,
                "col2": col2,
                "col3": col3
            })

        return posts

    def save_to_parquet(self, data, output_path="./bulk_data.parquet"):
        """ Save the dynamically generated data to a Parquet file.
        
        Args:
            data: List of dictionaries representing the dataset.
            output_path: Output path for the Parquet file.
        """
        df = self.spark.createDataFrame(data)
        df = df.withColumn("created_time", F.to_timestamp(F.col("created_time")))
        print("Saving data to Parquet file...")
        df.write.mode("overwrite").parquet(output_path)
        print(f"Data saved to: {output_path}")
