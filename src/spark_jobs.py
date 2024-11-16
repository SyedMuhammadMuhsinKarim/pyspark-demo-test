from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from contextlib import contextmanager



class SparkDataProcessor:
    def __init__(self, app_name="Spark Example Program"):
        """
        Initializes the SparkDataProcessor class with a Spark session.
        
        Args:
            app_name (str): The name of the Spark application.
            
        Returns:
            None
        """
        self.spark = self.init_spark(app_name)
        self.packages = 'org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026,net.java.dev.jets3t:jets3t:0.9.3'
        
    def init_spark(self, app_name="Spark Example Program", use_bucketing=False, s3_credentials=None):
        """
        Initialize and return a Spark session.
        
        Args:
            app_name (str): The name of the Spark application.
        
        Returns:
            SparkSession: Spark session object.
        """
        spark_builder = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("spark.sql.shuffle.partitions", 200) \
            .enableHiveSupport()

        if use_bucketing and s3_credentials:
            if not self.check_s3_credentials(s3_credentials):
                raise ValueError("S3 credentials are missing or incomplete.")

            s3_access_key = s3_credentials["AWS_ACCESS_KEY_ID"]
            s3_secret_key = s3_credentials["AWS_SECRET_ACCESS_KEY"]
            s3_endpoint_url = s3_credentials["AWS_S3_ENDPOINT_URL"]
            
            spark_builder = spark_builder \
                .config("spark.jars.packages", self.packages) \
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
                .config("spark.hadoop.fs.s3a.access.key", s3_access_key) \
                .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key) \
                .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint_url) \
                .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                .config("spark.hadoop.fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        return spark_builder.getOrCreate()
    
    @staticmethod
    def check_s3_credientials(creds: dict):
        """
        Check if the S3 credentials are present in the dictionary.
        
        Args:
            creds (dict): Dictionary containing the S3 credentials.
        
        Returns:
            bool: True if the credentials are present, False otherwise.
        """
        if not all(key in creds for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_S3_ENDPOINT_URL")):
            return False
        return True
    
    @staticmethod
    def load_parquet_data(spark, path):
        """ Load data from a Parquet file into a DataFrame.
        
        Args:
            spark (SparkSession): Spark session object.
            path (str): Path to the Parquet file.
        
        Returns:
            DataFrame: Loaded DataFrame.
        """
        return spark.read.parquet(path)

    @staticmethod
    def add_social_post_count(df):
        """ Add a column to the DataFrame with the number of posts per profile.
        
        Args:
            df (DataFrame): Input DataFrame.
        
        Returns:
            DataFrame: DataFrame with the additional 'number_of_posts' column.
        """
        window_spec = Window.partitionBy("profile_id")
        df_with_count = df.withColumn("number_of_posts", F.count("post_id").over(window_spec))
        counts = df.groupBy("profile_id").agg(F.count("post_id").alias("number_of_posts"))
        df_with_count = df.join(counts, on="profile_id", how="left")
        return df_with_count

    @staticmethod
    def partition_and_bucket_data(df, output_path):
        """ Partition and bucket data before saving it to S3.
        
        Args:
            df (DataFrame): Input DataFrame.
            output_path (str): Output path in S3.
        """
        df_partitioned = df.withColumn("created_year", F.year("created_time")) \
                           .withColumn("created_month", F.month("created_time"))
        
        print("Partitioning and bucketing the data...")
        print(f"Saving data to {output_path}...")
        df_partitioned.write \
            .partitionBy("created_year", "created_month") \
            .bucketBy(200, "profile_id") \
            .format("parquet") \
            .mode("overwrite") \
            .option("path", output_path) \
            .saveAsTable("social_posts")

    @contextmanager
    def get_spark_session(self):
        """
        Context manager for Spark session lifecycle.

        Returns:
            SparkSession: A Spark session object.
        """
        try:
            yield self.spark
        finally:
            self.spark.stop()