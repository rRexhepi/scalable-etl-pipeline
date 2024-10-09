from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, col
import logging
import sys
import os

def setup_logger(log_level=logging.INFO):
    """
    Sets up the logger for the transformation script.
    Logs are output to both stdout and a log file.
    """
    logger = logging.getLogger("SparkTransformLogger")
    logger.setLevel(log_level)
    
    # Formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # Stream Handler (stdout)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    
    # File Handler
    log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, 'transform.log')
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger

def main():
    """
    Main function to perform data transformation using Spark.
    """
    logger = setup_logger()
    logger.info("Starting Spark Transformation Process.")
    
    try:
        # Initialize Spark Session
        spark = SparkSession.builder \
            .appName("NYC Taxi ETL Transformation") \
            .getOrCreate()
        logger.info("Spark session initialized.")
        
        # Define MinIO (S3) configurations
        s3_endpoint = "http://minio:9000"
        s3_access_key = "minioadmin"
        s3_secret_key = "minioadmin"
        
        # Configure Spark to interact with MinIO
        hadoop_conf = spark._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.endpoint", s3_endpoint)
        hadoop_conf.set("fs.s3a.access.key", s3_access_key)
        hadoop_conf.set("fs.s3a.secret.key", s3_secret_key)
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
        
        logger.info("Configured Spark to connect to MinIO.")
        
        # Define S3 paths
        raw_train_path = "s3a://nyc-taxi/raw/train.csv"
        raw_test_path = "s3a://nyc-taxi/raw/test.csv"
        processed_train_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'processed', 'cleaned_train.csv')
        processed_test_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'processed', 'cleaned_test.csv')
        
        # Read raw train data
        logger.info(f"Reading raw train data from {raw_train_path}")
        train_df = spark.read.csv(raw_train_path, header=True, inferSchema=True)
        logger.info(f"Train DataFrame Schema:\n{train_df.printSchema()}")
        logger.info(f"Number of records in train data: {train_df.count()}")
        
        # Read raw test data
        logger.info(f"Reading raw test data from {raw_test_path}")
        test_df = spark.read.csv(raw_test_path, header=True, inferSchema=True)
        logger.info(f"Test DataFrame Schema:\n{test_df.printSchema()}")
        logger.info(f"Number of records in test data: {test_df.count()}")
        
        # Transformation Logic
        def transform_dataframe(df, dataset_type):
            """
            Transforms the DataFrame by calculating trip duration and filtering records.
            
            Args:
                df (DataFrame): Spark DataFrame to transform.
                dataset_type (str): Type of dataset ('train' or 'test').
            
            Returns:
                DataFrame: Transformed Spark DataFrame.
            """
            logger.info(f"Transforming {dataset_type} dataset.")
            
            # Calculate trip duration in minutes
            df = df.withColumn(
                "trip_duration_minutes",
                (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60
            )
            
            # Drop records with null trip duration
            df = df.dropna(subset=["trip_duration_minutes"])
            
            # Filter out unrealistic trip durations
            df = df.filter(
                (col("trip_duration_minutes") > 0) & 
                (col("trip_duration_minutes") < 300)  # 5 hours
            )
            
            logger.info(f"{dataset_type.capitalize()} DataFrame after transformation has {df.count()} records.")
            
            return df
        
        # Transform train and test data
        transformed_train_df = transform_dataframe(train_df, "train")
        transformed_test_df = transform_dataframe(test_df, "test")
        
        # Write transformed train data to CSV
        logger.info(f"Writing transformed train data to {processed_train_path}")
        transformed_train_df.coalesce(1).write.csv(processed_train_path, header=True, mode="overwrite")
        logger.info("Transformed train data written successfully.")
        
        # Write transformed test data to CSV
        logger.info(f"Writing transformed test data to {processed_test_path}")
        transformed_test_df.coalesce(1).write.csv(processed_test_path, header=True, mode="overwrite")
        logger.info("Transformed test data written successfully.")
        
        logger.info("Spark Transformation Process Completed Successfully.")
    
    except Exception as e:
        logger.error(f"An error occurred during transformation: {e}")
        sys.exit(1)
    
    finally:
        spark.stop()
        logger.info("Spark session terminated.")

if __name__ == "__main__":
    main()