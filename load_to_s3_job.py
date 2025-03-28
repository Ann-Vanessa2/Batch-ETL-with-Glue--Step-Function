import sys
import boto3
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Get arguments (AWS Glue passes parameters)
args = getResolvedOptions(sys.argv, ["JOB_NAME", "RDS_HOST", "RDS_USER", "RDS_PASSWORD", "S3_BUCKET", "RDS_DB"])

# Initialize Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Extract parameters
s3_client = boto3.client("s3")
RDS_HOST = args["RDS_HOST"]
RDS_USER = args["RDS_USER"]
RDS_PASSWORD = args["RDS_PASSWORD"]
RDS_DB = args["RDS_DB"]
S3_BUCKET = args["S3_BUCKET"]

# RDS connection properties
# jdbc_url = f"jdbc:{RDS_HOST}:3306/{RDS_DB}?useSSL=false"
jdbc_url = f"jdbc:mysql://{RDS_HOST}:3306/{RDS_DB}?useSSL=false&allowPublicKeyRetrieval=true"

tables = ["apartment_attributes", "apartments", "bookings", "user_viewing"]
for table in tables:
    try:
        logger.info(f"Processing table: {table}...")
        df = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table) \
            .option("user", RDS_USER) \
            .option("password", RDS_PASSWORD) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()

        if df.isEmpty():
            logger.info(f"Skipping {table} - No data found.")
            continue
        
        logger.info(f"Successfully fetched {df.count()} rows from {table}")
        
        # Save as CSV in S3
        s3_path = f's3://{S3_BUCKET}/raw-data/{table}/'
        df.write.mode("overwrite").csv(s3_path, header=True)
        
        logger.info(f"{table} uploaded to S3 successfully.")
    
    except Exception as e:
        logger.error(f"Error processing {table}: {str(e)}")

logger.info("Glue Job Completed Successfully! Data written to S3.")

# Commit Job
job.commit()
