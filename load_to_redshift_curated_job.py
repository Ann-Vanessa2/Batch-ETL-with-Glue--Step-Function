import sys
import boto3
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, mean
from pyspark.sql.types import IntegerType, DoubleType, StringType

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Getting arguments (AWS Glue passes parameters)
args = getResolvedOptions(sys.argv, ["JOB_NAME", "REDSHIFT_URL", "REDSHIFT_USER", "REDSHIFT_PASSWORD", "S3_BUCKET", "IAM_ROLE", "REDSHIFT_DB"])

# Initialize Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Extract parameters
redshift_url = args["REDSHIFT_URL"]
redshift_user = args["REDSHIFT_USER"]
redshift_password = args["REDSHIFT_PASSWORD"]
s3_bucket = args["S3_BUCKET"]
iam_role = args["IAM_ROLE"]
redshift_db = args["REDSHIFT_DB"]

tables = ["apartment_attributes", "apartments", "bookings", "user_viewing"]

# Defining tables and their expected schema
TABLE_SCHEMA = {
    "apartment_attributes": {
        "id": "int",
        "bathrooms": "int",
        "bedrooms": "int",
        "fee": "decimal(10,2)",
        "square_feet": "int",
        "latitude": "decimal(10,8)",
        "longitude": "decimal(11,8)"
    },
    "apartments": {
        "id": "int",
        "price": "decimal(10,2)",
        "listing_created_on": "date",
        "last_modified_timestamp": "date"
    },
    "bookings": {
        "booking_id": "int",
        "user_id": "int",
        "apartment_id": "int",
        "booking_date": "date",
        "checkin_date": "date",
        "checkout_date": "date",
        "total_price": "decimal(10,2)"
    },
    "user_viewing": {
        "user_id": "int",
        "apartment_id": "int",
        "viewed_at": "date"}
}

# Define default values for missing data (excluding dates)
DEFAULT_VALUES = {
    "int": 0,
    "decimal(10,2)": 0.0,
    "decimal(10,8)": 0.0,
    "decimal(11,8)": 0.0,
    "string": "N/A"
}

def clean_data(df, table_name):
    """
    Perform data cleaning operations on the DataFrame by filling missing values and removing duplicates.
    Args:
        df (pyspark.sql.DataFrame): The DataFrame to clean.
        table_name (str): The name of the table being processed.
    Returns:
        pyspark.sql.DataFrame: The cleaned DataFrame.
    """
    logger.info(f"Starting data cleaning for {table_name}")

    # Dropping duplicates
    df = df.dropDuplicates()

    # Handling missing values
    for column, dtype in schema.items():
        if isinstance(dtype, StringType):
            df = df.fillna({column: "Unknown"})  # Replace null strings with 'Unknown'
        elif isinstance(dtype, IntegerType):
            df = df.fillna({column: 0})  # Replace null integers with 0
        elif isinstance(dtype, DoubleType):
            df = df.fillna(df.select(mean(col(column))).collect()[0][0] or 0.0)  # Replace null doubles with mean
    
    return df

# Establish Connection
# jdbc_url = f"jdbc:redshift://{redshift_url}:5439/{redshift_db}"
jdbc_url = f"jdbc:redshift://redshift-cluster-1.c8qaashqkmcg.eu-west-1.redshift.amazonaws.com:5439/dev"

for table, schema in tables:
    try:
        s3_path = f's3://{s3_bucket}/raw-data/{table}/'
        logger.info(f"Processing {table} from {s3_path} to Redshift...")

        # Reading CSV from S3
        df = spark.read.option("header", "true").csv(s3_path)

        # Performing data cleaning
        df = clean_data(df, table)
        
        # Applying schema casting only for numeric columns
        if table in TABLE_SCHEMA:
            for column, dtype in TABLE_SCHEMA[table].items():
                if column in df.columns:
                    df = df.withColumn(column, col(column).cast(dtype))

        # Converting Spark DataFrame to Glue DynamicFrame
        dynamic_df = DynamicFrame.fromDF(df, glueContext, "dynamic_df")

        # Loading into Redshift Raw Layer
        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_df,
            connection_type="redshift",
            connection_options={
                "url": jdbc_url,
                "dbtable": f"curated.{table}",
                "user": redshift_user,
                "password": redshift_password,
                "redshiftTmpDir": f"s3://{s3_bucket}/temp/",
                "aws_iam_role": iam_role
            },
            
        )
        
        # df = spark.read.format("jdbc").options(**connection_options).load()
        logger.info(f"{table} successfully loaded into Redshift Raw Layer.")

    except Exception as e:
        logger.error(f"Error loading {table} into Redshift: {str(e)}")

logger.info("Glue Job Completed Successfully! Data loaded into Redshift.")

# Commit Job
job.commit()

# Close Spark Session
spark.stop()