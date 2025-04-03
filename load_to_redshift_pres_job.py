import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, avg, count, sum, round, abs, weekofyear, month, datediff, lead, to_date, countDistinct
from pyspark.sql.window import Window

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Get arguments from AWS Glue
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

# JDBC Connection URL
# jdbc_url = f"jdbc:redshift://redshift-cluster-1.c8qaashqkmcg.eu-west-1.redshift.amazonaws.com:5439/dev"
jdbc_url = f"jdbc:redshift://{redshift_url}:5439/{redshift_db}"

# Load Redshift tables into DataFrames
logger.info("Extracting data from Redshift...")
def load_redshift_table(table_name):
    """
    Loads a table from Redshift into a Spark DataFrame.

    Parameters:
        table_name (str): Name of the table to load.

    Returns:
        pyspark.sql.DataFrame: A DataFrame containing the data from the specified table.
    """
    
    logger.info(f"Loading {table_name} from Redshift...")

    # Read data from Redshift
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", f"curated.{table_name}")
        .option("user", redshift_user)
        .option("password", redshift_password)
        .option("driver", "com.amazon.redshift.jdbc.Driver")
        .load()
    )

# Load data into DataFrames
apartment_attributes_df = load_redshift_table("apartment_attributes")
apartments_df = load_redshift_table("apartments")
bookings_df = load_redshift_table("bookings")
user_viewing_df = load_redshift_table("user_viewing")

# Rental Performance Metrics
logger.info("Calculating rental performance metrics...")

# Average Listing Price per Week
avg_price_weekly = (
    apartments_df.filter(col("is_active") == True)
    .withColumn("listing_created_on", to_date(col("listing_created_on")))
    .groupBy(weekofyear(col("listing_created_on")).cast("int").alias("week"))
    .agg(
        round(avg(col("price")), 2)
        .cast("decimal(10,2)") # Cast to decimal
        .alias("average_listing_price")
    )
)

# Occupancy Rate per Month
occupancy_rate_monthly = (
    bookings_df.groupBy(month(col("checkin_date")).alias("month"))
    .agg((count("booking_id") / 30 * 100).alias("occupancy_rate"))
    .orderBy(col("month"))
)

# Most frequently booked cities per week
popular_cities = (
    bookings_df.join(apartment_attributes_df, bookings_df.apartment_id == apartment_attributes_df.id)
    .groupBy("cityname", weekofyear(col("booking_date")).alias("week"))
    # .agg(count(col("booking_id").cast("int")).alias("booking_count"))
    .agg(
        count(col("booking_id"))
        .cast("int")  # Ensure Redshift-compatible INT type
        .alias("booking_count")
    )
    .orderBy(col("booking_count").desc())
)

# Top Performing Listings by Revenue per Week
top_listings = (
    bookings_df.filter(col("booking_status") == "confirmed")
    .groupBy("apartment_id", weekofyear(col("booking_date")).alias("week"))
    # .agg(sum(col("total_price").cast("decimal(10,2)")).alias("total_revenue"))
    .agg(
        sum(col("total_price"))
        .cast("decimal(10,2)")  # Ensure correct decimal type
        .alias("total_revenue")
    )
    .orderBy(col("total_revenue").desc())
)

# User Engagement Metrics
logger.info("Calculating user engagement metrics...")

# Total Bookings per User
bookings_per_user = (
    bookings_df.groupBy("user_id", weekofyear(col("booking_date")).alias("week"))
    # .agg(count("booking_id").alias("total_bookings"))
    .agg(
        count(col("booking_id"))
        .cast("int")  # Ensure Redshift-compatible INT type
        .alias("total_bookings")
    )
    .orderBy(col("user_id"))
)

# Average Booking Duration per Week
avg_booking_duration = (
    bookings_df.filter(col("booking_status") == "confirmed")
    .withColumn("booking_duration", abs(datediff(col("checkout_date"), col("checkin_date"))))
    .withColumn("week", weekofyear(col("booking_date")))
    .groupBy("week")
    # .agg(avg("booking_duration").alias("avg_booking_duration"))
    .agg(
        round(avg("booking_duration"), 2)  # Round to 2 decimal places
        .cast("decimal(10,2)")  # Ensure correct type for Redshift
        .alias("avg_booking_duration")
    )
    .orderBy("week")
)

# Repeat Customer Rate per Week

# Calculate the next booking date for each user
window_spec = Window.partitionBy("user_id").orderBy("booking_date")

# Calculate repeat customers for each week
repeat_customers = (
    bookings_df.withColumn("next_booking_date", lead("booking_date").over(window_spec))
    .withColumn("days_between_bookings", datediff(col("next_booking_date"), col("booking_date")))
    .filter(col("days_between_bookings") <= 30)
    .groupBy(weekofyear(col("booking_date")).alias("week"))
    # .agg(countDistinct("user_id").alias("repeat_customers"))
    .agg(
        countDistinct("user_id")
        .cast("int")  # Ensure Redshift-compatible INT type
        .alias("repeat_customers")
    )
    .orderBy("week")
)

# Load Metrics into Redshift Presentation Layer
def write_to_redshift(df, table_name):
    """
    Write a Spark DataFrame to a Redshift table in the Presentation Layer.

    Args:
        df (Spark DataFrame): The DataFrame to write to Redshift.
        table_name (str): The name of the table to write to.

    Returns:
        None

    """
    
    logger.info(f"Loading {table_name} into Redshift Presentation Layer...")

    # Convert Spark DataFrame to Glue DynamicFrame
    dynamic_df = DynamicFrame.fromDF(df, glueContext, table_name)

    # Load into Redshift Presentation Layer
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_df,
        connection_type="redshift",
        connection_options={
            "url": jdbc_url,
            "dbtable": f"presentation.{table_name}",
            "user": redshift_user,
            "password": redshift_password,
            "redshiftTmpDir": f"s3://{s3_bucket}/temp/",
            "aws_iam_role": iam_role
        }
    )
    logger.info(f"Loaded {table_name} into Redshift Presentation Layer.")

# Writing results
write_to_redshift(avg_price_weekly, "average_listing_price")
write_to_redshift(occupancy_rate_monthly, "occupancy_rate")
write_to_redshift(popular_cities, "popular_cities")
write_to_redshift(top_listings, "top_listings")
write_to_redshift(bookings_per_user, "bookings_per_user")
write_to_redshift(avg_booking_duration, "avg_booking_duration")
write_to_redshift(repeat_customers, "repeat_customers")

logger.info("Glue Job Completed Successfully! Metrics loaded into Redshift Presentation Layer.")

# Commit Job
job.commit()

# Close Spark Session
spark.stop()
