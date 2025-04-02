
# Batch Data Processing for Rental Marketplace Analytics

## Overview

This ETL pipeline is designed to process batch data for a rental marketplace platform using AWS Glue and AWS Step Functions. The pipeline extracts data from AWS MySQL, validates and transforms it using AWS Glue (PySpark), and loads the processed data into Amazon Redshift for analytical reporting.

## Architecture Overview

### Components Used
- **AWS Aurora MySQL**: Stores rental marketplace application data.
- **AWS S3**: Serves as an intermediate storage layer for raw and processed data.
- **AWS Glue**: Handles ETL transformations using PySpark.
- **Amazon Redshift**: Stores transformed data for analytical queries.
- **AWS Step Functions**: Orchestrates the workflow efficiently.
- **AWS IAM Roles**: Ensures secure access control for Glue and Redshift.

### Pipeline Flow
1. **Data Extraction**: AWS Glue extracts data from MySQL and writes it to S3 in CSV format.
2. **Validation & Cleaning**: Ensures data integrity by handling missing values and duplicates.
3. **AWS Glue Processing**: Cleans and transforms raw data, computes business KPIs, and loads results into Amazon Redshift.
4. **Multi-Layer Redshift Architecture**: Data is organized into Raw, Curated, and Presentation layers for efficient analytical queries.

### **Architecture Diagram**  
![ETL Architecture](images/architecture_diagram.png)

## AWS Glue Job Implementation

### Key Transformations in PySpark
- Reads apartment listing, user interaction, and booking data from S3.
- Validates and cleans raw data.
- Applies transformations such as handling missing values, removing duplicates, and normalizing formats.
- Computes rental performance and user engagement KPIs using PySpark.
- Loads processed data into Amazon Redshift.

## Amazon Redshift Multi-Layer Architecture

Redshift is structured into three layers to optimize data processing and reporting:
1. **Raw Layer**: Stores unprocessed data loaded directly from S3.
2. **Curated Layer**: Contains cleaned and transformed data, removing duplicates and handling inconsistencies.
3. **Presentation Layer**: Stores aggregated and precomputed metrics for business intelligence and reporting.


## **Resources to Be Created**

### **1. RDS Tables (MySQL - Source Data)**
The following tables exist in the **RDS MySQL** database and will be extracted:
- `apartment_attributes`
- `apartments`
- `bookings`
- `user_viewing`

### **2. Amazon S3 Buckets and Folder Structure**
The data pipeline uses **Amazon S3** to temporarily store extracted data before loading it into Redshift.

- **Bucket Name:** `s3://rental-marketplace/`
- **Folder Structure:**
  - `s3://rental-marketplace/raw-data/` → Stores extracted CSV data from RDS.
  - `s3://rental-marketplace/temp/` → Temporary storage for Redshift operations.

### **3. Amazon Redshift Tables**
The pipeline creates and populates tables in the **Amazon Redshift** data warehouse within different schemas:

#### **Raw Layer (`raw_data` Schema)**
- `apartment_attributes`
- `apartments`
- `bookings`
- `user_viewing`

#### **Curated Layer (`curated` Schema)**
- Cleaned and transformed versions of the raw tables.

#### **Presentation Layer (`presentation` Schema)**
The final aggregated business insights:
- `average_listing_price` → Average rental listing price per week.
- `occupancy_rate` → Monthly occupancy rate based on bookings.
- `popular_cities` → Most frequently booked cities per week.
- `top_listings` → Top-performing listings ranked by revenue.
- `bookings_per_user` → Total number of bookings per user.
- `avg_booking_duration` → Average length of stay for confirmed bookings.
- `repeat_customers` → Number of repeat customers who booked within 30 days.

### **4. AWS Glue Jobs**
The ETL pipeline is orchestrated using **AWS Glue** with the following jobs:

1. **ExtractRDSDataToS3** → Extracts MySQL data from RDS and saves it as CSV files in S3.
2. **LoadRawDataToRedshift** → Loads raw CSV files from S3 into the `raw_data` schema in Redshift.
3. **TransformAndLoadCuratedData** → Cleans and processes data before loading it into the `curated` schema.
4. **ComputeMetricsAndLoadPresentation** → Computes business metrics and loads results into the `presentation` schema.


## Orchestration using AWS Step Functions

- Step Functions define the ETL workflow for automation.
- AWS Glue jobs are executed in sequence, ensuring correct processing.
- Transitions between Redshift layers (Raw -> Curated -> Presentation) are automated via Step Functions.
- Error handling & retries are enabled for fault tolerance.

### Step Function Workflow
1. Extract Data from Aurora MySQL
2. Load Raw Data into Redshift
3. Transform Data in the Curated Layer
4. Generate Metrics in the Presentation Layer
5. Archive Processed Data

## Logging & Error Handling

- Detailed logging in AWS Glue and Step Functions for debugging.
- Data validation ensures integrity before processing.
- Retries enabled for AWS Glue jobs to handle failures.

## Conclusion

This ETL pipeline automates batch data ingestion and validation with AWS Glue, transforms rental marketplace data using PySpark, organizes data into Raw, Curated, and Presentation layers in Amazon Redshift, computes key KPIs, and enables structured analytical reporting. The workflow is orchestrated using AWS Step Functions, ensuring reliable and efficient execution.