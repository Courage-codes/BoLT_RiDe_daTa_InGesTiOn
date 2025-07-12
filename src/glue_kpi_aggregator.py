import sys
import boto3
import logging
import os
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg, max as spark_max, min as spark_min, coalesce, lit, when, isnan, isnull
from pyspark.sql.types import DoubleType
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
import json
from datetime import datetime, timezone
from config import Config
from utils import get_logger, retry

logger = get_logger("glue_kpi_aggregator", Config.LOG_LEVEL)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Optimize Spark configuration for DynamoDB operations
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

def fetch_completed_trips_spark():
    """
    Fetch completed trips using Spark with DynamoDB connector for distributed processing
    """
    logger.info("Reading completed trips from DynamoDB using Spark...")
    
    try:
        # Create dynamic frame from DynamoDB
        trips_dynamic_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="dynamodb",
            connection_options={
                "dynamodb.input.tableName": Config.DYNAMODB_TABLE,
                "dynamodb.throughput.read.percent": str(Config.DYNAMODB_READ_THROUGHPUT_PERCENT),
                "dynamodb.splits": str(Config.DYNAMODB_SPLITS),
                "dynamodb.sts.roleArn": "",  # Use default role
                "dynamodb.region": Config.REGION
            },
            transformation_ctx="read_dynamodb"
        )
        
        # Convert to DataFrame for SQL operations
        trips_df = trips_dynamic_frame.toDF()
        
        # Filter for completed trips
        completed_trips_df = trips_df.filter(col("sk").startswith("COMPLETED#"))
        
        # Cache the DataFrame since we'll use it multiple times
        completed_trips_df.cache()
        
        trip_count = completed_trips_df.count()
        logger.info(f"Fetched {trip_count} completed trips using Spark")
        
        return completed_trips_df
        
    except Exception as e:
        logger.error(f"Error reading from DynamoDB: {str(e)}")
        raise

def aggregate_kpis_spark(trips_df):
    """
    Aggregate KPIs using Spark SQL for distributed processing
    """
    logger.info("Aggregating KPIs using Spark...")
    
    try:
        from datetime import timezone
        
        # Filter for trips with fare_amount only
        trips_with_fare = trips_df.filter(
            col("fare_amount").isNotNull() & 
            ~isnan(col("fare_amount").cast(DoubleType()))
        ).withColumn("fare_amount", col("fare_amount").cast(DoubleType()))
        
        # Perform aggregations
        kpis = trips_with_fare.agg(
            count("*").alias("count_trips"),
            spark_sum("fare_amount").alias("total_fare"),
            avg("fare_amount").alias("average_fare"),
            spark_max("fare_amount").alias("max_fare"),
            spark_min("fare_amount").alias("min_fare")
        ).collect()[0]
        
        # Get current date
        now = datetime.now(timezone.utc)
        date_str = now.strftime('%Y-%m-%d')
        
        # Convert to dictionary with rounded values
        result = {
            "date": date_str,
            "total_fare": round(float(kpis["total_fare"]) if kpis["total_fare"] else 0.0, 2),
            "count_trips": kpis["count_trips"],
            "average_fare": round(float(kpis["average_fare"]) if kpis["average_fare"] else 0.0, 2),
            "max_fare": round(float(kpis["max_fare"]) if kpis["max_fare"] else 0.0, 2),
            "min_fare": round(float(kpis["min_fare"]) if kpis["min_fare"] else 0.0, 2),
            "generated_at": now.isoformat()
        }
        
        logger.info(f"KPI Summary: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Error aggregating KPIs: {str(e)}")
        raise

def write_kpis_to_json(kpi_result):
    """
    Write KPIs as JSON to S3 with date-based partitioning
    """
    try:
        import json
        from datetime import timezone
        
        now = datetime.now(timezone.utc)
        date_str = kpi_result["date"]
        
        s3 = boto3.client("s3", region_name=Config.REGION)
        
        # Create output key with date partitioning
        output_key = (
            f"daily_metrics/{date_str.replace('-', '/')}/"
            f"metrics_{date_str.replace('-', '')}_{now.strftime('%H%M%S')}.json"
        )
        
        body = json.dumps(kpi_result, indent=2)
        
        logger.info(f"Writing KPIs to s3://{Config.KPI_BUCKET}/{output_key}")
        s3.put_object(Bucket=Config.KPI_BUCKET, Key=output_key, Body=body.encode("utf-8"))
        
        logger.info("Successfully wrote KPIs in JSON format")
        
    except Exception as e:
        logger.error(f"Error writing JSON to S3: {str(e)}")
        raise

def create_s3_folder_structure():
    """
    Ensure S3 folder structure exists for JSON output
    """
    try:
        s3 = boto3.client("s3", region_name=Config.REGION)
        
        # Check if bucket exists
        try:
            s3.head_bucket(Bucket=Config.KPI_BUCKET)
            logger.info(f"S3 bucket {Config.KPI_BUCKET} is accessible")
        except Exception as e:
            logger.warning(f"S3 bucket check failed: {str(e)}")
        
    except Exception as e:
        logger.warning(f"Could not verify S3 setup: {str(e)}")

def main():
    """
    Main execution function with error handling and cleanup
    """
    trips_df = None
    try:
        logger.info("Starting KPI aggregation job (JSON output only)")
        
        # Verify S3 setup
        create_s3_folder_structure()
        
        # Fetch data using Spark
        trips_df = fetch_completed_trips_spark()
        
        # Aggregate KPIs
        kpi_result = aggregate_kpis_spark(trips_df)
        
        # Write results to S3 as JSON
        write_kpis_to_json(kpi_result)
        
        logger.info("KPI aggregation job completed successfully")
        
    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        raise
    finally:
        # Cleanup cached DataFrames
        if trips_df:
            trips_df.unpersist()
        
        # Stop Spark context
        sc.stop()

if __name__ == "__main__":
    main()
