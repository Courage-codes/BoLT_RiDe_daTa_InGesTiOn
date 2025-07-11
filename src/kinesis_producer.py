import boto3
import json
import time
import random
import csv
import logging
import os
from datetime import datetime, timezone
from src.config import Config
from src.utils import get_logger

logger = get_logger("kinesis_producer", Config.LOG_LEVEL)
kinesis = boto3.client("kinesis", region_name=Config.REGION)

def send_event_to_kinesis(event, partition_key):
    try:
        # Log the event being sent for debugging
        logger.info(f"Attempting to send event: {json.dumps(event, indent=2)}")
        
        response = kinesis.put_record(
            StreamName=Config.KINESIS_STREAM,
            Data=json.dumps(event),
            PartitionKey=partition_key
        )
        logger.info(f"Successfully sent event for trip_id {partition_key} to Kinesis. SequenceNumber: {response['SequenceNumber']}")
        return True
    except Exception as e:
        logger.error(f"Failed to send event for trip_id {partition_key}: {e}", exc_info=True)
        return False

def read_csv_events(filepath, event_type):
    events = []
    try:
        logger.info(f"Reading CSV file: {filepath}")
        with open(filepath, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                row["event_type"] = event_type
                row["ingest_timestamp"] = datetime.now(timezone.utc).isoformat()
                events.append(row)
        logger.info(f"Read {len(events)} events from {filepath}")
    except FileNotFoundError:
        logger.warning(f"File not found: {filepath}")
    except Exception as e:
        logger.error(f"Error reading CSV file {filepath}: {e}", exc_info=True)
    return events

def validate_aws_credentials():
    """Validate AWS credentials and permissions"""
    try:
        # Test AWS credentials
        sts = boto3.client('sts')
        identity = sts.get_caller_identity()
        logger.info(f"AWS Identity: {identity}")
        
        # Test Kinesis permissions
        kinesis.describe_stream(StreamName=Config.KINESIS_STREAM)
        logger.info(f"Successfully connected to Kinesis stream: {Config.KINESIS_STREAM}")
        return True
    except Exception as e:
        logger.error(f"AWS credentials or permissions issue: {e}", exc_info=True)
        return False

def main():
    # Log configuration
    logger.info(f"Configuration:")
    logger.info(f"  KINESIS_STREAM: {Config.KINESIS_STREAM}")
    logger.info(f"  REGION: {Config.REGION}")
    logger.info(f"  LOG_LEVEL: {Config.LOG_LEVEL}")
    
    # Validate AWS setup
    if not validate_aws_credentials():
        logger.error("AWS validation failed. Exiting.")
        return
    
    # Check if CSV files exist
    csv_files = ["data/trip_start.csv", "data/trip_end.csv"]
    for file in csv_files:
        if not os.path.exists(file):
            logger.error(f"CSV file not found: {file}")
            return
        else:
            logger.info(f"Found CSV file: {file}")
    
    # Read events
    trip_begin_events = read_csv_events("data/trip_start.csv", "trip_begin")
    trip_end_events = read_csv_events("data/trip_end.csv", "trip_end")
    
    if not trip_begin_events and not trip_end_events:
        logger.error("No events found in CSV files. Exiting.")
        return
    
    all_events = trip_begin_events + trip_end_events
    random.shuffle(all_events)
    
    logger.info(f"Streaming {len(all_events)} events to Kinesis stream '{Config.KINESIS_STREAM}'...")
    
    success_count = 0
    error_count = 0
    
    for i, event in enumerate(all_events):
        trip_id = event.get("trip_id", "unknown")
        
        # Log progress
        if i % 10 == 0:
            logger.info(f"Processing event {i+1}/{len(all_events)}")
        
        if send_event_to_kinesis(event, partition_key=trip_id):
            success_count += 1
        else:
            error_count += 1
        
        # Add sleep between requests
        time.sleep(random.uniform(0.05, 0.2))
    
    logger.info(f"Streaming complete. Success: {success_count}, Errors: {error_count}")

if __name__ == "__main__":
    main()
