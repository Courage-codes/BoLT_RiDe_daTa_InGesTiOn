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
        response = kinesis.put_record(
            StreamName=Config.KINESIS_STREAM,
            Data=json.dumps(event),
            PartitionKey=partition_key
        )
        logger.info(f"Sent event for trip_id {partition_key} to Kinesis. SequenceNumber: {response['SequenceNumber']}")
    except Exception as e:
        logger.error(f"Failed to send event for trip_id {partition_key}: {e}", exc_info=True)

def read_csv_events(filepath, event_type):
    events = []
    try:
        with open(filepath, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                row["event_type"] = event_type
                row["ingest_timestamp"] = datetime.now(timezone.utc).isoformat()
                events.append(row)
    except FileNotFoundError:
        logger.warning(f"File not found: {filepath}")
    return events

def main():
    trip_begin_events = read_csv_events("data/trip_start.csv", "trip_begin")
    trip_end_events = read_csv_events("data/trip_end.csv", "trip_end")
    all_events = trip_begin_events + trip_end_events
    random.shuffle(all_events)
    logger.info(f"Streaming {len(all_events)} events to Kinesis stream '{Config.KINESIS_STREAM}'...")
    for event in all_events:
        trip_id = event.get("trip_id", "unknown")
        send_event_to_kinesis(event, partition_key=trip_id)
        time.sleep(random.uniform(0.05, 0.2))
    logger.info("All events streamed.")

if __name__ == "__main__":
    main()
