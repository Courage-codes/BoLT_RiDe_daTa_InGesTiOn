import boto3
import base64
import json
from datetime import datetime, timedelta
from decimal import Decimal
from src.config import Config
from src.utils import get_logger, retry
import os
from typing import Dict, Any

# Environment variables
TRIPS_TABLE_NAME = os.getenv("TRIPS_TABLE_NAME")
KINESIS_STREAM_NAME = os.getenv("KINESIS_STREAM_NAME")
KPI_BUCKET_NAME = os.getenv("KPI_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

logger = get_logger("lambda_kinesis_to_dynamo", Config.LOG_LEVEL)

class TripIngestionProcessor:
    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb', region_name=Config.REGION)
        self.table = self.dynamodb.Table(Config.DYNAMODB_TABLE)
        
    def process_trip_event(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Process and validate trip event data for ingestion"""
        trip_id = payload.get('trip_id')
        event_type = payload.get('event_type')
        
        if not trip_id or not event_type:
            raise ValueError(f"Missing required fields: trip_id={trip_id}, event_type={event_type}")
        
        # Apply data transformations
        processed_payload = self._apply_transformations(payload)
        
        # Create raw event record
        timestamp = datetime.now().isoformat()
        raw_item = {
            'trip_id': trip_id,
            'sk': f'RAW#{trip_id}#{event_type}#{timestamp}',
            'event_type': event_type,
            'ingestion_timestamp': timestamp,
            'processing_date': datetime.now().strftime('%Y-%m-%d'),
            'ttl': int((datetime.now() + timedelta(days=7)).timestamp()),  # 7-day TTL for raw events
            **processed_payload
        }
        
        return raw_item
    
    def _apply_transformations(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Apply data quality transformations"""
        processed = payload.copy()
        
        # Convert float values to Decimal for DynamoDB compatibility
        for key, value in processed.items():
            if isinstance(value, float):
                processed[key] = Decimal(str(value))
        
        # Standardize datetime formats
        datetime_fields = ['pickup_datetime', 'dropoff_datetime', 'estimated_dropoff_datetime']
        for field in datetime_fields:
            if field in processed and processed[field]:
                processed[field] = self._standardize_datetime(processed[field])
        
        # Data validation and cleaning
        processed = self._validate_and_clean_data(processed)
        
        return processed
    
    def _standardize_datetime(self, dt_str: str) -> str:
        """Standardize datetime format to ISO format"""
        if not dt_str:
            return dt_str
            
        try:
            # Handle various datetime formats
            if isinstance(dt_str, str):
                # Remove timezone suffix and convert to ISO
                dt_str = dt_str.replace('Z', '+00:00')
                dt = datetime.fromisoformat(dt_str)
                return dt.isoformat()
            return dt_str
        except Exception as e:
            logger.warning(f"Could not standardize datetime {dt_str}: {e}")
            return dt_str
    
    def _validate_and_clean_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and clean data based on event type"""
        event_type = data.get('event_type')
        
        if event_type == 'trip_begin':
            return self._validate_trip_start(data)
        elif event_type == 'trip_end':
            return self._validate_trip_end(data)
        else:
            return data
    
    def _validate_trip_start(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate trip start event data"""
        # Required fields for trip start
        required_fields = ['trip_id', 'pickup_location_id', 'vendor_id', 'pickup_datetime']
        
        for field in required_fields:
            if field not in data or data[field] is None:
                logger.warning(f"Missing required field {field} in trip_start event")
        
        # Validate pickup location ID
        if 'pickup_location_id' in data:
            try:
                pickup_id = int(data['pickup_location_id'])
                if pickup_id < 1 or pickup_id > 300:  # Assuming valid range
                    logger.warning(f"Invalid pickup_location_id: {pickup_id}")
            except (ValueError, TypeError):
                logger.warning(f"Invalid pickup_location_id format: {data['pickup_location_id']}")
        
        # Validate vendor ID
        if 'vendor_id' in data:
            try:
                vendor_id = int(data['vendor_id'])
                if vendor_id not in [1, 2]:  # Assuming valid vendors
                    logger.warning(f"Invalid vendor_id: {vendor_id}")
            except (ValueError, TypeError):
                logger.warning(f"Invalid vendor_id format: {data['vendor_id']}")
        
        return data
    
    def _validate_trip_end(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate trip end event data"""
        # Required fields for trip end
        required_fields = ['trip_id', 'dropoff_datetime', 'fare_amount']
        
        for field in required_fields:
            if field not in data or data[field] is None:
                logger.warning(f"Missing required field {field} in trip_end event")
        
        # Validate fare amount
        if 'fare_amount' in data:
            try:
                fare = float(data['fare_amount'])
                if fare < 0:
                    logger.warning(f"Negative fare amount: {fare}")
                elif fare > 1000:  # Reasonable upper limit
                    logger.warning(f"Unusually high fare amount: {fare}")
            except (ValueError, TypeError):
                logger.warning(f"Invalid fare_amount format: {data['fare_amount']}")
        
        # Validate tip amount
        if 'tip_amount' in data:
            try:
                tip = float(data['tip_amount'])
                if tip < 0:
                    logger.warning(f"Negative tip amount: {tip}")
            except (ValueError, TypeError):
                logger.warning(f"Invalid tip_amount format: {data['tip_amount']}")
        
        # Validate passenger count
        if 'passenger_count' in data:
            try:
                passengers = int(data['passenger_count'])
                if passengers < 0 or passengers > 8:  # Reasonable range
                    logger.warning(f"Invalid passenger_count: {passengers}")
            except (ValueError, TypeError):
                logger.warning(f"Invalid passenger_count format: {data['passenger_count']}")
        
        return data
    
    def write_raw_event(self, raw_item: Dict[str, Any]) -> None:
        """Write raw event to DynamoDB"""
        try:
            retry(lambda: self.table.put_item(Item=raw_item))
            logger.info(f"Successfully wrote raw event: {raw_item['sk']}")
        except Exception as e:
            logger.error(f"Failed to write raw event {raw_item['sk']}: {e}")
            raise

def lambda_handler(event, context):
    """Streamlined Lambda handler focused on data ingestion"""
    processor = TripIngestionProcessor()
    
    processed_count = 0
    error_count = 0
    events_by_type = {}
    
    # Process records in batches for better performance
    batch_size = 25  # DynamoDB batch limit
    batch_items = []
    
    for record in event['Records']:
        try:
            # Decode Kinesis record
            payload = json.loads(base64.b64decode(record['kinesis']['data']))
            
            # Process the event
            raw_item = processor.process_trip_event(payload)
            
            # Track event types for monitoring
            event_type = raw_item.get('event_type', 'unknown')
            events_by_type[event_type] = events_by_type.get(event_type, 0) + 1
            
            # Add to batch
            batch_items.append(raw_item)
            
            # Process batch when it reaches limit
            if len(batch_items) >= batch_size:
                _process_batch(processor, batch_items)
                processed_count += len(batch_items)
                batch_items = []
            
        except Exception as e:
            error_count += 1
            logger.error(f"Failed to process record: {e}", exc_info=True)
            
            # Could implement dead letter queue here
            # _send_to_dlq(record, str(e))
    
    # Process remaining items in batch
    if batch_items:
        _process_batch(processor, batch_items)
        processed_count += len(batch_items)
    
    # Log processing summary
    logger.info(f"Processing complete - Processed: {processed_count}, Errors: {error_count}")
    logger.info(f"Events by type: {events_by_type}")
    
    return {
        "statusCode": 200,
        "body": json.dumps({
            "processed": processed_count,
            "errors": error_count,
            "events_by_type": events_by_type
        })
    }

def _process_batch(processor: TripIngestionProcessor, batch_items: list) -> None:
    """Process a batch of items using DynamoDB batch writer"""
    try:
        with processor.table.batch_writer() as batch:
            for item in batch_items:
                batch.put_item(Item=item)
        
        logger.info(f"Successfully processed batch of {len(batch_items)} items")
        
    except Exception as e:
        logger.error(f"Batch processing failed, falling back to individual writes: {e}")
        
        # Fallback to individual writes
        for item in batch_items:
            try:
                processor.write_raw_event(item)
            except Exception as individual_error:
                logger.error(f"Failed to write individual item: {individual_error}")

def _send_to_dlq(record: Dict[str, Any], error_message: str) -> None:
    """Send failed record to dead letter queue (placeholder)"""
    # Implementation depends on your DLQ setup (SQS, SNS, etc.)
    logger.info(f"Would send to DLQ: {error_message}")
    pass