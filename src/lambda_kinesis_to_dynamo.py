import boto3
import base64
import json
from datetime import datetime, timedelta
from decimal import Decimal
from src.config import Config
from src.utils import get_logger, retry, send_to_dlq
import os
from typing import Dict, Any

logger = get_logger("lambda_kinesis_to_dynamo", Config.LOG_LEVEL)

def remove_nulls(d: Dict[str, Any]) -> Dict[str, Any]:
    """Return a new dict omitting any keys with value None."""
    return {k: v for k, v in d.items() if v is not None}

class TripIngestionProcessor:
    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb', region_name=Config.REGION)
        self.table = self.dynamodb.Table(Config.DYNAMODB_TABLE)

    def process_trip_event(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        # Modular cleaning
        processed = self._clean_data_types(payload)
        processed = self._clean_datetime_fields(processed)
        processed = self._clean_numeric_fields(processed)
        processed = self._clean_string_fields(processed)
        processed = self._remove_invalid_values(processed)

        # Event-type-specific validation
        event_type = processed.get('event_type')
        if event_type == 'trip_begin':
            processed = self._validate_trip_begin(processed)
        elif event_type == 'trip_end':
            processed = self._validate_trip_end(processed)

        # Compose DynamoDB item
        now = datetime.now()
        timestamp = now.isoformat()
        raw_item = {
            'trip_id': processed.get('trip_id'),
            'sk': f"RAW#{processed.get('trip_id')}#{event_type}#{timestamp}",
            'event_type': event_type,
            'ingestion_timestamp': timestamp,
            'processing_date': now.strftime('%Y-%m-%d'),
            'ttl': int((now + timedelta(days=7)).timestamp()),
            **processed
        }
        return remove_nulls(raw_item)

    def _clean_data_types(self, data: Dict[str, Any]) -> Dict[str, Any]:
        cleaned = data.copy()
        for key, value in cleaned.items():
            if isinstance(value, float):
                cleaned[key] = Decimal(str(value))
        return cleaned

    def _clean_datetime_fields(self, data: Dict[str, Any]) -> Dict[str, Any]:
        cleaned = data.copy()
        datetime_fields = ['pickup_datetime', 'dropoff_datetime', 'estimated_dropoff_datetime']
        for field in datetime_fields:
            if field in cleaned and cleaned[field]:
                cleaned[field] = self._standardize_datetime(cleaned[field])
        return cleaned

    def _clean_numeric_fields(self, data: Dict[str, Any]) -> Dict[str, Any]:
        cleaned = data.copy()
        numeric_fields = {
            'fare_amount': {'min': 0, 'max': 1000},
            'tip_amount': {'min': 0, 'max': 500},
            'trip_distance': {'min': 0, 'max': 500},
            'passenger_count': {'min': 0, 'max': 8},
            'rate_code': {'min': 1, 'max': 6},
            'payment_type': {'min': 1, 'max': 6},
            'trip_type': {'min': 1, 'max': 2},
            'vendor_id': {'min': 1, 'max': 2},
            'pickup_location_id': {'min': 1, 'max': 300},
            'dropoff_location_id': {'min': 1, 'max': 300}
        }
        for field, constraints in numeric_fields.items():
            if field in cleaned and cleaned[field] is not None:
                try:
                    if field in ['passenger_count', 'rate_code', 'payment_type', 'trip_type', 'vendor_id', 'pickup_location_id', 'dropoff_location_id']:
                        value = int(float(cleaned[field]))
                    else:
                        value = float(cleaned[field])
                    if value < constraints['min'] or value > constraints['max']:
                        logger.warning(f"Value {value} out of bounds for {field}")
                        cleaned[field] = None
                    else:
                        cleaned[field] = Decimal(str(value))
                except (ValueError, TypeError):
                    logger.warning(f"Invalid numeric value for {field}: {cleaned[field]}")
                    cleaned[field] = None
        return cleaned

    def _clean_string_fields(self, data: Dict[str, Any]) -> Dict[str, Any]:
        cleaned = data.copy()
        string_fields = ['trip_id']
        for field in string_fields:
            if field in cleaned and cleaned[field]:
                cleaned[field] = str(cleaned[field]).strip()
                if not cleaned[field]:
                    cleaned[field] = None
        return cleaned

    def _remove_invalid_values(self, data: Dict[str, Any]) -> Dict[str, Any]:
        cleaned = data.copy()
        invalid_values = ['', 'null', 'NULL', 'N/A', 'n/a', 'None', 'none']
        for key, value in cleaned.items():
            if value in invalid_values or (isinstance(value, str) and value.strip() in invalid_values):
                cleaned[key] = None
        return cleaned

    def _standardize_datetime(self, dt_str: str) -> str:
        if not dt_str:
            return dt_str
        try:
            if isinstance(dt_str, str):
                dt_str = dt_str.replace('Z', '+00:00')
                dt = datetime.fromisoformat(dt_str)
                return dt.isoformat()
            return dt_str
        except Exception as e:
            logger.warning(f"Could not standardize datetime {dt_str}: {e}")
            return dt_str

    def _validate_trip_begin(self, data: Dict[str, Any]) -> Dict[str, Any]:
        required_fields = ['trip_id', 'pickup_location_id', 'vendor_id', 'pickup_datetime']
        for field in required_fields:
            if field not in data or data[field] is None:
                logger.warning(f"Missing required field {field} in trip_begin event")
        return data

    def _validate_trip_end(self, data: Dict[str, Any]) -> Dict[str, Any]:
        required_fields = ['trip_id', 'dropoff_datetime', 'fare_amount']
        for field in required_fields:
            if field not in data or data[field] is None:
                logger.warning(f"Missing required field {field} in trip_end event")
        return data

    def write_raw_event(self, raw_item: Dict[str, Any]) -> None:
        try:
            retry(lambda: self.table.put_item(Item=raw_item))
            logger.info(f"Successfully wrote raw event: {raw_item['sk']}")
        except Exception as e:
            logger.error(f"Failed to write raw event {raw_item['sk']}: {e}")
            raise

def lambda_handler(event, context):
    processor = TripIngestionProcessor()
    processed_count = 0
    error_count = 0
    events_by_type = {}
    batch_size = 25
    batch_items = []

    for record in event['Records']:
        try:
            payload = json.loads(base64.b64decode(record['kinesis']['data']))
            raw_item = processor.process_trip_event(payload)
            event_type = raw_item.get('event_type', 'unknown')
            events_by_type[event_type] = events_by_type.get(event_type, 0) + 1
            batch_items.append(raw_item)
            if len(batch_items) >= batch_size:
                _process_batch(processor, batch_items)
                processed_count += len(batch_items)
                batch_items = []
        except Exception as e:
            error_count += 1
            logger.error(f"Failed to process record: {e}", exc_info=True)
            # Send failed record to DLQ
            try:
                send_to_dlq(record, str(e), dlq_url=Config.DLQ_URL, region=Config.REGION)
                logger.info("Sent failed record to DLQ.")
            except Exception as dlq_error:
                logger.error(f"Failed to send to DLQ: {dlq_error}")

    if batch_items:
        _process_batch(processor, batch_items)
        processed_count += len(batch_items)

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
    try:
        with processor.table.batch_writer() as batch:
            for item in batch_items:
                batch.put_item(Item=remove_nulls(item))
        logger.info(f"Successfully processed batch of {len(batch_items)} items")
    except Exception as e:
        logger.error(f"Batch processing failed, falling back to individual writes: {e}")
        for item in batch_items:
            try:
                processor.write_raw_event(remove_nulls(item))
            except Exception as individual_error:
                logger.error(f"Failed to write individual item: {individual_error}")
