import boto3
from decimal import Decimal
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import json
from config import Config
from utils import get_logger, retry

logger = get_logger("lambda_dynamo_matcher", Config.LOG_LEVEL)

class TripMatcher:
    def __init__(self):
        self.dynamodb = boto3.resource("dynamodb", region_name=Config.REGION)
        self.table = self.dynamodb.Table(Config.DYNAMODB_TABLE)
        
    def process_stream_record(self, record: Dict[str, Any]) -> None:
        """Process a single DynamoDB stream record"""
        event_name = record["eventName"]
        
        # Only process INSERT and MODIFY events
        if event_name not in ("INSERT", "MODIFY"):
            logger.debug(f"Skipping event type: {event_name}")
            return
            
        # Get the new image
        new_image = record["dynamodb"]["NewImage"]
        trip_id = self._extract_string_value(new_image, "trip_id")
        sk = self._extract_string_value(new_image, "sk")
        
        if not trip_id or not sk:
            logger.warning(f"Missing trip_id or sk in record: {record}")
            return
            
        # Process different record types
        if sk.startswith("RAW#"):
            self._handle_raw_event(trip_id, sk, new_image)
        elif sk.startswith("STATE#"):
            self._handle_state_event(trip_id, sk, new_image)
        else:
            logger.debug(f"Ignoring non-RAW/STATE record: {sk}")
    
    def _extract_string_value(self, dynamo_item: Dict[str, Any], key: str) -> Optional[str]:
        """Extract string value from DynamoDB item format"""
        if key not in dynamo_item:
            return None
        value = dynamo_item[key]
        if isinstance(value, dict) and "S" in value:
            return value["S"]
        return str(value) if value else None
    
    def _extract_value(self, dynamo_item: Dict[str, Any], key: str) -> Any:
        """Extract any value from DynamoDB item format"""
        if key not in dynamo_item:
            return None
        value = dynamo_item[key]
        if isinstance(value, dict):
            if "S" in value:
                return value["S"]
            elif "N" in value:
                return Decimal(value["N"])
            elif "BOOL" in value:
                return value["BOOL"]
            elif "NULL" in value:
                return None
        return value
    
    def _handle_raw_event(self, trip_id: str, sk: str, new_image: Dict[str, Any]) -> None:
        """Handle RAW event processing"""
        # Parse event type from SK
        sk_parts = sk.split("#")
        if len(sk_parts) < 3:
            logger.warning(f"Invalid RAW SK format: {sk}")
            return
            
        event_type = sk_parts[2]  # RAW#{trip_id}#{event_type}#{timestamp}
        
        # Only process trip_start and trip_end events
        if event_type not in ("trip_start", "trip_end"):
            logger.debug(f"Skipping non-trip event: {event_type}")
            return
            
        logger.info(f"Processing RAW event: {sk}")
        
        # Check if we already have a completed trip
        if self._completed_trip_exists(trip_id):
            logger.info(f"Trip {trip_id} already completed, skipping")
            return
            
        # Try to find counterpart event
        counterpart_type = "trip_end" if event_type == "trip_begin" else "trip_begin"
        counterpart_event = self._find_counterpart_event(trip_id, counterpart_type)
        
        if counterpart_event:
            # We have both events, create completed trip
            self._create_completed_trip(trip_id, event_type, new_image, counterpart_event)
        else:
            logger.info(f"Counterpart event not found for {sk}, will wait for matching event")
    
    def _handle_state_event(self, trip_id: str, sk: str, new_image: Dict[str, Any]) -> None:
        """Handle STATE event processing"""
        status = self._extract_string_value(new_image, "status")
        logger.info(f"Processing STATE event: {sk}, status: {status}")
        
        if status == "ORPHANED_END":
            # Try to find matching start event
            start_event = self._find_counterpart_event(trip_id, "trip_start")
            if start_event:
                logger.info(f"Found matching start event for orphaned end: {trip_id}")
                end_event_data = self._extract_value(new_image, "end_event")
                if end_event_data:
                    self._create_completed_trip_from_state(trip_id, start_event, end_event_data)
    
    def _completed_trip_exists(self, trip_id: str) -> bool:
        """Check if completed trip already exists"""
        try:
            response = self.table.get_item(
                Key={"trip_id": trip_id, "sk": f"COMPLETED#{trip_id}"}
            )
            return "Item" in response
        except Exception as e:
            logger.error(f"Error checking completed trip for {trip_id}: {e}")
            return False
    
    def _find_counterpart_event(self, trip_id: str, event_type: str) -> Optional[Dict[str, Any]]:
        """Find counterpart RAW event"""
        try:
            # Query for all RAW events for this trip
            response = self.table.query(
                KeyConditionExpression="trip_id = :trip_id AND begins_with(sk, :sk_prefix)",
                ExpressionAttributeValues={
                    ":trip_id": trip_id,
                    ":sk_prefix": f"RAW#{trip_id}#{event_type}"
                }
            )
            
            # Return the first (most recent) matching event
            items = response.get("Items", [])
            if items:
                return items[0]  # Items are sorted by SK (which includes timestamp)
            return None
            
        except Exception as e:
            logger.error(f"Error finding counterpart event for {trip_id}, {event_type}: {e}")
            return None
    
    def _create_completed_trip(self, trip_id: str, current_event_type: str, 
                             current_image: Dict[str, Any], counterpart_event: Dict[str, Any]) -> None:
        """Create completed trip from two RAW events"""
        try:
            # Determine which is start and which is end
            if current_event_type == "trip_start":
                start_event = self._convert_dynamo_image_to_item(current_image)
                end_event = counterpart_event
            else:
                start_event = counterpart_event
                end_event = self._convert_dynamo_image_to_item(current_image)
            
            # Create completed trip record
            completed_trip = self._merge_trip_events(trip_id, start_event, end_event)
            
            # Write completed trip
            logger.info(f"Writing COMPLETED trip: {trip_id}")
            retry(lambda: self.table.put_item(Item=completed_trip))
            
            # Update trip state to completed
            self._update_trip_state_to_completed(trip_id, start_event, end_event)
            
        except Exception as e:
            logger.error(f"Error creating completed trip for {trip_id}: {e}", exc_info=True)
    
    def _create_completed_trip_from_state(self, trip_id: str, start_event: Dict[str, Any], 
                                        end_event_data: Dict[str, Any]) -> None:
        """Create completed trip from state event data"""
        try:
            completed_trip = self._merge_trip_events(trip_id, start_event, end_event_data)
            
            logger.info(f"Writing COMPLETED trip from state: {trip_id}")
            retry(lambda: self.table.put_item(Item=completed_trip))
            
            # Update trip state to completed
            self._update_trip_state_to_completed(trip_id, start_event, end_event_data)
            
        except Exception as e:
            logger.error(f"Error creating completed trip from state for {trip_id}: {e}", exc_info=True)
    
    def _convert_dynamo_image_to_item(self, dynamo_image: Dict[str, Any]) -> Dict[str, Any]:
        """Convert DynamoDB image format to regular item format"""
        item = {}
        for key, value in dynamo_image.items():
            item[key] = self._extract_value(dynamo_image, key)
        return item
    
    def _merge_trip_events(self, trip_id: str, start_event: Dict[str, Any], 
                          end_event: Dict[str, Any]) -> Dict[str, Any]:
        """Merge start and end events into completed trip"""
        timestamp = datetime.now().isoformat()
        
        # Calculate trip metrics
        pickup_time = start_event.get('pickup_datetime')
        dropoff_time = end_event.get('dropoff_datetime')
        
        trip_duration = None
        if pickup_time and dropoff_time:
            try:
                pickup_dt = datetime.fromisoformat(str(pickup_time).replace('Z', '+00:00'))
                dropoff_dt = datetime.fromisoformat(str(dropoff_time).replace('Z', '+00:00'))
                trip_duration = (dropoff_dt - pickup_dt).total_seconds()
            except Exception as e:
                logger.warning(f"Could not calculate trip duration for {trip_id}: {e}")
        
        # Calculate fare variance
        estimated_fare = start_event.get('estimated_fare_amount', 0)
        actual_fare = end_event.get('fare_amount', 0)
        fare_variance = None
        if estimated_fare and actual_fare:
            try:
                fare_variance = float(actual_fare) - float(estimated_fare)
            except:
                pass
        
        # Create completed trip record
        completed_trip = {
            'trip_id': trip_id,
            'sk': f'COMPLETED#{trip_id}',
            'status': 'completed',
            
            # Start event fields
            'pickup_location_id': start_event.get('pickup_location_id'),
            'dropoff_location_id': start_event.get('dropoff_location_id'),
            'vendor_id': start_event.get('vendor_id'),
            'pickup_datetime': pickup_time,
            'estimated_dropoff_datetime': start_event.get('estimated_dropoff_datetime'),
            'estimated_fare_amount': estimated_fare,
            
            # End event fields
            'dropoff_datetime': dropoff_time,
            'rate_code': end_event.get('rate_code'),
            'passenger_count': end_event.get('passenger_count'),
            'trip_distance': end_event.get('trip_distance'),
            'fare_amount': actual_fare,
            'tip_amount': end_event.get('tip_amount'),
            'payment_type': end_event.get('payment_type'),
            'trip_type': end_event.get('trip_type'),
            
            # Calculated fields
            'trip_duration_seconds': Decimal(str(trip_duration)) if trip_duration else None,
            'fare_variance': Decimal(str(fare_variance)) if fare_variance is not None else None,
            'completion_timestamp': timestamp,
            'processing_date': datetime.now().strftime('%Y-%m-%d'),
            
            # Metadata
            'matched_by': 'stream_matcher',
            'ttl': int((datetime.now() + timedelta(days=90)).timestamp())  # 90-day TTL
        }
        
        return completed_trip
    
    def _update_trip_state_to_completed(self, trip_id: str, start_event: Dict[str, Any], 
                                      end_event: Dict[str, Any]) -> None:
        """Update trip state to completed"""
        try:
            state_update = {
                'trip_id': trip_id,
                'sk': f'STATE#{trip_id}',
                'status': 'COMPLETED',
                'start_event': start_event,
                'end_event': end_event,
                'last_updated': datetime.now().isoformat(),
                'completed_by': 'stream_matcher',
                'ttl': int((datetime.now() + timedelta(days=30)).timestamp())
            }
            
            retry(lambda: self.table.put_item(Item=state_update))
            logger.info(f"Updated trip state to completed: {trip_id}")
            
        except Exception as e:
            logger.error(f"Error updating trip state for {trip_id}: {e}")

def lambda_handler(event, context):
    """Enhanced Lambda handler for DynamoDB stream processing"""
    matcher = TripMatcher()
    
    processed_count = 0
    error_count = 0
    matched_trips = []
    
    for record in event["Records"]:
        try:
            matcher.process_stream_record(record)
            processed_count += 1
            
            # Track matched trips for logging
            if record["eventName"] in ("INSERT", "MODIFY"):
                new_image = record["dynamodb"]["NewImage"]
                trip_id = matcher._extract_string_value(new_image, "trip_id")
                sk = matcher._extract_string_value(new_image, "sk")
                if sk and sk.startswith("COMPLETED#"):
                    matched_trips.append(trip_id)
                    
        except Exception as e:
            error_count += 1
            logger.error(f"Failed to process stream record: {e}", exc_info=True)
    
    # Log summary
    logger.info(f"Processed {processed_count} records, {error_count} errors")
    if matched_trips:
        logger.info(f"Matched trips: {matched_trips}")
    
    return {
        "statusCode": 200,
        "body": json.dumps({
            "processed": processed_count,
            "errors": error_count,
            "matched_trips": len(matched_trips)
        })
    }