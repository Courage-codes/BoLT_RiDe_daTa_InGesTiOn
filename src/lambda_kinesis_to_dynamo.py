import boto3
import base64
import json
from src.config import Config
from src.utils import get_logger, retry
import os

TRIPS_TABLE_NAME = os.getenv("TRIPS_TABLE_NAME")
KINESIS_STREAM_NAME = os.getenv("KINESIS_STREAM_NAME")
KPI_BUCKET_NAME = os.getenv("KPI_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


logger = get_logger("lambda_kinesis_to_dynamo", Config.LOG_LEVEL)

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb', region_name=Config.REGION)
    table = dynamodb.Table(Config.DYNAMODB_TABLE)

    for record in event['Records']:
        try:
            payload = json.loads(base64.b64decode(record['kinesis']['data']))
            trip_id = payload['trip_id']
            event_type = payload['event_type']
            sk = f"RAW#{trip_id}#{event_type}"
            item = {
                "trip_id": trip_id,
                "sk": sk,
                **payload
            }
            logger.info(f"Writing RAW event: {sk}")
            retry(lambda: table.put_item(Item=item))
        except Exception as e:
            logger.error(f"Failed to process record: {e}", exc_info=True)
    return {"status": "ok"}
