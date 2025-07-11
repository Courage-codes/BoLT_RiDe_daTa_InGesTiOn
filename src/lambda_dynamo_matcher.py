import boto3
from src.config import Config
from src.utils import get_logger, retry
import os

TRIPS_TABLE_NAME = os.getenv("TRIPS_TABLE_NAME")
KINESIS_STREAM_NAME = os.getenv("KINESIS_STREAM_NAME")
KPI_BUCKET_NAME = os.getenv("KPI_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


logger = get_logger("lambda_dynamo_matcher", Config.LOG_LEVEL)

def lambda_handler(event, context):
    dynamodb = boto3.resource("dynamodb", region_name=Config.REGION)
    table = dynamodb.Table(Config.DYNAMODB_TABLE)

    for record in event["Records"]:
        if record["eventName"] not in ("INSERT", "MODIFY"):
            continue
        new_image = record["dynamodb"]["NewImage"]
        trip_id = new_image["trip_id"]["S"]
        sk = new_image["sk"]["S"]
        if not sk.startswith("RAW#"):
            continue

        # Determine counterpart event_type
        event_type = sk.split("#")[-1]
        counterpart_type = "trip_end" if event_type == "trip_begin" else "trip_begin"
        counterpart_sk = f"RAW#{trip_id}#{counterpart_type}"

        try:
            # Query for counterpart RAW event
            resp = table.get_item(Key={"trip_id": trip_id, "sk": counterpart_sk})
            if "Item" not in resp:
                logger.info(f"Counterpart not found for {sk}")
                continue

            # Merge both RAW events
            merged = {**{k: v["S"] if isinstance(v, dict) and "S" in v else v for k, v in new_image.items()},
                      **resp["Item"]}
            completed_sk = f"COMPLETED#{trip_id}"
            merged["sk"] = completed_sk
            merged["status"] = "completed"

            logger.info(f"Writing COMPLETED event: {completed_sk}")
            retry(lambda: table.put_item(Item=merged))

        except Exception as e:
            logger.error(f"Failed to match/merge for trip_id {trip_id}: {e}", exc_info=True)
    return {"status": "ok"}
