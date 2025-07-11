import sys
import boto3
import os
import json
from datetime import datetime, timezone
from src.config import Config
from src.utils import get_logger, retry

logger = get_logger("glue_kpi_aggregator", Config.LOG_LEVEL)

def aggregate_kpis(date_str):
    dynamodb = boto3.resource('dynamodb', region_name=Config.REGION)
    table = dynamodb.Table(Config.DYNAMODB_TABLE)
    s3 = boto3.client('s3', region_name=Config.REGION)
    bucket = Config.KPI_BUCKET

    scan_kwargs = {
        'FilterExpression': 'begins_with(sk, :completed) AND #d = :date',
        'ExpressionAttributeNames': {'#d': 'completion_date'},
        'ExpressionAttributeValues': {
            ':completed': 'COMPLETED#',
            ':date': date_str
        }
    }

    completed_trips = []
    try:
        response = retry(lambda: table.scan(**scan_kwargs))
        completed_trips.extend(response.get('Items', []))
        while 'LastEvaluatedKey' in response:
            scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
            response = retry(lambda: table.scan(**scan_kwargs))
            completed_trips.extend(response.get('Items', []))
    except Exception as e:
        logger.error(f"Error scanning DynamoDB: {e}", exc_info=True)
        sys.exit(1)

    if not completed_trips:
        logger.warning(f"No completed trips found for {date_str}")
        return

    fares = [float(trip['fare_amount']) for trip in completed_trips if 'fare_amount' in trip]
    kpis = {
        "date": date_str,
        "total_fare": round(sum(fares), 2),
        "count_trips": len(fares),
        "average_fare": round(sum(fares) / len(fares), 2) if fares else 0,
        "max_fare": round(max(fares), 2) if fares else 0,
        "min_fare": round(min(fares), 2) if fares else 0,
        "generated_at": datetime.now(timezone.utc).isoformat()
    }

    output_key = (
        f"daily_metrics/{date_str.replace('-', '/')}/"
        f"metrics_{date_str.replace('-', '')}_{datetime.now(timezone.utc).strftime('%H%M%S')}.json"
    )
    try:
        retry(lambda: s3.put_object(
            Bucket=bucket,
            Key=output_key,
            Body=json.dumps(kpis, indent=2).encode('utf-8'),
            ContentType='application/json'
        ))
        logger.info(f"KPI file written to s3://{bucket}/{output_key}")
    except Exception as e:
        logger.error(f"Error writing KPI to S3: {e}", exc_info=True)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        date_str = sys.argv[1]
    else:
        date_str = datetime.now(timezone.utc).date().isoformat()
    aggregate_kpis(date_str)
