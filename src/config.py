import os

class Config:
    DYNAMODB_TABLE = os.getenv("TRIPS_TABLE_NAME", "nsp_bolt_trips")
    KINESIS_STREAM = os.getenv("KINESIS_STREAM_NAME", "nsp-bolt-trip-events")
    KPI_BUCKET = os.getenv("KPI_BUCKET_NAME", "nsp-bolt-kpi-output")
    REGION = os.getenv("AWS_REGION", "us-east-1")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    DYNAMODB_READ_THROUGHPUT_PERCENT = float(os.getenv("DYNAMODB_READ_PERCENT", "0.5"))
    DYNAMODB_SPLITS = int(os.getenv("DYNAMODB_SPLITS", "100"))
    DLQ_URL = os.getenv("DLQ_URL", "https://sqs.us-east-1.amazonaws.com/643303011741/bolt-ride-dlq")
