import os

class Config:
    DYNAMODB_TABLE = os.getenv("TRIPS_TABLE_NAME", "nsp_bolt_trips")
    KINESIS_STREAM = os.getenv("KINESIS_STREAM_NAME", "nsp-bolt-trip-events")
    KPI_BUCKET = os.getenv("KPI_BUCKET_NAME", "nsp-bolt-kpi-output")
    REGION = os.getenv("AWS_REGION", "us-east-1")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
