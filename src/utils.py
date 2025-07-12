import logging
import time
import random
import boto3
import json
from datetime import datetime
import os

def get_logger(name, level="INFO"):
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('[%(levelname)s] %(asctime)s %(name)s: %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger

def retry(func, max_attempts=3, backoff=0.5, *args, **kwargs):
    for attempt in range(1, max_attempts + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if attempt == max_attempts:
                raise
            time.sleep(backoff * (2 ** (attempt - 1)) + random.uniform(0, 0.1))

def send_to_dlq(record, error_message, dlq_url=None, region=None):
    """
    Send a failed record and error message to an SQS dead letter queue.
    - record: The original event or record that failed.
    - error_message: String describing the error.
    - dlq_url: The SQS queue URL (optional; will use config if not provided).
    - region: AWS region (optional; will use config if not provided).
    """
    dlq_url = dlq_url or os.getenv("DLQ_URL")
    region = region or os.getenv("AWS_REGION", "us-east-1")
    if not dlq_url:
        raise ValueError("DLQ_URL must be provided as an argument or environment variable.")
    sqs = boto3.client("sqs", region_name=region)
    payload = {
        "failed_record": record,
        "error_message": error_message,
        "timestamp": datetime.utcnow().isoformat()
    }
    sqs.send_message(
        QueueUrl=dlq_url,
        MessageBody=json.dumps(payload)
    )
