# BoLT Ride Data Ingestion Project

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Data Flow](#data-flow)
4. [Data Schema](#data-schema)
5. [Prerequisites](#prerequisites)
6. [Quick Start](#quick-start)
7. [Detailed Setup](#detailed-setup)
8. [Configuration](#configuration)
9. [Data Processing](#data-processing)
10. [Monitoring & Observability](#monitoring--observability)
11. [Troubleshooting](#troubleshooting)
12. [Best Practices](#best-practices)
13. [Performance Considerations](#performance-considerations)
14. [Security](#security)
15. [FAQ](#faq)

## Overview

The BoLT Ride Data Ingestion Project is a **real-time, serverless data pipeline** designed to process ride-sharing trip events at scale. Built on AWS managed services, it provides robust data validation, event correlation, and automated KPI generation.

### Key Features
- ✅ **Real-time processing** with sub-second latency
- ✅ **Automatic data validation** and cleaning
- ✅ **Event correlation** (trip start/end matching)
- ✅ **Scalable serverless architecture**
- ✅ **Built-in monitoring** and error handling
- ✅ **Cost-optimized** with automatic data expiry
- ✅ **Audit trail** for all data transformations

### Use Cases
- Real-time ride analytics dashboard
- Operational metrics and KPIs
- Regulatory compliance reporting

## Architecture

![](images/architecture.svg)

### Components

| Component | Purpose | Technology | Scaling |
|-----------|---------|------------|---------|
| **Producer** | Generate trip events | Custom/SDK | Horizontal |
| **Kinesis Stream** | Real-time event ingestion | AWS Kinesis | Auto-scaling |
| **Ingestion Lambda** | Validate & store raw events | AWS Lambda | Auto-scaling |
| **Matcher Lambda** | Correlate trip start/end | AWS Lambda | Auto-scaling |
| **DynamoDB** | Event storage & querying | AWS DynamoDB | On-demand |
| **Glue Job** | Batch KPI aggregation | AWS Glue | Configurable |
| **S3** | Long-term KPI storage | AWS S3 | Unlimited |

## Data Flow

### 1. Event Ingestion
```
Trip Event → Kinesis → Lambda → DynamoDB (RAW)
```

### 2. Event Matching
```
DynamoDB Streams → Lambda → DynamoDB (COMPLETED)
```

### 3. KPI Generation
```
DynamoDB → Glue Job → S3 (Partitioned by Date)
```

### Event Types
- **`trip_begin`**: Trip start event with pickup details
- **`trip_end`**: Trip completion event with dropoff and fare details
- **`trip_cancelled`**: Trip cancellation event
- **`trip_updated`**: Trip modification event (future enhancement)

## Data Schema

### DynamoDB Table: `nsp_bolt_trips`

#### Primary Keys
- **Partition Key**: `trip_id` (String) - Unique trip identifier
- **Sort Key**: `sk` (String) - Event classification and timestamp

#### Sort Key Formats
- **Raw Events**: `RAW#trip_id#event_type#timestamp`
- **Completed Trips**: `COMPLETED#trip_id`
- **Failed Events**: `FAILED#trip_id#error_code#timestamp`

#### Core Attributes

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `trip_id` | String | ✅ | Unique trip identifier | `trip_12345` |
| `sk` | String | ✅ | Sort key for event classification | `RAW#trip_12345#trip_begin#2024-01-15T10:30:00Z` |
| `event_type` | String | ✅ | Type of event | `trip_begin`, `trip_end` |
| `ingestion_timestamp` | String | ✅ | When event was processed | `2024-01-15T10:30:15.123Z` |
| `processing_date` | String | ✅ | Date partition for queries | `2024-01-15` |
| `ttl` | Number | ✅ | Auto-expiry timestamp | `1705320615` |

#### Trip-Specific Attributes

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `vendor_id` | String | ✅ | Service provider ID | `vendor_001` |
| `pickup_datetime` | String | ✅ | Trip start time | `2024-01-15T10:30:00Z` |
| `dropoff_datetime` | String | ❌ | Trip end time | `2024-01-15T11:15:00Z` |
| `pickup_location_id` | String | ✅ | Pickup zone ID | `zone_123` |
| `dropoff_location_id` | String | ❌ | Dropoff zone ID | `zone_456` |
| `passenger_count` | Number | ❌ | Number of passengers | `2` |
| `fare_amount` | Number | ❌ | Total fare (USD) | `25.50` |
| `tip_amount` | Number | ❌ | Tip amount (USD) | `5.00` |
| `payment_type` | String | ❌ | Payment method | `credit_card` |
| `trip_distance` | Number | ❌ | Distance in miles | `5.2` |

#### Metadata Attributes

| Field | Type | Description |
|-------|------|-------------|
| `data_version` | String | Schema version for evolution |
| `source_system` | String | Originating system |
| `validation_status` | String | Data quality status |
| `error_details` | String | Validation error information |

## Prerequisites

### AWS Services Required
- ✅ **Amazon Kinesis Data Streams**
- ✅ **AWS Lambda**
- ✅ **Amazon DynamoDB**
- ✅ **AWS Glue**
- ✅ **Amazon S3**
- ✅ **Amazon CloudWatch**

### IAM Permissions
- Kinesis: `PutRecord`, `PutRecords`, `DescribeStream`
- Lambda: `InvokeFunction`, `GetFunction`
- DynamoDB: `PutItem`, `GetItem`, `Query`, `Scan`, `DescribeTable`
- S3: `PutObject`, `GetObject`, `ListBucket`
- Glue: `StartJobRun`, `GetJobRun`
- CloudWatch: `PutMetricData`, `CreateLogGroup`, `CreateLogStream`

### Development Environment
- **Python**: 3.8+ (3.9+ recommended)
- **AWS CLI**: 2.0+
- **boto3**: Latest version
- **Virtual Environment**: venv or conda

## Quick Start

### 1. Clone and Setup
```bash
git clone <repository-url>
cd bolt-ride-ingestion
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure AWS
```bash
aws configure
# Enter your AWS credentials and region
```

### 3. Set Environment Variables
```bash
export TRIPS_TABLE_NAME="nsp_bolt_trips"
export KINESIS_STREAM_NAME="nsp-bolt-trip-events"
export KPI_BUCKET_NAME="nsp-bolt-kpi-output"
export AWS_REGION="us-east-1"
export LOG_LEVEL="INFO"
```

### 4. Deploy Infrastructure
```bash
# Create DynamoDB table
aws dynamodb create-table \
  --table-name nsp_bolt_trips \
  --attribute-definitions \
    AttributeName=trip_id,AttributeType=S \
    AttributeName=sk,AttributeType=S \
  --key-schema \
    AttributeName=trip_id,KeyType=HASH \
    AttributeName=sk,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST \
  --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES

# Create Kinesis stream
aws kinesis create-stream \
  --stream-name nsp-bolt-trip-events \
  --shard-count 2

# Create S3 bucket
aws s3 mb s3://nsp-bolt-kpi-output
```

### 5. Test the Pipeline
```bash
# Send test events
python -m src.kinesis_producer --test-mode

# Monitor processing
aws logs tail /aws/lambda/bolt-ride-ingestion --follow
```

## Detailed Setup

### DynamoDB Configuration

#### Enable TTL
```bash
aws dynamodb update-time-to-live \
  --table-name nsp_bolt_trips \
  --time-to-live-specification \
    Enabled=true,AttributeName=ttl
```

#### Create Global Secondary Index (Optional)
```bash
aws dynamodb update-table \
  --table-name nsp_bolt_trips \
  --attribute-definitions \
    AttributeName=processing_date,AttributeType=S \
    AttributeName=event_type,AttributeType=S \
  --global-secondary-index-updates \
    '[{
      "Create": {
        "IndexName": "processing_date-event_type-index",
        "KeySchema": [
          {"AttributeName": "processing_date", "KeyType": "HASH"},
          {"AttributeName": "event_type", "KeyType": "RANGE"}
        ],
        "Projection": {"ProjectionType": "ALL"},
        "BillingMode": "PAY_PER_REQUEST"
      }
    }]'
```

### Lambda Deployment

#### Package Lambda Functions
```bash
# Create deployment package
mkdir lambda-package
cp -r src/ lambda-package/
cd lambda-package
zip -r ../lambda-deployment.zip .
cd ..
```

#### Deploy Ingestion Lambda
```bash
aws lambda create-function \
  --function-name bolt-ride-ingestion \
  --runtime python3.9 \
  --role arn:aws:iam::YOUR_ACCOUNT:role/lambda-execution-role \
  --handler src.lambda_kinesis_to_dynamo.lambda_handler \
  --zip-file fileb://lambda-deployment.zip \
  --timeout 60 \
  --memory-size 512 \
  --environment Variables='{
    "TRIPS_TABLE_NAME": "nsp_bolt_trips",
    "LOG_LEVEL": "INFO"
  }'
```

#### Configure Kinesis Trigger
```bash
aws lambda create-event-source-mapping \
  --function-name bolt-ride-ingestion \
  --event-source-arn arn:aws:kinesis:us-east-1:YOUR_ACCOUNT:stream/nsp-bolt-trip-events \
  --starting-position LATEST \
  --batch-size 100
```

### Glue Job Setup

#### Create Glue Role
```bash
aws iam create-role \
  --role-name GlueJobRole \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Principal": {"Service": "glue.amazonaws.com"},
        "Action": "sts:AssumeRole"
      }
    ]
  }'

aws iam attach-role-policy \
  --role-name GlueJobRole \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
```

#### Upload Glue Script
```bash
aws s3 cp src/glue_kpi_aggregator.py s3://nsp-bolt-kpi-output/scripts/
```

#### Create Glue Job
```bash
aws glue create-job \
  --name bolt-kpi-aggregator \
  --role arn:aws:iam::YOUR_ACCOUNT:role/GlueJobRole \
  --command '{
    "Name": "glueetl",
    "ScriptLocation": "s3://nsp-bolt-kpi-output/scripts/glue_kpi_aggregator.py",
    "PythonVersion": "3"
  }' \
  --default-arguments '{
    "--TRIPS_TABLE_NAME": "nsp_bolt_trips",
    "--KPI_BUCKET_NAME": "nsp-bolt-kpi-output",
    "--AWS_REGION": "us-east-1"
  }'
```

## Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `TRIPS_TABLE_NAME` | ✅ | - | DynamoDB table name |
| `KINESIS_STREAM_NAME` | ✅ | - | Kinesis stream name |
| `KPI_BUCKET_NAME` | ✅ | - | S3 bucket for KPI output |
| `AWS_REGION` | ✅ | us-east-1 | AWS region |
| `LOG_LEVEL` | ❌ | INFO | Logging level |
| `TTL_DAYS` | ❌ | 30 | Days before data expires |
| `BATCH_SIZE` | ❌ | 100 | Kinesis batch size |
| `MAX_RETRIES` | ❌ | 3 | Maximum retry attempts |
| `VALIDATION_STRICT` | ❌ | true | Strict validation mode |

### Lambda Configuration

#### Memory and Timeout Settings
- **Ingestion Lambda**: 512MB memory, 60s timeout
- **Matcher Lambda**: 256MB memory, 30s timeout
- **Dead Letter Queue**: Enabled for error handling

#### Reserved Concurrency
- **Ingestion Lambda**: 100 concurrent executions
- **Matcher Lambda**: 50 concurrent executions

## Data Processing

### Validation Rules

#### Required Fields
- `trip_id`: Must be non-empty string
- `event_type`: Must be in allowed values
- `pickup_datetime`: Must be valid ISO 8601 timestamp
- `vendor_id`: Must be non-empty string


### Event Matching Logic

#### Trip Completion Detection
1. **Trigger**: DynamoDB Streams event for new RAW item
2. **Query**: Find matching trip_begin/trip_end events
3. **Validation**: Ensure both events exist and are valid
4. **Aggregation**: Calculate trip duration, distance, fare totals
5. **Storage**: Write COMPLETED event with aggregated data

#### Matching Criteria
- Same `trip_id`
- Valid `trip_begin` event exists
- Valid `trip_end` event exists
- Events within reasonable time window (< 24 hours)

### KPI Calculations

#### Trip-Level Metrics
- **Duration**: `dropoff_datetime - pickup_datetime`
- **Revenue**: `fare_amount + tip_amount`
- **Average Speed**: `trip_distance / duration_hours`
- **Utilization**: Percentage of time with passengers

#### Aggregate Metrics
- **Trips per Hour**: Count of completed trips
- **Average Fare**: Mean fare amount
- **Average Duration**: Mean trip duration
- **Revenue per Hour**: Total revenue by hour
- **Geographic Distribution**: Trips by pickup/dropoff zones

## Monitoring & Observability

### CloudWatch Metrics

#### Custom Metrics
- `EventsProcessed`: Count of events processed
- `ValidationErrors`: Count of validation failures
- `MatchingSuccessRate`: Percentage of successfully matched trips
- `ProcessingLatency`: Time from event to storage
- `KPIGenerationDuration`: Time to generate KPIs

#### Dashboard Components
- Real-time event throughput
- Error rates and types
- Lambda performance metrics
- DynamoDB read/write capacity
- Cost tracking

### Logging Strategy

#### Log Levels
- **ERROR**: System failures, validation errors
- **WARN**: Data quality issues, retries
- **INFO**: Processing milestones, KPI generation
- **DEBUG**: Detailed processing information

#### Structured Logging
```python
logger.info("Event processed", extra={
    "trip_id": trip_id,
    "event_type": event_type,
    "processing_time_ms": processing_time,
    "validation_status": "success"
})
```

### Alerting

#### Critical Alerts
- Lambda function errors > 1%
- DynamoDB throttling events
- Kinesis stream shard iterator age > 5 minutes
- S3 upload failures

#### Warning Alerts
- Validation error rate > 5%
- Processing latency > 10 seconds
- Unmatched events > 10%

## Troubleshooting

### Common Issues

#### Lambda Import Errors
**Symptom**: `ModuleNotFoundError` in Lambda logs
**Solution**: 
```bash
# Ensure src/ folder is at ZIP root
zip -r lambda-deployment.zip src/
# Verify handler path: src.module_name.function_name
```

#### DynamoDB Validation Errors
**Symptom**: `ValidationException` when writing to DynamoDB
**Solution**: 
- Verify table schema matches code
- Check for None values in required fields
- Ensure Decimal types for numeric values

#### Kinesis Shard Iterator Errors
**Symptom**: `ExpiredIteratorException` in Lambda
**Solution**: 
- Increase Lambda timeout
- Reduce batch size
- Add retry logic with exponential backoff

#### Memory Issues in Lambda
**Symptom**: Lambda function timeout or memory exceeded
**Solution**: 
- Increase memory allocation
- Optimize data processing logic
- Process events in smaller batches

### Debug Commands

#### Check DynamoDB Items
```bash
aws dynamodb scan \
  --table-name nsp_bolt_trips \
  --filter-expression "begins_with(sk, :sk_prefix)" \
  --expression-attribute-values '{":sk_prefix":{"S":"RAW#"}}'
```

#### Monitor Kinesis Stream
```bash
aws kinesis describe-stream \
  --stream-name nsp-bolt-trip-events

aws kinesis get-shard-iterator \
  --stream-name nsp-bolt-trip-events \
  --shard-id shardId-000000000000 \
  --shard-iterator-type LATEST
```

#### Check Lambda Logs
```bash
aws logs tail /aws/lambda/bolt-ride-ingestion --follow
aws logs filter-log-events \
  --log-group-name /aws/lambda/bolt-ride-ingestion \
  --filter-pattern "ERROR"
```

## Best Practices

### Development
- ✅ Use virtual environments for isolation
- ✅ Pin dependency versions in requirements.txt
- ✅ Implement comprehensive unit tests
- ✅ Use type hints for better code documentation
- ✅ Follow PEP 8 coding standards

### Deployment
- ✅ Use Infrastructure as Code (CloudFormation/CDK)
- ✅ Implement CI/CD pipelines
- ✅ Use separate environments (dev/staging/prod)
- ✅ Version Lambda functions
- ✅ Enable AWS X-Ray for distributed tracing

### Data Management
- ✅ Always validate input data
- ✅ Implement idempotent processing
- ✅ Use DynamoDB TTL for automatic cleanup
- ✅ Partition S3 data by date for efficient querying
- ✅ Implement data retention policies

### Security
- ✅ Use least privilege IAM roles
- ✅ Enable CloudTrail for audit logging
- ✅ Encrypt data in transit and at rest
- ✅ Use AWS Secrets Manager for sensitive data
- ✅ Implement VPC endpoints for private communication

### Cost Optimization
- ✅ Use DynamoDB on-demand billing
- ✅ Set appropriate Lambda memory sizes
- ✅ Use S3 lifecycle policies
- ✅ Monitor and optimize Kinesis shard count
- ✅ Implement intelligent tiering for S3

## Performance Considerations

### Scaling Guidelines

#### Kinesis Shards
- **1 shard**: Up to 1,000 records/second or 1 MB/second
- **Scaling**: Add shards based on throughput requirements
- **Monitoring**: Watch for shard iterator age and throttling

#### Lambda Concurrency
- **Reserved Concurrency**: Set based on downstream capacity
- **Burst Concurrency**: 1000 concurrent executions per region
- **Monitoring**: Track concurrent executions and throttling

#### DynamoDB Scaling
- **On-Demand**: Automatically scales based on traffic
- **Provisioned**: Manual scaling based on capacity units
- **Hot Partitions**: Distribute load across partition keys

### Performance Optimization

#### Batch Processing
```python
# Process Kinesis records in batches
def process_kinesis_batch(records):
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_record, record) for record in records]
        for future in as_completed(futures):
            try:
                result = future.result()
                # Handle result
            except Exception as e:
                logger.error(f"Processing error: {e}")
```

#### Connection Pooling
```python
# Reuse DynamoDB connections
import boto3
from botocore.config import Config

config = Config(
    retries={'max_attempts': 3},
    max_pool_connections=50
)
dynamodb = boto3.resource('dynamodb', config=config)
```

## Security

### IAM Roles and Policies

#### Lambda Execution Role
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:PutItem",
        "dynamodb:GetItem",
        "dynamodb:Query",
        "dynamodb:Scan"
      ],
      "Resource": "arn:aws:dynamodb:*:*:table/nsp_bolt_trips"
    },
    {
      "Effect": "Allow",
      "Action": [
        "kinesis:DescribeStream",
        "kinesis:GetShardIterator",
        "kinesis:GetRecords"
      ],
      "Resource": "arn:aws:kinesis:*:*:stream/nsp-bolt-trip-events"
    }
  ]
}
```

#### Data Encryption
- **DynamoDB**: Server-side encryption enabled
- **Kinesis**: Server-side encryption with KMS
- **S3**: AES-256 encryption at rest
- **Lambda**: Environment variables encrypted

### Network Security
- **VPC**: Deploy Lambda in private subnets
- **Security Groups**: Restrict inbound/outbound traffic
- **VPC Endpoints**: Private communication with AWS services
- **WAF**: Web Application Firewall for API protection

## FAQ

### General Questions

**Q: How long does it take to process an event?**
A: Typically 100-500ms from Kinesis ingestion to DynamoDB storage, depending on validation complexity.

**Q: What happens if Lambda fails to process an event?**
A: Events are retried up to 3 times, then sent to a Dead Letter Queue for manual investigation.

**Q: How much does this solution cost?**
A: Costs vary by volume. For 1M events/month: ~$50-100 including all AWS services.

**Q: Can I modify the data schema?**
A: Yes, but plan for backward compatibility. Use data versioning for schema evolution.

### Technical Questions

**Q: Why use DynamoDB instead of RDS?**
A: DynamoDB provides better scaling, lower latency, and is more cost-effective for high-volume event storage.

**Q: How do you handle duplicate events?**
A: The combination of trip_id and timestamp in the sort key ensures natural deduplication.

**Q: Can I add new event types?**
A: Yes, add them to the validation rules and update the matching logic accordingly.

**Q: How do I backup the data?**
A: Enable DynamoDB Point-in-Time Recovery and use AWS Backup for automated backups.

### Operations Questions

**Q: How do I monitor the pipeline health?**
A: Use CloudWatch dashboards, set up alarms for key metrics, and monitor Lambda logs.

**Q: How do I handle a failed Glue job?**
A: Check CloudWatch logs, verify IAM permissions, and ensure the script is accessible in S3.

**Q: Can I replay events?**
A: Yes, use Kinesis shard iterators to replay from specific timestamps or sequence numbers.

**Q: How do I handle high traffic spikes?**
A: The serverless architecture auto-scales, but monitor for throttling and adjust concurrency limits.

---

