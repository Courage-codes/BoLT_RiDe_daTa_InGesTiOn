# BoLT Ride Data Ingestion Project

## Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Data Flow](#data-flow)
- [Data Schema](#data-schema)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Detailed Setup](#detailed-setup)
- [Configuration](#configuration)
- [Data Processing](#data-processing)
- [Monitoring & Observability](#monitoring--observability)
- [Troubleshooting](#troubleshooting)
- [Best Practices](#best-practices)
- [Performance Considerations](#performance-considerations)
- [Security](#security)
- [FAQ](#faq)

## Overview
The BoLT Ride Data Ingestion Project is a real-time, serverless data pipeline for processing ride-sharing trip events at scale. Built on AWS managed services, it provides robust data validation, event correlation, automated KPI generation, and end-to-end reliability with error capture.

### Key Features
- Real-time processing with sub-second latency
- Automatic data validation and cleaning
- Event correlation (trip start/end matching)
- Scalable serverless architecture
- Built-in monitoring and error handling (including DLQ)
- Cost-optimized with automatic data expiry
- Audit trail for all data transformations

### Use Cases
- Real-time ride analytics dashboards
- Operational metrics and KPIs
- Regulatory compliance reporting

## Architecture
![](images/architecture.svg)

### Components
| Component | Purpose | Technology | Scaling |
|-----------|---------|------------|---------|
| Producer | Generate trip events | Custom/SDK | Horizontal |
| Kinesis Stream | Real-time event ingestion | AWS Kinesis | Auto-scaling |
| Ingestion Lambda | Validate & store raw events | AWS Lambda | Auto-scaling |
| DLQ (Dead Letter Queue) | Capture failed/invalid records | AWS SQS | Auto-scaling |
| Matcher Lambda | Correlate trip start/end | AWS Lambda | Auto-scaling |
| DynamoDB | Event storage & querying | AWS DynamoDB | On-demand |
| Glue Job | Batch KPI aggregation | AWS Glue | Configurable |
| S3 | Long-term KPI storage | AWS S3 | Unlimited |

## Data Flow
1. **Event Ingestion**  
   Trip Event → Kinesis → Ingestion Lambda → DynamoDB (RAW)  
   ↘ DLQ (on failure)

2. **Event Matching**  
   DynamoDB Streams → Matcher Lambda → DynamoDB (COMPLETED)

3. **KPI Generation**  
   DynamoDB → Glue Job → S3 (Partitioned by Date)

### Event Types
- `trip_begin`: Trip start event with pickup details
- `trip_end`: Trip completion event with dropoff and fare details
- `trip_cancelled`: Trip cancellation event
- `trip_updated`: Trip modification event (future enhancement)

## Data Schema

### DynamoDB Table: `nsp_bolt_trips`

#### Primary Keys
- **Partition Key**: `trip_id` (String) — Unique trip identifier
- **Sort Key**: `sk` (String) — Event classification and timestamp

#### Sort Key Formats
- Raw Events: `RAW#trip_id#event_type#timestamp`
- Completed Trips: `COMPLETED#trip_id`
- Failed Events: `FAILED#trip_id#error_code#timestamp`

#### Core Attributes
| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| trip_id | String | ✅ | Unique trip identifier | trip_12345 |
| sk | String | ✅ | Sort key for event class | RAW#trip_12345#trip_begin#2024-01-15T10:30:00Z |
| event_type | String | ✅ | Type of event | trip_begin, trip_end |
| ingestion_timestamp | String | ✅ | When event was processed | 2024-01-15T10:30:15.123Z |
| processing_date | String | ✅ | Date partition for queries | 2024-01-15 |
| ttl | Number | ✅ | Auto-expiry timestamp | 1705320615 |

#### Trip-Specific Attributes
| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| vendor_id | String | ✅ | Service provider ID | vendor_001 |
| pickup_datetime | String | ✅ | Trip start time | 2024-01-15T10:30:00Z |
| dropoff_datetime | String | ❌ | Trip end time | 2024-01-15T11:15:00Z |
| pickup_location_id | String | ✅ | Pickup zone ID | zone_123 |
| dropoff_location_id | String | ❌ | Dropoff zone ID | zone_456 |
| passenger_count | Number | ❌ | Number of passengers | 2 |
| fare_amount | Number | ❌ | Total fare (USD) | 25.50 |
| tip_amount | Number | ❌ | Tip amount (USD) | 5.00 |
| payment_type | String | ❌ | Payment method | credit_card |
| trip_distance | Number | ❌ | Distance in miles | 5.2 |

#### Metadata Attributes
| Field | Type | Description |
|-------|------|-------------|
| data_version | String | Schema version for evolution |
| source_system | String | Originating system |
| validation_status | String | Data quality status |
| error_details | String | Validation error information |

## Prerequisites

### AWS Services Required
- Amazon Kinesis Data Streams
- AWS Lambda
- Amazon DynamoDB
- AWS Glue
- Amazon S3
- Amazon CloudWatch
- Amazon SQS (for DLQ)

### IAM Permissions
- Kinesis: `PutRecord`, `PutRecords`, `DescribeStream`
- Lambda: `InvokeFunction`, `GetFunction`
- DynamoDB: `PutItem`, `GetItem`, `Query`, `Scan`, `DescribeTable`
- S3: `PutObject`, `GetObject`, `ListBucket`
- Glue: `StartJobRun`, `GetJobRun`
- CloudWatch: `PutMetricData`, `CreateLogGroup`, `CreateLogStream`
- SQS: `SendMessage`

### Development Environment
- Python 3.8+ (3.9+ recommended)
- AWS CLI 2.0+
- boto3 (latest)
- Virtual Environment: `venv` or `conda`

## Quick Start
1. **Clone and Setup**
   ```bash
   git clone <repository-url>
   cd bolt-ride-ingestion
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Configure AWS**
   ```bash
   aws configure
   # Enter your AWS credentials and region
   ```

3. **Set Environment Variables**
   ```bash
   export TRIPS_TABLE_NAME="nsp_bolt_trips"
   export KINESIS_STREAM_NAME="nsp-bolt-trip-events"
   export KPI_BUCKET_NAME="nsp-bolt-kpi-output"
   export AWS_REGION="us-east-1"
   export LOG_LEVEL="INFO"
   export DLQ_URL="https://sqs.us-east-1.amazonaws.com/your-account/bolt-ride-dlq"
   ```

4. **Deploy Infrastructure**
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

   # Create SQS DLQ
   aws sqs create-queue --queue-name bolt-ride-dlq
   ```

5. **Test the Pipeline**
   ```bash
   # Send test events
   python -m src.kinesis_producer --test-mode

   # Monitor processing
   aws logs tail /aws/lambda/bolt-ride-ingestion --follow
   ```

## Detailed Setup

### DynamoDB Configuration
**Enable TTL**
```bash
aws dynamodb update-time-to-live \
  --table-name nsp_bolt_trips \
  --time-to-live-specification \
    Enabled=true,AttributeName=ttl
```

**Create Global Secondary Index (Optional)**
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
**Package Lambda Functions**
```bash
mkdir lambda-package
cp -r src/ lambda-package/
cd lambda-package
zip -r ../lambda-deployment.zip .
cd ..
```

**Deploy Ingestion Lambda**
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
    "LOG_LEVEL": "INFO",
    "DLQ_URL": "https://sqs.us-east-1.amazonaws.com/your-account-id/bolt-ride-dlq"
  }'
```

**Configure Kinesis Trigger**
```bash
aws lambda create-event-source-mapping \
  --function-name bolt-ride-ingestion \
  --event-source-arn arn:aws:kinesis:us-east-1:YOUR_ACCOUNT:stream/nsp-bolt-trip-events \
  --starting-position LATEST \
  --batch-size 100
```

### Glue Job Setup
**Create Glue Role**
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

**Upload Glue Script**
```bash
aws s3 cp src/glue_kpi_aggregator.py s3://nsp-bolt-kpi-output/scripts/
```

**Create Glue Job**
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
| TRIPS_TABLE_NAME | ✅ | - | DynamoDB table name |
| KINESIS_STREAM_NAME | ✅ | - | Kinesis stream name |
| KPI_BUCKET_NAME | ✅ | - | S3 bucket for KPI output |
| AWS_REGION | ✅ | us-east-1 | AWS region |
| LOG_LEVEL | ❌ | INFO | Logging level |
| DLQ_URL | ❌ | - | SQS URL for Dead Letter Queue |
| TTL_DAYS | ❌ | 30 | Days before data expires |
| BATCH_SIZE | ❌ | 100 | Kinesis batch size |
| MAX_RETRIES | ❌ | 3 | Maximum retry attempts |
| VALIDATION_STRICT | ❌ | true | Strict validation mode |

### Lambda Configuration
- **Ingestion Lambda**: 512MB memory, 60s timeout, DLQ enabled
- **Matcher Lambda**: 256MB memory, 30s timeout
- **DLQ**: SQS queue for failed records

## Data Processing

### Validation Rules
- `trip_id`: Must be non-empty string
- `event_type`: Must be in allowed values
- `pickup_datetime`: Must be valid ISO 8601 timestamp
- `vendor_id`: Must be non-empty string

### Event Matching Logic
- **Trigger**: DynamoDB Streams event for new RAW item
- **Query**: Find matching `trip_begin`/`trip_end` events
- **Validation**: Ensure both events exist and are valid
- **Aggregation**: Calculate trip duration, distance, fare totals
- **Storage**: Write COMPLETED event with aggregated data

### Matching Criteria
- Same `trip_id`
- Valid `trip_begin` event exists
- Valid `trip_end` event exists
- Events within reasonable time window (< 24 hours)

### KPI Calculations
- **Trip-Level Metrics**: Duration, revenue, average speed, utilization
- **Aggregate Metrics**: Trips per hour, average fare, average duration, revenue per hour, geographic distribution

## Monitoring & Observability

### CloudWatch Metrics
- `EventsProcessed`: Count of events processed
- `ValidationErrors`: Count of validation failures
- `MatchingSuccessRate`: Percentage of successfully matched trips
- `ProcessingLatency`: Time from event to storage
- `KPIGenerationDuration`: Time to generate KPIs

### Logging Strategy
- **ERROR**: System failures, validation errors
- **WARN**: Data quality issues, retries
- **INFO**: Processing milestones, KPI generation
- **DEBUG**: Detailed processing information

### Structured Logging Example
```python
logger.info("Event processed", extra={
    "trip_id": trip_id,
    "event_type": event_type,
    "processing_time_ms": processing_time,
    "validation_status": "success"
})
```

### Alerting
- **Critical Alerts**: Lambda errors > 1%, DynamoDB throttling, Kinesis iterator age > 5 min, S3 upload failures
- **Warning Alerts**: Validation error rate > 5%, processing latency > 10s, unmatched events > 10%

## Troubleshooting

### Common Issues
- **Lambda Import Errors**: Ensure `src/` folder is at ZIP root and handler path is correct.
- **DynamoDB Validation Errors**: Verify schema, check for `None` in required fields, ensure `Decimal` types.
- **Kinesis Shard Iterator Errors**: Increase Lambda timeout, reduce batch size, add retry logic.
- **Memory Issues in Lambda**: Increase memory, optimize processing, use smaller batches.

### Debug Commands
**Check DynamoDB Items**
```bash
aws dynamodb scan \
  --table-name nsp_bolt_trips \
  --filter-expression "begins_with(sk, :sk_prefix)" \
  --expression-attribute-values '{":sk_prefix":{"S":"RAW#"}}'
```

**Monitor Kinesis Stream**
```bash
aws kinesis describe-stream --stream-name nsp-bolt-trip-events
aws kinesis get-shard-iterator --stream-name nsp-bolt-trip-events --shard-id shardId-000000000000 --shard-iterator-type LATEST
```

**Check Lambda Logs**
```bash
aws logs tail /aws/lambda/bolt-ride-ingestion --follow
aws logs filter-log-events --log-group-name /aws/lambda/bolt-ride-ingestion --filter-pattern "ERROR"
```

## Best Practices

### Development
- Use virtual environments for isolation
- Pin dependency versions in `requirements.txt`
- Implement comprehensive unit tests
- Use type hints for documentation
- Follow PEP 8 coding standards

### Deployment
- Use Infrastructure as Code (CloudFormation/CDK)
- Implement CI/CD pipelines
- Use separate environments (dev/staging/prod)
- Version Lambda functions
- Enable AWS X-Ray for tracing

### Data Management
- Always validate input data
- Implement idempotent processing
- Use DynamoDB TTL for cleanup
- Partition S3 data by date
- Implement data retention policies

## Performance Considerations

### Scaling Guidelines
- **Kinesis Shards**: 1 shard = up to 1,000 records/sec or 1 MB/sec; add shards as needed.
- **Lambda Concurrency**: Set reserved concurrency based on downstream capacity; default burst up to 1,000.
- **DynamoDB Scaling**: On-demand for auto-scaling; use partition keys to avoid hot partitions.

### Performance Optimization
- **Batch Processing**: Use concurrent processing in Lambda for high throughput.
- **Connection Pooling**: Reuse DynamoDB connections with increased pool size.

## Security

### IAM Roles and Policies
**Lambda Execution Role Example**
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
    },
    {
      "Effect": "Allow",
      "Action": [
        "sqs:SendMessage"
      ],
      "Resource": "arn:aws:sqs:us-east-1:your-account-id:bolt-ride-dlq"
    }
  ]
}
```

### Data Encryption
- **DynamoDB**: Server-side encryption enabled
- **Kinesis**: Server-side encryption with KMS
- **S3**: AES-256 encryption at rest
- **Lambda**: Environment variables encrypted

### Network Security
- Deploy Lambda in private VPC subnets
- Restrict traffic with Security Groups
- Use VPC Endpoints for private AWS service access
- Protect APIs with AWS WAF

## FAQ

### General
**Q: How long does it take to process an event?**  
A: Typically 100–500ms from Kinesis ingestion to DynamoDB storage, depending on validation complexity.

**Q: What happens if Lambda fails to process an event?**  
A: Events are retried up to 3 times, then sent to a Dead Letter Queue (DLQ) for manual investigation.

**Q: How much does this solution cost?**  
A: Costs vary by volume. For 1M events/month: ~$50–100 including all AWS services.

**Q: Can I modify the data schema?**  
A: Yes, but plan for backward compatibility. Use data versioning for schema evolution.

### Technical
**Q: Why use DynamoDB instead of RDS?**  
A: DynamoDB provides better scaling, lower latency, and is more cost-effective for high-volume event storage.

**Q: How do you handle duplicate events?**  
A: The combination of `trip_id` and timestamp in the sort key ensures natural deduplication.

**Q: Can I add new event types?**  
A: Yes, add them to the validation rules and update the matching logic accordingly.

**Q: How do I backup the data?**  
A: Enable DynamoDB Point-in-Time Recovery and use AWS Backup for automated backups.

### Operations
**Q: How do I monitor the pipeline health?**  
A: Use CloudWatch dashboards, set up alarms for key metrics, and monitor Lambda logs.

**Q: How do I handle a failed Glue job?**  
A: Check CloudWatch logs, verify IAM permissions, and ensure the script is accessible in S3.

**Q: Can I replay events?**  
A: Yes, use Kinesis shard iterators to replay from specific timestamps or sequence numbers.

**Q: How do I handle high traffic spikes?**  
A: The serverless architecture auto-scales, but monitor for throttling and adjust concurrency limits.