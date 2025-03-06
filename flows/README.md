# ETL Flow Examples

This directory contains examples of different ETL (Extract, Transform, Load) workflows implemented using Prefect.

## Examples

### 1. ETL CI/CD Pipeline
Located in `/etl-cicd-pipeline`

A complete production-ready ETL pipeline that demonstrates:
- GitHub API data extraction
- AWS S3 integration for data storage
- Data validation and cleaning
- Automated deployment via GitHub Actions
- Email notifications
- Error handling and retries
- Environment configuration management

Key features:
- Uses Docker infrastructure
- Includes CI/CD pipeline configuration
- Configurable via environment variables
- Production-ready monitoring setup

The pipeline extracts repository data from GitHub, processes it through multiple transformation stages, and loads it into S3 buckets. It includes comprehensive error handling and notification systems.

### 2. ETL S3 Upload Pipeline
Located in `/etl-s3-upload-pipeline`

A streamlined ETL pipeline demonstrating two implementation approaches:

#### Full Prefect Integration (`full-example-etl.py`)
- Uses Prefect's complete feature set
- Prefect Blocks for AWS credentials
- Built-in S3 integration with `prefect-aws`
- Email notifications using `prefect-email`
- Human-in-the-loop capabilities
- Automatic retries and error handling
- Flow run tracking and observability

#### Orchestration Only (`simple-example-etl.py`)
- Lightweight implementation using basic Prefect features
- Direct boto3 integration with AWS
- Simpler configuration management
- Minimal dependencies
- More control over AWS interactions
- Basic error handling and logging

## Getting Started

Each example contains:
- Detailed README with setup instructions
- Environment configuration examples
- Complete source code
- Deployment instructions

To use any example:
1. Navigate to the specific example directory
2. Follow the README instructions
3. Configure the required environment variables
4. Deploy using the provided scripts

## Prerequisites

### General Requirements
- Python 3.12+
- AWS account and credentials
- Prefect installation (`pip install prefect`)

### CI/CD Pipeline Additional Requirements
- Prefect Cloud account
- Docker
- GitHub account with repository access
- GitHub Actions enabled

### S3 Upload Pipeline Additional Requirements
- `prefect-aws` for full integration example
- `boto3` for simple example
- `pandas` for data processing
- `pydantic` for data validation

## Environment Configuration

### CI/CD Pipeline
```env
AWS_ACCESS_KEY_ID=your-aws-access-key
AWS_SECRET_ACCESS_KEY=your-aws-secret-key
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET_NAME=your-bucket-name
NOTIFICATION_EMAIL=alerts@example.com
```

### S3 Upload Pipeline
```env
AWS_ACCESS_KEY_ID=your-aws-access-key
AWS_SECRET_ACCESS_KEY=your-aws-secret-key
AWS_REGION=us-east-1
S3_BUCKET_NAME=your-bucket-name
```

## Pipeline Flow Structure

Both pipelines follow a similar ETL pattern:
1. Extract data from source
2. Store raw data in S3
3. Transform and validate data
4. Clean and structure data
5. Perform aggregations
6. Load final results to S3

The main difference is in the implementation approach and additional features included in each version.
