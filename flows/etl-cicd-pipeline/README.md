# ETL CI/CD Pipeline Example

This example demonstrates how to build an ETL (Extract, Transform, Load) pipeline with CI/CD integration using Prefect.
## GitHub Repository Secrets

For the CI/CD pipeline to work properly, you need to configure the following secrets in your GitHub repository settings (Settings > Secrets and variables > Actions):




## CI/CD Pipeline

This example uses GitHub Actions to automate the deployment process. The CI/CD pipeline:

1. Triggers on pushes to the main branch
2. Builds and tests the ETL pipeline code
3. Creates a Prefect deployment using the Docker infrastructure
4. Pushes the Docker image to Docker Hub
5. Updates the deployment in Prefect Cloud

The workflow is defined in `.github/workflows/deploy.yml` and follows the [Prefect CI/CD deployment guide](https://docs-3.prefect.io/v3/deploy/infrastructure-concepts/deploy-ci-cd#build-deployments-via-ci-cd). 

The marketplace doc is located here:[Prefect actions deploy](https://github.com/marketplace/actions/deploy-a-prefect-flow)

### GitHub Actions Workflow

The pipeline uses these key steps:

1. Create a new deployment in Prefect Cloud
2. Configure the deployment to use the `etl_cicd_pipeline` flow
3. Configure the deployment to use the `etl-cicd-pipeline` environment
4. Configure the deployment to use the `etl-cicd-pipeline` workpool
5. Configure the deployment to use the `etl-cicd-pipeline` schedule

### Required Secrets
```
# Prefect Authentication
PREFECT_API_KEY=your-prefect-cloud-api-key
PREFECT_API_URL=https://api.prefect.cloud/api/accounts/{ACCOUNT_ID}/workspaces/{WORKSPACE_ID}
PREFECT_WORKSPACE=your-prefect-workspace

# Docker Hub Authentication
DOCKER_USERNAME=your-dockerhub-username
DOCKER_PASSWORD=your-dockerhub-password

```

### Setting Up Secrets

1. Navigate to your GitHub repository
2. Go to Settings > Secrets and variables > Actions
3. Click "New repository secret"
4. Add each secret with its corresponding value

### Security Notes
- Never commit these secrets directly to your repository
- Rotate credentials periodically
- Use least-privilege access principles when creating service accounts
- Consider using GitHub's OIDC provider for AWS authentication in production


## Overview

The pipeline performs the following steps:

1. Extracts data from a GitHub repository URL provided via user input
2. Uploads raw data to S3
3. Flattens the JSON data structure 
4. Validates the data format
5. Cleans and processes the data
6. Performs data aggregation
7. Saves final results to S3
8. Sends email notification upon completion

## Requirements

- Python 3.12+
- Prefect Cloud account
- AWS S3 bucket
- Docker Hub account
- GitHub repository

## Environment Variables

The following environment variables need to be configured:

- `PREFECT_API_KEY`: Prefect API key
- `PREFECT_WORKSPACE`: Prefect workspace name
- `AWS_ACCESS_KEY_ID`: AWS access key ID
- `AWS_SECRET_ACCESS_KEY`: AWS secret access key
- `AWS_REGION`: AWS region
- `S3_BUCKET_NAME`: Name of the S3 bucket
- `EMAIL_SERVER_CREDENTIALS`: Email server credentials

## Setup

1. Clone the repository

```bash
git clone https://github.com/your-repo/etl-cicd-pipeline.git
cd etl-cicd-pipeline
```

2. Install dependencies

```bash
pip install -r requirements.txt
```

3. Configure environment variables

```bash
cp .env.example .env
```

4. Run the pipeline

```bash
python full-example-etl.py
```


## Monitoring

The pipeline is configured to send email notifications upon completion. The following steps are required to set up the monitoring system:

1. Configure the email server credentials in the `.env` file
