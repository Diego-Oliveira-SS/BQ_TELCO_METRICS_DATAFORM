# GCP Telco Metrics Workflows

This directory contains GitHub Actions workflows and Python scripts for generating and managing telco metrics data in Google Cloud Platform.

## Daily Workflows

### Daily Gross Data Generation (`gross.yml`)

This workflow runs the `Daily_Gross.py` script daily to generate customer data and upload it to Google Cloud Storage.

**Schedule**: Daily at 00:00 UTC

**Features**:
- Automatically runs daily via cron schedule
- Can be manually triggered via `workflow_dispatch`
- Generates customer data with random attributes (plans, locations, acquisition channels, etc.)
- Uploads CSV files to GCS bucket: `telco-metrics-raw/gross/`

**Required Secrets**:
- `GCP_SERVICE_ACCOUNT_KEY`: JSON key for a Google Cloud service account with permissions to:
  - Read from BigQuery dataset `telco_metrics_gold.customers`
  - Write to Cloud Storage bucket `telco-metrics-raw`

## Setup Instructions

1. **Create a Google Cloud Service Account**:
   ```bash
   gcloud iam service-accounts create github-actions \
     --display-name="GitHub Actions Service Account"
   ```

2. **Grant necessary permissions**:
   ```bash
   # BigQuery permissions
   gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
     --member="serviceAccount:github-actions@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
     --role="roles/bigquery.dataViewer"
   
   # Cloud Storage permissions
   gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
     --member="serviceAccount:github-actions@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
     --role="roles/storage.objectCreator"
   ```

3. **Create and download the JSON key**:
   ```bash
   gcloud iam service-accounts keys create key.json \
     --iam-account=github-actions@YOUR_PROJECT_ID.iam.gserviceaccount.com
   ```

4. **Add the secret to GitHub**:
   - Go to Repository Settings → Secrets and variables → Actions
   - Click "New repository secret"
   - Name: `GCP_SERVICE_ACCOUNT_KEY`
   - Value: Paste the entire contents of the `key.json` file

## Manual Triggering

To manually trigger the Daily Gross workflow:
1. Go to the "Actions" tab in the GitHub repository
2. Select "Daily Gross Data Generation" workflow
3. Click "Run workflow" button
4. Select the branch and click "Run workflow"

## Scripts

- `Daily_Gross.py`: Generates customer data with random attributes
- `Daily_Billing.py`: Generates billing data
- `Daily_Payments.py`: Generates payment data
- `Daily_ATH.py`: Generates ATH (Average Time to Handle) data
