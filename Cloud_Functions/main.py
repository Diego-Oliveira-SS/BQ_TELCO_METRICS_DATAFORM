import functions_framework
from google.cloud import bigquery
from gross import gcs_to_bq_gross
from billing import gcs_to_bq_billing
from payments import gcs_to_bq_payments
from ath import gcs_to_bq_ath

@functions_framework.cloud_event    # Create a Cloud Event function

def start_workflow(event):          # Entry point for the Cloud Function
    if event.data["name"].startswith("gross/gross"):
        gcs_to_bq_gross(event)
    elif event.data["name"].startswith("billing/billing"):
        gcs_to_bq_billing(event)
    elif event.data["name"].startswith("payments/payments"):
        gcs_to_bq_payments(event)
    elif event.data["name"].startswith("ath/ath"):
        gcs_to_bq_ath(event)
