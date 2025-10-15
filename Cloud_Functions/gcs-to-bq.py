from google.cloud import bigquery
from google.api_core.exceptions import NotFound


# Set table and GCS information
project_id = "telco-metrics-473116"
dataset_id = "telco_metrics_gold"
table_id = "customers"
gcs_uri = "gs://telco-metrics-raw/gross/gross_2025-10-08.csv"

table_ref = f"{project_id}.{dataset_id}.{table_id}"
client = bigquery.Client(project=project_id)

# Determine dataset location to keep job and lookups in the same region
dataset = client.get_dataset(f"{project_id}.{dataset_id}")
location = dataset.location

# Configure the load job
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,  # Assuming the first row is a header
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Append to existing table
    autodetect=False,  # Use existing destination table schema when appending
    ignore_unknown_values=True,
)

# Capture current row count to compute delta after load
try:
    before_rows = client.get_table(table_ref).num_rows
except NotFound:
    before_rows = 0

# Start the load job
load_job = client.load_table_from_uri(
    gcs_uri,
    table_ref,
    job_config=job_config,
    location=location,
)

print(f"Starting job {load_job.job_id}")

# Wait for the job to complete
job_result = load_job.result()

# Check the results
print(f"Job {load_job.job_id} finished.")
table = client.get_table(table_ref)
after_rows = table.num_rows
loaded_rows = getattr(job_result, "output_rows", None)
if loaded_rows is None:
    loaded_rows = max(0, after_rows - before_rows)
print(f"Loaded {loaded_rows} rows into {table_ref}. Total rows now {after_rows}.")

