
from google.cloud import bigquery
import os

def gcs_to_bq_gross(event):
    print("Starting GCS to BigQuery gross data load...")
    
    project_id = "telco-metrics-473116"
    dataset_id = "telco_metrics_raw"
    #table_id = "customers"

    name = event.data["name"]                           # ex.: gross/gross_loja_2025-10-20.csv
    table_id = name.split("/")[-1][0:-15]
                 # gross_loja_2025-10-20.csv  --> gross_loja
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    print(table_id) 
    print(table_ref)

    client = bigquery.Client(project=project_id)

    data = event.data               # capture event data dictionary
    bucket = data["bucket"]         # ex.: telco-metrics-raw
    file_name = data["name"]        # ex.: gross/gross_2025-10-08.csv
    uri = f"gs://{bucket}/{file_name}"   # ex.: gs://telco-metrics-raw/gross/gross_loja_2025-10-08.csv
    
    print(f"Processing file: {uri}")
    
    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Assuming the first row is a header
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Append to existing table
        autodetect=True,  # Use existing destination table schema when appending
        ignore_unknown_values=True,
    )
    
    print(f"Loading data into {table_ref}...")
    #before_rows = client.get_table(table_ref).num_rows

    load_job = client.load_table_from_uri(
        uri,
        table_ref,
        job_config=job_config,
    )  

    load_job.result()  # Waits for the job to complete.
    
    print(f"Load job finished for {table_ref}.")
    #after_rows = client.get_table(table_ref).num_rows
    #loaded_rows = max(0, after_rows - before_rows)
    #print(f"Loaded {loaded_rows} rows into {table_ref}. Total rows now {after_rows}.")

    # Move the processed file to the LOADED folder
    folder = name.split("/")[0]               # ex.: gross
    os.system(f"gsutil mv gs://{bucket}/{file_name} gs://{bucket}/{folder}/LOADED/")
    print(f"Moved file gs://{bucket}/{file_name} to gs://{bucket}/{folder}/LOADED/")

    # telco-metrics-raw/gross/LOADED
