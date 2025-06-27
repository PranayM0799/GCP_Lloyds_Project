import csv
import json
import logging
import os
import time
from typing import Iterator, Dict

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# ─────────────────────────────────────────────────────────────────────────────
# 0.  Cancel any previous ACTIVE job with the same prefix
# ─────────────────────────────────────────────────────────────────────────────
def stop_previous_jobs(project_id: str, region: str, job_prefix: str) -> None:
    """Cancel active Dataflow jobs that start with job_prefix."""
    from google.auth import default
    from googleapiclient.discovery import build

    creds, _ = default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    dataflow = build("dataflow", "v1b3", credentials=creds, cache_discovery=False)

    # Stop both ACTIVE and DONE (successful) jobs with the same prefix
    req = dataflow.projects().locations().jobs().list(
        projectId=project_id, location=region, filter="ALL"
    )
    resp = req.execute()
    for job in resp.get("jobs", []):
        if job["name"].startswith(job_prefix) and job["currentState"] in ["JOB_STATE_RUNNING", "JOB_STATE_PENDING", "JOB_STATE_DONE"]:
            job_id = job["id"]
            logging.info("Cancelling previous job %s (%s) with state %s", job_id, job["name"], job["currentState"])
            dataflow.projects().locations().jobs().update(
                projectId=project_id,
                location=region,
                jobId=job_id,
                body={"requestedState": "JOB_STATE_CANCELLED"},
            ).execute()

# ─────────────────────────────────────────────────────────────────────────────
# 1.  Beam DoFns
# ─────────────────────────────────────────────────────────────────────────────
class CleanCustomer(beam.DoFn):
    """Validate / type-cast customer rows."""

    def process(self, element: str) -> Iterator[Dict]:
        reader = csv.DictReader([element], fieldnames=[
            "customer_id","name","gender","past_3_years_bike_related_purchases","DOB","age","job_title","job_industry_category","wealth_segment","deceased_indicator","owns_car","tenure","address","postcode","state","country","property_valuation"
        ])
        for row in reader:
            # Skip header row if present
            if row["customer_id"] == "customer_id":
                continue
            # Type-cast fields
            row["age"] = int(row["age"]) if row["age"] and row["age"].isdigit() else None
            row["past_3_years_bike_related_purchases"] = int(row["past_3_years_bike_related_purchases"]) if row["past_3_years_bike_related_purchases"] and row["past_3_years_bike_related_purchases"].isdigit() else None
            for field in ["tenure", "postcode", "property_valuation"]:
                try:
                    row[field] = float(row[field]) if row[field] else None
                except Exception:
                    row[field] = None
            print("Customer row:", row)  # Debug print
            yield row


class CleanTransaction(beam.DoFn):
    """Validate / type-cast transaction rows."""

    def process(self, element: str) -> Iterator[Dict]:
        reader = csv.DictReader([element], fieldnames=[
            "transaction_id","product_id","customer_id","transaction_date","online_order","order_status","brand","product_line","product_class","product_size","list_price","standard_cost","product_first_sold_date"
        ])
        for row in reader:
            # Skip header row if present
            if row["transaction_id"] == "transaction_id":
                continue
            for field in ["list_price", "standard_cost"]:
                try:
                    row[field] = float(row[field]) if row[field] else None
                except Exception:
                    row[field] = None
            print("Transaction row:", row)  # Debug print
            yield row


# ─────────────────────────────────────────────────────────────────────────────
# 2.  Main runner
# ─────────────────────────────────────────────────────────────────────────────
def run() -> None:
    # 2-A  Environment safety guard
    for v in ("BUCKET_NAME", "PROJECT_ID"):
        if v not in os.environ:
            raise RuntimeError(f"Environment variable {v} must be set")
    BUCKET = os.environ["BUCKET_NAME"]
    PROJECT = os.environ["PROJECT_ID"]
    REGION = "us-central1"  # change to europe-west2 if that's where your bucket lives

    # 2-B  Cancel previous job if still running
    JOB_PREFIX = "customer-load"
    stop_previous_jobs(PROJECT, REGION, JOB_PREFIX)

    # 2-C  Unique job name
    job_name = f"{JOB_PREFIX}-{int(time.time())}"

    # 2-D  Paths
    CUSTOMERS_PATH = f"gs://{BUCKET}/customer.csv"
    TRANSACTIONS_PATH = f"gs://{BUCKET}/transactions.csv"
    TEMP_LOCATION = f"gs://{BUCKET}/tmp"

    # 2-E  Beam / Dataflow options
    options = PipelineOptions(
        runner="DataflowRunner",
        project=PROJECT,
        region=REGION,
        temp_location=TEMP_LOCATION,
        staging_location=TEMP_LOCATION,
        job_name=job_name,
        save_main_session=True,
    )

    # 2-F  BigQuery schemas
    CUSTOMERS_SCHEMA = (
        "customer_id:STRING,name:STRING,gender:STRING,past_3_years_bike_related_purchases:INTEGER,DOB:STRING,age:INTEGER,job_title:STRING,job_industry_category:STRING,wealth_segment:STRING,deceased_indicator:STRING,owns_car:STRING,tenure:FLOAT,address:STRING,postcode:FLOAT,state:STRING,country:STRING,property_valuation:FLOAT"
    )
    TRANSACTIONS_SCHEMA = (
        "transaction_id:STRING,product_id:STRING,customer_id:STRING,transaction_date:STRING,online_order:STRING,order_status:STRING,brand:STRING,product_line:STRING,product_class:STRING,product_size:STRING,list_price:FLOAT,standard_cost:FLOAT,product_first_sold_date:STRING"
    )

    # 2-G  Run pipeline
    logging.info("Launching Dataflow job %s …", job_name)
    with beam.Pipeline(options=options) as p:
        # Customers
        (
            p
            | "Read Customers" >> beam.io.ReadFromText(CUSTOMERS_PATH, skip_header_lines=1)
            | "Clean Customers" >> beam.ParDo(CleanCustomer())
            | "Write Customers" >> beam.io.WriteToBigQuery(
                table=f"{PROJECT}:customer_analytics.customers",
                schema=CUSTOMERS_SCHEMA,
                method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
                custom_gcs_temp_location=TEMP_LOCATION,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

        # Transactions
        (
            p
            | "Read Transactions" >> beam.io.ReadFromText(TRANSACTIONS_PATH, skip_header_lines=1)
            | "Clean Transactions" >> beam.ParDo(CleanTransaction())
            | "Write Transactions" >> beam.io.WriteToBigQuery(
                table=f"{PROJECT}:customer_analytics.transactions",
                schema=TRANSACTIONS_SCHEMA,
                method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
                custom_gcs_temp_location=TEMP_LOCATION,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

    logging.info("Job %s submitted.", job_name)


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    run()
