import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import csv

class CleanCustomer(beam.DoFn):
    def process(self, element):
        row = next(csv.DictReader([element]))
        if row['customer_id'] and row['name']:
            row['age'] = int(row['age']) if row['age'].isdigit() else None
            yield row

class CleanTransaction(beam.DoFn):
    def process(self, element):
        row = next(csv.DictReader([element]))
        try:
            row['amount'] = float(row['amount'])
            yield row
        except:
            pass

def run():
    # Read required environment variables
    BUCKET_NAME = os.environ.get("BUCKET_NAME")
    PROJECT_ID = os.environ.get("PROJECT_ID")

    # Validate they are set
    if not BUCKET_NAME or not PROJECT_ID:
        raise ValueError("Environment variables BUCKET_NAME and PROJECT_ID must be set")

    # Construct GCS paths and temp directory
    CUSTOMERS_PATH = f"gs://{BUCKET_NAME}/customers.csv"
    TRANSACTIONS_PATH = f"gs://{BUCKET_NAME}/transactions.csv"
    TEMP_LOCATION = f"gs://{BUCKET_NAME}/tmp"

    # Define Dataflow options
    options = PipelineOptions([
        "--runner=DataflowRunner",
        f"--project={PROJECT_ID}",
        f"--temp_location={TEMP_LOCATION}",
        "--region=us-central1"
    ])

    with beam.Pipeline(options=options) as p:
        # Customer Pipeline
        customers = (
            p | "ReadCustomers" >> beam.io.ReadFromText(CUSTOMERS_PATH, skip_header_lines=1)
              | "CleanCustomers" >> beam.ParDo(CleanCustomer())
              | "WriteCustomersBQ" >> beam.io.WriteToBigQuery(
                    table=f"{PROJECT_ID}:customer_analytics.customers",
                    schema='customer_id:STRING,name:STRING,age:INTEGER',
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                )
        )

        # Transactions Pipeline
        transactions = (
            p | "ReadTransactions" >> beam.io.ReadFromText(TRANSACTIONS_PATH, skip_header_lines=1)
              | "CleanTransactions" >> beam.ParDo(CleanTransaction())
              | "WriteTransactionsBQ" >> beam.io.WriteToBigQuery(
                    table=f"{PROJECT_ID}:customer_analytics.transactions",
                    schema='transaction_id:STRING,customer_id:STRING,amount:FLOAT,timestamp:TIMESTAMP',
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                )
        )

if __name__ == '__main__':
    run()
