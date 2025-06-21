# GCP Data Engineering Pipeline

## Architecture
- GCS for file ingestion
- Dataflow (Apache Beam) for ETL
- BigQuery for analytics
- Terraform for Infra-as-Code
- GitHub Actions for CI

## Setup Instructions
1. Upload CSVs to GCS
2. Deploy infra with Terraform
3. Run Dataflow pipeline
4. Create BQ views from SQL
5. View analytics on BigQuery

## Improvements
- Add Airflow DAG
- Add real-time ingestion with Pub/Sub
