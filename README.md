# GCP Data Engineering Pipeline – Customer Transaction Analytics

## Scenario

You've joined a data engineering team at a large financial institution that is migrating its data platform to Google Cloud. Your task is to help prototype a scalable and secure data pipeline that ingests, processes, and exposes data for analytics. Business stakeholders want to understand customer transaction behaviour over time.

---

## Architecture Overview

### High-Level Architecture
![High Level Architecture](High%20Level.png)

### Low-Level Architecture
![Low Level Architecture](Low%20Level.png)

**Components:**
- **GCS Buckets:** Store raw CSV data.
- **Dataflow (Apache Beam):** Ingests, cleans, and transforms data.
- **BigQuery:** Stores both staging and analytics tables/views.
- **Terraform:** Infrastructure as Code for provisioning GCP resources.
- **CI/CD:** Automated testing and deployment via GitHub Actions.

---

## Setup Instructions

### 1. GCP Permissions
- Ensure you have a GCP project with permissions to use GCS, Dataflow, and BigQuery.
- Enable the required APIs: Storage, Dataflow, BigQuery.

### 2. Infrastructure Provisioning (Terraform)
**Edit variables if needed:**
- Open `terraform/variables.tf` and set your `project_id`, `region`, and `dataflow_sa` (service account email for Dataflow).

**Deploy infrastructure:**
```bash
cd terraform
terraform init
terraform apply
```
This will create:
- A GCS bucket for raw/staged data: `<project_id>-data-lake`
- A BigQuery dataset: `customer_analytics`
- Assign the Dataflow worker role to your service account

### 3. Data Pipeline (Dataflow)
- Place your raw CSV files in the provisioned GCS bucket (or use the `data/` directory for local testing).
- Update any required environment variables (GCP project, bucket names, dataset/table names).
- Run the pipeline:
```bash
cd dataflow
python pipeline.py --runner DataflowRunner --project <your-gcp-project> --temp_location gs://<your-bucket>/temp --region <region>
```
- For local testing, use `DirectRunner`.

### 4. Analytics & SQL
- Use the provided SQL scripts in the `sql/` directory to create analytics tables/views in BigQuery.
- Example: Monthly spend, top 5% customers by LTV, etc.

### 5. Testing & CI/CD
- Tests are in the `tests/` directory.
- Run locally with:
  ```bash
  pytest tests/
  ```
- CI/CD is set up via GitHub Actions and runs on every push.

### 6. IAM Permissions

The Terraform scripts grant the following IAM permissions:

- **roles/dataflow.worker**:  
  Assigned to the Dataflow service account (`dataflow_sa`). This allows the service account to execute Dataflow jobs, access GCS buckets, and write to BigQuery.

**Why were only these permissions chosen?**
- The Dataflow pipeline is the core processing engine in this project. It requires the `roles/dataflow.worker` role to:
  - Launch and manage Dataflow jobs
  - Read input data from GCS
  - Write output data to BigQuery
- No additional custom IAM roles are provisioned for GCS or BigQuery because:
  - The default GCP project permissions are sufficient for this prototype and for the service account to interact with these resources via Dataflow.
  - This approach follows the principle of least privilege, granting only the minimum permissions required for the pipeline to function securely.

**Note:**
- You must provide the email of the Dataflow service account as the `dataflow_sa` variable in `terraform/variables.tf` or your Terraform command.
- Default GCP project permissions apply for BigQuery and GCS unless you customize them further.

---

## Assumptions

- Input data is well-formed CSV and matches the expected schema.
- GCP project and billing are set up.
- Terraform state is managed locally (for demo purposes).
- Dataflow and BigQuery quotas are sufficient for prototype workloads.

---

## Future Improvements

- Add data validation and error handling for edge cases.
- Parameterize pipeline for different data sources/formats.
- Add monitoring and alerting (Stackdriver, Dataflow/BigQuery metrics).
- Implement data retention and archival policies.
- Enhance CI/CD with deployment to staging/production environments.
- Add more analytics and reporting features.

---

## Extras

### GitHub Actions: CI/CD Workflow Example

To automatically run your CI/CD health check on every push or pull request, add the following workflow file:

Create a file at `.github/workflows/ci.yml` with this content:

```yaml
name: CI/CD Health Check

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

jobs:
  health-check:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.10'

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install pytest

      - name: Run CI/CD health check test
        run: pytest tests/CI_CD_pipeline_test.py
```

This workflow will:
- Run on every push and pull request to the `main` branch.
- Set up Python, install pytest, and run your health check test file.

---

### Useful Commands

#### Run the Dataflow Pipeline Locally
```bash
cd dataflow
python pipeline.py --runner DirectRunner
```

#### Run the Dataflow Pipeline on GCP Dataflow
```bash
cd dataflow
python pipeline.py --runner DataflowRunner --project <your-gcp-project> --temp_location gs://<your-bucket>/temp --region <region>
```

#### Run All Tests
```bash
pytest tests/
```

#### Run Only the CI/CD Health Check Test
```bash
pytest tests/CI_CD_pipeline_test.py
```

#### Apply Terraform Infrastructure
```bash
cd terraform
terraform init
terraform apply
```

#### Run a SQL Script in BigQuery Console
- Open the BigQuery Console in your browser.
- Copy the contents of any `.sql` file from the `sql/` directory and paste it into the query editor.
- Click "Run" to execute.
