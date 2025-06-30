import pytest
from dataflow.pipeline import CleanCustomer, CleanTransaction
import subprocess

@pytest.mark.parametrize("element, expected_id, expected_name, expected_age", [
    ("C001,John,Male,5,1990-01-01,30,Engineer,IT,Affluent,N,Y,10.0,123 Main St,12345,CA,USA,500000", "C001", "John", 30),
    ("C004,,Female,2,1989-09-09,34,Manager,HR,Affluent,Y,N,3.0,404 Ave,30303,FL,USA,250000", "C004", "", 34),
])
def test_customer_parsing(element, expected_id, expected_name, expected_age):
    results = list(CleanCustomer().process(element))
    if not results:
        pytest.fail("No result returned from CleanCustomer")
    
    record = results[0]
    if record.get('customer_id') != expected_id:
        pytest.fail(f"Expected customer_id '{expected_id}', got '{record.get('customer_id')}'")
    if record.get('name') != expected_name:
        pytest.fail(f"Expected name '{expected_name}', got '{record.get('name')}'")
    if record.get('age') != expected_age:
        pytest.fail(f"Expected age '{expected_age}', got '{record.get('age')}'")

@pytest.mark.parametrize("element, expected_price", [
    ("T100,P200,C999,2023-12-01,FALSE,Pending,BrandX,LineX,ClassX,Small,199.99,120.00,2023-10-01", 199.99),
    ("T101,P201,C998,2023-11-01,TRUE,Completed,BrandY,LineY,ClassY,Medium,invalid,120.00,2023-09-01", None),
])
def test_transaction_price_parsing(element, expected_price):
    results = list(CleanTransaction().process(element))
    if not results:
        pytest.fail("No result returned from CleanTransaction")
    
    record = results[0]
    actual_price = record.get('list_price')
    if actual_price != expected_price:
        pytest.fail(f"Expected list_price '{expected_price}', got '{actual_price}'")

def test_customer_skips_invalid_entry():
    # Simulate malformed row
    element = "MALFORMED,ENTRY,ONLY,3,FIELDS"
    try:
        results = list(CleanCustomer().process(element))
        if results:
            pytest.fail("Malformed row should not produce a valid result")
    except Exception:
        pass  # Expected to raise or skip

def test_transaction_missing_fields():
    # Missing important fields like list_price
    element = "T105,P205,C995,2024-01-01,TRUE,Completed,BrandZ,,,,,,"
    results = list(CleanTransaction().process(element))
    if results and results[0].get("list_price") is not None:
        pytest.fail("Expected list_price to be None for missing fields")

@pytest.mark.skip(reason="Integration test requires GCP resources")
def test_pipeline_runs():
    """Test that the pipeline runs end-to-end with DirectRunner and sample data."""
    result = subprocess.run(
        [
            "python", "dataflow/pipeline.py",
            "--runner=DirectRunner",
            "--input=data/customer.csv",  # adjust if your pipeline expects different args
            "--output=output/test_output.csv"  # adjust as needed
        ],
        capture_output=True,
        text=True
    )
    assert result.returncode == 0, f"Pipeline failed: {result.stderr}"

def test_ci_cd_pipeline_health_check():
    """This test always passes and is used to verify CI/CD pipeline runs."""
    assert True

def test_clean_customer_valid():
    element = "C001,John,Male,5,1990-01-01,30,Engineer,IT,Affluent,N,Y,10.0,123 Main St,12345,CA,USA,500000"
    result = list(CleanCustomer().process(element))
    assert result[0]['customer_id'] == 'C001'
    assert result[0]['name'] == 'John'
    assert result[0]['age'] == 30

@mock.patch.dict(os.environ, {"BUCKET_NAME": "dummy-bucket"})
def test_function_that_uses_bucket():
    # This test will see BUCKET_NAME as "dummy-bucket"
    ...
