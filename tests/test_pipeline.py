import pytest
from dataflow.pipeline import CleanCustomer, CleanTransaction

def test_clean_customer_valid():
    element = (
        "C001,John,Male,5,1990-01-01,30,Engineer,IT,Affluent,N,Y,10.0,123 Main St,12345,CA,USA,500000"
    )
    result = list(CleanCustomer().process(element))
    assert result[0]['customer_id'] == 'C001'
    assert result[0]['name'] == 'John'
    assert result[0]['age'] == 30
    assert result[0]['past_3_years_bike_related_purchases'] == 5

def test_clean_customer_invalid_age():
    element = (
        "C002,Alice,Female,3,1992-05-10,notanumber,Manager,Finance,Mass,N,N,8.0,456 Elm St,54321,NY,USA,300000"
    )
    result = list(CleanCustomer().process(element))
    assert result[0]['age'] is None

def test_clean_customer_missing_name():
    element = (
        "C003,,Male,2,1985-07-20,35,Analyst,IT,Affluent,N,Y,12.0,789 Oak St,67890,TX,USA,400000"
    )
    result = list(CleanCustomer().process(element))
    # Your CleanCustomer does not skip rows with missing name, so check for empty string
    assert result[0]['name'] == ''

def test_clean_transaction_valid():
    element = "T001,P001,C001,2024-01-01,TRUE,Completed,BrandA,LineA,ClassA,Large,123.45,100.00,2023-01-01"
    result = list(CleanTransaction().process(element))
    assert result[0]['transaction_id'] == 'T001'
    assert result[0]['list_price'] == 123.45

def test_clean_transaction_invalid_list_price():
    element = "T002,P002,C002,2024-01-01,TRUE,Completed,BrandB,LineB,ClassB,Medium,abc,100.00,2023-01-01"
    result = list(CleanTransaction().process(element))
    assert result[0]['list_price'] is None
