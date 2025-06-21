import pytest
from dataflow.pipeline import CleanCustomer, CleanTransaction

def test_clean_customer_valid():
    element = "customer_id,name,age\nC001,John,30"
    result = list(CleanCustomer().process(element))
    assert result[0]['customer_id'] == 'C001'
    assert result[0]['name'] == 'John'
    assert result[0]['age'] == 30

def test_clean_customer_invalid_age():
    element = "customer_id,name,age\nC002,Alice,notanumber"
    result = list(CleanCustomer().process(element))
    assert result[0]['age'] is None

def test_clean_customer_missing_name():
    element = "customer_id,name,age\nC003,,25"
    result = list(CleanCustomer().process(element))
    assert result == []

def test_clean_transaction_valid():
    element = "transaction_id,customer_id,amount,timestamp\nT001,C001,123.45,2024-01-01T10:00:00"
    result = list(CleanTransaction().process(element))
    assert result[0]['transaction_id'] == 'T001'
    assert result[0]['amount'] == 123.45

def test_clean_transaction_invalid_amount():
    element = "transaction_id,customer_id,amount,timestamp\nT002,C001,abc,2024-01-01T10:00:00"
    result = list(CleanTransaction().process(element))
    assert result == []
