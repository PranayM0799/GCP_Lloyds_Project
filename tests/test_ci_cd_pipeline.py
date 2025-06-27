import pytest

def test_ci_cd_pipeline_check():
    """Simple test to verify CI/CD pipeline is working"""
    assert True

def test_basic_math():
    """Basic math test to ensure pytest is working"""
    assert 2 + 2 == 4
    assert 10 - 5 == 5
    assert 3 * 4 == 12

def test_string_operations():
    """Test string operations"""
    name = "CI/CD Pipeline"
    assert len(name) == 15
    assert "Pipeline" in name
    assert name.upper() == "CI/CD PIPELINE"

def test_list_operations():
    """Test list operations"""
    numbers = [1, 2, 3, 4, 5]
    assert len(numbers) == 5
    assert sum(numbers) == 15
    assert max(numbers) == 5
    assert min(numbers) == 1

def test_dictionary_operations():
    """Test dictionary operations"""
    config = {
        "project": "GCP Data Pipeline",
        "language": "Python",
        "framework": "Apache Beam"
    }
    assert len(config) == 3
    assert config["project"] == "GCP Data Pipeline"
    assert "Python" in config.values()

def test_boolean_logic():
    """Test boolean logic"""
    assert True is True
    assert False is False
    assert not False is True
    assert True and True is True
    assert True or False is True

if __name__ == "__main__":
    # This allows running the file directly
    print("Running CI/CD pipeline verification tests...")
    test_ci_cd_pipeline_check()
    test_basic_math()
    test_string_operations()
    test_list_operations()
    test_dictionary_operations()
    test_boolean_logic()
    print("All tests passed! CI/CD pipeline is working correctly.")