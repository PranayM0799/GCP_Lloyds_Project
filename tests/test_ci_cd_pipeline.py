import pytest

class Calculator:
    """A simple calculator class for demonstration purposes."""
    def add(self, a, b):
        return a + b

    def divide(self, a, b):
        if b == 0:
            raise ValueError("Cannot divide by zero")
        return a / b

@pytest.fixture
def calculator():
    """Fixture to provide a Calculator instance."""
    return Calculator()

@pytest.mark.parametrize(
    "a, b, expected",
    [
        (1, 2, 3),
        (0, 0, 0),
        (-1, 1, 0),
        (100, 200, 300),
    ]
)
def test_addition(calculator, a, b, expected):
    """Test the add method with various inputs."""
    assert calculator.add(a, b) == expected

def test_division(calculator):
    """Test the divide method with valid input."""
    assert calculator.divide(10, 2) == 5
    assert calculator.divide(-9, 3) == -3

def test_division_by_zero(calculator):
    """Test that dividing by zero raises a ValueError."""
    with pytest.raises(ValueError, match="Cannot divide by zero"):
        calculator.divide(5, 0)

def test_type_error_on_addition(calculator):
    """Test that adding non-numeric types raises a TypeError."""
    with pytest.raises(TypeError):
        calculator.add("a", 1)

def test_python_version():
    """Sanity check: Python version is at least 3.6."""
    import sys
    assert sys.version_info >= (3, 6)
