import pytest
from unittest.mock import Mock

@pytest.fixture
def kafka_client():
    return Mock()

@pytest.fixture
def jena_client():
    return Mock()

@pytest.fixture
def trino_client():
    return Mock()

@pytest.fixture
def spark_session():
    return Mock()

@pytest.fixture
def sample_data():
    return {
        "customers": [{"id": "c1", "name": "Acme", "email": "acme@example.com"}],
        "invoices": [{"id": "i1", "customer_id": "c1", "amount": 100}]
    }
