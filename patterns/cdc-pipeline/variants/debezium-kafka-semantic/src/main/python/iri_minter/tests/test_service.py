import pytest
import json
from unittest.mock import Mock, patch
from iri_minter.service import IRIMinterService


def test_enrich_record_with_iri():
    """Service should add IRI to CDC record"""
    config = {
        "base_iri": "https://company.com",
        "entity_rules": {
            "customer": {
                "key_fields": ["email", "domain"],
                "source_topic": "postgres.public.customers"
            }
        }
    }

    service = IRIMinterService(config)
    record = {
        "email": "alice@acme.com",
        "domain": "acme.com",
        "name": "Alice"
    }

    enriched = service.enrich_record("customer", record)

    assert "iri" in enriched
    assert enriched["iri"].startswith("https://company.com/customer#")
    assert enriched["email"] == "alice@acme.com"


def test_process_kafka_record_from_postgres():
    """Service should process Debezium CDC records"""
    config = {
        "base_iri": "https://company.com",
        "entity_rules": {"customer": {"key_fields": ["email"]}}
    }
    service = IRIMinterService(config)

    kafka_record = {
        "topic": "postgres.public.customers",
        "key": "123",
        "value": {
            "after": {"email": "bob@stripe.com", "name": "Bob"}
        }
    }

    enriched = service.process_kafka_record(kafka_record)

    assert enriched is not None
    assert enriched["iri"].startswith("https://company.com/customer#")
    assert enriched["_entity_type"] == "customer"
