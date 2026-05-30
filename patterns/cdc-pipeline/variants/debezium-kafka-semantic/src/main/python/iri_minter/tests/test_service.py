import pytest
import json
from unittest.mock import Mock, patch
from iri_minter.service import IRIMinterService
from entity_resolver.resolver import DeduplicationCache, EntityResolver


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


# Error case tests


def test_enrich_record_invalid_type():
    """Enrich should reject non-dict records"""
    config = {"base_iri": "https://company.com", "entity_rules": {}}
    service = IRIMinterService(config)

    with pytest.raises(TypeError):
        service.enrich_record("customer", "not a dict")


def test_enrich_record_missing_key_fields():
    """Enrich should raise error if key_fields is empty list"""
    config = {
        "base_iri": "https://company.com",
        "entity_rules": {
            "customer": {"key_fields": []}  # Explicitly empty
        }
    }
    service = IRIMinterService(config)

    with pytest.raises(ValueError):
        service.enrich_record("customer", {"name": "test"})


def test_process_kafka_record_empty_payload():
    """Process should handle empty payload gracefully"""
    config = {"base_iri": "https://company.com", "entity_rules": {}}
    service = IRIMinterService(config)

    result = service.process_kafka_record({"value": {}})
    assert result is None


def test_process_kafka_record_unknown_topic():
    """Process should handle unknown topics gracefully"""
    config = {"base_iri": "https://company.com", "entity_rules": {}}
    service = IRIMinterService(config)

    result = service.process_kafka_record({
        "topic": "unknown.topic",
        "value": {"after": {"id": "123"}}
    })
    assert result is None


# EntityResolver tests


def test_entity_resolver_deduplicate():
    """EntityResolver should match duplicate records"""
    rules = {
        "customer": {"match_fields": ["email", "domain"]}
    }
    resolver = EntityResolver(rules)

    record = {"email": "alice@acme.com", "domain": "acme.com"}
    existing = [{"email": "ALICE@ACME.COM", "domain": "ACME.COM", "iri": "https://company.com/customer#123"}]

    matched_iri = resolver.deduplicate("customer", record, existing)
    assert matched_iri == "https://company.com/customer#123"


# DeduplicationCache tests


def test_deduplication_cache_lru():
    """Cache should evict oldest entries when full"""
    cache = DeduplicationCache(max_size=2)
    cache.put_iri("customer", "key1", "iri1")
    cache.put_iri("customer", "key2", "iri2")
    cache.put_iri("customer", "key3", "iri3")  # Should evict key1

    assert cache.get_iri("customer", "key1") is None
    assert cache.get_iri("customer", "key2") is not None
    assert cache.get_iri("customer", "key3") is not None


def test_deduplication_cache_invalid_size():
    """Cache should reject invalid max_size"""
    with pytest.raises(ValueError):
        DeduplicationCache(max_size=0)
