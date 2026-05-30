import pytest
from semantic.iri_minting.resolver import IRIResolver


def test_mint_customer_iri():
    """IRI should be deterministic for same entity"""
    resolver = IRIResolver(base_iri="https://company.com")

    iri1 = resolver.mint_iri("customer", {"email": "alice@acme.com", "domain": "acme.com"})
    iri2 = resolver.mint_iri("customer", {"email": "alice@acme.com", "domain": "acme.com"})

    assert iri1 == iri2
    assert iri1.startswith("https://company.com/customer#")


def test_different_entities_different_iris():
    """Different entities should get different IRIs"""
    resolver = IRIResolver(base_iri="https://company.com")

    iri1 = resolver.mint_iri("customer", {"email": "alice@acme.com", "domain": "acme.com"})
    iri2 = resolver.mint_iri("customer", {"email": "bob@stripe.com", "domain": "stripe.com"})

    assert iri1 != iri2


def test_normalize_email_domain():
    """normalize_email_domain should lowercase and strip whitespace"""
    resolver = IRIResolver()

    assert resolver.normalize_email_domain("ALICE@ACME.COM") == "alice@acme.com"
    assert resolver.normalize_email_domain("  bob@stripe.com  ") == "bob@stripe.com"


def test_custom_base_iri():
    """Custom base_iri should be used in generated IRIs"""
    resolver = IRIResolver(base_iri="https://example.org")

    iri = resolver.mint_iri("customer", {"email": "test@test.com"})
    assert iri.startswith("https://example.org/customer#")


def test_custom_key_fields():
    """Custom key_fields should use only specified fields for hash"""
    resolver = IRIResolver()

    record1 = {"email": "alice@acme.com", "name": "Alice", "age": 30}
    record2 = {"email": "alice@acme.com", "name": "ALICE", "age": 31}  # name + age differ

    # With key_fields=["email"], both should generate same IRI
    iri1 = resolver.mint_iri("customer", record1, key_fields=["email"])
    iri2 = resolver.mint_iri("customer", record2, key_fields=["email"])
    assert iri1 == iri2

    # With all fields, they should differ
    iri3 = resolver.mint_iri("customer", record1)
    iri4 = resolver.mint_iri("customer", record2)
    assert iri3 != iri4
