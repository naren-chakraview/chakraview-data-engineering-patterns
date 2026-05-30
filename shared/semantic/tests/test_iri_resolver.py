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
