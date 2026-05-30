import hashlib
from typing import Dict, Any, Optional


class IRIResolver:
    """Deterministic IRI (Internationalized Resource Identifier) minting for entities"""

    def __init__(self, base_iri: str = "https://company.com"):
        self.base_iri = base_iri

    def mint_iri(
        self,
        entity_type: str,
        entity_dict: Dict[str, Any],
        key_fields: Optional[list] = None,
    ) -> str:
        """
        Mint a stable IRI for an entity.

        Args:
            entity_type: "customer", "invoice", "order", etc.
            entity_dict: entity properties
            key_fields: fields to use for deduplication (default: all fields)

        Returns:
            Stable IRI: "https://company.com/{entity_type}#{hash}"
        """
        if key_fields is None:
            key_fields = sorted(entity_dict.keys())

        # Normalize key values
        key_parts = []
        for field in key_fields:
            if field in entity_dict:
                value = str(entity_dict[field]).lower().strip()
                key_parts.append(value)

        # Create deterministic hash
        key_string = "|".join(key_parts)
        hash_value = hashlib.sha256(key_string.encode()).hexdigest()[:16]

        return f"{self.base_iri}/{entity_type}#{hash_value}"

    def normalize_email_domain(self, email: str) -> str:
        """Normalize email for deduplication"""
        return email.lower().strip()
