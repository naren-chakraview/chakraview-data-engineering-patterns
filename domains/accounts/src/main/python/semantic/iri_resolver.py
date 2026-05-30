"""IRI resolver for deterministic customer and account identification"""
import hashlib
from typing import Dict, Any


class IriResolver:
    """Deterministic IRI minting for Accounts domain entities"""

    def __init__(self, iri_base_url: str = "https://chakracommerce.com"):
        """
        Initialize IRI resolver with base URL.

        Args:
            iri_base_url: Base URL for all minted IRIs (default: https://chakracommerce.com)
        """
        self.iri_base_url = iri_base_url

    def mint_customer_iri(self, email: str, kyc_id: str) -> str:
        """
        Mint stable IRI for customer using email + KYC ID.

        Same email + kyc_id always produces the same IRI (deterministic).
        This enables cross-domain linking and deduplication.

        Normalization:
        - Lowercase both email and kyc_id
        - Trim whitespace
        - Hash with SHA-256, take first 8 chars

        Args:
            email: Customer email address
            kyc_id: Customer KYC identifier

        Returns:
            Stable IRI string, e.g., https://chakracommerce.com/customer#abc12345
        """
        # Normalize inputs for deterministic hashing
        normalized = f"{email.lower().strip()}|{kyc_id.lower().strip()}"
        # Hash to create deterministic identifier
        hash_id = hashlib.sha256(normalized.encode()).hexdigest()[:8]
        return f"{self.iri_base_url}/customer#{hash_id}"

    def mint_account_iri(self, account_id: str) -> str:
        """
        Mint IRI for account using account_id directly.

        Account IDs are assumed to be globally unique within Chakra Commerce.
        No hashing needed - use account ID directly for human-readability.

        Args:
            account_id: Unique account identifier

        Returns:
            IRI string, e.g., https://chakracommerce.com/account#acct_001
        """
        return f"{self.iri_base_url}/account#{account_id}"
