from collections import OrderedDict
from typing import Dict, Any, List, Optional


class EntityResolver:
    """Entity resolution and deduplication logic"""

    def __init__(self, rules: Dict[str, Any]):
        self.rules = rules

    def deduplicate(
        self,
        entity_type: str,
        record: Dict[str, Any],
        existing_records: List[Dict[str, Any]]
    ) -> Optional[str]:
        """
        Check if record matches an existing entity

        Returns:
            Existing entity IRI if match found, None otherwise
        """

        rules = self.rules.get(entity_type, {})
        match_fields = rules.get("match_fields", [])

        if not match_fields:
            return None

        # Normalize current record
        current_key = self._normalize_key(record, match_fields)

        # Check against existing records
        for existing in existing_records:
            existing_key = self._normalize_key(existing, match_fields)
            if current_key == existing_key:
                return existing.get("iri")

        return None

    def _normalize_key(self, record: Dict[str, Any], fields: List[str]) -> str:
        """Create normalized key for comparison"""
        parts = []
        for field in fields:
            value = record.get(field, "")
            if isinstance(value, str):
                value = value.lower().strip()
            parts.append(str(value))
        return "|".join(parts)


class DeduplicationCache:
    """In-memory LRU cache for deduplication across Kafka topics"""

    def __init__(self, max_size: int = 10000):
        if max_size <= 0:
            raise ValueError("max_size must be > 0")
        self.cache = {}  # entity_type -> OrderedDict({key -> iri})
        self.max_size = max_size

    def get_iri(self, entity_type: str, key: str) -> Optional[str]:
        """Retrieve cached IRI for entity"""
        if entity_type not in self.cache:
            return None

        iri = self.cache[entity_type].get(key)
        if iri:
            # Move to end (mark as recently accessed)
            self.cache[entity_type].move_to_end(key)
        return iri

    def put_iri(self, entity_type: str, key: str, iri: str):
        """Cache IRI for entity, evicting oldest if needed"""
        if entity_type not in self.cache:
            self.cache[entity_type] = OrderedDict()

        # If cache full, evict oldest (first item)
        if len(self.cache[entity_type]) >= self.max_size:
            oldest_key = next(iter(self.cache[entity_type]))
            del self.cache[entity_type][oldest_key]

        self.cache[entity_type][key] = iri
        # Move to end (most recently used)
        self.cache[entity_type].move_to_end(key)
