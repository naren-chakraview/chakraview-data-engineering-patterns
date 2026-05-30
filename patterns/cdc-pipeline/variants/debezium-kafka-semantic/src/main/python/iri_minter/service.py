import json
import logging
from typing import Dict, Any, Optional
import sys
sys.path.insert(0, '/home/gundu/portfolio/chakraview-data-engineering-patterns/.worktrees/semantic-medallion-patterns')
from shared.semantic import IRIResolver


logger = logging.getLogger(__name__)


class IRIMinterService:
    """Kafka consumer that enriches CDC records with IRIs"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.resolver = IRIResolver(base_iri=config.get("base_iri", "https://company.com"))
        self.entity_rules = config.get("entity_rules", {})

    def enrich_record(self, entity_type: str, record: Dict[str, Any]) -> Dict[str, Any]:
        """Add IRI to a record based on entity resolution rules"""

        # Get entity-specific rules
        rules = self.entity_rules.get(entity_type, {})
        key_fields = rules.get("key_fields", list(record.keys()))

        # Mint IRI
        iri = self.resolver.mint_iri(entity_type, record, key_fields=key_fields)

        # Return enriched record
        enriched = record.copy()
        enriched["iri"] = iri
        enriched["_entity_type"] = entity_type
        enriched["_iri_minted_at"] = self._now_iso()

        return enriched

    def process_kafka_record(self, kafka_record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a Kafka record from Debezium CDC"""

        try:
            # Parse Debezium message
            payload = kafka_record.get("value", {})
            if not payload:
                return None

            # Determine entity type from topic
            topic = kafka_record.get("topic", "")
            entity_type = self._topic_to_entity_type(topic)

            if not entity_type:
                logger.warning(f"Could not determine entity type from topic: {topic}")
                return None

            # Extract after-image (new state)
            after = payload.get("after", {})
            if not after:
                return None

            # Enrich with IRI
            enriched = self.enrich_record(entity_type, after)
            enriched["_source_topic"] = topic
            enriched["_kafka_key"] = kafka_record.get("key")

            return enriched

        except Exception as e:
            logger.error(f"Error processing Kafka record: {e}")
            return None

    def _topic_to_entity_type(self, topic: str) -> Optional[str]:
        """Map Kafka topic to entity type"""
        topic_lower = topic.lower()
        if "customer" in topic_lower or "account" in topic_lower:
            return "customer"
        elif "invoice" in topic_lower:
            return "invoice"
        elif "transaction" in topic_lower:
            return "transaction"
        elif "order" in topic_lower:
            return "order"

        return None

    def _now_iso(self) -> str:
        """Return current timestamp in ISO format"""
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).isoformat()


class IRIMinterKafkaApp:
    """Kafka consumer application for IRI minting"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.service = IRIMinterService(config)
        self.kafka_config = config.get("kafka", {})

    def run(self):
        """Start consuming from Kafka (placeholder for actual implementation)"""
        logger.info(f"IRI Minter service starting with config: {self.config}")
