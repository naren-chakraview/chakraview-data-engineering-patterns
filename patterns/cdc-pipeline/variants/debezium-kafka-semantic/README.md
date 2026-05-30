# Semantic CDC Pattern

## Overview

The Semantic CDC (Change Data Capture) pattern enriches Debezium CDC records with IRIs (Internationalized Resource Identifiers) for cross-source entity deduplication.

## Architecture

1. **Debezium Connectors** capture changes from Postgres, Salesforce, and Stripe
2. **IRI Minter Service** enriches records with stable IRIs for entity deduplication
3. **Kafka Topics** carry enriched records downstream
4. **Deduplication Cache** prevents duplicate processing

## Usage

```bash
# Start infrastructure
docker-compose up -d

# Run IRI Minter service
python src/main/python/iri_minter/service.py

# Monitor Kafka topics
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic enriched.customers
```

## Configuration

See `src/main/python/iri_minter/config.yaml` for entity rules and Kafka settings.

## Testing

```bash
pytest src/main/python/iri_minter/tests/ -v
```
