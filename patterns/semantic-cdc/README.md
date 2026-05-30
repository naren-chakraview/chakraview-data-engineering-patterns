# Semantic CDC Pattern

## What This Does

The Semantic CDC (Change Data Capture) pattern captures real-time changes from fintech source systems and enriches them with semantic metadata before streaming to Kafka. It implements a two-stage enrichment pipeline:

1. **IRI Minting**: Assigns globally unique, semantically meaningful identifiers (IRIs) to all entities
2. **Entity Resolution**: Reconciles the same logical entity across multiple source systems, creating unified semantic identities

This pattern bridges technical database IDs with business semantics, enabling downstream systems to recognize "the same account" regardless of which system originated the data.

### Use Cases

- Real-time account reconciliation across legacy banking systems
- Cross-system transaction tracking with semantic lineage
- Dynamic entity resolution with machine learning scoring
- Semantic enrichment for GDPR data lineage and compliance
- Foundation for federated query layers and knowledge graphs

## Technology Stack

### Core Components

- **Apache Debezium**: CDC connectors for PostgreSQL, MySQL, and Oracle source systems
- **Apache Kafka**: High-throughput event streaming and semantic event topics
- **Python 3.9+**: Lightweight, maintainable ETL logic for semantic enrichment
- **FastAPI**: REST API for IRI lookup and entity resolution queries
- **Redis**: High-speed caching for IRI mappings and entity resolution results

### Supporting Libraries

- **Pydantic**: Data validation and schema enforcement
- **SQLAlchemy**: ORM for source system connectivity
- **pytest**: Comprehensive test coverage
- **Docker/Docker Compose**: Local development and CI/CD

## Getting Started

### Prerequisites

- Docker and Docker Compose 1.29+
- Python 3.9+ with pip
- Java 11+ (for Debezium)
- PostgreSQL 12+ client tools (for manual testing)

### Quick Start

#### 1. Start Services

```bash
# Navigate to pattern directory
cd patterns/semantic-cdc

# Start Kafka, PostgreSQL, and Redis
docker-compose up -d

# Verify services are running
docker-compose ps
```

#### 2. Create Source Data

```bash
# Connect to PostgreSQL and create sample data
docker exec -it semantic-cdc-postgres psql -U postgres -c \
  "CREATE TABLE accounts (
    id BIGINT PRIMARY KEY,
    name VARCHAR(256),
    currency VARCHAR(3),
    balance DECIMAL(18,2),
    created_at TIMESTAMP DEFAULT NOW()
  );"

# Insert sample accounts
docker exec -it semantic-cdc-postgres psql -U postgres -c \
  "INSERT INTO accounts (id, name, currency, balance) VALUES
    (1001, 'Acme Corp Checking', 'USD', 50000.00),
    (1002, 'Acme Corp Savings', 'USD', 500000.00),
    (2001, 'TechStartup Ops', 'EUR', 125000.00);"
```

#### 3. Deploy Debezium Connector

```bash
# Create PostgreSQL CDC connector
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium-configs/postgres-cdc-connector.json

# Verify connector status
curl http://localhost:8083/connectors/postgres-cdc-connector/status
```

#### 4. Run IRI Minting Service

```bash
# Install dependencies
pip install -r src/main/python/requirements.txt

# Start IRI minter service
python src/main/python/io/chakraview/semantic/iri_minter/run_iri_minter.py

# Test IRI minting endpoint
curl -X POST http://localhost:8001/mint \
  -H "Content-Type: application/json" \
  -d '{"entity_type": "account", "system_id": "1001", "source_system": "chase-tps"}'
```

#### 5. Run Entity Resolution Service

```bash
# Start entity resolver service
python src/main/python/io/chakraview/semantic/entity_resolver/run_entity_resolver.py

# Test entity resolution
curl -X POST http://localhost:8002/resolve \
  -H "Content-Type: application/json" \
  -d '{"entities": [
    {"entity_type": "account", "system_id": "1001", "source_system": "chase-tps"},
    {"entity_type": "account", "system_id": "ACC-1001", "source_system": "internal-ams"}
  ]}'
```

#### 6. Verify Semantic Events

```bash
# Listen to semantic event topic
docker exec -it semantic-cdc-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic semantic.accounts \
  --from-beginning
```

### Cleanup

```bash
# Stop all services
docker-compose down -v

# Remove cached IRI/entity resolution data
rm -rf redis_data/
```

## Key Concepts

### Internationalized Resource Identifiers (IRIs)

IRIs encode business semantics in a globally unique identifier. For example:

```
iri:account:USD-2024:Chase-1234567890@TPS-Chase
```

Structure: `iri:<entity_type>:<attributes>:<source_system_id>@<source_system>`

Benefits:
- **Immutable**: Never changes, even if underlying business data updates
- **Resolvable**: Maps back to source system identifiers
- **Standardized**: Follows W3C Internationalized Resource Identifier spec
- **Debuggable**: Business context encoded in the identifier

### Entity Resolution

Entity resolution matches the same logical entity across multiple source systems using:

- **Rule-Based Matching**: Exact name matches, account number patterns, customer ID equivalence
- **Fuzzy Matching**: Levenshtein distance on company names, phonetic matching on contact names
- **ML-Assisted Matching**: Trained classifiers that learn from historical matches and manual confirmations

Example resolution result:

```json
{
  "canonical_iri": "iri:account:USD-2024:Acme-Corp@Canonical",
  "matched_entities": [
    {
      "iri": "iri:account:USD-2024:Chase-1234567890@TPS-Chase",
      "source_system": "chase-tps",
      "system_id": "1234567890",
      "confidence": 0.98,
      "match_method": "EXACT_NAME_MATCH"
    },
    {
      "iri": "iri:account:USD-2024:ACC-1001@AMS-Internal",
      "source_system": "internal-ams",
      "system_id": "ACC-1001",
      "confidence": 0.95,
      "match_method": "CUSTOMER_ID_EQUIVALENCE"
    }
  ]
}
```

### Semantic Event Schema

Semantic CDC events follow RDF triples structure with optional JSON-LD context:

```json
{
  "iri": "iri:account:USD-2024:Chase-1234567890@TPS-Chase",
  "event_type": "CREATED",
  "timestamp": "2024-05-15T14:30:00Z",
  "properties": {
    "accountName": "Acme Corp Checking",
    "accountCurrency": "USD",
    "accountBalance": 50000.00,
    "riskClassification": "LOW",
    "settlementMethod": "FEDWIRE"
  },
  "source_metadata": {
    "source_system": "chase-tps",
    "source_id": "1234567890",
    "data_classification": "CONFIDENTIAL",
    "audit_id": "ev-2024-05-15-001"
  }
}
```

## Project Structure

```
patterns/semantic-cdc/
├── README.md                                      # This file
├── docker-compose.yml                             # Local development stack
├── debezium-configs/
│   ├── postgres-cdc-connector.json               # PostgreSQL CDC configuration
│   ├── oracle-cdc-connector.json                 # Oracle CDC configuration (optional)
│   └── mysql-cdc-connector.json                  # MySQL CDC configuration (optional)
├── src/main/python/
│   ├── requirements.txt                           # Python dependencies
│   ├── io/chakraview/semantic/
│   │   ├── iri_minter/
│   │   │   ├── run_iri_minter.py                # IRI minting service entry point
│   │   │   ├── iri_generator.py                 # IRI generation logic
│   │   │   ├── iri_registry.py                  # IRI persistence and lookup
│   │   │   ├── tests/
│   │   │   │   ├── test_iri_generator.py        # IRI generator tests
│   │   │   │   └── test_iri_registry.py         # IRI registry tests
│   │   │   └── schemas.py                       # Pydantic schemas for IRI service
│   │   ├── entity_resolver/
│   │   │   ├── run_entity_resolver.py           # Entity resolution service entry point
│   │   │   ├── matcher.py                       # Matching algorithms
│   │   │   ├── resolver.py                      # Resolution orchestration
│   │   │   ├── tests/
│   │   │   │   ├── test_matcher.py              # Matcher algorithm tests
│   │   │   │   └── test_resolver.py             # Resolution integration tests
│   │   │   └── schemas.py                       # Pydantic schemas for resolver service
│   │   ├── config.py                            # Configuration management
│   │   └── models.py                            # Domain models
├── tests/
│   ├── integration/
│   │   ├── test_debezium_to_kafka.py            # Debezium-Kafka integration
│   │   ├── test_iri_kafka_flow.py               # IRI minting with live Kafka
│   │   ├── test_entity_resolution_flow.py       # Entity resolution with live Kafka
│   │   └── test_semantic_event_schema.py        # Event schema validation
│   └── conftest.py                              # Pytest fixtures
└── .env.example                                  # Example environment configuration
```

## Configuration

### Environment Variables

```bash
# Kafka configuration
KAFKA_BROKERS=localhost:9092
KAFKA_SEMANTIC_TOPIC=semantic.accounts
KAFKA_SECURITY_PROTOCOL=PLAINTEXT

# PostgreSQL (for CDC sources)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=fintech

# IRI Minting Service
IRI_SERVICE_PORT=8001
IRI_SERVICE_HOST=0.0.0.0
IRI_CACHE_TTL_SECONDS=3600
IRI_REGISTRY_BACKEND=redis  # or postgresql

# Entity Resolution Service
RESOLVER_SERVICE_PORT=8002
RESOLVER_SERVICE_HOST=0.0.0.0
RESOLVER_MATCH_THRESHOLD=0.85
RESOLVER_CACHE_BACKEND=redis

# Redis (for caching)
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0

# Logging
LOG_LEVEL=INFO
```

## Running Tests

```bash
# Run all unit tests
pytest src/main/python/io/chakraview/semantic/iri_minter/tests
pytest src/main/python/io/chakraview/semantic/entity_resolver/tests

# Run integration tests (requires Docker services)
pytest tests/integration -v

# Run with coverage
pytest src/main/python --cov=io/chakraview/semantic --cov-report=html

# Run specific test
pytest src/main/python/io/chakraview/semantic/iri_minter/tests/test_iri_generator.py::test_iri_format
```

## Performance Characteristics

- **IRI Minting**: 1000+ IRIs/second (single instance, Redis-backed)
- **Entity Resolution**: 100-500 resolutions/second (depends on match complexity)
- **Kafka Throughput**: 100k+ events/second (3-node Kafka cluster)
- **End-to-End Latency**: P99 <100ms from CDC to semantic event

## Debugging

### Common Issues

#### Debezium Connector Fails to Start

```bash
# Check connector logs
curl http://localhost:8083/connectors/postgres-cdc-connector/status | jq .

# View Kafka Connect logs
docker logs semantic-cdc-connect
```

#### IRI Service Unavailable

```bash
# Check service health
curl http://localhost:8001/health

# View IRI service logs
docker logs semantic-cdc-iri-minter
```

#### Entity Resolution Timeouts

```bash
# Check Redis connectivity
redis-cli -h localhost ping

# Monitor Redis usage
redis-cli INFO memory
redis-cli MONITOR
```

## Production Considerations

- **Scalability**: Deploy IRI minting and entity resolution as separate microservices; use load balancers
- **Caching**: Use Redis with appropriate TTL settings for IRI lookups and entity matches
- **Data Quality**: Implement sampling-based validation of entity resolution matches
- **Monitoring**: Export Prometheus metrics from IRI and resolver services
- **Disaster Recovery**: Back up Redis and PostgreSQL registries regularly

## References

- [Debezium PostgreSQL Connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
- [Kafka Client Configuration](https://kafka.apache.org/documentation/#producerconfigs)
- [RFC 3987 - IRIs](https://tools.ietf.org/html/rfc3987)
- [Entity Resolution Best Practices](https://en.wikipedia.org/wiki/Record_linkage)
