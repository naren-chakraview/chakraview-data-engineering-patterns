# Fintech Semantic Integration Case Study

## Overview

This case study demonstrates a comprehensive semantic data integration architecture for fintech systems, building a unified knowledge graph from heterogeneous transaction, account, and market data sources. The integration layer combines semantic CDC (Change Data Capture), batch processing with Apache Iceberg, and federated query capabilities to create a coherent, queryable semantic layer that bridges siloed fintech systems.

The architecture showcases:

- **Semantic CDC Pipeline**: Real-time entity reconciliation and semantic enrichment using Debezium and Apache Kafka, with IRI (Internationalized Resource Identifier) minting for stable cross-system identity
- **Semantic Batch Lakehouse**: Historical aggregation and semantic harmonization of financial datasets using Apache Spark and Iceberg, materializing ontology-aligned fact tables and dimensions
- **Semantic Federated Query**: Unified SPARQL query interface across relational and RDF datastores using Apache Jena and Trino, enabling business intelligence teams to write once, query anywhere
- **Ontology-Driven Governance**: Shared semantic models, entity resolution rules, and data quality constraints enforced at all layers

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Java 11+
- Python 3.9+
- Scala 2.12+

### Running the Full Stack

```bash
# Clone and navigate to the project
cd docs/case-study/fintech-semantic-integration

# Start all services (Kafka, PostgreSQL, Trino, Jena, Redis)
docker-compose up -d

# Wait for services to be ready (approximately 30 seconds)
sleep 30

# Verify services are running
docker-compose ps

# View logs
docker-compose logs -f kafka
docker-compose logs -f postgres
```

### Quick Integration Test

```bash
# Run the semantic CDC pipeline
python patterns/semantic-cdc/src/main/python/io/chakraview/semantic/iri_minter/run_iri_minter.py

# Run entity resolution
python patterns/semantic-cdc/src/main/python/io/chakraview/semantic/entity_resolver/run_entity_resolver.py

# Execute a federated query
curl -X POST http://localhost:8080/api/query \
  -H "Content-Type: application/sparql-query" \
  -d @patterns/semantic-federated-query/queries/examples/accounts_by_risk_profile.sparql
```

### Cleanup

```bash
# Stop and remove all containers
docker-compose down -v
```

## Files

### Case Study Documentation

- **sample-data/**: Sample transaction, account, and market data in JSON and CSV formats for testing and demonstration
- **queries/**: Example SPARQL queries demonstrating semantic integration across fintech domains
- **docker-compose.yml**: Full-stack orchestration including Kafka, PostgreSQL, Trino, Jena, and Redis
- **README.md**: This file

### Patterns

- **patterns/semantic-cdc/**: Real-time semantic change capture and entity reconciliation
  - Debezium connectors for transaction and account sources
  - Python services for IRI minting and entity resolution
  - Kafka topics for semantic events

- **patterns/semantic-batch-lakehouse/**: Batch processing and historical semantic harmonization
  - Spark jobs for semantic aggregation
  - Iceberg tables for versioned semantic datasets
  - Data quality checks and ontology validation

- **patterns/semantic-federated-query/**: Unified semantic querying across stores
  - Trino catalog configuration for federated access
  - Jena SPARQL endpoint configuration
  - Example queries and performance benchmarks

### Shared Resources

- **shared/semantic/ontologies/**: Fintech ontology definitions including Account, Transaction, Risk, and MarketData concepts

## Architecture Layers

### Data Sources

Multiple fintech systems emit events:
- Transaction Processing System (TPS): Account transfers, payments, settlements
- Account Management System (AMS): Customer profiles, account hierarchies, risk classifications
- Market Data Feed: Currency pairs, interest rates, volatility indices

### Semantic CDC Layer

Debezium captures changes from source systems and publishes to Kafka topics. Python services enrich events with semantic metadata:
- **IRI Minting**: Assigns stable, globally unique identifiers to accounts, transactions, and entities
- **Entity Resolution**: Matches accounts across systems using rule-based and ML-assisted reconciliation
- **Semantic Tagging**: Enriches events with ontology properties (transaction type, risk level, asset class)

### Semantic Batch Layer

Scheduled Spark jobs aggregate and harmonize semantic events into Iceberg tables:
- Fact tables for transactions, settlements, and market events
- Dimension tables for accounts, counterparties, and risk profiles
- Aggregate tables for daily/weekly/monthly summaries

### Semantic Query Layer

Federated query engine (Trino + Jena) provides unified access:
- Relational SQL queries on lakehouse fact/dimension tables
- SPARQL queries on semantic RDF triples
- Federated joins across both data models
- Query optimization and cost estimation

## Key Concepts

### Internationalized Resource Identifiers (IRIs)

IRIs serve as stable, globally unique identifiers for semantic entities. Unlike technical database IDs, IRIs encode business semantics:

```
iri:account:USD-2024:Chase-1234567890@ACME-Bank
iri:transaction:2024-05-15:TXN-98765432@Chase-TPS
iri:counterparty:JPMorgan-Chase@CUSIP-00104M102
```

### Semantic Tagging

Entities are labeled with ontology properties:
```
iri:account:USD-2024:Chase-1234567890@ACME-Bank
  rdf:type onto:CheckingAccount ;
  onto:accountCurrency "USD" ;
  onto:riskClassification "LOW" ;
  onto:settlementMethod "FEDWIRE" ;
  dcterms:created "2020-01-15"^^xsd:date .
```

### Entity Resolution

Accounts matching across systems (e.g., Chase TPS ID vs. internal customer number) are consolidated:
```
iri:account:USD-2024:Chase-1234567890@ACME-Bank
  owl:sameAs "12345" ;  # Internal ID
  owl:sameAs "9876543210" ;  # Chase system ID
  onto:resolutionScore 0.98 ;
  onto:resolutionMethod "EXACT_MATCH" .
```

## Running Individual Patterns

Each pattern can be run in isolation. See pattern-specific READMEs:

- [Semantic CDC Pattern](../../patterns/semantic-cdc/README.md)
- [Semantic Batch Lakehouse Pattern](../../patterns/semantic-batch-lakehouse/README.md)
- [Semantic Federated Query Pattern](../../patterns/semantic-federated-query/README.md)

## Testing and Validation

The case study includes comprehensive tests:

```bash
# Run all tests
pytest patterns/semantic-cdc/tests/integration
pytest patterns/semantic-batch-lakehouse/tests/integration
pytest patterns/semantic-federated-query/tests/integration

# Run specific test suites
pytest patterns/semantic-cdc/src/main/python/io/chakraview/semantic/iri_minter/tests
pytest patterns/semantic-cdc/src/main/python/io/chakraview/semantic/entity_resolver/tests
```

## Performance Benchmarks

See pattern-specific benchmark documentation:

- **CDC Pattern**: Sub-100ms IRI minting and entity resolution per event (P99)
- **Batch Pattern**: 10 million transactions harmonized in <5 minutes (Spark cluster)
- **Query Pattern**: <1s response time for complex federated SPARQL queries over 100M triples

## References

- [Semantic Web Best Practices](https://www.w3.org/2001/sw/)
- [RDF 1.1 Specification](https://www.w3.org/TR/rdf11-concepts/)
- [SPARQL 1.1 Specification](https://www.w3.org/TR/sparql11-query/)
- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Debezium Documentation](https://debezium.io/)
