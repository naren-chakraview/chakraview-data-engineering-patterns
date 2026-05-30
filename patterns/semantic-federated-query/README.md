# Semantic Federated Query Pattern

## What This Does

The Semantic Federated Query pattern provides a unified query interface across relational (Iceberg/Postgres) and semantic (RDF) datastores using Apache Jena and Trino. It allows analysts and applications to write a single SPARQL query that federates across multiple physical data sources, automatically translating between semantic and relational representations.

Key capabilities:

1. **Federated SPARQL Queries**: Single SPARQL query executes across RDF triples, Iceberg tables, and PostgreSQL databases
2. **Semantic-to-Relational Mapping**: Automatic translation between RDF properties and dimensional table columns
3. **Query Optimization**: Cost-based query planning across heterogeneous sources
4. **Jena Semantic Store**: Native RDF storage and reasoning with OWL ontology inference
5. **Trino SQL Integration**: Relational queries on lakehouse data with semantic metadata
6. **Caching and Materialization**: Pre-computed semantic views for common query patterns

This pattern bridges semantic query languages (SPARQL) with traditional analytics (SQL), enabling both semantic reasoning and analytical queries on the same dataset.

### Use Cases

- Unified semantic and analytical queries for compliance reporting
- Cross-domain entity queries (accounts, transactions, counterparties in one query)
- Ontology-driven business intelligence dashboards
- Entity relationship exploration with semantic graph traversal
- Data lineage and impact analysis queries
- Semantic data governance and metadata searches

## Technology Stack

### Core Components

- **Apache Jena 4.5+**: RDF store with SPARQL endpoint and OWL reasoning
- **Trino 403+**: Federated SQL query engine with extensible connectors
- **Scala 2.12+**: High-performance query processing and optimization
- **PostgreSQL 12+**: Semantic metadata store and SPARQL cache
- **Elasticsearch 7.10+**: Full-text indexing for entity search
- **Redis**: Query result caching

### Supporting Libraries

- **Jena TDB2**: Disk-based RDF storage with high-performance indexing
- **Jena Fuseki**: SPARQL endpoint HTTP server
- **Trino Client Libraries**: JDBC/Python drivers for applications
- **OWL API**: Ontology processing and reasoning
- **pytest/ScalaTest**: Comprehensive test coverage
- **Docker Compose**: Full-stack local development

## Getting Started

### Prerequisites

- Docker and Docker Compose 1.29+
- Java 11+ (for Jena and Trino)
- Scala 2.12+ and SBT (for custom connectors)
- PostgreSQL 12+ client tools
- 16GB+ available disk space (for Jena TDB2 index)

### Quick Start

#### 1. Start Services

```bash
# Navigate to pattern directory
cd patterns/semantic-federated-query

# Start Jena, Trino, PostgreSQL, Elasticsearch, and Redis
docker-compose up -d

# Verify services are running
docker-compose ps

# Wait for services to be ready
sleep 30
```

#### 2. Load Ontologies

```bash
# Upload fintech ontology to Jena
curl -X POST http://localhost:3030/semantic/data \
  -H "Content-Type: application/rdf+xml" \
  -d @config/jena/ontologies/fintech-ontology.rdf

# Verify ontology loaded
curl http://localhost:3030/semantic/query \
  -d "query=SELECT COUNT(*) WHERE { ?s ?p ?o }"
```

#### 3. Load Semantic Data

```bash
# Load account RDF triples
curl -X POST http://localhost:3030/semantic/data \
  -H "Content-Type: application/n-triples" \
  -d @config/jena/sample-data/accounts.nt

# Load transaction RDF triples
curl -X POST http://localhost:3030/semantic/data \
  -H "Content-Type: application/n-triples" \
  -d @config/jena/sample-data/transactions.nt

# Query to verify data loaded
curl -X POST http://localhost:3030/semantic/query \
  -d "query=$(cat queries/examples/count_accounts.sparql)"
```

#### 4. Configure Trino Catalogs

```bash
# Verify Trino catalogs are configured
curl http://localhost:8080/v1/catalog

# Query Iceberg catalog
curl -X POST http://localhost:8080/v1/statement \
  -d "SELECT * FROM iceberg.default.accounts LIMIT 5"

# Query semantic catalog
curl -X POST http://localhost:8080/v1/statement \
  -d "SELECT * FROM semantic.accounts LIMIT 5"
```

#### 5. Run Example Federated Queries

```bash
# Example 1: Find high-risk accounts with recent transactions
curl -X POST http://localhost:8080/api/query \
  -H "Content-Type: application/sparql-query" \
  -d '
PREFIX onto: <http://example.org/fintech/ontology/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?account ?accountName ?riskLevel ?txnCount
WHERE {
  ?account a onto:CheckingAccount ;
    onto:accountName ?accountName ;
    onto:riskClassification ?riskLevel .
  
  FILTER (?riskLevel = "HIGH")
  
  {
    SELECT ?account (COUNT(?txn) as ?txnCount)
    WHERE {
      ?txn onto:fromAccount ?account ;
        onto:transactionDate ?date .
      FILTER (?date >= "2024-05-01"^^xsd:date)
    }
    GROUP BY ?account
  }
  
  FILTER (?txnCount > 0)
}
ORDER BY DESC(?txnCount)
'

# Example 2: Account lineage with counterparty resolution
curl -X POST http://localhost:8080/api/query \
  -H "Content-Type: application/sparql-query" \
  -d @queries/examples/account_lineage_with_counterparties.sparql

# Example 3: Risk profile analysis with aggregates
curl -X POST http://localhost:8080/api/query \
  -H "Content-Type: application/sparql-query" \
  -d @queries/examples/risk_profile_analysis.sparql
```

#### 6. Access Jena SPARQL Endpoint

```bash
# Interactive SPARQL query interface
open http://localhost:3030/

# Or use curl to query directly
curl -X POST http://localhost:3030/semantic/query \
  --data-urlencode query@queries/examples/accounts_by_currency.sparql \
  -H "Accept: application/sparql-results+json"
```

#### 7. Query Performance Benchmarking

```bash
# Run benchmark suite
python scripts/benchmark_queries.py

# Profile individual query
python scripts/benchmark_queries.py --query queries/performance/complex_join.sparql

# View results
less benchmark-results.json
```

### Cleanup

```bash
# Stop all services
docker-compose down -v

# Remove cached data
rm -rf jena-tdb2-data/
rm -rf elasticsearch-data/
```

## Key Concepts

### Federated SPARQL Queries

SPARQL queries execute across multiple data sources transparently:

```sparql
PREFIX onto: <http://example.org/fintech/ontology/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?accountIri ?accountName ?totalVolume ?riskLevel
WHERE {
  # Semantic data from RDF store (Jena)
  ?accountIri a onto:CheckingAccount ;
    onto:accountName ?accountName ;
    onto:riskClassification ?riskLevel ;
    onto:accountBalance ?balance .
  
  # Relational data from lakehouse (Trino/Iceberg)
  {
    SELECT ?accountIri (SUM(?amount) as ?totalVolume)
    FROM iceberg.default.fact_transactions
    WHERE {
      fact_transactions.account_iri = ?accountIri AND
      fact_transactions.transaction_date >= DATE '2024-05-01'
    }
    GROUP BY ?accountIri
  }
  
  FILTER (?balance > 100000)
}
ORDER BY DESC(?totalVolume)
```

The query optimizer:
1. Separates triple patterns into semantic (Jena) and relational (Trino) portions
2. Pushes predicates and filters to appropriate backends
3. Coordinates result sets and performs final joins
4. Caches intermediate results for performance

### Semantic-to-Relational Mapping

Automatic translation between RDF properties and relational columns:

```yaml
# config/jena/semantic-to-sql-mappings.yaml
mappings:
  - semantic_property: "http://example.org/fintech/ontology/accountName"
    relational_table: "dim_accounts"
    relational_column: "account_name"
    data_type: "varchar"
    
  - semantic_property: "http://example.org/fintech/ontology/riskClassification"
    relational_table: "dim_accounts"
    relational_column: "risk_classification"
    data_type: "varchar"
    
  - semantic_property: "http://example.org/fintech/ontology/accountBalance"
    relational_table: "fact_accounts_snapshot"
    relational_column: "balance"
    data_type: "decimal"
    
  - semantic_property: "http://example.org/fintech/ontology/transactionAmount"
    relational_table: "fact_transactions"
    relational_column: "amount"
    data_type: "decimal"
```

### Jena RDF Store

Apache Jena provides native RDF storage with reasoning:

```turtle
# Sample RDF data
@prefix onto: <http://example.org/fintech/ontology/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

# Account definition
<iri:account:USD-2024:Acme-Corp@Canonical>
  a onto:CheckingAccount ;
  onto:accountName "Acme Corporation Checking"@en ;
  onto:accountCurrency "USD" ;
  onto:riskClassification "LOW" ;
  onto:settlementMethod "FEDWIRE" ;
  dcterms:created "2020-01-15"^^xsd:date ;
  rdfs:comment "Primary operational account" .

# OWL class hierarchy (inferred)
onto:CheckingAccount rdfs:subClassOf onto:BankAccount .
onto:BankAccount rdfs:subClassOf onto:FinancialAccount .
```

### Trino Federation

Trino provides SQL-based federation with custom connectors:

```sql
-- Query across Iceberg lakehouse tables
SELECT 
  a.account_iri,
  a.account_name,
  COUNT(t.transaction_iri) as txn_count,
  SUM(t.amount) as total_volume,
  AVG(t.amount) as avg_amount
FROM iceberg.default.dim_accounts a
LEFT JOIN iceberg.default.fact_transactions t 
  ON a.account_iri = t.account_iri
WHERE a.risk_classification = 'HIGH'
  AND t.transaction_date >= DATE '2024-05-01'
GROUP BY a.account_iri, a.account_name
ORDER BY total_volume DESC;

-- Query semantic catalog (Jena via Trino connector)
SELECT * 
FROM semantic.accounts
WHERE risk_classification = 'HIGH'
  AND account_balance > 1000000;

-- Federated join across semantic and relational
SELECT 
  s.account_iri,
  s.account_name,
  r.total_volume
FROM semantic.accounts s
JOIN iceberg.default.agg_monthly_transactions r 
  ON s.account_iri = r.account_iri
WHERE r.month = '2024-05'
  AND r.total_volume > 1000000;
```

### Query Caching and Materialization

Pre-computed semantic views for common queries:

```sparql
# Materialized view: High-risk accounts with monthly volumes
PREFIX onto: <http://example.org/fintech/ontology/>

CONSTRUCT {
  ?account a onto:HighRiskMonthlyView ;
    onto:accountName ?accountName ;
    onto:totalVolume ?totalVolume ;
    onto:transactionCount ?txnCount ;
    onto:lastUpdated ?lastUpdated .
}
WHERE {
  ?account a onto:CheckingAccount ;
    onto:accountName ?accountName ;
    onto:riskClassification "HIGH" .
  
  ?account onto:monthlyTransactionVolume ?totalVolume ;
    onto:monthlyTransactionCount ?txnCount ;
    onto:lastUpdated ?lastUpdated .
    
  FILTER (?lastUpdated >= NOW() - P1D)
}
```

## Project Structure

```
patterns/semantic-federated-query/
├── README.md                                      # This file
├── docker-compose.yml                             # Full-stack orchestration
├── config/
│   ├── trino/
│   │   ├── catalog/
│   │   │   ├── iceberg.properties                 # Iceberg catalog config
│   │   │   ├── semantic.properties                # Semantic (Jena) catalog config
│   │   │   └── postgres.properties                # PostgreSQL catalog config
│   │   ├── config.properties                      # Trino server config
│   │   └── jvm.config                             # JVM configuration
│   ├── jena/
│   │   ├── fuseki-config.ttl                      # Fuseki server configuration
│   │   ├── ontologies/
│   │   │   ├── fintech-ontology.rdf               # Main fintech ontology
│   │   │   ├── account-ontology.rdf               # Account-specific ontology
│   │   │   ├── transaction-ontology.rdf           # Transaction ontology
│   │   │   └── risk-ontology.rdf                  # Risk classification ontology
│   │   ├── sample-data/
│   │   │   ├── accounts.nt                        # Sample account RDF data
│   │   │   ├── transactions.nt                    # Sample transaction RDF data
│   │   │   └── counterparties.nt                  # Counterparty RDF data
│   │   └── semantic-to-sql-mappings.yaml          # Semantic to relational mapping
│   └── elasticsearch/
│       ├── elasticsearch.yml                      # Elasticsearch config
│       └── mappings/
│           ├── accounts-index.json                # Account search index mapping
│           └── transactions-index.json            # Transaction search index
├── queries/
│   ├── examples/
│   │   ├── accounts_by_currency.sparql            # Simple semantic query
│   │   ├── high_risk_accounts.sparql              # Risk-based semantic query
│   │   ├── accounts_by_risk_profile.sparql        # Complex semantic query
│   │   ├── account_lineage_with_counterparties.sparql # Graph traversal
│   │   └── risk_profile_analysis.sparql           # Aggregation query
│   ├── performance/
│   │   ├── simple_join.sparql                     # Baseline query
│   │   ├── complex_join.sparql                    # Multi-source join
│   │   ├── large_result_set.sparql                # Large result performance
│   │   └── deep_traversal.sparql                  # Graph depth traversal
│   └── benchmarks/
│       ├── benchmark_suite.sparql                 # Full benchmark suite
│       └── results.json                           # Performance results
├── src/
│   └── main/scala/io/chakraview/semantic/
│       ├── query/
│       │   ├── FederatedQueryExecutor.scala       # Query execution engine
│       │   ├── TrinoPlanOptimizer.scala           # Query optimization
│       │   └── SemanticQueryBuilder.scala         # Query construction
│       ├── connector/
│       │   ├── JenaConnector.scala                # Jena/Trino integration
│       │   ├── IcebergConnector.scala             # Iceberg/Trino integration
│       │   └── PostgresConnector.scala            # PostgreSQL/Trino integration
│       ├── mapping/
│       │   ├── SemanticToSqlMapper.scala          # RDF-to-SQL mapping
│       │   └── DataTypeTransformer.scala          # Type conversion
│       └── tests/
│           ├── FederatedQueryExecutorSpec.scala
│           └── SemanticToSqlMapperSpec.scala
├── scripts/
│   ├── load_ontologies.sh                         # Load ontology definitions
│   ├── load_sample_data.sh                        # Load sample RDF data
│   ├── benchmark_queries.py                       # Performance benchmarking
│   ├── validate_federation.py                     # Validation suite
│   └── generate_query_report.py                   # Query performance reporting
└── sql/
    └── setup/
        ├── create_jena_dataset.sql                # Jena dataset initialization
        └── create_semantic_metadata.sql           # Metadata tables
```

## Configuration

### Trino Configuration (config/trino/config.properties)

```properties
# Coordinator/worker
coordinator=true
node-scheduler.include-coordinator=true

# Port and HTTP configuration
http-server.http.port=8080
discovery.uri=http://localhost:8080

# Query execution
query.max-run-time=30m
query.queue-config-file=/etc/trino/queue-config.properties

# Catalog discovery
catalog.config-dir=/etc/trino/catalog

# Memory configuration
memory.heap-headroom-per-node=1GB
```

### Jena Configuration (config/jena/fuseki-config.ttl)

```turtle
@prefix fuseki:  <http://jena.apache.org/fuseki/config/> .
@prefix rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs:    <http://www.w3.org/2000/01/rdf-schema#> .
@prefix tdb:     <http://jena.apache.org/2016/tdb#> .
@prefix xsd:     <http://www.w3.org/2001/XMLSchema#> .
@prefix :        <#> .

[] rdf:type fuseki:Server ;
    fuseki:jettyConfigFile "jetty.xml" ;
    fuseki:services (
        :service1
    ) .

:service1 rdf:type fuseki:Service ;
    fuseki:name                       "semantic" ;
    fuseki:serviceQuery               "query" ;
    fuseki:serviceQuery               "sparql" ;
    fuseki:serviceUpdate              "update" ;
    fuseki:serviceUpload              "upload" ;
    fuseki:serviceReadGraphStore      "get" ;
    fuseki:serviceReadWriteGraphStore "data" ;
    fuseki:dataset                    :dataset1 .

:dataset1 rdf:type tdb:DatasetTDB ;
    tdb:location "/var/lib/fuseki/fintech-semantic" .
```

### Environment Variables

```bash
# Trino
TRINO_PORT=8080
TRINO_COORDINATOR_PORT=8081

# Jena/Fuseki
JENA_PORT=3030
JENA_DATA_DIR=/var/lib/fuseki/data

# PostgreSQL (metadata)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_DB=semantic_metadata

# Elasticsearch
ELASTICSEARCH_HOST=localhost
ELASTICSEARCH_PORT=9200

# Redis (caching)
REDIS_HOST=localhost
REDIS_PORT=6379

# Query configuration
QUERY_TIMEOUT_SECONDS=300
QUERY_CACHE_TTL_MINUTES=60
QUERY_MAX_RESULT_ROWS=1000000
```

## Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run specific test suite
pytest tests/integration/test_federated_queries.py -v

# Run performance benchmarks
python scripts/benchmark_queries.py --suite all

# Run with coverage
pytest tests/ --cov=io/chakraview/semantic --cov-report=html

# Run Scala tests
sbt test
```

## Performance Characteristics

- **Simple SPARQL Query**: <100ms (single RDF store)
- **Federated Join Query**: 200-500ms (across 2-3 sources)
- **Complex Aggregation**: 1-2 seconds (100M+ triple RDF store)
- **Large Result Set**: <5 seconds (1M+ results)
- **Query Caching**: 10-50ms (cached results)
- **Elasticsearch Full-Text Search**: <200ms over 10M entities

## Monitoring and Debugging

### Jena Query Monitoring

```bash
# View query execution statistics
curl http://localhost:3030/system/stats

# Monitor dataset metrics
curl http://localhost:3030/$/datasets

# Check dataset cache
curl http://localhost:3030/$/datasets?pretty=true
```

### Trino Query Monitoring

```bash
# Access Trino UI
open http://localhost:8080/ui/

# Monitor running queries
curl http://localhost:8080/v1/query

# Get query details
curl http://localhost:8080/v1/query/{query_id}

# View query execution plan
curl http://localhost:8080/v1/statement/{query_id}/explain
```

### Debugging Federated Queries

```bash
# Enable detailed logging
curl -X POST http://localhost:8080/v1/query \
  -H "X-Trino-Session: debug=true" \
  -d "SELECT * FROM semantic.accounts LIMIT 5"

# View query plan
EXPLAIN (FORMAT JSON) SELECT * FROM semantic.accounts;

# Check connector statistics
curl http://localhost:8080/v1/connector
```

## Production Considerations

- **Scalability**: Deploy Trino and Jena on separate clusters; use load balancing
- **Caching**: Implement Redis-based query result caching with TTL policies
- **Security**: Enable SSL/TLS for all service communications
- **Monitoring**: Export Prometheus metrics from both Trino and Jena
- **Data Freshness**: Define refresh policies for materialized semantic views
- **Query Limits**: Set timeouts and result row limits to prevent runaway queries
- **Backup**: Regular snapshots of Jena TDB2 data directory

## References

- [Apache Jena Documentation](https://jena.apache.org/)
- [SPARQL 1.1 Query Language](https://www.w3.org/TR/sparql11-query/)
- [Apache Trino Documentation](https://trino.io/docs/current/)
- [RDF 1.1 Concepts and Abstract Syntax](https://www.w3.org/TR/rdf11-concepts/)
- [OWL 2 Web Ontology Language](https://www.w3.org/TR/owl2-overview/)
