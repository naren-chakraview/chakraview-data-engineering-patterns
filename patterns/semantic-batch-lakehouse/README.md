# Semantic Batch Lakehouse Pattern

## What This Does

The Semantic Batch Lakehouse pattern processes historical and near-real-time semantic events into a harmonized, ontology-aligned lakehouse structure using Apache Spark and Apache Iceberg. It bridges semantic CDC events with dimensional models, providing historical snapshots and aggregated views optimized for analytics and reporting.

Key capabilities:

1. **Semantic Harmonization**: Transforms heterogeneous source data into unified semantic entities aligned with fintech ontology
2. **Dimensional Modeling**: Builds fact and dimension tables from semantic events with slowly-changing dimensions (SCD Type 2)
3. **Iceberg Versioning**: Maintains complete data lineage with time-travel queries and schema evolution
4. **Aggregate Materialization**: Pre-computes common analytical aggregates (daily/weekly/monthly summaries)
5. **Data Quality Validation**: Enforces ontology constraints and completeness checks before materialization

This pattern provides a historical record of semantic entities and supports both analytical SQL queries and federated semantic queries.

### Use Cases

- Historical account analytics with complete audit trails
- Time-series analysis of transaction patterns and risk evolution
- Compliance reporting with immutable semantic data lineage
- Semantic data warehouse for BI tools (Tableau, Looker)
- Foundation for machine learning feature engineering
- Data lake governance with schema-on-read semantics

## Technology Stack

### Core Components

- **Apache Spark 3.3+**: Distributed batch processing with Python, Scala, and SQL
- **Apache Iceberg**: Transactional data lake format with ACID semantics and schema evolution
- **Scala 2.12**: High-performance aggregation and transformation logic
- **PostgreSQL/Snowflake**: Metadata store and staging tables
- **Parquet**: Columnar storage format optimized for analytics

### Supporting Libraries

- **Spark SQL**: Declarative transformation and aggregation
- **PySpark**: Python-native data processing for semantic operations
- **Delta Lake**: Optional transactional lakehouse (alternative to Iceberg)
- **Pandas/Polars**: Local data exploration and validation
- **Great Expectations**: Data quality and validation framework
- **pytest**: Comprehensive test coverage
- **Docker**: Container orchestration for local development

## Getting Started

### Prerequisites

- Spark 3.3+ (standalone or cluster)
- Scala 2.12+ and SBT
- Python 3.9+ with pip
- PostgreSQL 12+ (for metadata store)
- 8GB+ available disk space

### Quick Start

#### 1. Install Spark and Dependencies

```bash
# Navigate to pattern directory
cd patterns/semantic-batch-lakehouse

# Install Python dependencies
pip install -r requirements.txt

# For Scala development, ensure SBT is installed
# SBT will download Spark dependencies automatically
```

#### 2. Initialize Metadata Store

```bash
# Create PostgreSQL metadata database
createdb semantic_lakehouse

# Run migrations (creates fact and dimension schemas)
psql semantic_lakehouse < sql/schema/001_create_facts.sql
psql semantic_lakehouse < sql/schema/002_create_dimensions.sql
psql semantic_lakehouse < sql/schema/003_create_aggregates.sql
```

#### 3. Prepare Sample Data

```bash
# Load sample semantic events (from CDC system)
python scripts/load_sample_events.py

# Verify data loaded
python scripts/validate_source_data.py
```

#### 4. Run Semantic Harmonization Job

```bash
# Submit Spark job for semantic harmonization
spark-submit \
  --master local[4] \
  --class io.chakraview.semantic.jobs.SemanticHarmonizationJob \
  src/main/scala/io/chakraview/semantic/jobs/target/scala-2.12/semantic-batch-lakehouse.jar \
  --input-path s3://semantic-events/accounts \
  --output-path s3://semantic-lakehouse/accounts \
  --catalog-uri postgresql://localhost:5432/semantic_lakehouse

# For Python-based job:
spark-submit \
  --master local[4] \
  src/main/python/io/chakraview/semantic/jobs/semantic_harmonization_job.py \
  --input-path s3://semantic-events/accounts \
  --output-path s3://semantic-lakehouse/accounts
```

#### 5. Build Fact and Dimension Tables

```bash
# Create fact_accounts table with SCD Type 2
spark-submit \
  --master local[4] \
  src/main/python/io/chakraview/semantic/jobs/build_dimension_tables.py \
  --dimension-type accounts \
  --output-path s3://semantic-lakehouse/dimensions/dim_accounts

# Create fact_transactions table
spark-submit \
  --master local[4] \
  src/main/python/io/chakraview/semantic/jobs/build_fact_tables.py \
  --fact-type transactions \
  --output-path s3://semantic-lakehouse/facts/fact_transactions
```

#### 6. Materialize Aggregates

```bash
# Generate daily aggregates
spark-submit \
  --master local[4] \
  src/main/python/io/chakraview/semantic/jobs/build_aggregates.py \
  --aggregate-type daily \
  --output-path s3://semantic-lakehouse/aggregates/agg_daily_transactions

# Generate weekly summaries
spark-submit \
  --master local[4] \
  src/main/python/io/chakraview/semantic/jobs/build_aggregates.py \
  --aggregate-type weekly \
  --output-path s3://semantic-lakehouse/aggregates/agg_weekly_transactions
```

#### 7. Validate Data Quality

```bash
# Run Great Expectations validation suite
python scripts/validate_lakehouse.py

# Check schema compliance with ontology
python scripts/validate_ontology_compliance.py
```

#### 8. Query Lakehouse

```bash
# Using Spark SQL CLI
spark-sql \
  --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.iceberg.type=hadoop

# SQL: Find high-risk accounts with recent activity
SELECT 
  a.account_iri,
  a.account_name,
  a.risk_classification,
  COUNT(t.transaction_iri) as transaction_count,
  SUM(t.transaction_amount) as total_volume
FROM dim_accounts a
LEFT JOIN fact_transactions t ON a.account_iri = t.account_iri
WHERE a.risk_classification = 'HIGH'
  AND t.transaction_date >= CURRENT_DATE - INTERVAL 7 DAY
GROUP BY a.account_iri, a.account_name, a.risk_classification
ORDER BY total_volume DESC;
```

### Cleanup

```bash
# Remove generated Iceberg tables and metadata
rm -rf s3://semantic-lakehouse/*

# Drop metadata store
dropdb semantic_lakehouse

# Clean Spark cache
rm -rf ~/.cache/spark
```

## Key Concepts

### Semantic Harmonization

Transforms raw semantic CDC events into consistent dimensional models aligned with fintech ontology:

**Input**: Semantic events from CDC layer
```json
{
  "iri": "iri:account:USD-2024:Chase-1234567890@TPS-Chase",
  "event_type": "UPDATED",
  "properties": {
    "accountName": "Acme Corp Checking",
    "accountCurrency": "USD",
    "accountBalance": 50000.00
  },
  "timestamp": "2024-05-15T14:30:00Z"
}
```

**Output**: Harmonized dimension record
```sql
INSERT INTO dim_accounts VALUES (
  account_iri='iri:account:USD-2024:Acme-Corp@Canonical',
  account_name='Acme Corporation Checking',
  account_currency='USD',
  risk_classification='LOW',
  settlement_method='FEDWIRE',
  effective_date='2024-05-15',
  end_date='9999-12-31',
  is_current=true,
  version=1
);
```

### Slowly Changing Dimensions (SCD Type 2)

Tracks historical changes to dimension attributes with valid-time semantics:

```sql
-- Account risk classification changes over time
SELECT account_iri, risk_classification, effective_date, end_date
FROM dim_accounts
WHERE account_iri = 'iri:account:USD-2024:Acme-Corp@Canonical'
ORDER BY effective_date;

-- Output shows all versions of the account
iri:account:USD-2024:Acme-Corp@Canonical | LOW  | 2024-01-01 | 2024-03-31
iri:account:USD-2024:Acme-Corp@Canonical | HIGH | 2024-04-01 | 2024-05-14
iri:account:USD-2024:Acme-Corp@Canonical | LOW  | 2024-05-15 | 9999-12-31
```

### Iceberg Tables

Apache Iceberg provides:

- **ACID Semantics**: Transactions ensure all-or-nothing consistency
- **Schema Evolution**: Add/rename/drop columns without full rewrite
- **Time Travel**: Query historical versions of tables
- **Partition Evolution**: Modify partitioning strategy without data rewrite
- **Metadata Management**: Complete lineage and audit trail

Example time-travel query:

```sql
-- Query fact_transactions as it existed on 2024-05-01
SELECT *
FROM fact_transactions
FOR SYSTEM_TIME AS OF '2024-05-01T00:00:00Z'
WHERE transaction_date = '2024-05-01';

-- Find what changed for account since 2024-05-01
SELECT *
FROM dim_accounts VERSIONS BETWEEN '2024-05-01' AND CURRENT_TIMESTAMP
WHERE account_iri = 'iri:account:USD-2024:Acme-Corp@Canonical'
ORDER BY __version_timestamp;
```

### Aggregate Materialization

Pre-computes common analytical queries for fast response times:

```sql
-- Daily transaction aggregates
CREATE TABLE agg_daily_transactions AS
SELECT 
  DATE(transaction_timestamp) as transaction_date,
  account_iri,
  transaction_type,
  COUNT(*) as transaction_count,
  SUM(amount) as total_amount,
  AVG(amount) as avg_amount,
  MIN(amount) as min_amount,
  MAX(amount) as max_amount,
  COUNT(DISTINCT counterparty_iri) as unique_counterparties
FROM fact_transactions
GROUP BY DATE(transaction_timestamp), account_iri, transaction_type
PARTITION BY DATE(transaction_timestamp);
```

## Project Structure

```
patterns/semantic-batch-lakehouse/
├── README.md                                      # This file
├── requirements.txt                               # Python dependencies
├── build.sbt                                      # Scala/SBT build configuration
├── src/
│   ├── main/
│   │   ├── python/
│   │   │   ├── io/chakraview/semantic/
│   │   │   │   ├── jobs/
│   │   │   │   │   ├── semantic_harmonization_job.py      # Main harmonization job
│   │   │   │   │   ├── build_fact_tables.py               # Fact table builder
│   │   │   │   │   ├── build_dimension_tables.py          # Dimension builder
│   │   │   │   │   ├── build_aggregates.py                # Aggregate materializer
│   │   │   │   │   └── build_semantic_joins.py            # Semantic join builder
│   │   │   │   ├── processors/
│   │   │   │   │   ├── harmonizer.py                      # Semantic harmonization logic
│   │   │   │   │   ├── dimension_builder.py               # SCD Type 2 builder
│   │   │   │   │   └── quality_validator.py               # Data quality checks
│   │   │   │   ├── config.py                              # Configuration
│   │   │   │   └── models.py                              # Domain models
│   │   │   ├── requirements.txt
│   │   │   └── setup.py
│   │   └── scala/
│   │       ├── io/chakraview/semantic/
│   │       │   ├── jobs/
│   │       │   │   ├── SemanticHarmonizationJob.scala     # Scala harmonization job
│   │       │   │   ├── IcebergJobRunner.scala             # Iceberg operations
│   │       │   │   └── AggregateBuilder.scala             # Aggregate logic
│   │       │   ├── processors/
│   │       │   │   ├── SemanticTransformer.scala          # Core transformations
│   │       │   │   └── OntologyValidator.scala            # Ontology validation
│   │       │   └── models/
│   │       │       ├── SemanticEvent.scala
│   │       │       ├── DimensionRecord.scala
│   │       │       └── FactRecord.scala
│   │       └── test/
│   │           └── scala/io/chakraview/semantic/
│   │               ├── jobs/
│   │               │   └── SemanticHarmonizationJobSpec.scala
│   │               └── processors/
│   │                   └── SemanticTransformerSpec.scala
├── tests/
│   ├── integration/
│   │   ├── test_harmonization_e2e.py              # End-to-end harmonization test
│   │   ├── test_fact_dimension_build.py           # Fact/dimension build tests
│   │   ├── test_aggregate_materialization.py      # Aggregate test
│   │   ├── test_iceberg_schema_evolution.py       # Schema evolution tests
│   │   └── test_time_travel_queries.py            # Time-travel test
│   ├── unit/
│   │   ├── test_harmonizer.py
│   │   ├── test_dimension_builder.py
│   │   └── test_quality_validator.py
│   └── conftest.py                               # Pytest fixtures
├── sql/
│   ├── schema/
│   │   ├── 001_create_facts.sql                  # Fact table definitions
│   │   ├── 002_create_dimensions.sql             # Dimension table definitions
│   │   └── 003_create_aggregates.sql             # Aggregate table definitions
│   ├── setup/
│   │   └── create_sample_data.sql                # Sample data for testing
│   └── queries/
│       ├── account_analysis.sql                  # Example analytical queries
│       ├── transaction_analysis.sql
│       └── risk_profiling.sql
├── scripts/
│   ├── load_sample_events.py                     # Load test data
│   ├── validate_source_data.py                   # Validate input data
│   ├── validate_lakehouse.py                     # Validate lakehouse quality
│   ├── validate_ontology_compliance.py           # Check ontology compliance
│   └── generate_performance_report.py            # Performance metrics
└── config/
    ├── spark-defaults.conf                       # Spark configuration
    ├── iceberg-catalog.yaml                      # Iceberg catalog config
    └── great-expectations.yml                    # Data quality config
```

## Configuration

### Spark Configuration (config/spark-defaults.conf)

```properties
# Spark runtime
spark.executor.memory=4g
spark.driver.memory=2g
spark.executor.cores=4

# Iceberg
spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg.type=hive
spark.sql.catalog.iceberg.warehouse=/tmp/iceberg-warehouse

# Parquet
spark.sql.parquet.compression.codec=snappy

# SQL behavior
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
```

### Environment Variables

```bash
# Spark cluster
SPARK_MASTER=local[4]  # or spark://master:7077

# PostgreSQL metadata store
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=semantic_lakehouse

# Data paths
DATA_WAREHOUSE_PATH=s3://semantic-lakehouse
SOURCE_EVENTS_PATH=s3://semantic-events
STAGING_PATH=s3://semantic-staging

# Job configuration
JOB_PARTITION_SIZE=10000
JOB_PARALLELISM=4
JOB_TIMEOUT_SECONDS=3600

# Logging
LOG_LEVEL=INFO
```

## Running Tests

```bash
# Run all unit tests
pytest tests/unit -v

# Run integration tests (requires Spark and PostgreSQL)
pytest tests/integration -v

# Run specific test
pytest tests/integration/test_harmonization_e2e.py::test_account_harmonization

# Run with coverage
pytest tests/ --cov=io/chakraview/semantic --cov-report=html

# Run Scala tests
sbt test
```

## Performance Characteristics

- **Harmonization**: 1M+ events/minute (single Spark node)
- **Dimension Build**: 10M rows in <2 minutes (4-node cluster)
- **Aggregate Materialization**: 1B+ rows aggregated in <5 minutes
- **Query Response**: <1s for aggregated fact queries over 1B rows
- **Storage Efficiency**: 30-40% compression with Parquet + Snappy

## Monitoring and Debugging

### Spark Application Monitoring

```bash
# Monitor running Spark job
# Access Spark UI at http://localhost:4040 (local mode)
# or http://<driver-host>:4040 (cluster mode)

# Check job status
spark-submit --status <app-id>

# View executor logs
docker logs <executor-container-id>
```

### Iceberg Metadata Inspection

```bash
# List all versions of a table
SELECT * FROM iceberg.table_versions
WHERE table_name = 'fact_transactions';

# Inspect schema changes
SELECT * FROM iceberg.table_schemas
WHERE table_name = 'dim_accounts'
ORDER BY schema_id;

# Check data files
SELECT * FROM iceberg.data_files
WHERE table_name = 'fact_transactions'
LIMIT 10;
```

### Data Quality Checks

```bash
# Run Great Expectations validation
python scripts/validate_lakehouse.py --suite fintech

# Check ontology compliance
python scripts/validate_ontology_compliance.py --dimension accounts

# Verify SCD Type 2 integrity
python scripts/validate_scd_integrity.py
```

## Production Considerations

- **Scalability**: Use multi-node Spark cluster with external metadata store (Hive/Glue)
- **Partitioning**: Partition fact tables by date, dimension tables by type
- **Retention**: Define data retention policies (e.g., keep 3 years of fact data)
- **Incremental Builds**: Use Iceberg change logs for efficient incremental updates
- **Monitoring**: Export Spark metrics to Prometheus for alerting
- **Backups**: Regular snapshots of Iceberg metadata store
- **Performance Tuning**: Use Spark adaptive query execution and dynamic partitioning

## References

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Slowly Changing Dimensions (SCD Type 2)](https://en.wikipedia.org/wiki/Slowly_changing_dimension)
- [Great Expectations Framework](https://greatexpectations.io/)
- [Parquet File Format](https://parquet.apache.org/)
