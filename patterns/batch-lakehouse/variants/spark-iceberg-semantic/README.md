# Semantic Batch Lakehouse

Transforms Silver Iceberg tables to RDF triples (Gold layer) using Apache Spark + Jena.

## Architecture

This pattern implements semantic data transformation in a medallion architecture:

- **Silver Layer**: Structured, cleaned data in Iceberg tables
- **Gold Layer**: RDF triples stored in Iceberg and Jena TDB2
- **Transformer**: Scala/Spark job that converts records to semantic triples
- **Writer**: Outputs to both Iceberg (for analytics) and Jena TDB2 (for semantic queries)

## Build

Requires Scala 2.12 and SBT 1.9+.

```bash
sbt assembly
```

This creates an uber-JAR with all dependencies: `target/scala-2.12/semantic-batch-lakehouse-assembly-1.0.0.jar`

## Run

1. Start services:

```bash
docker-compose up -d
```

2. Set environment variables:

```bash
export BASE_IRI=https://company.com
export ICEBERG_REST_URI=http://localhost:8181
export TDB2_PATH=/tmp/tdb2
```

3. Submit job:

```bash
spark-submit \
  --class io.chakraview.semantic.Main \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5:1.5.0 \
  target/scala-2.12/semantic-batch-lakehouse-assembly-1.0.0.jar
```

## Testing

Run the full test suite (4 tests: 2 transformer, 2 writer):

```bash
sbt test
```

Tests use ScalaTest 3.2.17 and include:
- SemanticTransformerSuite: RDF triple generation and reference creation
- JenaWriterSuite: Configuration and triple writing

## Configuration

Via environment variables (`.env.example`):

- `BASE_IRI`: Base IRI for semantic entities (default: https://company.com)
- `ICEBERG_REST_URI`: Iceberg REST catalog endpoint (default: http://localhost:8181)
- `TDB2_PATH`: Local path for Jena TDB2 dataset (default: /tmp/tdb2)

## Components

- **RDFConfig**: Configuration case class with validation
- **SemanticTransformer**: Converts records to RDF triples with camelCase predicates and literal escaping
- **JenaWriter**: Writes triples to Iceberg (parquet) and Jena TDB2 (in-memory/disk)
- **Main**: Entry point that orchestrates the batch job

## References

- [Apache Spark 3.5](https://spark.apache.org/releases/spark-release-3-5-0.html)
- [Apache Iceberg 1.5](https://iceberg.apache.org/)
- [Apache Jena 4.9](https://jena.apache.org/)
- [Medallion Architecture](https://www.databricks.com/blog/2022/06/24/use-the-medallion-lakehouse-architecture-to-build-data-platforms.html)
