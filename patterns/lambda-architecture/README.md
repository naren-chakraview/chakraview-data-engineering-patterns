# Lambda Architecture

Two parallel paths — streaming (low latency, approximate) and batch (high latency, accurate) —
merged at query time. Use only when both are genuinely required by different stakeholders.

## Variants

| Variant | Streaming engine | Batch engine | Table format | Pull branch |
|---|---|---|---|---|
| `flink-spark-iceberg` | Flink 1.19 (Java) | Spark 3.5 (Scala) | Iceberg 1.5 | `pattern/lambda-architecture/flink-spark-iceberg` |
| `spark-streaming-batch-delta` | Spark 3.5 Structured Streaming (Scala) | Spark 3.5 Batch (Scala) | Delta 3.1 | `pattern/lambda-architecture/spark-streaming-batch-delta` |

## Merge strategy

Both paths write to the same Iceberg/Delta table with a `path` column: `streaming` or `batch`.
At query time, Iceberg time travel or Delta's versioning lets analysts choose the freshest streaming
snapshot OR the latest batch-corrected snapshot. The serving layer queries the batch path for
reporting and the streaming path for real-time dashboards.

## Warning

Lambda doubles your operational surface: two pipelines, two sets of SLAs, two codebases.
Before committing, validate that a single streaming path (Streaming Lakehouse pattern) cannot
meet your requirements.
