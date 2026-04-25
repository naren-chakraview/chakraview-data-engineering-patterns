# Lambda Architecture

Two parallel processing paths — a streaming path (low latency, approximate) and a batch path (high latency, accurate) — whose outputs are merged at query time. Use only when both sub-minute freshness AND batch-level accuracy are simultaneously required.

## When to use

- Stakeholders require both: approximate results within seconds AND fully accurate results within hours
- Reprocessing is required regularly (batch corrects streaming approximations)
- Team can operate both Flink/Spark Streaming AND a separate Spark batch job
- The query layer (Iceberg/Delta time travel, or a serving layer) can merge streaming and batch output

## When NOT to use

- Sub-minute latency is not required — use Batch Lakehouse (half the operational complexity)
- Batch accuracy is not required — use Streaming Lakehouse
- Your team cannot staff two processing paths — the operational cost is high; Lambda is only justified when the business requirement genuinely demands it

## Variants

| Variant | Stack | Use when |
|---|---|---|
| [flink-spark-iceberg](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/lambda-architecture/variants/flink-spark-iceberg) | Flink 1.19 (streaming) + Spark 3.5 (batch) + Iceberg | Need exactly-once streaming AND Iceberg portability |
| [spark-streaming-batch-delta](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/lambda-architecture/variants/spark-streaming-batch-delta) | Spark Structured Streaming + Spark Batch + Delta | Single Spark team, Delta as unifying format |

## Related patterns

- [Streaming Lakehouse](streaming-lakehouse.md) — if you can drop the batch path
- [Batch Lakehouse](batch-lakehouse.md) — if you can drop the streaming path
