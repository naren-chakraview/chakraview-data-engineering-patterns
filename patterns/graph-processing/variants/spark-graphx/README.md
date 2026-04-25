# Graph Processing — Spark GraphX

## When to use

Batch graph algorithms (PageRank, connected components, triangle count) on large distributed graphs.

## How to run locally

```bash
cp .env.example .env
docker compose up -d

# Upload a sample edge list (src_id,dst_id,weight):
echo -e "1,2,1.0\n2,3,0.5\n3,1,0.8\n1,4,1.2" | \
  docker exec -i <minio-container> sh -c \
  'mc alias set local http://localhost:9000 minioadmin minioadmin && \
   cat > /tmp/edges.csv && \
   mc cp /tmp/edges.csv local/chakra-lakehouse/raw/graph-edges/edges.csv'

sbt assembly
spark-submit --class io.chakraview.graph.Main --master local[*] \
  target/scala-2.12/graph-processing-spark-graphx-assembly-0.1.0.jar
```

## Algorithms

| Algorithm | Output | Use case |
|---|---|---|
| `GraphAlgorithms.pageRank` | vertex → score | Link/authority ranking |
| `GraphAlgorithms.connectedComponents` | vertex → component_id | Fraud rings, community detection |
| `GraphAlgorithms.triangleCount` | vertex → count | Social network density |
