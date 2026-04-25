# ML Feature Pipeline — Feast + Redis + Spark

## When to use

Standard online/offline split. Feast handles point-in-time correct training data retrieval.

## How to run locally

```bash
cp .env.example .env && source .env
pip install -e ".[dev]"
docker compose up -d          # Redis + MinIO

# 1. Compute offline features (run daily or hourly)
spark-submit pipeline/compute_features.py

# 2. Apply Feast feature definitions
feast apply

# 3. Materialise offline -> online (Redis)
feast materialize-incremental $(date -u +%Y-%m-%dT%H:%M:%S)

# 4. Serve online features (in your model serving code)
python -c "
from feast import FeatureStore
store = FeatureStore(repo_path='.')
features = store.get_online_features(
    features=['order_stats:amount_usd'],
    entity_rows=[{'order_id': 'ord-001'}],
).to_dict()
print(features)
"
```

## Architecture

```
Spark batch job (hourly)
    ↓ writes Parquet to data/features/
Feast offline store (local Parquet)
    ↓ feast materialize-incremental
Redis (online store) — ms latency serving
```
