# Semantic Federated Query

Unified query layer combining Trino SQL + SPARQL for ontology-driven analytics.

## Usage

```bash
docker-compose up -d
# Run SPARQL queries against Jena: curl http://localhost:3030/sparql
# Run SQL queries against Trino: curl http://localhost:8080
```

## Example Queries

**Active Customers (SPARQL):** See queries/examples/active-customers.sparql
**Customer Invoices (SPARQL):** See queries/examples/customer-invoices.sparql

## Testing

```bash
sbt test
```
