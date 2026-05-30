# Accounts Domain - Fintech Semantic Integration

**Owner:** Accounts Domain Team  
**Status:** Production (RDF semantic layer)  
**Last Updated:** 2026-05-30

---

## Overview

The Accounts domain is the **authoritative source for Customer and Account entities** in the fintech data mesh. It generates RDF triples representing customers and accounts, mints deterministic IRIs for entity deduplication across sources, and exposes a SPARQL endpoint for downstream queries.

### Key Responsibilities

- **Customer Master Data:** Email, KYC status, account relationships
- **Account Master Data:** Balance, status, account type, customer ownership
- **Cross-Domain Entity Linking:** Minting stable Customer IRIs used by other domains (Transactions, Risk/Compliance)
- **Semantic Availability:** SPARQL endpoint for federated queries

### Domain Principles (Shift-Left Semantics)

This domain operates under **shift-left semantic architecture**:
- ✅ Domain owns its RDF generation (no central semantic layer)
- ✅ Domain owns its IRI minting rules (deterministic for deduplication)
- ✅ Domain exposes SPARQL endpoint (for consumer queries)
- ✅ Other domains reference Customer IRIs minted here (no re-identification)

---

## Architecture

### Data Flow

```
Salesforce/Shopify (source systems)
        ↓
    Kafka (events)
        ↓
    CDC Pipeline (Accounts domain)
        ↓
   IRI Minting (deterministic, email + kyc_id)
        ↓
   Silver Tables (PostgreSQL)
        ↓
   RDF Transformation (Python + rdflib)
        ↓
   RDF Triples (N-Triples format)
        ↓
   Jena TDB2 (persistent storage)
        ↓
   Fuseki SPARQL Endpoint (http://localhost:3030/accounts/sparql)
        ↓
   Federated Queries (consumers query via SERVICE {} clauses)
```

### IRI Minting Rules

Customer IRIs are minted **deterministically** from email + KYC ID:

```
Key Fields: [email, kyc_id]
Normalization: lowercase + trim + concatenate
Hash: SHA-256 (8-char prefix)
Format: https://chakracommerce.com/customer#{hash}

Example:
  Input:  ("JOHN@ACME.COM", "KYC_123")
  Normalized: "john@acme.com|kyc_123"
  Hashed: "a1b2c3d4"
  Output: https://chakracommerce.com/customer#a1b2c3d4
```

Account IRIs are minted **directly** from account ID:

```
Key Fields: [account_id]
Format: https://chakracommerce.com/account#{account_id}

Example:
  Input:  "acct_001"
  Output: https://chakracommerce.com/account#acct_001
```

### RDF Schema (Semantic Model)

Accounts domain owns these RDF entity classes and properties:

**Entity Classes:**
- `fintech:Customer` - Legal entity holding accounts
- `fintech:Account` - Individual account within customer

**Properties Owned by Accounts:**
- `fintech:customerName` - Customer full name
- `fintech:customerEmail` - Customer email address
- `fintech:customerStatus` - Customer status (active, suspended, closed)
- `fintech:accountId` - Account identifier
- `fintech:accountBalance` - Account balance (decimal)
- `fintech:accountStatus` - Account status (active, inactive, closed)
- `fintech:accountType` - Account type (checking, savings, etc.)
- `fintech:accountOwner` - Reference to Customer IRI
- `fintech:iri` - Stable IRI for this entity
- `fintech:sourceSystem` - Source system identifier (always "accounts")
- `fintech:sourceIngestionTime` - When this data was ingested

**Example RDF (Turtle format):**

```turtle
@prefix fintech: <https://chakracommerce.com/ontology/fintech/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

<https://chakracommerce.com/customer#a1b2c3d4> a fintech:Customer ;
  rdf:type fintech:Customer ;
  fintech:customerName "John Doe" ;
  fintech:customerEmail "john@acme.com" ;
  fintech:customerStatus "active" ;
  fintech:sourceSystem "accounts" ;
  fintech:sourceIngestionTime "2026-05-30T12:00:00Z" .

<https://chakracommerce.com/account#acct_001> a fintech:Account ;
  rdf:type fintech:Account ;
  fintech:accountId "acct_001" ;
  fintech:accountBalance "5000.00"^^xsd:decimal ;
  fintech:accountStatus "active" ;
  fintech:accountType "checking" ;
  fintech:accountOwner <https://chakracommerce.com/customer#a1b2c3d4> ;
  fintech:sourceSystem "accounts" ;
  fintech:sourceIngestionTime "2026-05-30T12:00:00Z" .
```

---

## Project Structure

```
domains/accounts/
├── README.md                          # This file
├── requirements.txt                   # Python dependencies
├── config/                            # Configuration files
├── src/
│   └── main/
│       └── python/
│           └── semantic/
│               ├── __init__.py
│               ├── iri_resolver.py           # IRI minting (deterministic hashing)
│               ├── silver_to_rdf.py          # Silver → RDF transformation
│               ├── rdf_writer.py             # Jena Fuseki + Iceberg writer
│               └── tests/                    # Unit tests (IRI, RDF generation)
└── tests/
    ├── __init__.py
    └── integration/
        ├── __init__.py
        └── test_accounts_rdf_e2e.py         # End-to-end integration tests
```

---

## Getting Started

### Prerequisites

- Python 3.9+
- Docker (for Jena Fuseki)
- PostgreSQL 15
- pandas, rdflib, pytest

### Installation

1. **Install dependencies:**

```bash
cd domains/accounts
pip install -r requirements.txt
```

### Running the Transformation Pipeline

#### Step 1: Load Sample Data

The Accounts domain transformation uses sample customer and account CSVs for demonstration:

```bash
# View sample data
cd /home/gundu/portfolio/chakraview-data-engineering-patterns/.worktrees/fintech-semantic-integration

cat docs/case-study/fintech-semantic-integration/sample-data/accounts-customers.csv
# Output:
# customer_id,email,kyc_id,name,status
# cust_001,john@acme.com,kyc_9999,John Doe,active
# cust_002,jane@techcorp.io,kyc_8888,Jane Smith,active
# cust_003,bob@widgets.com,kyc_7777,Bob Wilson,suspended

cat docs/case-study/fintech-semantic-integration/sample-data/accounts-accounts.csv
# Output:
# account_id,customer_id,balance,status,account_type
# acct_001,cust_001,5000.00,active,checking
# acct_002,cust_001,2500.50,active,savings
# acct_003,cust_002,12000.00,active,checking
```

#### Step 2: Run RDF Transformation

From `domains/accounts/`:

```bash
python3 << 'EOF'
import pandas as pd
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, "src/main/python")

from semantic.iri_resolver import IriResolver
from semantic.silver_to_rdf import SilverToRdfTransformer

# Load Silver data
customers_df = pd.read_csv(
    "../../docs/case-study/fintech-semantic-integration/sample-data/accounts-customers.csv"
)
accounts_df = pd.read_csv(
    "../../docs/case-study/fintech-semantic-integration/sample-data/accounts-accounts.csv"
)

# Transform
transformer = SilverToRdfTransformer(
    IriResolver(),
    "../../docs/case-study/fintech-semantic-integration/shared-ontology.ttl"
)
customers_graph = transformer.transform_customers_to_rdf(customers_df)
accounts_graph = transformer.transform_accounts_to_rdf(accounts_df, customers_df)

print(f"Generated {len(customers_graph)} customer triples")
print(f"Generated {len(accounts_graph)} account triples")

# Save graphs for inspection
customers_graph.serialize("customers.ttl", format="turtle")
accounts_graph.serialize("accounts.ttl", format="turtle")
print("Saved to customers.ttl and accounts.ttl")
EOF
```

**Output:**
```
Generated 18 customer triples
Generated 21 account triples
Saved to customers.ttl and accounts.ttl
```

#### Step 3: Inspect Generated RDF

```bash
# View customer RDF
head -30 customers.ttl

# View account RDF
head -30 accounts.ttl

# Count total triples
grep -c "^" customers.ttl
grep -c "^" accounts.ttl
```

### Querying RDF Locally (Without Jena)

You can query RDF locally using rdflib before deploying to Jena:

```bash
python3 << 'EOF'
from rdflib import Graph

# Load the customer graph
g = Graph()
g.parse("customers.ttl", format="turtle")

# Query customers by email
query = """
  PREFIX fintech: <https://chakracommerce.com/ontology/fintech/>
  PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
  
  SELECT ?customer ?name ?email
  WHERE {
    ?customer rdf:type fintech:Customer ;
              fintech:customerName ?name ;
              fintech:customerEmail ?email .
  }
  LIMIT 10
"""

for row in g.query(query):
    print(f"{row.name} ({row.email})")
EOF
```

**Output:**
```
John Doe (john@acme.com)
Jane Smith (jane@techcorp.io)
Bob Wilson (bob@widgets.com)
```

---

## Testing

### Unit Tests (IRI Resolver, RDF Transformer)

```bash
pytest src/main/python/semantic/tests/ -v
```

Expected test coverage:
- ✅ IRI determinism (same input → same IRI)
- ✅ Case-insensitive IRI minting (email normalization)
- ✅ RDF triple generation (correct predicates and objects)
- ✅ Type triple generation (rdf:type for semantic querying)
- ✅ Account-customer linking (accountOwner references)
- ✅ Metadata tagging (sourceSystem, ingestionTime)
- ✅ Cross-domain account querying (federated query support)

### Integration Tests (End-to-end transformation + SPARQL)

```bash
pytest tests/integration/ -v
```

Expected test output (9+ tests):
```
test_accounts_rdf_e2e.py::TestAccountsRdfE2E::test_customer_iri_minting_deterministic PASSED
test_accounts_rdf_e2e.py::TestAccountsRdfE2E::test_customer_iri_minting_cross_domain PASSED
test_accounts_rdf_e2e.py::TestAccountsRdfE2E::test_customer_iri_different_customers PASSED
test_accounts_rdf_e2e.py::TestAccountsRdfE2E::test_account_iri_minting PASSED
test_accounts_rdf_e2e.py::TestAccountsRdfE2E::test_silver_to_rdf_customers PASSED
test_accounts_rdf_e2e.py::TestAccountsRdfE2E::test_silver_to_rdf_accounts PASSED
test_accounts_rdf_e2e.py::TestAccountsRdfE2E::test_customer_account_linking PASSED
test_accounts_rdf_e2e.py::TestAccountsRdfE2E::test_rdf_metadata_tagging PASSED
test_accounts_rdf_e2e.py::TestAccountsRdfE2E::test_rdf_type_uses_standard_vocabulary PASSED
test_accounts_rdf_e2e.py::TestAccountsRdfE2E::test_query_customer_by_email PASSED
test_accounts_rdf_e2e.py::TestAccountsRdfE2E::test_query_account_by_customer PASSED
test_accounts_rdf_e2e.py::TestAccountsRdfE2E::test_rdf_graph_union PASSED
```

---

## Deploying to Jena Fuseki (Optional)

The Accounts domain can be deployed to Apache Jena Fuseki for persistent RDF storage and SPARQL endpoint exposure.

### Prerequisites for Jena Deployment

- Docker
- Jena Fuseki image (see fintech-semantic-integration case study for docker-compose)

### Deployment Steps

1. **Create Jena dataset configuration** (from case study):

```bash
# The case study provides a docker-compose with Jena pre-configured
cd docs/case-study/fintech-semantic-integration
docker-compose up -d jena
```

2. **Write RDF to Jena:**

```bash
python3 << 'EOF'
import pandas as pd
import sys
sys.path.insert(0, "src/main/python")

from semantic.iri_resolver import IriResolver
from semantic.silver_to_rdf import SilverToRdfTransformer
from semantic.rdf_writer import RdfWriter

# Load and transform
customers_df = pd.read_csv(
    "../../docs/case-study/fintech-semantic-integration/sample-data/accounts-customers.csv"
)
accounts_df = pd.read_csv(
    "../../docs/case-study/fintech-semantic-integration/sample-data/accounts-accounts.csv"
)

transformer = SilverToRdfTransformer(
    IriResolver(),
    "../../docs/case-study/fintech-semantic-integration/shared-ontology.ttl"
)
customers_graph = transformer.transform_customers_to_rdf(customers_df)
accounts_graph = transformer.transform_accounts_to_rdf(accounts_df, customers_df)

# Combine and write
combined = customers_graph + accounts_graph

writer = RdfWriter(endpoint="http://localhost:3030/accounts/sparql")
success = writer.write_to_jena(combined)

if success:
    print("RDF successfully written to Jena Fuseki!")
else:
    print("Failed to write RDF. Ensure Jena is running: docker-compose ps")
EOF
```

3. **Query via SPARQL HTTP API:**

```bash
curl -X GET 'http://localhost:3030/accounts/sparql' \
  --data-urlencode 'query=
    PREFIX fintech: <https://chakracommerce.com/ontology/fintech/>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    
    SELECT ?customer ?name ?email
    WHERE {
      ?customer rdf:type fintech:Customer ;
                fintech:customerName ?name ;
                fintech:customerEmail ?email .
    }
    LIMIT 10
  ' \
  -H 'Accept: application/json'
```

**Output:**
```json
{
  "head": {
    "vars": ["customer", "name", "email"]
  },
  "results": {
    "bindings": [
      {
        "customer": { "type": "uri", "value": "https://chakracommerce.com/customer#a1b2c3d4" },
        "name": { "type": "literal", "value": "John Doe" },
        "email": { "type": "literal", "value": "john@acme.com" }
      }
    ]
  }
}
```

### Verifying Jena Deployment

```bash
# Check if Jena is running
curl -s http://localhost:3030/ | grep -q Fuseki && echo "Jena Fuseki is running"

# Query for all customers
curl -s -X GET 'http://localhost:3030/accounts/sparql' \
  --data-urlencode 'query=PREFIX fintech: <https://chakracommerce.com/ontology/fintech/> SELECT (COUNT(?c) as ?count) WHERE { ?c a fintech:Customer . }' \
  -H 'Accept: application/json' | grep -o '"value":"[0-9]*"'
```

---

## Troubleshooting

### Problem: "URIRef(None) failed" during transformation

**Cause:** Account references a customer that doesn't exist in the IRI map.

**Solution:** Verify all accounts in the input CSV have corresponding customers:

```bash
# Extract unique customer IDs from accounts
cut -d',' -f2 accounts-accounts.csv | sort -u

# Extract unique customer IDs from customers
cut -d',' -f1 accounts-customers.csv | sort -u

# Compare (all account customer_ids should be in customer list)
```

### Problem: SPARQL query returns 0 results

**Cause:** RDF may not be loaded into Jena yet, or query syntax is incorrect.

**Solution:**
1. Verify Jena Fuseki is running: `docker-compose ps`
2. Verify RDF was written: Check Jena UI at http://localhost:3030/
3. Test local query first:

```bash
python3 -c "
from rdflib import Graph
g = Graph()
g.parse('customers.ttl', format='turtle')
query = '''PREFIX fintech: <https://chakracommerce.com/ontology/fintech/>
SELECT (COUNT(?c) as ?count) WHERE { ?c a fintech:Customer . }'''
for row in g.query(query):
    print(f'Local customer count: {row.count}')
"
```

### Problem: Case-sensitivity issues in entity matching

**Cause:** Email addresses not normalized before IRI lookup.

**Solution:** IRI minting normalizes automatically (lowercase + trim). Ensure input data has consistent structure or normalize in CDC layer:

```bash
# Clean CSV before loading
sed 's/[[:space:]]*$//' accounts-customers.csv | tr 'A-Z' 'a-z' > accounts-customers-clean.csv
```

### Problem: Jena connection refused

**Cause:** Jena Fuseki not running or not accessible at localhost:3030

**Solution:**
```bash
# Start Jena (from case study directory)
cd docs/case-study/fintech-semantic-integration
docker-compose up -d

# Wait for startup
sleep 10

# Verify
curl http://localhost:3030/
```

---

## Cross-Domain Integration

### How Other Domains Reference Customers

The Transactions and Risk/Compliance domains reference Customer IRIs minted here, enabling federated queries without centralizing entity resolution.

**Transactions Domain (transactionDebtor):**
```sparql
PREFIX fintech: <https://chakracommerce.com/ontology/fintech/>

SELECT ?transaction ?debtor ?debtorEmail
WHERE {
  # Query Accounts domain for customer email
  SERVICE <http://localhost:3030/accounts/sparql> {
    ?debtor fintech:customerEmail ?debtorEmail .
  }
  # Query Transactions domain
  ?transaction fintech:transactionDebtor ?debtor .
}
```

**Risk/Compliance Domain (RiskProfile):**
```sparql
PREFIX fintech: <https://chakracommerce.com/ontology/fintech/>

SELECT ?riskProfile ?customer ?status
WHERE {
  # Get customer status from Accounts
  SERVICE <http://localhost:3030/accounts/sparql> {
    ?customer fintech:customerStatus ?status .
  }
  # Get risk profile from Risk domain
  ?riskProfile fintech:forCustomer ?customer .
}
```

### IRI Stability Guarantee

The Accounts domain guarantees IRI stability: **same customer input always produces the same IRI**, enabling:

- **Deduplication:** Multiple data sources with same customer are merged at query time
- **Non-authoritative copies:** Other domains safely reference Accounts-minted IRIs without owning them
- **Evolution:** If customer data changes, IRI remains stable; only customer properties change

---

## Performance Characteristics

| Operation | Time | Notes |
|-----------|------|-------|
| IRI minting (10K customers) | <1s | SHA-256 hashing is fast |
| RDF generation (10K customers) | <5s | Python + rdflib, single-threaded |
| Local SPARQL query (10K triples) | <100ms | rdflib in-memory graph |
| Jena SPARQL query (100K triples) | <500ms | TDB2 optimized for SPARQL |
| Federated query (3 domains) | <500ms | Network dependent, service discovery overhead |

---

## Contributing

### Adding New Customer Properties

1. **Define in shared ontology** (`shared-ontology.ttl`):
```turtle
fintech:newProperty
  a rdf:Property ;
  rdfs:label "New Property"@en ;
  rdfs:domain fintech:Customer ;
  rdfs:range xsd:string ;
  fintech:owningDomain "accounts" .
```

2. **Add to transformer** (`src/main/python/semantic/silver_to_rdf.py`):
```python
if 'new_field' in row:
    output_graph.add((iri_ref, self.FINTECH.newProperty, Literal(row['new_field'])))
```

3. **Add test case** (`tests/integration/test_accounts_rdf_e2e.py`):
```python
def test_new_property(self, transformer, sample_customers_df):
    """Test: New property is added to RDF triples"""
    graph = transformer.transform_customers_to_rdf(sample_customers_df)
    query = """
        PREFIX fintech: <https://chakracommerce.com/ontology/fintech/>
        SELECT ?value WHERE {
            ?customer fintech:newProperty ?value .
        }
    """
    results = list(graph.query(query))
    assert len(results) > 0, "New property should appear in triples"
```

4. **Update this README** with new property documentation

---

## Related Documentation

- **Shared Ontology:** [`docs/case-study/fintech-semantic-integration/shared-ontology.ttl`](../../docs/case-study/fintech-semantic-integration/shared-ontology.ttl)
- **Domain Registry:** [`domains/federation/domain-registry.yaml`](../federation/domain-registry.yaml)
- **Federated Query Examples:** [`domains/federation/queries/`](../federation/queries/)
- **Case Study Guide:** [`docs/case-study/fintech-semantic-integration/README.md`](../../docs/case-study/fintech-semantic-integration/README.md)
- **SPARQL Query Tutorial:** https://www.w3.org/TR/sparql11-query/

---

## Contact & Support

**Accounts Domain Team:** accounts-team@chakracommerce.com  
**Semantic Architecture Lead:** semantic-arch@chakracommerce.com  
**Data Mesh Platform:** datamesh-platform@chakracommerce.com

---

**Last Revised:** 2026-05-30  
**Version:** 1.0  
**Status:** Production
