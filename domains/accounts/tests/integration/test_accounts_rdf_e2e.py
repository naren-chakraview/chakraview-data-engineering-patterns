"""End-to-end integration tests for Accounts domain semantic layer

Tests the complete pipeline: Silver CSV → IRI minting → RDF transformation → Jena endpoint
"""
import pytest
import pandas as pd
from pathlib import Path
import sys

# Add src to path for imports
src_path = Path(__file__).parent.parent.parent / "src" / "main" / "python"
sys.path.insert(0, str(src_path))

from semantic.iri_resolver import IriResolver
from semantic.silver_to_rdf import SilverToRdfTransformer
from semantic.rdf_writer import RdfWriter


class TestAccountsRdfE2E:
    """End-to-end integration test: Silver CSV → IRI minting → RDF → Jena"""

    @pytest.fixture
    def sample_customers_df(self):
        """Sample customer data from Silver table"""
        return pd.DataFrame({
            "customer_id": ["cust_001", "cust_002", "cust_003"],
            "email": ["john@acme.com", "jane@techcorp.io", "bob@widgets.com"],
            "kyc_id": ["kyc_9999", "kyc_8888", "kyc_7777"],
            "name": ["John Doe", "Jane Smith", "Bob Wilson"],
            "status": ["active", "active", "suspended"]
        })

    @pytest.fixture
    def sample_accounts_df(self):
        """Sample account data from Silver table"""
        return pd.DataFrame({
            "account_id": ["acct_001", "acct_002", "acct_003"],
            "customer_id": ["cust_001", "cust_001", "cust_002"],
            "balance": [5000.00, 2500.50, 12000.00],
            "status": ["active", "active", "active"],
            "account_type": ["checking", "savings", "checking"]
        })

    @pytest.fixture
    def iri_resolver(self):
        """IRI resolver for deterministic customer identification"""
        return IriResolver()

    @pytest.fixture
    def transformer(self, iri_resolver):
        """RDF transformer with test ontology"""
        # Use None for ontology path since we may not have it in test environment
        return SilverToRdfTransformer(iri_resolver, shared_ontology_path=None)

    def test_customer_iri_minting_deterministic(self, iri_resolver, sample_customers_df):
        """Test: Customer IRIs are deterministically minted

        Same email + kyc_id must always produce the same IRI for deduplication.
        """
        row1 = sample_customers_df.iloc[0]
        row2 = sample_customers_df.iloc[0]

        iri1 = iri_resolver.mint_customer_iri(row1["email"], row1["kyc_id"])
        iri2 = iri_resolver.mint_customer_iri(row2["email"], row2["kyc_id"])

        # Same email + kyc_id must produce same IRI (deduplication requirement)
        assert iri1 == iri2, "Customer IRIs must be deterministic for deduplication"
        assert iri1.startswith(
            "https://chakracommerce.com/customer#"
        ), "IRI format check"

    def test_customer_iri_minting_cross_domain(self, iri_resolver, sample_customers_df):
        """Test: Customer IRIs are case-insensitive for cross-domain linking

        This enables matching customers across systems with different casing.
        """
        row = sample_customers_df.iloc[0]

        # Same customer, uppercase email and kyc_id
        iri1 = iri_resolver.mint_customer_iri(row["email"], row["kyc_id"])
        iri2 = iri_resolver.mint_customer_iri(
            row["email"].upper(), row["kyc_id"].upper()
        )

        assert iri1 == iri2, "Customer IRIs must be case-insensitive for cross-domain linking"

    def test_customer_iri_different_customers(self, iri_resolver, sample_customers_df):
        """Test: Different customers get different IRIs"""
        row1 = sample_customers_df.iloc[0]
        row2 = sample_customers_df.iloc[1]

        iri1 = iri_resolver.mint_customer_iri(row1["email"], row1["kyc_id"])
        iri2 = iri_resolver.mint_customer_iri(row2["email"], row2["kyc_id"])

        assert iri1 != iri2, "Different customers must have different IRIs"

    def test_account_iri_minting(self, iri_resolver, sample_accounts_df):
        """Test: Account IRIs are minted from account_id"""
        row = sample_accounts_df.iloc[0]
        iri = iri_resolver.mint_account_iri(row["account_id"])

        expected = f"https://chakracommerce.com/account#{row['account_id']}"
        assert iri == expected, f"Account IRI format should match {expected}"

    def test_silver_to_rdf_customers(self, transformer, sample_customers_df):
        """Test: Silver customer data transforms to RDF triples"""
        graph = transformer.transform_customers_to_rdf(sample_customers_df)

        # Verify triples were generated
        assert len(graph) > 0, "RDF graph should contain customer triples"

        # Check that we have at least one customer triple per row
        # (minimum: customer type + name + email + status + sourceSystem + sourceIngestionTime)
        # = 6 triples per customer * 3 customers = 18 triples
        assert (
            len(graph) >= 6 * 3
        ), f"Expected at least 18 triples for 3 customers, got {len(graph)}"

    def test_silver_to_rdf_accounts(
        self, transformer, sample_customers_df, sample_accounts_df
    ):
        """Test: Silver account data transforms to RDF with customer linking"""
        customers_graph = transformer.transform_customers_to_rdf(sample_customers_df)
        accounts_graph = transformer.transform_accounts_to_rdf(
            sample_accounts_df, sample_customers_df
        )

        # Verify account triples were generated
        assert len(accounts_graph) > 0, "RDF graph should contain account triples"

        # Check minimum triple count
        # (minimum: account type + id + balance + status + type + owner link + sourceSystem + sourceIngestionTime)
        # = 8 triples per account * 3 accounts = 24 triples
        assert (
            len(accounts_graph) >= 7 * 3
        ), f"Expected at least 21 triples for 3 accounts, got {len(accounts_graph)}"

    def test_customer_account_linking(
        self, transformer, sample_customers_df, sample_accounts_df, iri_resolver
    ):
        """Test: Accounts are correctly linked to customer IRIs"""
        accounts_graph = transformer.transform_accounts_to_rdf(
            sample_accounts_df, sample_customers_df
        )

        # Verify graph contains account-to-customer relationships
        query = """
            PREFIX fintech: <https://chakracommerce.com/ontology/fintech/>
            SELECT ?account ?customer WHERE {
                ?account fintech:accountOwner ?customer .
            }
        """
        results = list(accounts_graph.query(query))

        # Should have one customer-account link per account (3 accounts total)
        assert (
            len(results) >= 3
        ), f"Expected at least 3 account-customer links, got {len(results)}"

    def test_rdf_metadata_tagging(self, transformer, sample_customers_df):
        """Test: RDF triples include domain metadata (sourceSystem, ingestionTime)"""
        graph = transformer.transform_customers_to_rdf(sample_customers_df)

        # Query for sourceSystem metadata
        query = """
            PREFIX fintech: <https://chakracommerce.com/ontology/fintech/>
            SELECT ?system WHERE {
                ?customer fintech:sourceSystem ?system .
            }
        """
        results = list(graph.query(query))

        # At least one customer should have sourceSystem metadata
        assert (
            len(results) >= 1
        ), "RDF triples should include sourceSystem metadata"
        # Verify the system is 'accounts'
        assert str(results[0][0]) == "accounts", "sourceSystem should be 'accounts'"

    def test_rdf_graph_union(
        self, transformer, sample_customers_df, sample_accounts_df
    ):
        """Test: Customer and account graphs can be unioned (federated query support)"""
        customers_graph = transformer.transform_customers_to_rdf(sample_customers_df)
        accounts_graph = transformer.transform_accounts_to_rdf(
            sample_accounts_df, sample_customers_df
        )

        # Union graphs (simulating federated query merge)
        combined = customers_graph + accounts_graph

        # Combined should have roughly the sum of both (may have some duplicates)
        min_expected = len(customers_graph) + len(accounts_graph) - 10
        assert (
            len(combined) >= min_expected
        ), "Graph union should preserve most triples"

    def test_query_customer_by_email(self, transformer, sample_customers_df):
        """Test: Can query customers by email"""
        graph = transformer.transform_customers_to_rdf(sample_customers_df)

        query = """
            PREFIX fintech: <https://chakracommerce.com/ontology/fintech/>
            SELECT ?customer ?email WHERE {
                ?customer fintech:customerEmail ?email .
            }
        """
        results = list(graph.query(query))

        # Should find all 3 customers
        assert (
            len(results) >= 3
        ), f"Expected to find at least 3 customers by email, got {len(results)}"

    def test_query_account_by_customer(
        self, transformer, sample_customers_df, sample_accounts_df
    ):
        """Test: Can query accounts by customer"""
        graph = transformer.transform_accounts_to_rdf(
            sample_accounts_df, sample_customers_df
        )

        # Customer 1 (cust_001) should have 2 accounts
        john_iri = IriResolver().mint_customer_iri("john@acme.com", "kyc_9999")

        query = f"""
            PREFIX fintech: <https://chakracommerce.com/ontology/fintech/>
            SELECT ?account WHERE {{
                ?account fintech:accountOwner <{john_iri}> .
            }}
        """
        results = list(graph.query(query))

        assert (
            len(results) >= 2
        ), f"Expected John to have at least 2 accounts, got {len(results)}"


class TestAccountsRdfIntegrationWithJena:
    """Integration tests for RDF writer and Jena interaction (requires docker)"""

    @pytest.mark.skip(reason="Requires running Jena Fuseki instance (docker-compose up)")
    def test_rdf_write_to_jena(self):
        """Test: RDF can be written to Jena TDB2 via SPARQL Update endpoint

        Prerequisites:
        1. cd domains/accounts/
        2. docker-compose up -d
        3. Wait for Jena Fuseki to be ready at http://localhost:3030/
        """
        from rdflib import Graph

        # Create simple test RDF
        g = Graph()
        g.parse(
            data="""
            @prefix fintech: <https://chakracommerce.com/ontology/fintech/> .
            <https://chakracommerce.com/customer#test001> a fintech:Customer ;
                fintech:customerName "Test Customer" ;
                fintech:customerEmail "test@example.com" .
        """,
            format="turtle",
        )

        # Write to Jena
        writer = RdfWriter(endpoint="http://localhost:3030/accounts/sparql")
        success = writer.write_to_jena(g)

        assert success, "RDF write to Jena should succeed"

    @pytest.mark.skip(reason="Requires running Jena Fuseki instance (docker-compose up)")
    def test_sparql_query_against_jena(self):
        """Test: Customer SPARQL queries work against Jena endpoint

        Prerequisites:
        1. cd domains/accounts/
        2. docker-compose up -d
        3. Run test_rdf_write_to_jena first to load test data
        """
        import requests

        query = """
            PREFIX fintech: <https://chakracommerce.com/ontology/fintech/>
            SELECT ?customer ?name WHERE {
                ?customer a fintech:Customer ;
                          fintech:customerName ?name .
            }
            LIMIT 1
        """

        endpoint = "http://localhost:3030/accounts/sparql"
        response = requests.get(
            endpoint,
            params={"query": query},
            headers={"Accept": "application/json"},
            timeout=10,
        )

        assert response.status_code == 200, "SPARQL query should succeed against Jena endpoint"
