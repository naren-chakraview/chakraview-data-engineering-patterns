"""Transform Accounts domain Silver tables to RDF triples"""
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from rdflib import Graph, Literal, Namespace, RDF, URIRef

from .iri_resolver import IriResolver


class SilverToRdfTransformer:
    """Transform Silver layer data to RDF triples for Accounts domain"""

    def __init__(
        self,
        iri_resolver: IriResolver,
        shared_ontology_path: Optional[str] = None,
    ):
        """
        Initialize transformer with IRI resolver and shared ontology.

        Args:
            iri_resolver: IriResolver instance for minting deterministic IRIs
            shared_ontology_path: Path to shared fintech ontology (Turtle format)
        """
        self.iri_resolver = iri_resolver
        self.g = Graph()

        # Load shared ontology if provided
        if shared_ontology_path:
            self.g.parse(str(shared_ontology_path), format="turtle")

        # Define fintech ontology namespace
        self.FINTECH = Namespace("https://chakracommerce.com/ontology/fintech/")

    def transform_customers_to_rdf(self, customers_df: pd.DataFrame) -> Graph:
        """
        Transform customers Silver table to RDF.

        Expected columns:
        - email: customer email
        - kyc_id: KYC identifier
        - name: customer name
        - status: account status (active, suspended, closed)

        Generates triples:
        - customer IRI -> rdf:type -> fintech:Customer
        - customer IRI -> fintech:customerName -> name
        - customer IRI -> fintech:customerEmail -> email
        - customer IRI -> fintech:status -> status
        - customer IRI -> fintech:sourceSystem -> "accounts"
        - customer IRI -> fintech:sourceIngestionTime -> ingestion_time

        Args:
            customers_df: DataFrame with customer Silver data

        Returns:
            RDF Graph with customer triples
        """
        # Create fresh graph for this transformation
        output_graph = Graph()

        # Copy ontology if loaded
        for s, p, o in self.g:
            output_graph.add((s, p, o))

        for _, row in customers_df.iterrows():
            # Mint deterministic IRI for customer
            iri = self.iri_resolver.mint_customer_iri(row["email"], row["kyc_id"])
            iri_ref = URIRef(iri)

            # Add type triple
            output_graph.add((iri_ref, RDF.type, self.FINTECH.Customer))

            # Add customer properties
            output_graph.add((iri_ref, self.FINTECH.customerName, Literal(row["name"])))
            output_graph.add(
                (iri_ref, self.FINTECH.customerEmail, Literal(row["email"]))
            )

            # Add status if present
            if "status" in row:
                output_graph.add((iri_ref, self.FINTECH.status, Literal(row["status"])))

            # Add provenance metadata
            output_graph.add(
                (iri_ref, self.FINTECH.sourceSystem, Literal("accounts"))
            )
            output_graph.add(
                (
                    iri_ref,
                    self.FINTECH.sourceIngestionTime,
                    Literal(datetime.now()),
                )
            )

        return output_graph

    def transform_accounts_to_rdf(
        self,
        accounts_df: pd.DataFrame,
        customers_df: pd.DataFrame,
    ) -> Graph:
        """
        Transform accounts Silver table to RDF with customer linking.

        Expected columns in accounts_df:
        - account_id: unique account identifier
        - customer_id: reference to customer
        - balance: account balance (decimal)
        - status: account status
        - account_type: type of account (checking, savings, etc.)

        Expected columns in customers_df:
        - customer_id: unique customer identifier
        - email: customer email
        - kyc_id: customer KYC ID

        Generates triples:
        - account IRI -> rdf:type -> fintech:Account
        - account IRI -> fintech:accountBalance -> balance
        - account IRI -> fintech:accountStatus -> status
        - account IRI -> fintech:accountType -> type
        - account IRI -> fintech:accountOwner -> customer IRI
        - account IRI -> fintech:sourceSystem -> "accounts"
        - account IRI -> fintech:sourceIngestionTime -> ingestion_time

        Args:
            accounts_df: DataFrame with account Silver data
            customers_df: DataFrame with customer Silver data (for IRI lookup)

        Returns:
            RDF Graph with account triples
        """
        # Create fresh graph for this transformation
        output_graph = Graph()

        # Copy ontology if loaded
        for s, p, o in self.g:
            output_graph.add((s, p, o))

        # Create customer lookup map: customer_id -> (email, kyc_id)
        customer_lookup = {}
        for _, row in customers_df.iterrows():
            customer_lookup[row["customer_id"]] = (row["email"], row["kyc_id"])

        for _, row in accounts_df.iterrows():
            # Mint IRI for account
            account_iri = URIRef(
                self.iri_resolver.mint_account_iri(row["account_id"])
            )

            # Get customer IRI
            if row["customer_id"] in customer_lookup:
                email, kyc_id = customer_lookup[row["customer_id"]]
                customer_iri = URIRef(
                    self.iri_resolver.mint_customer_iri(email, kyc_id)
                )
            else:
                # Skip if customer not found (data integrity issue)
                continue

            # Add type triple
            output_graph.add((account_iri, RDF.type, self.FINTECH.Account))

            # Add account properties
            output_graph.add(
                (
                    account_iri,
                    self.FINTECH.accountBalance,
                    Literal(row["balance"], datatype=self.FINTECH.Decimal),
                )
            )
            output_graph.add(
                (account_iri, self.FINTECH.accountStatus, Literal(row["status"]))
            )
            output_graph.add(
                (
                    account_iri,
                    self.FINTECH.accountType,
                    Literal(row["account_type"]),
                )
            )

            # Link account to customer
            output_graph.add((account_iri, self.FINTECH.accountOwner, customer_iri))

            # Add provenance metadata
            output_graph.add(
                (account_iri, self.FINTECH.sourceSystem, Literal("accounts"))
            )
            output_graph.add(
                (
                    account_iri,
                    self.FINTECH.sourceIngestionTime,
                    Literal(datetime.now()),
                )
            )

        return output_graph
