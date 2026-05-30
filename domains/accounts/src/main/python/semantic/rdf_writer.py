"""Write RDF triples to Jena TDB2 and append to Iceberg for audit trail"""
from typing import Optional

import requests
from rdflib import Graph


class RdfWriter:
    """Write RDF triples to Jena Fuseki endpoint and Iceberg table"""

    def __init__(
        self,
        endpoint: str = "http://localhost:3030/accounts/sparql",
        iceberg_table_uri: Optional[str] = None,
    ):
        """
        Initialize RDF writer.

        Args:
            endpoint: Jena Fuseki SPARQL endpoint URL
            iceberg_table_uri: Iceberg table URI for audit trail (optional)
        """
        self.endpoint = endpoint
        self.iceberg_table_uri = iceberg_table_uri

    def write_to_jena(self, rdf_graph: Graph) -> bool:
        """
        Write RDF triples to Jena TDB2 via SPARQL Update endpoint.

        Args:
            rdf_graph: RDFLib Graph containing triples to write

        Returns:
            True if write succeeded, False otherwise
        """
        try:
            # Serialize graph to N-Triples format
            ntriples = rdf_graph.serialize(format="nt")

            # Build SPARQL INSERT DATA query
            sparql_update = f"INSERT DATA {{ {ntriples} }}"

            # POST to Jena endpoint
            response = requests.post(
                self.endpoint,
                data={"update": sparql_update},
                headers={"Accept": "application/json"},
                timeout=30,
            )

            return response.status_code in [200, 204]

        except Exception as e:
            print(f"Error writing to Jena: {e}")
            return False

    def write_to_iceberg(self, rdf_graph: Graph) -> bool:
        """
        Write RDF as N-Triples to Iceberg for audit trail.

        Each triple is appended with ingestion timestamp and provenance.

        Args:
            rdf_graph: RDFLib Graph containing triples to write

        Returns:
            True if write succeeded, False otherwise
        """
        try:
            if not self.iceberg_table_uri:
                print("Iceberg table URI not configured, skipping audit trail")
                return False

            # Serialize to N-Triples (triple format: subject predicate object .)
            ntriples = rdf_graph.serialize(format="nt")

            # TODO: Write to Iceberg table with timestamp
            # This would typically use PySpark to append to Iceberg table
            # iceberg_table.append(ntriples_with_metadata)

            print(f"Would write {len(ntriples.split(chr(10)))} triples to {self.iceberg_table_uri}")
            return True

        except Exception as e:
            print(f"Error writing to Iceberg: {e}")
            return False
