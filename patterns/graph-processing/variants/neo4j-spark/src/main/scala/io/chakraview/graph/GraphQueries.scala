package io.chakraview.graph

// Cypher queries used by the Neo4j Spark Connector.
// Keep queries here so they can be reviewed and versioned separately from the Spark job.
object GraphQueries {

  // Load all orders with their customers
  val ordersWithCustomers: String =
    """MATCH (o:Order)-[:PLACED_BY]->(c:Customer)
      |RETURN o.id AS order_id, c.id AS customer_id,
      |       o.amount AS amount_cents, o.status AS status""".stripMargin

  // Find customers with more than N orders (fraud ring seed)
  val highVolumeCustomers: String =
    """MATCH (c:Customer)-[:PLACED_BY]-(o:Order)
      |WITH c, count(o) AS order_count
      |WHERE order_count > $minOrders
      |RETURN c.id AS customer_id, order_count""".stripMargin

  // Find all nodes reachable within 3 hops from a seed customer
  val reachableWithin3Hops: String =
    """MATCH path = (seed:Customer {id: $customerId})-[*1..3]-(connected)
      |RETURN DISTINCT connected.id AS node_id, labels(connected)[0] AS node_type,
      |       length(path) AS hops""".stripMargin
}
