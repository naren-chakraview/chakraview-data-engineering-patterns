package io.chakraview.cdc

import org.apache.spark.sql.types._

object CdcSchema {
  val schema: StructType = StructType(Seq(
    StructField("order_id",    StringType,    nullable = false),
    StructField("customer_id", StringType,    nullable = true),
    StructField("status",      StringType,    nullable = true),
    StructField("amount_cents", LongType,     nullable = true),
    StructField("placed_at",   TimestampType, nullable = true),
    StructField("updated_at",  TimestampType, nullable = true),
    StructField("__deleted",   StringType,    nullable = true),
  ))
}
