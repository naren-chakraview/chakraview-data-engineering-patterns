package io.chakraview.streaming

import org.apache.spark.sql.types._

object EventSchema {
  val schema: StructType = StructType(Seq(
    StructField("order_id",     StringType,    nullable = false),
    StructField("customer_id",  StringType,    nullable = false),
    StructField("amount_cents", LongType,      nullable = false),
    StructField("status",       StringType,    nullable = true),
    StructField("placed_at",    TimestampType, nullable = false),
  ))
}
