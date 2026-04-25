package io.chakraview.streaming;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

public final class IcebergSchema {
    private IcebergSchema() {}

    public static final Schema ORDERS_SCHEMA = new Schema(
        Types.NestedField.required(1, "order_id",     Types.StringType.get()),
        Types.NestedField.required(2, "customer_id",  Types.StringType.get()),
        Types.NestedField.required(3, "amount_cents", Types.LongType.get()),
        Types.NestedField.optional(4, "status",       Types.StringType.get()),
        Types.NestedField.required(5, "placed_at",    Types.TimestampType.withZone())
    );
}
