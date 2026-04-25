package io.chakraview.lambda;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

public class EventParser implements MapFunction<String, Row> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public Row map(String json) throws Exception {
        JsonNode node = MAPPER.readTree(json);
        Row row = Row.withNames();
        row.setField("order_id",    node.path("order_id").asText());
        row.setField("customer_id", node.path("customer_id").asText());
        row.setField("status",      node.path("status").asText());
        row.setField("amount_cents", node.path("amount_cents").asLong());
        row.setField("processed_at", java.time.Instant.now().toString());
        row.setField("path",        "streaming");
        return row;
    }
}
