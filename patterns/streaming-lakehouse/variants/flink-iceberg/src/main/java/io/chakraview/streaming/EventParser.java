package io.chakraview.streaming;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

public class EventParser implements MapFunction<String, Row> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public Row map(String json) throws Exception {
        JsonNode node = MAPPER.readTree(json);
        Row row = new Row(5);
        row.setField(0, node.get("order_id").asText());
        row.setField(1, node.get("customer_id").asText());
        row.setField(2, node.get("amount_cents").asLong());
        row.setField(3, node.get("status").asText());
        row.setField(4, node.get("placed_at").asLong());
        return row;
    }

    public static long extractTimestamp(String json) {
        try {
            return MAPPER.readTree(json).get("placed_at").asLong();
        } catch (Exception e) {
            return System.currentTimeMillis();
        }
    }
}
