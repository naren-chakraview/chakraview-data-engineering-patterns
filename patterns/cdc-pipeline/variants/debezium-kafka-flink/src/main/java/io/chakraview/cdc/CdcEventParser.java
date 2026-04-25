package io.chakraview.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;

public class CdcEventParser implements MapFunction<String, CdcEvent> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public CdcEvent map(String json) throws Exception {
        JsonNode node = MAPPER.readTree(json);
        CdcEvent event = new CdcEvent();
        event.orderId     = node.path("order_id").asText();
        event.customerId  = node.path("customer_id").asText();
        event.status      = node.path("status").asText();
        event.amountCents = node.path("amount_cents").asLong();
        event.placedAt    = node.path("placed_at").asText();
        // Debezium ExtractNewRecordState adds __deleted = "true" for deletes
        event.deleted     = "true".equals(node.path("__deleted").asText());
        return event;
    }
}
