package io.chakraview.features;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

// Flink online feature job: reads order events, computes features, writes to Redis.
// Freshness: ~checkpoint interval (30s). Guarantees: at-least-once (Redis sink is idempotent).
public class OnlineFeatureJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000, CheckpointingMode.AT_LEAST_ONCE);

        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(System.getenv("KAFKA_BOOTSTRAP_SERVERS"))
            .setTopics("events.orders")
            .setGroupId("flink-online-features")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<String> stream = env.fromSource(
            source, WatermarkStrategy.noWatermarks(), "kafka-orders");

        stream.addSink(new RedisFeatureSink(
            System.getenv("REDIS_HOST"),
            Integer.parseInt(System.getenv("REDIS_PORT"))
        ));

        env.execute("ml-online-feature-job");
    }

    static class RedisFeatureSink extends RichSinkFunction<String> {
        private final String host;
        private final int    port;
        private transient Jedis jedis;
        private static final ObjectMapper MAPPER = new ObjectMapper();

        RedisFeatureSink(String host, int port) { this.host = host; this.port = port; }

        @Override
        public void open(org.apache.flink.configuration.Configuration cfg) {
            jedis = new Jedis(host, port);
        }

        @Override
        public void invoke(String json, Context ctx) throws Exception {
            JsonNode node = MAPPER.readTree(json);
            String orderId = node.get("order_id").asText();
            long   amount  = node.get("amount_cents").asLong();
            // Store as Redis HASH: feature:order_stats:<order_id>
            jedis.hset("feature:order_stats:" + orderId,
                "amount_usd", String.valueOf(amount / 100.0));
            jedis.expire("feature:order_stats:" + orderId, 86_400); // 24h TTL
        }

        @Override
        public void close() { if (jedis != null) jedis.close(); }
    }
}
