package io.chakraview.cdc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000, CheckpointingMode.EXACTLY_ONCE);

        // Debezium publishes to <topic.prefix>.<schema>.<table>
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(System.getenv("KAFKA_BOOTSTRAP_SERVERS"))
            .setTopics("chakra.public.orders")
            .setGroupId("flink-cdc-orders")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<String> cdcStream = env.fromSource(
            source, WatermarkStrategy.noWatermarks(), "debezium-kafka-source"
        );

        // Parse CDC envelope (after unwrap transform: flat JSON with __deleted field)
        DataStream<CdcEvent> events = cdcStream.map(new CdcEventParser());

        // Route inserts/updates and deletes separately
        DataStream<CdcEvent> upserts = events.filter(e -> !e.isDeleted());
        DataStream<CdcEvent> deletes = events.filter(CdcEvent::isDeleted);

        // Upsert sink — write to Iceberg (or log for local dev)
        upserts.print("UPSERT");
        deletes.print("DELETE");

        // TODO: replace print() with FlinkSink.forRow() writing to Iceberg
        // See streaming-lakehouse/variants/flink-iceberg for the Iceberg sink wiring

        env.execute("cdc-pipeline-flink");
    }
}
