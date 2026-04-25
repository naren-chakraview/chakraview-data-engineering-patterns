package io.chakraview.lambda;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

// Streaming path: low-latency approximate results written to Iceberg with path="streaming"
public class StreamingJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000, CheckpointingMode.EXACTLY_ONCE);

        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(System.getenv("KAFKA_BOOTSTRAP_SERVERS"))
            .setTopics("events.orders")
            .setGroupId("lambda-streaming-flink")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<String> stream = env.fromSource(
            source, WatermarkStrategy.noWatermarks(), "kafka-orders");

        // Tag all rows with path="streaming" before writing to shared Iceberg table
        DataStream<Row> rows = stream.map(new EventParser());

        // Wire FlinkSink to Iceberg table (same wiring as streaming-lakehouse/flink-iceberg)
        // Table schema must include path VARCHAR column
        rows.print("streaming-path");

        env.execute("lambda-streaming-flink");
    }
}
