package io.chakraview.streaming;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Exactly-once: Flink checkpoint + Iceberg two-phase commit are atomic.
        // Files committed to Iceberg only on checkpoint — no partial writes visible.
        env.enableCheckpointing(60_000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30_000);
        env.getCheckpointConfig().setCheckpointTimeout(120_000);

        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(System.getenv("KAFKA_BOOTSTRAP_SERVERS"))
            .setTopics("events.orders")
            .setGroupId("flink-streaming-lakehouse")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<String> rawStream = env.fromSource(
            source,
            WatermarkStrategy.<String>forMonotonousTimestamps()
                .withTimestampAssigner((event, ts) -> EventParser.extractTimestamp(event)),
            "kafka-orders-source"
        );

        DataStream<Row> rows = rawStream.map(new EventParser());

        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("type",      "rest");
        catalogProps.put("uri",       System.getenv("ICEBERG_REST_URI"));
        catalogProps.put("warehouse", System.getenv("LAKEHOUSE_WAREHOUSE"));
        catalogProps.put("io-impl",   "org.apache.iceberg.aws.s3.S3FileIO");
        catalogProps.put("s3.endpoint", System.getenv("S3_ENDPOINT"));
        catalogProps.put("s3.path-style-access", "true");

        CatalogLoader catalogLoader = CatalogLoader.custom(
            "lakehouse", catalogProps,
            new org.apache.hadoop.conf.Configuration(),
            "org.apache.iceberg.rest.RESTCatalog"
        );
        TableLoader tableLoader = TableLoader.fromCatalog(
            catalogLoader,
            TableIdentifier.of("orders", "events")
        );

        FlinkSink.forRow(rows, IcebergSchema.ORDERS_SCHEMA)
            .tableLoader(tableLoader)
            .upsert(false)
            .append();

        env.execute("streaming-lakehouse-flink-iceberg");
    }
}
