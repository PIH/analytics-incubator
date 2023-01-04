package org.pih.analytics.flink;

import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.junit.jupiter.api.Test;
import org.pih.analytics.flink.debezium.ChangeEvent;
import org.pih.analytics.flink.function.sink.RocksDbSinkFunction;

public class KafkaFlinkTest {

    OpenmrsStreamProcessor processor = new OpenmrsStreamProcessor("testStream");

    @Test
    public void shouldPrintStream() throws Exception {

        long timestamp = System.currentTimeMillis();

        DataStream<ChangeEvent> personStream = processor
                .fromKafkaTopic("humci.humci.*", OffsetsInitializer.earliest(), OffsetsInitializer.timestamp(timestamp));

        personStream.addSink(new RocksDbSinkFunction("kafkaflinktest"));
        processor.start();
    }
}
