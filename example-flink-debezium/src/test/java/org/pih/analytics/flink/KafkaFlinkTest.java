package org.pih.analytics.flink;

import org.apache.commons.io.FileUtils;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.junit.jupiter.api.Test;
import org.pih.analytics.flink.debezium.ChangeEvent;
import org.pih.analytics.flink.function.flatmap.EventToCompositeFlatMapper;
import org.pih.analytics.flink.function.sink.ProgressLogger;
import org.pih.analytics.flink.function.sink.TestSinkFunction;
import org.pih.analytics.flink.model.Patient;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class KafkaFlinkTest {

    public String getServerName() {
        return "hum";
    }

    public String getDatabaseName() {
        return "hum";
    }

    public List<String> getTables() {
        return Arrays.asList(
                "patient", "person", "person_name", "person_address", "person_attribute",
                "obs", "visit", "encounter"
        );
    }

    public String getTopicPattern() {
        String pattern = getServerName() + "." + getDatabaseName() + ".(";
        pattern += String.join("|", getTables());
        pattern += ")";
        return pattern;
    }

    @Test
    public void shouldPrintStream() throws Exception {

        FileUtils.deleteDirectory(new File("/tmp/flinktesting/rocksdb"));

        long timestamp = System.currentTimeMillis();

        String name = getServerName() + "_" + getDatabaseName();

        OpenmrsStreamProcessor processor = new OpenmrsStreamProcessor(name);

        OffsetsInitializer fromOffset = OffsetsInitializer.earliest();
        OffsetsInitializer toOffset = OffsetsInitializer.timestamp(timestamp);

        DataStream<ChangeEvent> eventStream = processor.fromKafkaTopic(getTopicPattern(), fromOffset, toOffset);
        eventStream.addSink(new ProgressLogger());

        DataStream<Patient> patientStream = eventStream.flatMap(new EventToCompositeFlatMapper(name));
        patientStream.addSink(new TestSinkFunction());

        processor.start();
    }
}
