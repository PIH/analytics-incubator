package org.pih.analytics.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.jupiter.api.Test;
import org.pih.analytics.flink.config.StreamConfig;
import org.pih.analytics.flink.debezium.DebeziumEvent;
import org.pih.analytics.flink.filter.DeletedOnReadFilter;
import org.pih.analytics.flink.function.EnhancedEventFunction;
import org.pih.analytics.flink.function.TableOperationCountAggregator;
import org.pih.analytics.flink.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class BasicDebeziumFlinkTest {

    private static final Logger log = LoggerFactory.getLogger(BasicDebeziumFlinkTest.class);

    @Test
    public void shouldExecuteStream() {
        StreamConfig config = Util.readJsonResource("/config.json", StreamConfig.class);
        OpenmrsStreamProcessor processor = new OpenmrsStreamProcessor("testStream");

        DataStreamSource<String> binlogStream = processor.fromMySQLBinlog(
                config.getMysqlSource(), config.getMysqlTables(), config.getDebeziumProperties()
        );

        // Transform the stream of JSON into a Stream of Debezium Event objects
        DataStream<DebeziumEvent> eventStream = binlogStream
                .map((MapFunction<String, DebeziumEvent>) DebeziumEvent::new);

        // Filter out any Events that are part of the initiol READ/SNAPSHOT and are already voided
        eventStream = eventStream.filter(new DeletedOnReadFilter());

        // Partition out the stream by table
        KeyedStream<DebeziumEvent, String> keyedStream = eventStream.keyBy(DebeziumEvent::getTable);

        // Window the input data into sessions with 5 seconds or more of inactivity, to group all related updates together for a given patient
        // Reduce this down to the most recent event per table row, which represents the final state at the end of the window
        DataStream<DebeziumEvent> tableStream = keyedStream
                .keyBy(event -> event.getKey().toString())
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .reduce((ReduceFunction<DebeziumEvent>) (e1, e2) -> (e1.getTimestamp() > e2.getTimestamp()) ? e1 : e2)
                .keyBy(event -> event.getKey().toString())
                .process(new EnhancedEventFunction(config.getMysqlSource()));

        // This will write out files.  They will remain in the in-progress state until a checkpoint is reached
        tableStream.addSink(StreamingFileSink.forRowFormat(
                new Path("/tmp/flinktesting/enhanced-stream"),
                new SimpleStringEncoder<DebeziumEvent>("UTF-8"))
                .build()
        );

        DataStream<Map<String, Integer>> aggregateStream = tableStream
                .windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .aggregate(new TableOperationCountAggregator());

        aggregateStream.print().setParallelism(10);

        processor.start();
    }

}
