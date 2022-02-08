package org.pih.analytics.flink;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.pih.analytics.flink.util.Json;
import org.pih.analytics.flink.util.Props;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Load OpenMRS Data
 */
public class DebeziumFlinkConnector {

    private static final Logger log = LoggerFactory.getLogger(DebeziumFlinkConnector.class);

    public DebeziumFlinkConnector() {}

    public static void main(String[] args) throws Exception {
        DebeziumFlinkConnector connector = new DebeziumFlinkConnector();
        connector.start();
    }

    public void start() throws Exception {
        Properties p = Props.readResource("/mysql.properties");

        // TODO: Consider something like this
        final String databaseName = p.getProperty("database.name", "openmrs");
        final TableList tableList = Json.readJsonResource("/tables.json", TableList.class);
        final String[] tables = tableList.getTables(databaseName);

        /*
        Create a source of binlog events.  The provided JsonDebeziumDeserializationSchema seems to work well, but
        this adds a custom JsonDeserializationSchema to explicitly include the key as well as the value in the resulting
        json.  This also provides a customizable class for possible further enhancements.
         */
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .debeziumProperties(p)
                .serverId(p.getProperty("database.server.id", "0"))
                .hostname(p.getProperty("database.hostname", "localhost"))
                .port(Integer.parseInt(p.getProperty("database.port", "3308")))
                .databaseList(databaseName)
                .tableList(tables)
                .username(p.getProperty("database.user"))
                .password(p.getProperty("database.password"))
                .deserializer(new JsonDeserializationSchema())
                .build();

        // Setup an execution environment.  Needs more research on best configuration of options.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(600000);

        /*
        Set up a stream from the MySQL Bin Log.  We set up a strategy of no watermarks, as we make the assumption
        that, with events streaming from the bin log, that processing time and event time will always match.
        From https://nightlies.apache.org/flink/flink-docs-release-1.11/api/java/org/apache/flink/api/common/eventtime/WatermarkStrategy.html
        "Creates a watermark strategy that generates no watermarks at all.
        This may be useful in scenarios that do pure processing-time based stream processing."
         */
        DataStreamSource<String> binlogStream = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "OpenMRS MySQL");

        // Set the initial parallelism to 1, to ensure we process event in order until we can partition out to appropriate streams
        binlogStream.setParallelism(1);

        // Transform the stream of JSON into a Stream of Event objects
        DataStream<Event> eventStream = binlogStream
                .map((MapFunction<String, Event>) s -> new Event(s))
                .setParallelism(1);

        // Output to a File sink.
        eventStream.addSink(getFileSink("/home/mseaton/environments/humci/flink/raw-events")).setParallelism(1);

        // Filter out any Events that were read in during an initial snapshot and already voided to start out with
        eventStream = eventStream
                .filter((FilterFunction<Event>) event -> event.operation != Operation.READ_VOID)
                .setParallelism(1);

        // Output to a File sink.
        eventStream.addSink(getFileSink("/home/mseaton/environments/humci/flink/cleaned-events")).setParallelism(1);

        // Partition out the Event stream by table
        KeyedStream<Event, String> keyedStream = eventStream
                .keyBy(event -> event.table);

        keyedStream.addSink(getFileSink("/home/mseaton/environments/humci/flink/keyed-table-events")).setParallelism(1);

        // Output to a Print sink.  Use up to 10 different partitions
        eventStream.print().setParallelism(10);

        // Execute the job
        env.execute("OpenMRS ETL Pipeline");
    }

    public static class EventAggregator implements AggregateFunction<Event, List<Event>, List<Event>> {
        @Override
        public List<Event> createAccumulator() {
            return new ArrayList<>();
        }

        @Override
        public List<Event> add(Event event, List<Event> events) {
            events.add(event);
            return events;
        }

        @Override
        public List<Event> getResult(List<Event> events) {
            return null;
        }

        @Override
        public List<Event> merge(List<Event> events, List<Event> acc1) {
            acc1.addAll(events);
            return acc1;
        }
    }

    public StreamingFileSink<Event> getFileSink(String outputPath) {
        final StreamingFileSink<Event> sink = StreamingFileSink
                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<Event>("UTF-8"))
                .build();
        return sink;
    }
}
