package org.pih.analytics.flink;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.pih.analytics.flink.experimental.EnhancedEventFunction;
import org.pih.analytics.flink.experimental.EventCountAggregator;
import org.pih.analytics.flink.util.Props;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

        String databaseName = p.getProperty("database.name", "openmrs");
        String[] prefixedTables = p.getProperty("database.tables").split(",");

        List<String> tables = new ArrayList<>();
        for (int i=0; i<prefixedTables.length; i++) {
            String tableName = prefixedTables[i].trim().toLowerCase();
            prefixedTables[i] = databaseName + "." + tableName;
            tables.add(tableName);
        }

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
                .tableList(prefixedTables)
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

        // Filter out any Events that were read in during an initial snapshot and already voided to start out with
        eventStream = eventStream
                .filter((FilterFunction<Event>) event -> event.operation != Operation.READ_VOID)
                .setParallelism(1);

        // Partition out the stream by table
        KeyedStream<Event, String> keyedStream = eventStream
                .keyBy(event -> event.table);

        // Iterate over each of the tables that contain patient data of interest, and partition out events into one stream per table
        Map<String, DataStream<Event>> tableStreams = new HashMap<>();
        for (String table : tables) {
            DataStream<Event> tableStream = keyedStream
                    .filter(event -> event.table.equals(table))
                    .setParallelism(1);

            // Window the input data into sessions with 5 seconds or more of inactivity, to group all related updates together for a given patient
            // Reduce this down to the most recent event per table row, which represents the final state at the end of the window
            tableStream = tableStream
                    .keyBy(event -> event.key)
                    .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                    .reduce((ReduceFunction<Event>) (e1, e2) -> (e1.timestamp > e2.timestamp) ? e1 : e2)
                    .setParallelism(1);

            // TODO: This represents a point in which we could update a raw data store by table

            // Run each stream of table events through a process function that can enhance them, for example add patient_id to events that lack it
            tableStream = tableStream
                    .keyBy(event -> event.key)
                    .process(new EnhancedEventFunction());

            tableStream.addSink(getFileSink("/home/mseaton/environments/humci/flink/" + table + "-enhanced")).setParallelism(1);

            tableStreams.put(table, tableStream);
        }

        // At this point, we have all of our data events in table-keyed partition streams.
        // However, we may want to re-partition by patient, TBD





        // Write out a summary of the data for testing and debugging
        for (String table : tableStreams.keySet()) {
            DataStream<Map<String, Integer>> tableStream = tableStreams.get(table)
                    .windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                    .aggregate(new EventCountAggregator());

            tableStream.print().setParallelism(10);
        }

        // Execute the job
        env.execute("OpenMRS ETL Pipeline");
    }

    public StreamingFileSink<Event> getFileSink(String outputPath) {
        final StreamingFileSink<Event> sink = StreamingFileSink
                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<Event>("UTF-8"))
                .build();
        return sink;
    }
}
