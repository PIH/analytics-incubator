package org.pih.analytics.flink;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        Properties p = loadDebeziumProperties();

        /*
        Create a source of binlog events.  The provided JsonDebeziumDeserializationSchema seems to work well, but
        I created a custom JsonDeserializationSchema just to explicitly include the timestamp and key json as properties
        in addition to the value json.  This also provides a guide to how one would further customize this.
         */
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .debeziumProperties(p)
                .serverId(p.getProperty("database.server.id", "0"))
                .hostname(p.getProperty("database.hostname", "localhost"))
                .port(Integer.parseInt(p.getProperty("database.port", "3308")))
                .databaseList(p.getProperty("database.include.list", "openmrs"))
                .tableList(p.getProperty("table.include.list"))
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

        // Set the initial parallelism to 1, to ensure we process event in order until we can group by patient
        binlogStream.setParallelism(1);

        // Transform the stream of JSON into a Stream of Event objects
        DataStream<Event> eventStream = binlogStream
                .map((MapFunction<String, Event>) s -> new Event(s))
                .setParallelism(1);

        // Filter out any Events that were read in during an initial snapshot and already voided to start out with
        eventStream = eventStream
                .filter((FilterFunction<Event>) event -> event.operation != Operation.READ_VOID)
                .setParallelism(1);

        // Output to a Print sink.  Use up to 10 different partitions
        eventStream.print().setParallelism(10);

        // Execute the job
        env.execute("OpenMRS ETL Pipeline");
    }

    public Properties loadDebeziumProperties() {
        try {
            Properties props = new Properties();
            props.load(DebeziumFlinkConnector.class.getResourceAsStream("/debezium.properties"));
            return props;
        }
        catch(Exception e) {
            throw new RuntimeException("Unable to load debezium.properties", e);
        }
    }
}
