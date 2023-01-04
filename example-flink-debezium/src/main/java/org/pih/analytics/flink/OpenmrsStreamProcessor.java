package org.pih.analytics.flink;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.pih.analytics.flink.config.MysqlDataSource;
import org.pih.analytics.flink.debezium.JsonDeserializationSchema;
import org.pih.analytics.flink.debezium.ChangeEventDeserializationSchema;
import org.pih.analytics.flink.debezium.ChangeEvent;

import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Processes a stream of changes from an OpenMRS database
 */
public class OpenmrsStreamProcessor {

    private final String streamId;
    private final StreamExecutionEnvironment env;

    /**
     * Instantiates a new stream processor
     * This sets up the execution environment for the stream processing
     * By default, we set up checkpointing every 10 minutes
     * We also set the default parallelism to 1.  This means that, by default, a given operation will only have one
     * instance executing.  Any operation can override this to indicate it should be executed in parallel by X tasks.
     * Needs more research on best configuration of options.
     */
    public OpenmrsStreamProcessor(String streamId) {
        this.streamId = streamId;
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // This sets the parallelism to 1 by default for anything executed
    }

    /**
     * Create a MySQL source from the binlog using the mysql flink debezium connector
     * We register a custom deserializer.  See JsonDeserializationSchema javadoc for details
     * We set up a strategy of no watermarks, as we make the assumption that, with events streaming from the bin log,
     * that processing time and event time will always match.  From the flink WatermarkStrategy javadocs, one should
     * use a no watermarks strategy in scenarios that do pure processing-time based stream processing
     * We set the initial parallelism to 1, to ensure we process events in order until we can partition out
     * @param dataSource the connection details for MySQL
     * @param config any additional configuration properties to set
     */
    public DataStreamSource<String> fromMySQLBinlog(MysqlDataSource dataSource, List<String> tables, Properties config) {

        if (tables == null || tables.isEmpty()) {
            tables = dataSource.getTables();
        }
        String schemaPrefix = dataSource.getDatabaseName() + ".";
        String[] tableList = tables.stream().map(t -> t.startsWith(schemaPrefix) ? t : schemaPrefix + t).toArray(String[]::new);

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .serverId(Integer.toString(dataSource.getServerId()))
                .hostname(dataSource.getHost())
                .port(dataSource.getPort())
                .databaseList(dataSource.getDatabaseName())
                .username(dataSource.getUser())
                .password(dataSource.getPassword())
                .tableList(tableList)
                .deserializer(new JsonDeserializationSchema())
                .debeziumProperties(config)
                .build();

        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy.forMonotonousTimestamps();
        DataStreamSource<String> binlogStream = env.fromSource(mySqlSource, watermarkStrategy, streamId);
        binlogStream.setParallelism(1);
        return binlogStream;
    }

    public DataStreamSource<ChangeEvent> fromKafkaTopic(String topic, OffsetsInitializer startingOffsets, OffsetsInitializer endingOffsets) {
        KafkaSource<ChangeEvent> kafkaSource = KafkaSource.<ChangeEvent>builder()
                .setBootstrapServers("localhost:9092")
                .setTopicPattern(Pattern.compile(topic))
                .setGroupId(streamId)
                .setStartingOffsets(startingOffsets)
                .setBounded(endingOffsets)
                .setDeserializer(new ChangeEventDeserializationSchema())
                .build();

        WatermarkStrategy<ChangeEvent> watermarkStrategy = WatermarkStrategy.noWatermarks();
        return env.fromSource(kafkaSource, watermarkStrategy, streamId);
    }

    /**
     * Starts the stream processor
     */
    public void start() {
        try {
            env.execute(streamId);
        }
        catch (Exception e) {
            throw new RuntimeException("An error occurred while executing the stream processor", e);
        }
    }
}
