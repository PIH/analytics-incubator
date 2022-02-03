package org.pih.analytics.flink;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(p.getProperty("database.hostname", "localhost"))
                .port(Integer.parseInt(p.getProperty("database.port", "3308")))
                .databaseList(p.getProperty("database.include.list", "openmrs"))
                .tableList(p.getProperty("table.include.list"))
                .username(p.getProperty("database.user"))
                .password(p.getProperty("database.password"))
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        env.enableCheckpointing(3000);

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .setParallelism(4) // set 4 parallel source tasks
                .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute("Print MySQL Snapshot + Binlog");
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
