package org.pih.analytics.flink;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Load OpenMRS Data
 */
public class FlinkDebeziumConnector {

    private static final Logger log = LoggerFactory.getLogger(FlinkDebeziumConnector.class);

    public FlinkDebeziumConnector() {}

    public static void main(String[] args) {
        final Properties props = loadDebeziumProperties();

        try (DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
                .using(props)
                .notifying((records, committer) -> {
                    for (ChangeEvent<String, String> r : records) {
                        System.out.println("Key = '" + r.key() + "' value = '" + r.value() + "'");
                        committer.markProcessed(r);
                    }})
                .build()
        ) {
            // Run the engine asynchronously ...
            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.execute(engine);

            // Do something else or wait for a signal or an event
            TimeUnit.MINUTES.sleep(10);
        }
        catch (Exception e) {
            log.error("An error occurred", e);
        }

    }

    public static Properties loadDebeziumProperties() {
        try {
            Properties props = new Properties();
            props.load(FlinkDebeziumConnector.class.getResourceAsStream("/debezium.properties"));
            return props;
        }
        catch(Exception e) {
            throw new RuntimeException("Unable to load debezium.properties", e);
        }
    }
}
