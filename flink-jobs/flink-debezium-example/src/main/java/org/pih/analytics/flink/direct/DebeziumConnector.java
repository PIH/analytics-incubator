package org.pih.analytics.flink.direct;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Load OpenMRS Data
 */
public class DebeziumConnector {

    private static final Logger log = LoggerFactory.getLogger(DebeziumConnector.class);

    DebeziumEngine<ChangeEvent<String, String>> engine;
    ExecutorService executor = Executors.newSingleThreadExecutor();

    public DebeziumConnector() {}

    public static void main(String[] args) {
        DebeziumConnector connector = new DebeziumConnector();
        connector.start();
    }

    public void start() {
        if (engine == null) {
            Properties props = loadDebeziumProperties();
            engine = DebeziumEngine.create(Json.class).using(props).notifying(new DebeziumConsumer()).build();
        }
        executor.execute(engine);
    }

    public void stop() {
        try {
            if (engine != null) {
                engine.close();
            }
        }
        catch (IOException e) {
            log.warn("An error occurred while attempting to close the engine", e);
        }
        try {
            executor.shutdown();
            while (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                log.info("Waiting another 5 seconds for the embedded engine to shut down");
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public Properties loadDebeziumProperties() {
        try {
            Properties props = new Properties();
            props.load(DebeziumConnector.class.getResourceAsStream("/debezium.properties"));
            return props;
        }
        catch(Exception e) {
            throw new RuntimeException("Unable to load debezium.properties", e);
        }
    }
}
