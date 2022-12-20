package org.pih.analytics.debezium;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.pih.analytics.debezium.consumer.CassandraConsumer;
import org.pih.analytics.debezium.consumer.DebeziumConsumer;
import org.pih.analytics.debezium.consumer.TableCountingConsumer;

import java.io.File;
import java.util.Arrays;

/**
 * Tests the SqlServerImportJob
 */
public class DebeziumStreamTest {

    private static final Logger log = LogManager.getLogger(DebeziumStreamTest.class);

    String schema = "hum";

    // The order here matters.  We need to make sure that tables are in the order in which they would have originally been inserted for a given patient
    String[] tables = {"person", "patient", "patient_identifier", "person_name", "person_address", "encounter", "obs" };

    public DebeziumStream getStream(Integer streamId) {
        DataSource dataSource = new DataSource();
        dataSource.setDatabaseType("mysql");
        dataSource.setHost("localhost");
        dataSource.setPort("3308");
        dataSource.setUser("root");
        dataSource.setPassword("root");
        dataSource.setDatabaseName(schema);
        return new DebeziumStream(streamId, dataSource, Arrays.asList(tables), new File("/tmp"));
    }

    protected void executeStream(DebeziumStream stream, DebeziumConsumer consumer) throws Exception {
        stream.reset();
        stream.start(consumer);
        while (consumer.getNumRecordsProcessed() == 0 || System.currentTimeMillis() - consumer.getLastProcessTime() < 1000) {
            Thread.sleep(1000);
            log.info("Number of records processed: " + consumer.getNumRecordsProcessed());
        }
        stream.stop(consumer);

        int processTime = (int)(consumer.getLastProcessTime() - consumer.getFirstProcessTime())/1000;

        log.info("Changelog read.  Num: " + consumer.getNumRecordsProcessed() + "; Time: " + processTime + "s");
    }

    @Test
    public void testTableCountingConsumer() throws Exception {
        TableCountingConsumer consumer = new TableCountingConsumer();
        executeStream(getStream(100002), consumer);
        for (String tableName : consumer.getNumPerTable().keySet()) {
            log.info(tableName + ": " + consumer.getNumPerTable().get(tableName));
        }
    }

    @Test
    public void testCassandraConsumer() throws Exception {
        executeStream(getStream(100005), new CassandraConsumer());
    }
}
