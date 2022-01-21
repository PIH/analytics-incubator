package org.pih.analytics.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

/**
 * Main class for the PETL application that starts up the Spring Boot Application
 */
public class LoadOpenmrsJob {

    public LoadOpenmrsJob() {}

    public static void main(String[] args) {
        LoadOpenmrsJob job = new LoadOpenmrsJob();
        job.execute();
    }

    public void execute() {

        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.executeSql("" +
                "CREATE TABLE person (\n" +
                "  person_id INT,\n" +
                "  uuid STRING, \n" +
                "  gender STRING,\n" +
                "  birthdate BIGINT,\n" +
                "  birthdate_estimated BOOLEAN,\n" +
                "  dead BOOLEAN,\n" +
                "  cause_of_death INT,\n" +
                "  cause_of_death_non_coded STRING,\n" +
                "  death_date BIGINT,\n" +
                "  death_date_estimated BOOLEAN,\n" +
                "  creator INT,\n" +
                "  date_created BIGINT,\n" +
                "  PRIMARY KEY (person_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'openmrs-humci.openmrs.person',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'properties.group.id' = 'connect-cluster-1',\n" +
                "    'format' = 'debezium-json',\n" +
                "    'scan.startup.mode' = 'earliest-offset'\n" +
                ")");

        TableResult result = tEnv.executeSql("select * from person");
        result.print();
    }
}
