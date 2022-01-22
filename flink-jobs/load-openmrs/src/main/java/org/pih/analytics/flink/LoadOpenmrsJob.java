package org.pih.analytics.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

/**
 * Load OpenMRS Data
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

        tEnv.executeSql("" +
                "CREATE TABLE es_person (\n" +
                "  person_id INT PRIMARY KEY NOT ENFORCED,\n" +
                "  uuid STRING,\n" +
                "  gender STRING,\n" +
                "  birthdate BIGINT\n" +
                ") WITH (\n" +
                "  'connector' = 'elasticsearch-7',\n" +
                "  'hosts' = 'http://localhost:9200',\n" +
                "  'index' = 'es_person_index'\n" +
                ")"
        );

        tEnv.executeSql("" +
                "INSERT INTO es_person\n" +
                "SELECT p.person_id,\n" +
                "       p.uuid,\n" +
                "       p.gender,\n" +
                "       p.birthdate\n" +
                "FROM person p\n"
        );
    }
}
