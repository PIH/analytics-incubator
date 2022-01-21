package org.pih.flink;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

/**
 * Main class for the PETL application that starts up the Spring Boot Application
 */
@SpringBootApplication
public class Application {

    private static final Log log = LogFactory.getLog(Application.class);

    public Application() {}

    public static void main(String[] args) {

        // Initialize environment
        ApplicationContext context = SpringApplication.run(Application.class, args);
        Application app = context.getBean(Application.class);

        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        log.info("JAVA VM: " + runtimeMxBean.getVmName());
        log.info("JAVA VENDOR: " + runtimeMxBean.getSpecVendor());
        log.info("JAVA VERSION: " + runtimeMxBean.getSpecVersion() + " (" + runtimeMxBean.getVmVersion() + ")");
        log.info("JAVA_OPTS: " + runtimeMxBean.getInputArguments());

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
                "    'properties.group.id' = '1',\n" +
                "    'format' = 'debezium-json',\n" +
                "    'scan.startup.mode' = 'earliest-offset'\n" +
                ")");

        TableResult numPersons = tEnv.executeSql("select * from person");
        numPersons.print();
    }
}
