package org.pih.analytics.flink;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.pih.analytics.flink.functions.DateFunction;
import org.pih.analytics.flink.functions.PreferredFunction;
import org.pih.analytics.flink.functions.TimestampFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * Load OpenMRS Data
 */
public class LoadOpenmrsJob {

    private static final Logger log = LoggerFactory.getLogger(LoadOpenmrsJob.class);

    public LoadOpenmrsJob() {}

    public static void main(String[] args) {
        LoadOpenmrsJob job = new LoadOpenmrsJob();
        job.execute();
    }

    public void executeSql(TableEnvironment tEnv, String resource) {
        try {
            String query = IOUtils.resourceToString(resource, StandardCharsets.UTF_8);
            for (String statement : query.split(";")) {
                if (StringUtils.isNotBlank(statement)) {
                    tEnv.executeSql(statement);
                }
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Error loading sql from " + resource, e);
        }
    }

    public void execute() {

        log.warn("Executing job");

        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.createTemporarySystemFunction("To_Date", DateFunction.class);
        tEnv.createTemporarySystemFunction("To_Timestamp", TimestampFunction.class);
        tEnv.createTemporarySystemFunction("Preferred", PreferredFunction.class);

        executeSql(tEnv, "/openmrs/database.sql");

        executeSql(tEnv, "/openmrs/tables/patient_identifier_type.sql");

        executeSql(tEnv, "/openmrs/tables/person.sql");
        executeSql(tEnv, "/openmrs/tables/person_name.sql");
        executeSql(tEnv, "/openmrs/tables/person_address.sql");

        executeSql(tEnv, "/openmrs/tables/patient.sql");
        executeSql(tEnv, "/openmrs/tables/patient_identifier.sql");

        executeSql(tEnv, "/openmrs/views/preferred_identifier.sql");
        executeSql(tEnv, "/openmrs/views/preferred_name.sql");
        executeSql(tEnv, "/openmrs/views/preferred_address.sql");

        executeSql(tEnv, "/zl/database.sql");

        executeSql(tEnv, "/zl/patient.sql");

        executeSql(tEnv, "/elasticsearch/patient.sql");

        TableResult result = tEnv.executeSql("select count(*) as num_patients from patient");
        result.print();
    }
}
