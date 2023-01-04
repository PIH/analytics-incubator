package org.pih.analytics.flink;

import org.junit.jupiter.api.Test;
import org.pih.analytics.flink.config.MysqlDataSource;
import org.pih.analytics.flink.config.StreamConfig;
import org.pih.analytics.flink.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class TableAnalysisTest {

    private static final Logger log = LoggerFactory.getLogger(TableAnalysisTest.class);

    @Test
    public void shouldGetTablesByColumn() throws Exception {
        Map<String, Set<String>> tablesWithColumn = new TreeMap<>();
        StreamConfig config = Util.readJsonResource("/config.json", StreamConfig.class);
        MysqlDataSource dataSource = config.getMysqlSource();
        try (Connection connection = dataSource.openConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet rs = metaData.getColumns("humci", null, "%", "%");
            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME").toLowerCase();
                String columnName = rs.getString("COLUMN_NAME").toLowerCase();
                Set<String> tables = tablesWithColumn.computeIfAbsent(columnName, k -> new TreeSet<>());
                tables.add(tableName);
            }
        }
        for (String column : tablesWithColumn.keySet()) {
            System.out.println(column + ": " + tablesWithColumn.get(column));
        }
    }
}
