package org.pih.analytics.flink.config;

import java.util.List;
import java.util.Properties;

public class StreamConfig {

    private String streamId;
    private MysqlDataSource mysqlSource;
    private List<String> mysqlTables;
    private Properties debeziumProperties;

    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    public MysqlDataSource getMysqlSource() {
        return mysqlSource;
    }

    public List<String> getMysqlTables() {
        return mysqlTables;
    }

    public void setMysqlTables(List<String> mysqlTables) {
        this.mysqlTables = mysqlTables;
    }

    public void setMysqlSource(MysqlDataSource mysqlSource) {
        this.mysqlSource = mysqlSource;
    }

    public Properties getDebeziumProperties() {
        return debeziumProperties;
    }

    public void setDebeziumProperties(Properties debeziumProperties) {
        this.debeziumProperties = debeziumProperties;
    }
}
