package org.pih.analytics.flink.config;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Encapsulates a data source configuration
 */
public class MysqlDataSource implements Serializable {

    private static final Logger log = LogManager.getLogger(MysqlDataSource.class);

    private int serverId; // Unique id corresponding to replication node
    private String host;
    private int port;
    private String databaseName;
    private String options;
    private String url; // Alternative to the above piecemeal settings
    private String user;
    private String password;

    //***** CONSTRUCTORS *****

    public MysqlDataSource() {}

    //***** INSTANCE METHODS *****

    /**
     * Gets a new Connection to the Data Source represented by this configuration
     */
    public Connection openConnection() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            return DriverManager.getConnection(getJdbcUrl(), getUser(), getPassword());
        }
        catch (Exception e) {
            throw new RuntimeException("An error occured trying to open a connection to the database", e);
        }
    }

    public boolean testConnection() throws SQLException {
        try (Connection c = openConnection()) {
            DatabaseMetaData metadata = c.getMetaData();
            log.trace("Successfully connected to datasource: " + metadata.toString());
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }

    public void executeUpdate(String sql) throws SQLException {
        try (Connection connection = openConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(sql);
            }
        }
    }

    public List<String> getTables() {
        List<String> tables = new ArrayList<>();
        try (Connection c = openConnection()) {
            DatabaseMetaData metaData = c.getMetaData();
            ResultSet rs = metaData.getTables(null, null, "%", new String[]{ "TABLE" });
            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                tables.add(tableName);
            }
        }
        catch (Exception e) {
            throw new RuntimeException("An error occurred trying to get tables from the database", e);
        }
        return tables;
    }

    public static PreparedStatement prepareStatement(Connection connection, String tableName, String lookupColumn, String whereColumn) {
        String statement = "select " + lookupColumn + " from " + tableName + " where " + whereColumn + " = ?";
        try {
            return connection.prepareStatement(statement);
        }
        catch (Exception e) {
            throw new RuntimeException("Error creating prepared statement: " + statement, e);
        }
    }

    public static Object lookupValue(Connection connection, String tableName, String lookupColumn, String whereColumn, Object whereValue) {
        try (PreparedStatement ps = prepareStatement(connection, tableName, lookupColumn, whereColumn)) {
            ps.setObject(1, whereValue);
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                return resultSet.getObject(1);
            }
            return null;
        }
        catch (Exception e) {
            throw new RuntimeException("Error selecting " + lookupColumn + " from " + tableName, e);
        }
    }

    public String getJdbcUrl() {
        if (StringUtils.isNotBlank(url)) {
            return url;
        }
        else {
            StringBuilder sb = new StringBuilder();
            sb.append("jdbc:mysql://").append(host).append(":").append(port);
            sb.append("/").append(databaseName).append("?");
            if (StringUtils.isNotBlank(options)) {
                sb.append(options);
            }
            else {
                sb.append("autoReconnect=true");
                sb.append("&sessionVariables=default_storage_engine%3DInnoDB");
                sb.append("&useUnicode=true");
                sb.append("&characterEncoding=UTF-8");
            }
            return sb.toString();
        }
    }

    //***** PROPERTY ACCESS *****


    public int getServerId() {
        return serverId;
    }

    public void setServerId(int serverId) {
        this.serverId = serverId;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getOptions() {
        return options;
    }

    public void setOptions(String options) {
        this.options = options;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
