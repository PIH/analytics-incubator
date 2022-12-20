package org.pih.analytics.flink.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

/**
 * TODO: This is temporary, for experimentation.
 */
public class Mysql {

    public static Connection createConnection(String host, String port, String user, String password, String database) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            String args = "?useUnicode=true&characterEncoding=UTF-8&useSSL=false";
            return DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/" + database + args, user, password);
        }
        catch (Exception e) {
            throw new RuntimeException("Unable to connect to MySQL", e);
        }
    }

    public static Connection createConnection(Properties p) {
        String host = p.getProperty("database.hostname");
        String port = p.getProperty("database.port");
        String user = p.getProperty("database.user");
        String password = p.getProperty("database.password");
        String database = p.getProperty("database.name");
        return createConnection(host, port, user, password, database);
    }

    public static Connection createConnection() {
        Properties p = Props.readResource("/mysql.properties");
        return createConnection(p);
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
}
