package org.pih.analytics.flink;

import java.io.Serializable;
import java.util.List;

/**
 * Represents a table from tables.json
 */
public class Table implements Serializable {
    public String tableName;
    public List<String> primaryKeys;
}