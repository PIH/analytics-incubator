package org.pih.analytics.flink;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a table from tables.json
 */
public class TableList extends ArrayList<Table> {

    public String[] getTables(String databasePrefix) {
        List<String> ret = new ArrayList<>();
        for (Table table : this) {
            ret.add(StringUtils.isNotBlank(databasePrefix) ? databasePrefix + "." : "" + table.tableName);
        }
        return ret.toArray(ret.toArray(new String[0]));
    }

    public Map<String, List<String>> getPrimaryKeyColumns() {
        Map<String, List<String>> ret = new HashMap<>();
        for (Table table : this) {
            ret.put(table.tableName, table.primaryKeys);
        }
        return ret;
    }

}