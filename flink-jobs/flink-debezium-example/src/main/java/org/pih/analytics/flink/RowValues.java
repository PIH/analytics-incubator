package org.pih.analytics.flink;

import java.util.HashMap;

/**
 * Represents a Debezium Change Event
 */
public class RowValues extends HashMap<String, Object> {

    public RowValues() {
        super();
    }

    public Integer getInteger(String key) {
        Object ret = get(key);
        return ret == null ? null : (Integer)ret;
    }

    public String getString(String key) {
        Object ret = get(key);
        return ret == null ? null : ret.toString();
    }

    public Boolean getBoolean(String key) {
        Object ret = get(key);
        if (ret == null) { return null; }
        if (ret instanceof Boolean) { return (Boolean)ret; }
        if (ret instanceof Number) { return ((Number)ret).intValue() == 0; }
        return Boolean.parseBoolean(ret.toString());
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        Boolean b = getBoolean(key);
        return b == null ? defaultValue : b;
    }
}