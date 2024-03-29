package org.pih.analytics.flink.util;

import java.util.Date;
import java.util.HashMap;

/**
 * Represents a Row of data
 */
public class ObjectMap extends HashMap<String, Object> {

    public ObjectMap() {
        super();
    }

    public Integer getInteger(String key) {
        return (Integer) get(key);
    }

    public Date getDate(String key) {
        return (Date) get(key);
    }

    public String getString(String key) {
        Object ret = get(key);
        return ret == null ? null : ret.toString();
    }

    public Boolean getBoolean(String key) {
        Object ret = get(key);
        if (ret == null) { return null; }
        if (ret instanceof Boolean) { return (Boolean)ret; }
        if (ret instanceof Number) { return ((Number)ret).intValue() == 1; }
        return Boolean.parseBoolean(ret.toString());
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        Boolean b = getBoolean(key);
        return b == null ? defaultValue : b;
    }
}