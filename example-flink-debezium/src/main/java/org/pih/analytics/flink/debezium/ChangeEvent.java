package org.pih.analytics.flink.debezium;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.flink.api.java.tuple.Tuple3;
import org.pih.analytics.flink.util.ObjectMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Represents a Debezium Change Event from Kafka
 */
public class ChangeEvent implements Serializable {

    private static final JsonMapper mapper = new JsonMapper();

    private final String key;
    private final String value;
    private final Long timestamp;
    private final DebeziumOperation operation;
    private final String table;
    private final ObjectMap before;
    private final ObjectMap after;
    private final ObjectMap source;

    public ChangeEvent(String key, String value) {
        try {
            this.key = key;
            this.value = value;
            JsonNode eventNode = mapper.readTree(value);
            before = mapper.treeToValue(eventNode.get("before"), ObjectMap.class);
            after = mapper.treeToValue(eventNode.get("after"), ObjectMap.class);
            source = mapper.treeToValue(eventNode.get("source"), ObjectMap.class);
            timestamp = eventNode.get("ts_ms").longValue();
            operation = DebeziumOperation.parse(eventNode.get("op").textValue());
            table = source.getString("table");
        }
        catch (Exception e) {
            throw new RuntimeException("Unable to create a ChangeEvent from: " + key + " = " + value, e);
        }
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public String getServerName() {
        return source.getString("name");
    }

    public String getTable() {
        return table;
    }

    public DebeziumOperation getOperation() {
        return operation;
    }

    /**
     * snapshot values are "true", "false", "last"
     */
    public boolean isSnapshot() {
        String snapshot = source.getString("snapshot");
        return !"false".equals(snapshot);
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public ObjectMap getBefore() {
        return before;
    }

    public ObjectMap getAfter() {
        return after;
    }

    public ObjectMap getValues() {
        return operation == DebeziumOperation.DELETE ? getBefore() : getAfter();
    }

    public void addValue(String key, Object value) {
        if (before != null && !before.isEmpty()) {
            before.put(key, value);
        }
        if (after != null && !after.isEmpty()) {
            after.put(key, value);
        }
    }

    public List<Tuple3<String, Object, Object>> getChanges() {
        List<Tuple3<String, Object, Object>> ret = new ArrayList<>();
        for (String key : getValues().keySet()) {
            Object before = getBefore() == null ? null : getBefore().get(key);
            Object after = getAfter() == null ? null : getAfter().get(key);
            boolean changed = (before == null ? after != null : !before.equals(after));
            if (changed) {
                ret.add(new Tuple3<>(key, before, after));
            }
        }
        return ret;
    }

    public String getUuid() {
        return getValues().getString("uuid");
    }

    public boolean isDeleted() {
        return getAfter() == null || getAfter().isEmpty() || getAfter().getBoolean("voided", false);
    }

    public Date getDateCreated() {
        return getValues().getDate("date_created");
    }

    public Integer getPersonId() {
        return getValues().getInteger("person_id");
    }

    public Integer getPatientId() {
        return getValues().getInteger("patient_id");
    }

    public Integer getPersonOrPatientId() {
        Integer personId = getPersonId();
        return personId == null ? getPatientId() : personId;
    }

    public Integer getEncounterId() {
        return getValues().getInteger("encounter_id");
    }

    public Integer getPatientProgramId() {
        return getValues().getInteger("patient_program_id");
    }

    public ObjectMap getSource() {
        return source;
    }

    @Override
    public String toString() {
        return table + key + ":" + getOperation() + " " + value;
    }
}