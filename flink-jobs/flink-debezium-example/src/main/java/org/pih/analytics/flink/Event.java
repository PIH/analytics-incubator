package org.pih.analytics.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;

import java.io.Serializable;

/**
 * Represents a Debezium Change Event
 */
public class Event implements Serializable {

    static final JsonMapper mapper = new JsonMapper();

    public String serverName;
    public String table;
    public Operation operation;
    public int patientId;
    public long timestamp;
    public RowValues values;

    public Event(String json) {
        try {
            JsonNode eventNode = getMapper().readTree(json);
            JsonNode sourceNode = eventNode.get("source");
            serverName = sourceNode.get("name").textValue();
            table = sourceNode.get("table").textValue();
            operation = Operation.parse(eventNode.get("op").textValue());
            timestamp = eventNode.get("ts_ms").longValue();
            JsonNode valueNode = eventNode.get("after");
            if (operation == Operation.DELETE) {
                valueNode = eventNode.get("before");
            }
            values = getMapper().treeToValue(valueNode, RowValues.class);
            Integer patientIdVal = values.getInteger("patient_id");
            if (patientIdVal == null) {
                patientIdVal = values.getInteger("person_id");
            }
            patientId = (patientIdVal == null ? -1 : patientIdVal);
            boolean voidedVal = values.getBoolean("voided", false);
            if (voidedVal) {
                if (operation == Operation.READ) {
                    operation = Operation.READ_VOID;
                }
                else {
                    operation = Operation.DELETE;
                }
            }
        }
        catch (Exception e) {
            System.out.println("Error parsing event from JSON: " + json + ": " + e.getMessage());
            e.printStackTrace();
        }
    }

    static synchronized JsonMapper getMapper() {
        return mapper;
    }

    @Override
    public String toString() {
        return timestamp + "," + serverName + "," + operation + "," + patientId + "," + table + " " + values;
    }
}