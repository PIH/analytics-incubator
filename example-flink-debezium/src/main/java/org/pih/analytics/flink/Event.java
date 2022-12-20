package org.pih.analytics.flink;

import com.fasterxml.jackson.databind.JsonNode;
import org.pih.analytics.flink.util.Json;

import java.io.Serializable;

/**
 * Represents a Debezium Change Event
 */
public class Event implements Serializable {

    public String serverName;
    public String table;
    public String key;
    public Operation operation;
    public long timestamp;
    public RowValues values;

    public Event(String json) {
        try {
            JsonNode eventNode = Json.readJsonString(json);
            JsonNode eventKeyNode = eventNode.get("key");
            JsonNode eventValueNode = eventNode.get("value");
            JsonNode sourceNode = eventValueNode.get("source");
            serverName = sourceNode.get("name").textValue();
            table = sourceNode.get("table").textValue();
            operation = Operation.parse(eventValueNode.get("op").textValue());
            timestamp = eventValueNode.get("ts_ms").longValue();
            key = eventKeyNode.toString();
            JsonNode valueNode = eventValueNode.get("after");
            if (operation == Operation.DELETE) {
                valueNode = eventValueNode.get("before");
            }
            values = Json.readJsonNode(valueNode, RowValues.class);
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

    @Override
    public String toString() {
        return timestamp + "," + serverName + "," + operation + "," + table + "," + key + "," + values;
    }
}