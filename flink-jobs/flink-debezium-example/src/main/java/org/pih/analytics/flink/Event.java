package org.pih.analytics.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;

import java.io.Serializable;

/**
 * Represents a Debezium Change Event
 */
public class Event implements Serializable {

    static final JsonMapper mapper = new JsonMapper();

    public String table;
    public Operation operation;
    public int patientId;
    public long timestamp;
    public JsonNode values;

    public Event(String json) {
        try {
            JsonNode eventNode = getMapper().readTree(json);
            table = eventNode.get("source").get("table").textValue();
            operation = Operation.parse(eventNode.get("op").textValue());
            timestamp = eventNode.get("ts_ms").longValue();
            if (operation == Operation.DELETE) {
                values = eventNode.get("before");
            }
            else {
                values = eventNode.get("after");
            }
            JsonNode patientIdNode = values.get("patient_id");
            if (patientIdNode == null) {
                patientIdNode = values.get("person_id");
            }
            if (patientIdNode != null) {
                patientId = patientIdNode.intValue();
            }
            else {
                patientId = -1;
            }
            JsonNode voidedNode = values.get("voided");
            if (voidedNode != null && voidedNode.intValue() == 1) {
                if (operation == Operation.READ) {
                    operation = Operation.READ_VOID;
                }
                else {
                    operation = Operation.DELETE;
                }
            }
        }
        catch (Exception e) {
            System.out.println("Error parsing event from JSON: " + json);
        }
    }

    static synchronized JsonMapper getMapper() {
        return mapper;
    }

    @Override
    public String toString() {
        return timestamp + "," + operation + "," + patientId + "," + table + " " + values;
    }
}