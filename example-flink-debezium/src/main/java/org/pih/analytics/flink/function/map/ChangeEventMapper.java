package org.pih.analytics.flink.function.map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.pih.analytics.flink.debezium.ChangeEvent;

/**
 * This doesn't really need its own class, but doing so just to explicitly have an example MapFunction
 */
public class ChangeEventMapper implements MapFunction<String, ChangeEvent> {

    private static final JsonMapper mapper = new JsonMapper();

    @Override
    public ChangeEvent map(String s) {
        try {
            JsonNode eventNode = mapper.readTree(s);
            String key = mapper.writeValueAsString(eventNode.get("key"));
            String value = mapper.writeValueAsString(eventNode.get("value"));
            return new ChangeEvent(key, value);
        }
        catch (Exception e) {
            throw new RuntimeException("Unable to convert string to change event", e);
        }
    }
}
