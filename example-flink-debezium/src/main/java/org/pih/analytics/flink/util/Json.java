package org.pih.analytics.flink.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Json ser/de utility
 */
public class Json {

    private static final Logger log = LoggerFactory.getLogger(Json.class);

    private static final JsonMapper mapper = new JsonMapper();

    public static JsonNode readJsonString(String json) {
        try {
            return mapper.readTree(json);
        }
        catch (Exception e) {
            log.error("Error reading as JsonNode: " + json, e);
            throw new RuntimeException(e);
        }
    }

    public static JsonNode readJsonResource(String resourceName) {
        try {
            String json = IOUtils.resourceToString(resourceName, StandardCharsets.UTF_8);
            return readJsonString(json);
        }
        catch (IOException e) {
            log.error("Error reading resource as JsonNode: " + resourceName, e);
            throw new RuntimeException(e);
        }
    }

    public static <T> T readJsonNode(JsonNode jsonNode, Class<T> type) {
        try {
            return mapper.treeToValue(jsonNode, type);
        }
        catch (Exception e) {
            log.error("Error converting JsonNode to " + type, e);
            throw new RuntimeException(e);
        }
    }

    public static <T> T readJsonString(String json, Class<T> type) {
        return readJsonNode(readJsonString(json), type);
    }

    public static <T> T readJsonResource(String resourceName, Class<T> type) {
        return readJsonNode(readJsonResource(resourceName), type);
    }
}