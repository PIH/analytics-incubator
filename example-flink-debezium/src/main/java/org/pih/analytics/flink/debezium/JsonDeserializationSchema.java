package org.pih.analytics.flink.debezium;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.ConverterType;

import java.util.HashMap;
import java.util.Map;

/**
 * This is a copy of the JsonDebeziumDeserializationSchema, with the modification that it includes both the
 * key and the value in the payload.  We may not want this, but this demonstrates what can be done and how to
 * customize the default behavior for any enhancements that may be needed.
 */
public class JsonDeserializationSchema implements DebeziumDeserializationSchema<String> {

    private static final long serialVersionUID = 1L;
    private transient JsonConverter jsonConverter;
    private final Boolean includeSchema;

    public JsonDeserializationSchema() {
        this(false);
    }

    public JsonDeserializationSchema(Boolean includeSchema) {
        this.includeSchema = includeSchema;
    }

    public void deserialize(SourceRecord record, Collector<String> out) {
        if (this.jsonConverter == null) {
            this.jsonConverter = new JsonConverter();
            Map<String, Object> configs = new HashMap<>();
            configs.put("converter.type", ConverterType.VALUE.getName());
            configs.put("schemas.enable", this.includeSchema);
            this.jsonConverter.configure(configs);
        }
        String key = new String(this.jsonConverter.fromConnectData(record.topic(), record.keySchema(), record.key()));
        String value = new String(this.jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value()));
        out.collect("{\"key\": " + key + ", \"value\": " + value + "}");
    }

    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}