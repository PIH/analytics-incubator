package org.pih.analytics.debezium;

import io.debezium.engine.ChangeEvent;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.Serializable;

/**
 * Represents a Debezium Change Event
 */
public class DebeziumEvent implements Serializable {

    private final ChangeEvent<SourceRecord, SourceRecord> changeEvent;
    private final Long timestamp;
    private final DebeziumOperation operation;
    private final ObjectMap key;
    private final ObjectMap before;
    private final ObjectMap after;
    private final ObjectMap source;

    public DebeziumEvent(ChangeEvent<SourceRecord, SourceRecord> changeEvent) {
        this.changeEvent = changeEvent;
        try {
            SourceRecord record = changeEvent.value();
            key = new ObjectMap((Struct) record.key());
            Struct valueStruct = (Struct) record.value();
            timestamp = valueStruct.getInt64("ts_ms");
            operation = DebeziumOperation.parse(valueStruct.getString("op"));
            before = new ObjectMap(valueStruct.getStruct("before"));
            after = new ObjectMap(valueStruct.getStruct("after"));
            source = new ObjectMap(valueStruct.getStruct("source"));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public ChangeEvent<SourceRecord, SourceRecord> getChangeEvent() {
        return changeEvent;
    }

    public String getServerName() {
        return source.getString("name");
    }

    public String getTable() {
        return source.getString("table");
    }

    public DebeziumOperation getOperation() {
        return operation;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public ObjectMap getKey() {
        return key;
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

    public String getUuid() {
        return getValues().getString("uuid");
    }

    public ObjectMap getSource() {
        return source;
    }

    @Override
    public String toString() {
        return getChangeEvent().toString();
    }
}