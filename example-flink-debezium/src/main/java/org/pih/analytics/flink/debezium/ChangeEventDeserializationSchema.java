package org.pih.analytics.flink.debezium;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Converts a Kafka key/value into a KafkaDebeziumEvent
 */
public class ChangeEventDeserializationSchema implements KafkaRecordDeserializationSchema<ChangeEvent> {

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        KafkaRecordDeserializationSchema.super.open(context);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<ChangeEvent> collector) {
        String key = (consumerRecord.key() == null ? null : new String(consumerRecord.key()));
        String value = (consumerRecord.value() == null ? null : new String(consumerRecord.value()));
        collector.collect(new ChangeEvent(key, value));
    }

    public TypeInformation<ChangeEvent> getProducedType() {
        return TypeInformation.of(ChangeEvent.class);
    }
}