package org.pih.analytics.flink.function.flatmap;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.pih.analytics.flink.debezium.ChangeEvent;

/**
 * This FlatMapper takes in a Debezium Event.
 * If this Event is for a Relationship, it splits it into two events, one for each person in the relationship
 * Otherwise, it just keeps the event unchanged
 */
public class RelationshipFlatMapper implements FlatMapFunction<ChangeEvent, ChangeEvent> {

    @Override
    public void flatMap(ChangeEvent event, Collector<ChangeEvent> collector) throws Exception {
        if (event.getTable().equals("relationship")) {
            ChangeEvent personA = new ChangeEvent(event.getKey(), event.getValue());
            personA.addValue("person_id", personA.getValues().get("person_a"));
            personA.addValue("related_to", personA.getValues().get("person_b"));
            collector.collect(personA);
            ChangeEvent personB = new ChangeEvent(event.getKey(), event.getValue());
            personB.addValue("person_id", personA.getValues().get("person_b"));
            personB.addValue("related_to", personA.getValues().get("person_a"));
            collector.collect(personB);
        }
        else {
            collector.collect(event);
        }
    }
}
