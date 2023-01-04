package org.pih.analytics.flink.function.flatmap;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.pih.analytics.flink.debezium.ChangeEvent;
import org.pih.analytics.flink.model.Person;

/**
 * Maintains a person in keyed state.  Updates this person with the given Debezium Event, updates it, and
 * outputs the updated Person if it has changed.
 * TODO: Not sure if this is right, or if a different construct should be used
 */
public class EventToPersonFlatMapper extends RichFlatMapFunction<ChangeEvent, Person> {

    private static final Logger log = LogManager.getLogger(EventToPersonFlatMapper.class);

    private transient ValueState<Person> person;

    @Override
    public void open(Configuration config) {
        log.warn("Creating value state for persons..." + config);
        person = getRuntimeContext().getState(new ValueStateDescriptor<>("personState", Person.class));
        log.warn("Value state for persons created.");
    }

    @Override
    public void flatMap(ChangeEvent event, Collector<Person> collector) throws Exception {
        log.warn("Mapping event to person: " + event);
        Person currentPerson = person.value();
        boolean updated = currentPerson.addEvent(event);
        if (updated) {
            person.update(currentPerson);
            collector.collect(currentPerson);
        }
    }
}
