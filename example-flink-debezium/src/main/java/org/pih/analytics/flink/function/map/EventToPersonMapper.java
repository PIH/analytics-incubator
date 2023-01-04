package org.pih.analytics.flink.function.map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.pih.analytics.flink.debezium.ChangeEvent;
import org.pih.analytics.flink.model.Person;

public class EventToPersonMapper implements MapFunction<ChangeEvent, Person> {

    private static final Logger log = LogManager.getLogger(EventToPersonMapper.class);

    @Override
    public Person map(ChangeEvent event) {
        Person person = new Person();
        person.addEvent(event);
        return person;
    }
}
