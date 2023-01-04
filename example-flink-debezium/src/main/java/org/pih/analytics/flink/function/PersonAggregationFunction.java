package org.pih.analytics.flink.function;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.pih.analytics.flink.debezium.ChangeEvent;
import org.pih.analytics.flink.model.Person;

/**
 * This function aggregates person events into a Person object
 */
public class PersonAggregationFunction extends KeyedProcessFunction<Integer, ChangeEvent, Person> {

    @Override
    public void open(Configuration parameters) throws Exception {

    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void processElement(ChangeEvent event, Context context, Collector<Person> collector) throws Exception {
        Person person = new Person(); // TODO: Get this person from state, create new if not yet in state.  Add data from event to person
        collector.collect(person);
    }
}