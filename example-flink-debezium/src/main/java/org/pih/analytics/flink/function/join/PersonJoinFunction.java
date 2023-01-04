package org.pih.analytics.flink.function.join;

import org.apache.flink.api.common.functions.JoinFunction;
import org.pih.analytics.flink.model.Person;

public class PersonJoinFunction implements JoinFunction<Person, Person, Person> {

    @Override
    public Person join(Person p1, Person p2) {
        p1.mergeIn(p2);
        return p1;
    }
}