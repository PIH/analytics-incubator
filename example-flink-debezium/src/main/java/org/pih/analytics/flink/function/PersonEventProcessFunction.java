package org.pih.analytics.flink.function;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.pih.analytics.flink.debezium.ChangeEvent;
import org.pih.analytics.flink.model.Patient;

/**
 * This function aggregates person events into a Person object
 */
public class PersonEventProcessFunction extends KeyedProcessFunction<Integer, ChangeEvent, Patient> {

    private ValueState<Patient> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("personState", Patient.class));
    }

    @Override
    public void processElement(ChangeEvent event,
                               KeyedProcessFunction<Integer, ChangeEvent, Patient>.Context context,
                               Collector<Patient> collector) throws Exception {
    }
        /*

        Patient personAggregate = state.value();
        if (personAggregate == null) {
            personAggregate = new Patient();
            personAggregate.setPatientId(event.getPatientId());
        }

        if (event.getTable().equals("person")) {
            personAggregate.setPerson(event.getValues());
        }

    }


    protected ObjectMap getPreferred(ObjectMap o1, ObjectMap o2) {
        if (o1.isDeleted()) {
            return event2;
        }
        if (event2.isDeleted()) {
            return event1;
        }
        if (isPreferred(event1) && !isPreferred(event2)) {
            return event1;
        }
        if (!isPreferred(event1) && isPreferred(event2)) {
            return event2;
        }
        return event1.getDateCreated().after(event2.getDateCreated()) ? event1 : event2;
    }


        // update the state's count
        current.count++;

        // set the state's timestamp to the record's assigned event time timestamp
        current.lastModified = ctx.timestamp();

        // write the state back
        state.update(current);

        // schedule the next timer 60 seconds from the current event time
        ctx.timerService().registerEventTimeTimer(current.lastModified + 60000);
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<Tuple2<String, Long>> out) throws Exception {

        // get the state for the key that scheduled the timer
        CountWithTimestamp result = state.value();

        // check if this is an outdated timer or the latest timer
        if (timestamp == result.lastModified + 60000) {
            // emit the state on timeout
            out.collect(new Tuple2<String, Long>(result.key, result.count));
        }
    }
     */
}