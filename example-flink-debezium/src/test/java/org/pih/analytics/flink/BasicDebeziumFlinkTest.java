package org.pih.analytics.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.jupiter.api.Test;
import org.pih.analytics.flink.config.StreamConfig;
import org.pih.analytics.flink.debezium.ChangeEvent;
import org.pih.analytics.flink.function.map.ChangeEventMapper;
import org.pih.analytics.flink.function.reduce.LatestEventReducer;
import org.pih.analytics.flink.util.Util;

public class BasicDebeziumFlinkTest {

    OpenmrsStreamProcessor processor = new OpenmrsStreamProcessor("testStream");

    /**
     * This gets a Data Stream of ChangeEvent objects.  This stream returns only the most recent event for a given
     * database row (table+key) within a processing window of 2 seconds
     */
    protected DataStream<ChangeEvent> getEventStream() {
        StreamConfig config = Util.readJsonResource("/config.json", StreamConfig.class);

        DataStreamSource<String> binlogStream = processor.fromMySQLBinlog(
                config.getMysqlSource(), config.getMysqlTables(), config.getDebeziumProperties()
        );

        // Transform the stream of JSON into a Stream of Debezium Event objects
        DataStream<ChangeEvent> eventStream = binlogStream.map(new ChangeEventMapper());

        // Partition out the events by table, and primary key in table, and reduce down to most recent ending value per table row
        DataStream<ChangeEvent> latestRowStream = eventStream
                .keyBy(ChangeEvent::getTable)
                .keyBy(ChangeEvent::getKey)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(2)))
                .reduce(new LatestEventReducer());

        return latestRowStream;
    }

    @Test
    public void shouldPrintStream() throws Exception {
        getEventStream().addSink(new PrintSinkFunction<>());
        processor.start();
    }

    @Test
    public void shouldUpdateData() throws Exception {
        /*
        JdbcSink jdbcSink = JdbcSink.sink(

                sqlDmlStatement,                       // mandatory
                jdbcStatementBuilder,                  // mandatory
                jdbcExecutionOptions,                  // optional
                jdbcConnectionOptions                  // mandatory
        );


        getEventStream().addSink(new PatientSink<>());
        processor.start();
        
         */
    }

    /*
    @Test
    public void shouldMapToPersons() throws Exception {
        DataStream<ChangeEvent> eventStream = getEventStream();

        SingleOutputStreamOperator<Patient> personEvent = eventStream
                .filter(new TableFilter("person"))
                .map(new EventToPersonMapper());

        SingleOutputStreamOperator<Patient> personNameEvent = eventStream
                .filter(new TableFilter("person_name"))
                .map(new EventToPersonMapper());

        personEvent = personEvent
                .join(personNameEvent)
                .where(Patient::getPersonId).equalTo(Patient::getPersonId)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(2)))
                .apply(new PersonJoinFunction())
                .map(p -> p);


        SingleOutputStreamOperator<Patient> personAddressEvent = eventStream
                .filter(new TableFilter("person_address"))
                .map(new EventToPersonMapper());

        personEvent = personEvent
                .join(personAddressEvent)
                .where(Patient::getPersonId).equalTo(Patient::getPersonId)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(2)))
                .apply(new PersonJoinFunction())
                .map(p -> p);

        SingleOutputStreamOperator<Patient> personAttributeEvent = eventStream
                .filter(new TableFilter("person_attribute"))
                .map(new EventToPersonMapper());

        personEvent = personEvent
                .join(personAttributeEvent)
                .where(Patient::getPersonId).equalTo(Patient::getPersonId)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(2)))
                .apply(new PersonJoinFunction())
                .map(p -> p);

        personEvent.print();
                /*
                .join(personAddressEvent)
                .where(Person::getPersonId).equalTo(Person::getPersonId)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(2)))
                .apply(new PersonJoinFunction())
                .join(personAttributeEvent)
                .where(Person::getPersonId).equalTo(Person::getPersonId)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(2)))
                .apply(new PersonJoinFunction())
                .print();

        processor.start();
    }

*/
/*

        // Run each stream of table events through a process function that can enhance them, for example add patient_id to events that lack it
        tableStream = tableStream
                .keyBy(event -> event.key)
                .process(new EnhancedEventFunction());

        latestRowStream.addSink(StreamingFileSink.forRowFormat(
                        new Path("/tmp/flinktesting/latestevents"),
                        new SimpleStringEncoder<DebeziumEvent>("UTF-8")).build());


 */
}
