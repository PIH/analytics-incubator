package org.pih.analytics.flink.direct;

import io.debezium.engine.ChangeEvent;

import java.util.function.Consumer;

/**
 * Handle a change event
 */
public class DebeziumConsumer implements Consumer<ChangeEvent<String, String>> {

    @Override
    public void accept(ChangeEvent<String, String> event) {
        System.out.println("=============================");
        System.out.println(event.key());
        System.out.println("=============================");
        System.out.println(event.value());
        System.out.println("");
    }
}
