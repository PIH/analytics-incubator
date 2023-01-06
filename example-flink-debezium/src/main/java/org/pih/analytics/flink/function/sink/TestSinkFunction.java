package org.pih.analytics.flink.function.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.pih.analytics.flink.model.Patient;
import org.pih.analytics.flink.util.Util;

import java.util.HashSet;
import java.util.Set;

public class TestSinkFunction extends RichSinkFunction<Patient> {

    private long startTime = System.currentTimeMillis();
    private long endTime = System.currentTimeMillis();
    private int numInvocations = 0;
    private final Set<Integer> patientIds = new HashSet<>();

    @Override
    public void open(Configuration parameters) {
    }

    @Override
    public void close() {
    }

    @Override
    public void invoke(Patient value, Context context) throws Exception {
        numInvocations++;
        if (value.getPatientId() != null) {
            patientIds.add(value.getPatientId());
        }
        if (numInvocations % 100000 == 0) {
            System.out.println("Number of invocations: " + numInvocations);
            System.out.println("Number of patients: " + patientIds.size());
            System.out.println("Patient #" + value.getPatientId());
            System.out.println("**************************");
            System.out.println(Util.toJson(value));
            System.out.println("**************************");
        }
    }
}