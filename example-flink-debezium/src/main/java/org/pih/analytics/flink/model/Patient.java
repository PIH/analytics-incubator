package org.pih.analytics.flink.model;

import lombok.Data;
import org.pih.analytics.flink.util.ObjectMap;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Data
public class Patient implements Serializable {

    private final Integer patientId;
    private ObjectMap person;
    private ObjectMap patient;
    private Map<Integer, ObjectMap> names = new HashMap<>();
    private Map<Integer, ObjectMap> addresses = new HashMap<>();
    private Map<Integer, ObjectMap> attributes = new HashMap<>();
    private Map<Integer, ObjectMap> standaloneObs = new HashMap<>();
    private Map<Integer, Encounter> encounters = new HashMap<>();
    private Map<Integer, Visit> visits = new HashMap<>();

    public Patient(Integer patientId) {
        this.patientId = patientId;
    }

    @Override
    public String toString() {
        return "Patient-" + patientId;
    }
}
