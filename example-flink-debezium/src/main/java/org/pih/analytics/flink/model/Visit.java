package org.pih.analytics.flink.model;

import lombok.Data;
import org.pih.analytics.flink.util.ObjectMap;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Data
public class Visit implements Serializable {

    private Integer visitId;
    private Integer patientId;
    private ObjectMap visit;
    private Set<Integer> encounters = new HashSet<>();
    private Map<Integer, ObjectMap> attributes = new HashMap<>();

    public Visit(Integer visitId) {
        this.visitId = visitId;
    }

    @Override
    public String toString() {
        return "Visit (" + visitId + "): " + encounters.size() + " encounters, " + attributes.size() + " attributes";
    }
}
