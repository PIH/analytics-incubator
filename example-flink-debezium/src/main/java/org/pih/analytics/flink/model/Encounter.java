package org.pih.analytics.flink.model;

import lombok.Data;
import org.pih.analytics.flink.util.ObjectMap;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Data
public class Encounter implements Serializable {

    private final Integer encounterId;
    private Integer patientId;
    private ObjectMap encounter;
    private ObjectMap visit;
    private Map<Integer, ObjectMap> providers = new HashMap<>();
    private Map<Integer, ObjectMap> obs = new HashMap<>();
    private Map<Integer, ObjectMap> orders = new HashMap<>();

    public Encounter(Integer encounterId) {
        this.encounterId = encounterId;
    }

    @Override
    public String toString() {
        return "Encounter (" + encounterId + "): " + providers.size() + " providers, " + obs.size() + " obs, " + orders.size() + " orders";
    }
}
