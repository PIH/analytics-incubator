package org.pih.analytics.flink.function.flatmap;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.pih.analytics.flink.debezium.ChangeEvent;
import org.pih.analytics.flink.debezium.DebeziumOperation;
import org.pih.analytics.flink.model.Encounter;
import org.pih.analytics.flink.model.Patient;
import org.pih.analytics.flink.model.Visit;
import org.pih.analytics.flink.util.ObjectMap;
import org.pih.analytics.flink.util.Rocks;

import java.util.Map;

/**
 * Maintains a key value store of composite objects.  For a given event,
 * updates each composite object, writes these back to the store, and emits any that have changed
 */
public class EventToCompositeFlatMapper extends RichFlatMapFunction<ChangeEvent, Patient> {

    private static final Logger log = LogManager.getLogger(EventToCompositeFlatMapper.class);

    private final String name;
    private transient Rocks patientDb;
    private transient Rocks encounterDb;

    public EventToCompositeFlatMapper(String name) {
        this.name = name;
    }

    @Override
    public void open(Configuration parameters) {
        patientDb = new Rocks(name + "patients");
        encounterDb = new Rocks(name + "encounters");
    }

    @Override
    public void close() {
        patientDb.close();
        encounterDb.close();
    }

    @Override
    public void flatMap(ChangeEvent event, Collector<Patient> collector) {
        boolean updated = true;
        Integer pId = event.getPersonOrPatientId();
        Integer encounterId = event.getEncounterId();
        Integer visitId = event.getVisitId();
        switch (event.getTable()) {
            case "patient": {
                Patient patient = patientDb.getOrDefault(pId, new Patient(pId));
                patient.setPatient(event.getValues());
                patientDb.put(pId, patient);
                break;
            }
            case "person": {
                Patient patient = patientDb.getOrDefault(pId, new Patient(pId));
                patient.setPerson(event.getValues());
                patientDb.put(pId, patient);
                break;
            }
            case "person_name": {
                Patient patient = patientDb.getOrDefault(pId, new Patient(pId));
                addOrUpdate(patient.getNames(), event, "person_name_id");
                patientDb.put(pId, patient);
                break;
            }
            case "person_address": {
                Patient patient = patientDb.getOrDefault(pId, new Patient(pId));
                addOrUpdate(patient.getAddresses(), event, "person_address_id");
                patientDb.put(pId, patient);
                break;
            }
            case "person_attribute": {
                Patient patient = patientDb.getOrDefault(pId, new Patient(pId));
                addOrUpdate(patient.getAttributes(), event, "person_attribute_id");
                patientDb.put(pId, patient);
                break;
            }
            case "obs": {
                Patient patient = patientDb.getOrDefault(pId, new Patient(pId));
                if (event.getEncounterId() == null) {
                    addOrUpdate(patient.getStandaloneObs(), event, "obs_id");
                } else {
                    Encounter encounter = encounterDb.getOrDefault(encounterId, new Encounter(encounterId));
                    addOrUpdate(encounter.getObs(), event, "obs_id");
                    encounterDb.put(encounterId, encounter);
                    patient.getEncounters().put(encounterId, encounter);
                }
                patientDb.put(pId, patient);
                break;
            }
            case "visit": {
                Patient patient = patientDb.getOrDefault(pId, new Patient(pId));
                Visit visit = patient.getVisits().getOrDefault(visitId, new Visit(visitId));
                visit.setVisit(event.getValues());
                patient.getVisits().put(visitId, visit);
                patientDb.put(pId, patient);
                break;
            }
            case "encounter": {
                Encounter encounter = encounterDb.getOrDefault(encounterId, new Encounter(encounterId));
                encounter.setPatientId(pId);
                encounter.setEncounter(event.getValues());
                encounterDb.put(encounterId, encounter);
                Patient patient = patientDb.getOrDefault(pId, new Patient(pId));
                patient.getEncounters().put(encounterId, encounter);
                if (visitId != null) {
                    Visit visit = patient.getVisits().getOrDefault(visitId, new Visit(visitId));
                    visit.getEncounters().add(encounterId);
                    patient.getVisits().put(visitId, visit);
                }
                patientDb.put(pId, patient);
                break;
            }
            default: {
                updated = false;
            }
        }
        if (updated) {
            Patient patient = patientDb.getOrDefault(pId, new Patient(pId));
            if (patient.getPatient() != null) {
                // TODO: Use a timer to emit after all updates in a session are complete
                collector.collect(patient);
            }
        }
    }

    protected void addOrUpdate(Map<Integer, ObjectMap> m, ChangeEvent event, String keyColumn) {
        Integer key = event.getValues().getInteger(keyColumn);
        if (event.getOperation() == DebeziumOperation.DELETE) {
            m.remove(key);
        }
        else {
            m.put(key, event.getAfter());
        }
    }
}
