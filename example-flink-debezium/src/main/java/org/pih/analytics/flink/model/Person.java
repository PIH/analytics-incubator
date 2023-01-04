package org.pih.analytics.flink.model;

import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.pih.analytics.flink.debezium.ChangeEvent;
import org.pih.analytics.flink.debezium.DebeziumOperation;
import org.pih.analytics.flink.function.map.EventToPersonMapper;
import org.pih.analytics.flink.util.ObjectMap;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Data
public class Person implements Serializable {

    private static final Logger log = LogManager.getLogger(EventToPersonMapper.class);

    private Integer personId;
    private ObjectMap person;
    private Map<String, ObjectMap> names = new HashMap<>();
    private Map<String, ObjectMap> addresses = new HashMap<>();
    private Map<String, ObjectMap> attributes = new HashMap<>();

    public void mergeIn(Person p) {
        if (person == null) {
            person = p.getPerson();
            log.warn(p.getPersonId() + ": person added; " + this);
        }
        if (!p.getNames().isEmpty()) {
            getNames().putAll(p.getNames());
            log.warn(p.getPersonId() + ": " + p.getNames().size() + " person names added; " + this);
        }
        if (!p.getAddresses().isEmpty()) {
            getAddresses().putAll(p.getAddresses());
            log.warn(p.getPersonId() + ": " + p.getAddresses().size() + " person addresses added; " + this);
        }
        if (!p.getAttributes().isEmpty()) {
            getAttributes().putAll(p.getAttributes());
            log.warn(p.getPersonId() + ": " + p.getAttributes().size() + " person attributes added; " + this);
        }
    }

    @Override
    public String toString() {
        return "Person (" + getPersonId() + "): " + names.size() + " names, " + addresses.size() + " addresses, " + attributes.size() + " attributes";
    }

    /**
     * Update the Person with the given event.  Returns true if the Person was updated
     */
    public boolean addEvent(ChangeEvent event) {
        boolean updated = false;
        if (personId == null && event.getPersonId() != null) {
            personId = event.getPersonId();
            updated = true;
        }
        switch (event.getTable()) {
            case "person":
                person = event.getAfter();
                updated = true;
                break;
            case "person_name":
                addOrUpdate(names, event);
                updated = true;
                break;
            case "person_address":
                addOrUpdate(addresses, event);
                updated = true;
                break;
            case "person_attribute":
                addOrUpdate(attributes, event);
                updated = true;
                break;
            default:
                log.warn("Not adding to person: " + event);
        }
        return updated;
    }

    protected void addOrUpdate(Map<String, ObjectMap> m, ChangeEvent event) {
        String key = event.getKey();
        if (event.getOperation() == DebeziumOperation.DELETE) {
            m.remove(key);
        }
        else {
            m.put(key, event.getAfter());
        }
    }
}
