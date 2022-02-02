package org.pih.analytics.flink.functions;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.functions.AggregateFunction;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class PreferredFunction extends AggregateFunction<String, PreferredFunction.PreferredAccumulator> {

    @Override
    public PreferredAccumulator createAccumulator() {
        return new PreferredAccumulator();
    }

    @Override
    public String getValue(PreferredAccumulator acc) {
        return acc.preferredId == null ? null : acc.accumulatedValues.get(acc.preferredId).f2;
    }

    public void accumulate(PreferredAccumulator acc, Integer id, Boolean preferred, Timestamp dateCreated, String fieldValue) {
        acc.add(id, new Tuple3<>(preferred, dateCreated, fieldValue));
    }

    public void merge(PreferredAccumulator acc, Iterable<PreferredAccumulator> it) {
        for (PreferredAccumulator a : it) {
            for (Integer id : acc.accumulatedValues.keySet()) {
                acc.add(id, acc.accumulatedValues.get(id));
            }
        }
    }

    public void retract(PreferredAccumulator acc, Integer id, Boolean preferred, Timestamp dateCreated, String fieldValue) {
        acc.remove(id);
    }

    public static class PreferredAccumulator {

        public Integer preferredId = null;
        public Map<Integer, Tuple3<Boolean, Timestamp, String>> accumulatedValues = new HashMap<>();

        void add(Integer id, Tuple3<Boolean, Timestamp, String> valueRow) {
            accumulatedValues.put(id, valueRow);
            updateValue();
        }

        void remove(Integer id) {
            accumulatedValues.remove(id);
            if (Objects.equals(preferredId, id)) {
                preferredId = null;
                updateValue();
            }
        }

        void updateValue() {
            for (Integer id : accumulatedValues.keySet()) {
                Tuple3<Boolean, Timestamp, String> row = accumulatedValues.get(id);
                if (preferredId == null) {
                    preferredId = id;
                }
                else {
                    Tuple3<Boolean, Timestamp, String> preferredRow = accumulatedValues.get(preferredId);
                    if (BooleanUtils.isTrue(row.f0) || BooleanUtils.isNotTrue(preferredRow.f0)) {
                        if (preferredRow.f1 == null || preferredRow.f1.before(row.f1)) {
                            preferredId = id;
                        }
                    }
                }
            }
        }
    }

}