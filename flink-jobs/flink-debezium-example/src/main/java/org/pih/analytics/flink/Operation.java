package org.pih.analytics.flink;

enum Operation {

    READ,
    READ_VOID,
    INSERT,
    UPDATE,
    DELETE;

    public static Operation parse(String operation) {
        if ("r".equals(operation)) {
            return READ;
        }
        else if ("i".equals(operation)) {
            return INSERT;
        }
        if ("u".equals(operation)) {
            return UPDATE;
        }
        if ("d".equals(operation)) {
            return DELETE;
        }
        throw new IllegalStateException("Unknown operation of '" + operation + "'. Unable to parse.");
    }
}