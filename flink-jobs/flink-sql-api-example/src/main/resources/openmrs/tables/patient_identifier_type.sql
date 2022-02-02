CREATE TABLE patient_identifier_type
(
    patient_identifier_type_id INT,
    name STRING,
    PRIMARY KEY (patient_identifier_type_id) NOT ENFORCED
) WITH (
      'connector' = 'kafka',
      'topic' = 'openmrs-humci.openmrs.patient_identifier_type',
      'properties.bootstrap.servers' = 'localhost:9092',
      'properties.group.id' = 'openmrs-patient-identifier-type-table',
      'format' = 'debezium-json',
      'scan.startup.mode' = 'earliest-offset'
)