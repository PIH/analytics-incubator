CREATE TABLE patient_identifier
(
    patient_identifier_id INT,
    patient_id INT,
    identifier STRING,
    identifier_type INT,
    preferred BOOLEAN,
    location_id INT,
    creator INT,
    date_created BIGINT,
    voided BOOLEAN,
    PRIMARY KEY (patient_id) NOT ENFORCED
) WITH (
      'connector' = 'kafka',
      'topic' = 'openmrs-humci.openmrs.patient_identifier',
      'properties.bootstrap.servers' = 'localhost:9092',
      'properties.group.id' = 'openmrs-patient-identifier-table',
      'format' = 'debezium-json',
      'scan.startup.mode' = 'earliest-offset'
)