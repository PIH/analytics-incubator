CREATE TABLE patient
(
    patient_id INT,
    creator INT,
    date_created BIGINT,
    voided BOOLEAN,
    PRIMARY KEY (patient_id) NOT ENFORCED
) WITH (
      'connector' = 'kafka',
      'topic' = 'openmrs-humci.openmrs.patient',
      'properties.bootstrap.servers' = 'localhost:9092',
      'properties.group.id' = 'connect-cluster-1',
      'format' = 'debezium-json',
      'scan.startup.mode' = 'earliest-offset'
)