CREATE TABLE person_name
(
    person_name_id INT,
    person_id INT,
    given_name STRING,
    middle_name STRING,
    family_name STRING,
    preferred BOOLEAN,
    date_created BIGINT,
    voided BOOLEAN,
    PRIMARY KEY (person_name_id) NOT ENFORCED
) WITH (
      'connector' = 'kafka',
      'topic' = 'openmrs-humci.openmrs.person_name',
      'properties.bootstrap.servers' = 'localhost:9092',
      'properties.group.id' = 'connect-cluster-1',
      'format' = 'debezium-json',
      'scan.startup.mode' = 'earliest-offset'
)