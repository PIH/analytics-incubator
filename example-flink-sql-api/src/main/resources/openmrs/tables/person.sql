CREATE TABLE person
(
    person_id INT,
    uuid STRING,
    gender STRING,
    birthdate BIGINT,
    birthdate_estimated BOOLEAN,
    dead BOOLEAN,
    cause_of_death INT,
    cause_of_death_non_coded STRING,
    death_date BIGINT,
    death_date_estimated BOOLEAN,
    creator INT,
    date_created BIGINT,
    voided BOOLEAN,
    PRIMARY KEY (person_id) NOT ENFORCED
) WITH (
      'connector' = 'kafka',
      'topic' = 'humci-humci.openmrs.person',
      'properties.bootstrap.servers' = 'localhost:9092',
      'properties.group.id' = 'openmrs-person-table',
      'format' = 'debezium-json',
      'scan.startup.mode' = 'earliest-offset'
)