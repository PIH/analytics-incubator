CREATE TABLE person_address
(
    person_address_id INT,
    person_id INT,
    state_province STRING,
    city_village STRING,
    address1 STRING,
    address2 STRING,
    address3 STRING,
    preferred BOOLEAN,
    date_created BIGINT,
    voided BOOLEAN,
    PRIMARY KEY (person_address_id) NOT ENFORCED
) WITH (
      'connector' = 'kafka',
      'topic' = 'openmrs-humci.openmrs.person_address',
      'properties.bootstrap.servers' = 'localhost:9092',
      'properties.group.id' = 'connect-cluster-1',
      'format' = 'debezium-json',
      'scan.startup.mode' = 'earliest-offset'
)