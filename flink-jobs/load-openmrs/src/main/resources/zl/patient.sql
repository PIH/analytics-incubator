CREATE TABLE patient
(
    patient_id INT,
    uuid STRING,
    gender STRING,
    birthdate DATE,
    birthdate_estimated BOOLEAN,
    dead BOOLEAN,
    cause_of_death INT,
    cause_of_death_non_coded STRING,
    death_date TIMESTAMP ,
    death_date_estimated BOOLEAN,
    creator INT,
    date_created TIMESTAMP,
    zlemr_id STRING,
    given_name STRING,
    family_name STRING,
    PRIMARY KEY (patient_id) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'openmrs-humci.zl.patient',
      'properties.bootstrap.servers' = 'localhost:9092',
      'properties.group.id' = 'connect-cluster-1',
      'key.format' = 'json',
      'value.format' = 'json'
);

INSERT INTO patient
SELECT      pat.patient_id, p.uuid, p.gender, To_Date(p.birthdate), p.birthdate_estimated, p.dead, p.cause_of_death,
            p.cause_of_death_non_coded, To_Timestamp(p.death_date), p.death_date_estimated, p.creator, To_Timestamp(p.date_created),
            zlemrid.identifier,
            n.given_name,
            n.family_name
FROM        openmrs.patient pat
INNER JOIN  openmrs.person p ON p.person_id = pat.patient_id
LEFT JOIN   openmrs.preferred_name n ON n.person_id = pat.patient_id
LEFT JOIN   openmrs.preferred_identifier zlemrid ON zlemrid.patient_id = pat.patient_id AND zlemrid.name = 'ZL EMR ID'
WHERE       pat.voided = false
AND         p.voided = false
;