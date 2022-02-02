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
            zlemr.identifier
FROM        openmrs.patient pat
INNER JOIN  openmrs.person p ON pat.patient_id = p.person_id
LEFT JOIN (
    SELECT      i.patient_id, Preferred(i.patient_identifier_id, i.preferred, To_Timestamp(i.date_created), i.identifier) as identifier
    FROM        openmrs.patient_identifier i
    INNER JOIN  openmrs.patient_identifier_type t on i.identifier_type = t.patient_identifier_type_id
    WHERE       t.name = 'ZL EMR ID'
    GROUP BY    i.patient_id
) zlemr ON pat.patient_id = zlemr.patient_id
;