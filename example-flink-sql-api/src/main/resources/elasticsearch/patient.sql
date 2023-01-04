CREATE TABLE patient_index
(
    patient_id INT,
    uuid STRING,
    zlemr_id STRING,
    given_name STRING,
    family_name STRING,
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
    PRIMARY KEY (patient_id) NOT ENFORCED
) WITH (
    'connector' = 'print'
)
;

INSERT INTO patient_index
SELECT patient_id,
       uuid,
       zlemr_id,
       given_name,
       family_name,
       gender,
       birthdate,
       birthdate_estimated,
       dead,
       cause_of_death,
       cause_of_death_non_coded,
       death_date,
       death_date_estimated,
       creator,
       date_created
FROM   zl.patient
;