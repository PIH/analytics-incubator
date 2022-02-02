CREATE VIEW preferred_identifier AS
SELECT      i.patient_id, t.name, Preferred(i.patient_identifier_id, i.preferred, To_Timestamp(i.date_created), i.identifier) as identifier
FROM        openmrs.patient_identifier i
INNER JOIN  openmrs.patient_identifier_type t on i.identifier_type = t.patient_identifier_type_id
WHERE       i.voided = false
GROUP BY    i.patient_id, t.name
;