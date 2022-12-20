CREATE VIEW preferred_name AS
SELECT      n.person_id,
            Preferred(n.person_name_id, n.preferred, To_Timestamp(n.date_created), n.given_name) as given_name,
            Preferred(n.person_name_id, n.preferred, To_Timestamp(n.date_created), n.family_name) as family_name
FROM        openmrs.person_name n
WHERE       n.voided = false
GROUP BY    n.person_id
;