CREATE VIEW preferred_address AS
SELECT      a.person_id,
            Preferred(a.person_address_id, a.preferred, To_Timestamp(a.date_created), a.state_province) as department,
            Preferred(a.person_address_id, a.preferred, To_Timestamp(a.date_created), a.city_village) as commune,
            Preferred(a.person_address_id, a.preferred, To_Timestamp(a.date_created), a.address3) as section_communal,
            Preferred(a.person_address_id, a.preferred, To_Timestamp(a.date_created), a.address1) as locality,
            Preferred(a.person_address_id, a.preferred, To_Timestamp(a.date_created), a.address2) as street_landmark
FROM        openmrs.person_address a
WHERE       a.voided = false
GROUP BY    a.person_id
;