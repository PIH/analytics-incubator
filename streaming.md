# Create a table to contain the most recent datetime each patient has been updated

```sql
create table changelog_patient
(
id           int primary key auto_increment,
patient_id   int unique not null,
date_changed datetime,
deleted      boolean
);
```

# Populate this table initially based on the patient table

```sql
insert into changelog_patient (patient_id, date_changed, deleted)
select  patient_id,
greatest(ifnull(date_created, 0), ifnull(date_changed, 0), ifnull(date_voided, 0)),
voided
from    patient
;
```

# Update this based on the encounter table

```sql
update      changelog_patient p
inner join  encounter e on p.patient_id = e.patient_id
set         p.date_changed = greatest(p.date_changed, ifnull(e.date_created, 0), ifnull(e.date_changed, 0), ifnull(e.date_voided, 0))
;
```

Add additional tables to check to further refine the latest date per patient based on existing data.
The goal here is not to be exact, but to provider sufficient variability in the initial data set to approximate
what this table would look like and to provide a means for downstream processes to batch processing of patient
changes based on an incrementing (or decrementing) last change date

Another way we could accomplish this would be to run through a full DBZ snapshot, and any event that has
patient_id or person_id, get the max of any of date_created, date_changed, date_voided that exist.
So basically, if the event is a READ, update patient date based on table dates, otherwise update based on event date