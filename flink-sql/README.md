# Flink SQL

## Overview

This builds on the [event-log](../event-log) section to experiment with Flink SQL as a means to ETL real-time changes
from OpenMRS into a warehouse database. This is inspired in particular from these resources, which are a great
introduction to what is possible with Flink consuming CDC events.

* [CDC With Flink SQL - Marta Paes](https://noti.st/morsapaes/QnBSAI/change-data-capture-with-flink-sql-and-debezium)
* [CDC With Flink SQL - Ververica](https://github.com/ververica/flink-sql-CDC)


```shell
docker-compose exec sql-client sql-client.sh
```

```
CREATE TABLE person (
  person_id INT,
  gender STRING,
  birthdate DATE,
  birthdate_estimated BOOLEAN,
  dead BOOLEAN,
  cause_of_death INT,
  cause_of_death_non_coded STRING,
  death_date TIMESTAMP(0),
  death_date_estimated BOOLEAN,
  creator INT,
  date_created TIMESTAMP(0),
  PRIMARY KEY (person_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'openmrs-humci.openmrs.person',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = '1',
    'format' = 'debezium-json',
    'scan.startup.mode' = 'earliest-offset'
);
```

TODO Thoughts:
* change from json -> avro
* change group.id (need to change above and in kafka-connect compose)


* https://github.com/ververica/flink-cdc-connectors
* https://ververica.github.io/flink-cdc-connectors/master/content/quickstart/mysql-postgres-tutorial.html#

## Direct Install of Flink

* Installed Flink 1.14.2 [as described here](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/try-flink/local_installation/) into `/opt/flink-1.14.2`.
* Downloaded the [mysql-cdc connector](https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-mysql-cdc/2.1.1/flink-sql-connector-mysql-cdc-2.1.1.jar) and installed into `/opt/flink-1.14.2/lib`

### Try Table/SQL API

From `/opt/flink-1.14.2`:

Start-up the Flink Cluster
`./bin/start-cluster.sh`

Start up the Flink SQL CLI:

`./bin/sql-client.sh`

First, enable checkpoints every 3 seconds:
 
```Flink SQL> SET execution.checkpointing.interval = 3s;```

Then create tables to capture source tables:

```
CREATE TABLE person (
  person_id INT,
  gender STRING,
  birthdate DATE,
  birthdate_estimated BOOLEAN,
  dead BOOLEAN,
  cause_of_death INT,
  cause_of_death_non_coded STRING,
  death_date TIMESTAMP(0),
  death_date_estimated BOOLEAN,
  creator INT,
  date_created TIMESTAMP(0),
  PRIMARY KEY (person_id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'localhost',
    'port' = '3308',
    'username' = 'root',
    'password' = 'root',
    'database-name' = 'openmrs',
    'table-name' = 'person'
);
```