# Flink Jobs

## Overview

Apache Flink is a framework and engine for fast and efficient distributed processing of data streams.  It is commonly employed to implement ETL and unified batch and streaming data pipelines off of real-time event streams.  The intention of these jobs is to connect the Kafka [event log](../event-log) source that is populated with real-time CDC events from Debezium, transform this data into analytics-friendly structures, and stream the resulting data into analytic data repositories that are fast, efficient, and intuitive to query.

### Pre-requisites

This connects with the Kafka-based [event-log](../event-log), and so this must be up-and-running to test out the various jobs in this section.

### Develop and test jobs

**Starting a Flink Cluster and using the SQL Client to execute a job**

[flink-docker](flink-docker) provides a base set of instructions and a working example of running a Flink cluster (job manager and task manager) in Docker.  It demonstrates using the SQL Client provided in the taskmanager container to demonstrate streaming data that changes in real time out of Kafka/Debezium/MySQL.

**Creating a Job as a Jar file that can be executed by Flink**

[flink-sql-api-example](flink-sql-api-example) builds a job as a jar file and demonstrates how this job can be run in Intellij to test out and see Flink running successfully.

**Running a job jar in a Flink Cluster**

One can combine the two above by:

* Modifying the [flink-sql-api-example](flink-sql-api-example) code - replacing "localhost:9092" with "kafka:9092" in the Flink SQL, and then building this with maven
* Modifying the [flink-docker](flink-docker) compose file by adding this built jar as an additional volume in the taskmanager service:
```
- "../flink-sql-api-example/target/flink-sql-api-example-1.0.0-SNAPSHOT.jar:/opt/flink/examples/flink-sql-api-example.jar"
```
* Starting up the Flink cluster by running `docker-compose up -d`
* Executing the job with Flink by running the following command: 
```shell
docker-compose exec taskmanager ./bin/flink run ./examples/flink-sql-api-example.jar
```
