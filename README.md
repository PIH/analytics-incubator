# analytics-incubator

This repository represents a collection of tools that are under evaluation for usage and to provide a means to document experiences, develop proof-of-concepts, and provide a set of building blocks for further solution designs in analytics, data integration, ETL, and reporting.

Many of the directories included here encapsulate a docker-compose setup that contains related services to achieve a particular function.  In order to connect the containers between these together, all of them will share a common network and will be able to communicate with each other using the container names and ports defined. The common docker network these will share is called "analytics", and should be created as follows:

```shell
docker network create analytics
```

## Getting data out of OpenMRS

For these examples, we are going to focus on Change Data Capture (CDC), using Debezium.  There are a few described ways to do this.  At a high-level, these approaches are as follows:

**Using Kafka**

This approach first configured Kafka as a sink for Debezium events, and then processes events from Kafka using Flink or other tools.  This is demonstrated by following the [event-log](kafka) and [flink-jobs](flink-jobs) READMEs.

**Using Flink without Kafka**

The idea here is that a Flink cluster can act as a direct consumer of the MySQL binlog using a Flink CDC connector that embeds Debezium.   This requires further investigation.  Additional resources here:

* [Flink CDC without Kafka - Github](https://github.com/ververica/flink-cdc-connectors)
* [Flink CDC without Kafka - Tutorial](https://ververica.github.io/flink-cdc-connectors/master/content/quickstart/mysql-postgres-tutorial.html#)

**Rolling our own custom application**

The idea here would be to build a custom application that could embed Debezium and other libraries.  This would require further investigation to pursue if desired.

## Data processing / ETL

* Apache Flink
* Apache Spark
* Apache Beam
* Apache Airflow

## Getting data into an Analytics Data Store

The following approachs and tools should be evaluated:

**ElasticSearch with Kibana**

In most of the Flink CDC and Kafka SQL demonstrations, ElasticSearch and Kibana used in Docker containers as a sink to store and visualize the data in the pipeline.  Some examples below which might be useful when these are built out:

* https://github.com/ververica/flink-sql-CDC
* https://flink.apache.org/2020/07/28/flink-sql-demo-building-e2e-streaming-application.html

**Druid**

[Druid](https://druid.apache.org/technology)

**Cassandra**

[Cassandra](https://cassandra.apache.org/_/index.html)

## Visualization tools

**Kibana**

[Kibana](https://www.elastic.co/kibana/)

**Apache SuperSet**

[Apache SuperSet](https://superset.apache.org/)

**Power BI**

The status quo


# Flink Jobs

## Overview

Apache Flink is a framework and engine for fast and efficient distributed processing of data streams.  It is commonly employed to implement ETL and unified batch and streaming data pipelines off of real-time event streams.  The intention of these jobs is to connect the Kafka [event log](../event-log) source that is populated with real-time CDC events from Debezium, transform this data into analytics-friendly structures, and stream the resulting data into analytic data repositories that are fast, efficient, and intuitive to query.

### Pre-requisites

This connects with the Kafka-based [event-log](../event-log), and so this must be up-and-running to test out the various jobs in this section.

### Develop and test jobs

**Starting a Flink Cluster and using the SQL Client to execute a job**

[flink-docker](flink) provides a base set of instructions and a working example of running a Flink cluster (job manager and task manager) in Docker.  It demonstrates using the SQL Client provided in the taskmanager container to demonstrate streaming data that changes in real time out of Kafka/Debezium/MySQL.

**Creating a Job as a Jar file that can be executed by Flink**

[flink-sql-api-example](flink-sql-api-example) builds a job as a jar file and demonstrates how this job can be run in Intellij to test out and see Flink running successfully.

**Running a job jar in a Flink Cluster**

One can combine the two above by:

* Modifying the [flink-sql-api-example](flink-sql-api-example) code - replacing "localhost:9092" with "kafka:9092" in the Flink SQL, and then building this with maven
* Modifying the [flink-docker](flink) compose file by adding this built jar as an additional volume in the taskmanager service:
```
- "../flink-sql-api-example/target/flink-sql-api-example-1.0.0-SNAPSHOT.jar:/opt/flink/examples/flink-sql-api-example.jar"
```
* Starting up the Flink cluster by running `docker-compose up -d`
* Executing the job with Flink by running the following command:
```shell
docker-compose exec taskmanager ./bin/flink run ./examples/flink-sql-api-example.jar
```
