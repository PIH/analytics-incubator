# analytics-incubator

This repository represents a collection of tools that are under evaluation for usage and to provide a means to document experiences, develop proof-of-concepts, and provide a set of building blocks for further solution designs in analytics, data integration, ETL, and reporting.

Many of the directories included here encapsulate a docker-compose setup that contains related services to achieve a particular function.  In order to connect the containers between these together, all of them will share a common network and will be able to communicate with each other using the container names and ports defined. The common docker network these will share is called "analytics-incubator-network", and should be created as follows:

```shell
docker network create analytics-incubator-network
```

## Getting data out of OpenMRS

For these examples, we are going to focus on Change Data Capture (CDC), using Debezium.  There are a few described ways to do this.  At a high-level, these approaches are as follows:

**Using Kafka**

This approach first configured Kafka as a sink for Debezium events, and then processes events from Kafka using Flink or other tools.  This is demonstrated by following the [event-log](event-log) and [flink-jobs](flink-jobs) READMEs.

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