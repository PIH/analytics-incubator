# openmrs-analytics-spike

This repository represents a series of spikes and experiments with a variety of software platforms that are used for data integration, ETL, analytics and reporting.

Each of the directories included here encapsulates a docker-compose setup that contains related services to achieve a particular function.  In order to connect the containers between these together, all of them will share a common network and will be able to communicate with each other using the container names and ports defined. The common docker network these will share is called "openmrs-analytics-network", and should be created as follows:

```shell
docker network create openmrs-analytics-network
```

## Getting data out of OpenMRS

For these examples, we are going to focus on Change Data Capture (CDC), using Debezium.  There are a few described ways to do this.  At a high-level, these approaches are as follows:

1. Configuring Kafka as a sink for Debezium events, and then processing events from Kafka using Flink or other tools.
2. Configuring a Flink as a direct consumer of MySQL binlog using a Flink CDC connector for MySQL that embeds Debezium
3. Rolling our own custom application that embeds Debezium

These approaches are described in the below sections.

FLINK CDC WITHOUT KAFKA:

* [Flink CDC without Kafka - Github](https://github.com/ververica/flink-cdc-connectors)
* [Flink CDC without Kafka - Tutorial](https://ververica.github.io/flink-cdc-connectors/master/content/quickstart/mysql-postgres-tutorial.html#)

DATABASES:

* [Druid](https://druid.apache.org/technology)
* [Cassandra](https://cassandra.apache.org/_/index.html)


TODO:

### Enable Avro Support

* Replace json with avro in pom dependencies and add confluent repository to repositories:

```
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-avro-confluent-registry</artifactId>
            <version>${flink.version}</version>
        </dependency>
        ...
        <repository>
            <id>confluent</id>
            <url>https://packages.confluent.io/maven/</url>
        </repository>
```

* Update Kafka connector configurations for Table API like follows:

```
    "    'format' = 'debezium-avro-confluent',\n" +
    "    'debezium-avro-confluent.schema-registry.url' = 'http://localhost:8085',\n" +
```