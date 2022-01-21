# openmrs-analytics-spike

This repository represents a series of spikes and experiments with a variety of software platforms that are used for data integration, ETL, analytics and reporting.

Many of the directories included here encapsulate a docker-compose setup that contains related services to achieve a particular function.  In order to connect the containers between these together, all of them will share a common network and will be able to communicate with each other using the container names and ports defined. The common docker network these will share is called "openmrs-analytics-network", and should be created as follows:

```shell
docker network create openmrs-analytics-network
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

## Getting data into an Analytics Data Store

The following approachs and tools should be evaluated:

**ElasticSearch with Kibana**

In most of the Flink CDC and Kafka SQL demonstrations, ElasticSearch and Kibana used in Docker containers as a sink to store and visualize the data in the pipeline.  Some examples below which might be useful when these are built out:

* https://github.com/ververica/flink-sql-CDC
* https://flink.apache.org/2020/07/28/flink-sql-demo-building-e2e-streaming-application.html

```
    elasticsearch:
      image: docker.elastic.co/elasticsearch/elasticsearch:7.6.0
      environment:
        - cluster.name=docker-cluster
        - bootstrap.memory_lock=true
        - "ES_JAVA_OPTS=-Xms512m -Xmx512m"
        - discovery.type=single-node
      ports:
        - "9200:9200"
        - "9300:9300"
      ulimits:
        memlock:
          soft: -1
          hard: -1
        nofile:
          soft: 65536
          hard: 65536
    kibana:
      image: docker.elastic.co/kibana/kibana:7.6.0
      ports:
      - "5601:5601"
      
      
```

**Druid**

[Druid](https://druid.apache.org/technology)

DATABASES:

**Cassandra**

[Cassandra](https://cassandra.apache.org/_/index.html)
