# OpenMRS Event Log

## Overview

This describes a setup process for setting up an event log for OpenMRS.  The heart of this event log is
[Apache Kafka](https://kafka.apache.org/), which provides an immutable commit log and streaming platform for events 
and libraries of connectors that allow publishing and subscribing to a large number of systems.

The primary source of events feeding the Kafka instance is the OpenMRS MySQL instance.  This is accomplished through
the [Debezium](https://debezium.io/documentation/reference/stable/index.html) platform.  
Debezium is a change-data-capture (CDC) library that consumes the MySQL bin log and produces a stream of database change events, 
including all inserts, updates, and deletes, as well as schema changes.  Debezium is connected to Kafka via
a Debezium connector library that is added to the [Kafka Connect](https://kafka.apache.org/documentation/#connect) service
and configured to publish event streams of [specific tables within the OpenMRS database](openmrs-connector.json) to individual Kafka topics.

There are several services and components to this platform that can be seen in the provided [docker-compose.yml](docker-compose.yml)
file.  These are as follows:

* MySQL.      This should be a MySQL instance with row-level bin logging enabled.  MySQL 8 enables bin logging by default.
* Kafka.      The Event Streaming platform
* Zookeeper.  Used by Kafka for management of configuration, naming, and other services across distributed cluster.  May be eliminated in the near future.  See [Zookeeper Docs](https://zookeeper.apache.org/)
* Connect.    The Kafka Connect service serves to connect various data sources and sinks to Kafka.  The version used here comes pre-configured with a Debezium connector.
* Kafdrop.    A UI for viewing Kafka topics.  Here to utilize and compare with other versions.
* Kowl.       A UI for viewing Kafka topics.  Here to utilize and compare with other versions.

Much of the setup and configuration seen here followed these resource guides:

* [Debezium Tutorial](https://debezium.io/documentation/reference/stable/tutorial.html)
* [Mounting External Volumes for Kafka and Zookeeper](https://docs.confluent.io/platform/current/installation/docker/operations/external-volumes.html)
* [Docker Documentation in Github for Debezium Kafka and Zookeeper Images](https://github.com/debezium/docker-images)
* [Docker Documentation in Dockerhub for for Debezium Kafka and Zookeeper Images](https://hub.docker.com/r/debezium)
* [Ampath work by Allan Kimaina](https://github.com/kimaina/openmrs-elt/tree/master/cdc)

Depending on how this is adopted and scaled, replacing the Debezium-published Docker images with our own images
that are based on official images and contain the connectors and configuration needed may be desirable down the road.

## Installation

* Create a new directory to use for this project.  All further commands and instructions are relative to this folder.
* Copy or create symbolic links between [docker-compose.yml](docker-compose.yml) and [openmrs-connector.json](openmrs-connector.json) to this folder
* Setup or identify existing MySQL database with row-level bin logging enabled.  See [openmrs-mysql](../openmrs-mysql) for examples.
* Modify the docker-compose configuration as needed to reflect specifics of the MySQL instance

### Persistent volumes

Due to the way the Docker containers seem to operate, if we want to use host-mounted persistent storage to allow data to
be persisted across container restarts, we need to do so with specific users/groups if we are to use the Debezium-published Docker images.
The official Kafka/Zookeeper images seem like they probably have similar needs, though slightly different configurations and directories.
If we need to modify this, we would likely need to adapt/create our own custom images.  In order for the Kafka and Zookeeper containers
to properly startup and use host-mounted volumes, these need to be pre-exist and the kafka user/group must be able to access them and
create subdirectories and files within them.  If the directories do not already exist on the host, then Docker-compose will create them as 
the root user, and they will not be accessible to the user within the container.

```shell
sudo groupadd -r kafka -g 1001
sudo useradd -u 1001 -r -g kafka -s /sbin/nologin -c "Kafka user" kafka
mkdir -p kafka/data
mkdir -p zookeeper/data
mkdir -p zookeeper/txns
sudo chown -R kafka: kafka/
sudo chown -R kafka: zookeeper/
```

When all of the services have started up, one will see files accumulate in these directories.  Monitoring the size
of these directories and the files within them is something that will be of particular interest, especially when
testing or running against a large production-scale system.

## Startup and Configuration

Fire up all of the services via docker-compose:  

```shell
docker-compose up -d
```

Configure Kafka by interacting with the Kafka Connect REST endpoint.
You should modify the values in [openmrs-connector.json](openmrs-connector.json) to match your environment and needs.

You can view a list of all connectors configured (will be initially empty):

```shell
curl -H "Accept:application/json" localhost:8083/connectors/
```

You can add a new OpenMRS source connector:

```shell
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @openmrs-connector.json
```

You can view the details of a connector that is already added:

```shell
curl -i -X GET -H "Accept:application/json" localhost:8083/connectors/openmrs-connector
```

The above connector registration only needs to be done at first installation, if the services are run with persistent volumes.
If the volumes are removed from the docker-compose configuration, then each time the Kafka data will reset.

## Monitoring the Event Log

You can fire up one of the provided Kafka UI services provided in the Docker image to view the event log.
All events are processed by a single Task (by design) and are published to Topics that mirror the source tables.

* Kowl:  `http://localhost:8282/`
* Kafdrop:  `http://localhost:9191/`

## Additional Reading and Helpful Links and Tutorials

* [Kafka Presentations by Robin Moffatt](https://rmoff.net/2020/09/23/a-collection-of-kafka-related-talks/)
* [Debezium Documentation](https://debezium.io/documentation/reference/stable/index.html)
* [Debezium Tutorial](https://debezium.io/documentation/reference/stable/tutorial.html)
* [Ampath OpenMRS ELT project](https://github.com/kimaina/openmrs-elt/tree/master/cdc)
