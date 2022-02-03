# Flink Debezium Example

This project exists to experiment with embedding the Debezium libraries for direct CDC event processing with Flink

## Dependencies 

* MySQL Source database.  See [mysql](../../mysql)

## Usage

**Start up Services**

First ensure that a shared network is available for all docker-compose projects.

```shell
docker network create analytics-incubator-network
```

Then, follow the steps in the relevant README files to bring up the various services defined in the dependences
