# Debezium Example

This project exists to experiment with embedding the Debezium libraries for direct CDC event processing.
This project does not use the Ververica MySQL connector or integrate with Flink for stream processing, but
demonstrates how to read from the MySQL binlog with the Debezium API and process each change event.

## Dependencies 

* MySQL Source database with row-level bin logging enabled.  See [mysql](../docker/mysql)

## Usage

* Start up the MySQL docker container
* Modify and execute the DebeziumStreamTest
