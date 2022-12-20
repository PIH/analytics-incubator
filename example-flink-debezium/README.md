# Flink Debezium Example

This project exists to experiment with using the Ververica Flink Connector for MySQL CDC.  This is built on top 
of Debezium and provides a layer on top of it that exposes the MySQL debezium change events as a Flink data stream.
This demonstrates how to get this set up and how Flink data stream processing can be used to ETL.

## Dependencies 

* MySQL Source database.  See [mysql](../docker/mysql)

## Usage

* Start up the MySQL docker container
* Modify resources/mysql.properties as needed, and hten run the DebeziumFlinkConnector.java class

