# Flink SQL API Example

This project exists to experiment with aspects of the Flink SQL API to support streaming of dynamic tables

## Dependencies 

* MySQL Source database.  See [mysql](../../mysql)
* Kafka/Debezium.  See [event-log](../../event-log)
* Elastic Search.  See [elasticsearch](../../elasticsearch)
* (optional) Trino to visualize results in Elastic Search.  See [trino](../../trino)

## Usage

**Start up Services**

First ensure that a shared network is available for all docker-compose projects.

```shell
docker network create analytics-incubator-network
```

Then, follow the steps in the relevant README files to bring up the various services defined in the dependences

**Execute in Intellij**

Open up [SqlApiJob](src/main/java/org/pih/analytics/flink/SqlApiJob.java) and run this in Intellij

**View Results**

You should:

* See output in the log that contains the number of patients in the dynamic zl.patient table
* View the "patient_index" table in Elastic Search and see all patients in the system, including ZL EMR ID and preferred name
* Make a change to the database (edit an existing person/patient/name/identifier) and see this reflected automatically in:
  * The number of patients in the console
  * The data in the ElasticSearch database
  