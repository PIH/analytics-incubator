# Flink Jobs

## Overview

Apache Flink is a framework and engine for fast and efficient distributed processing of data streams.  It is commonly employed to implement ETL and unified batch and streaming data pipelines off of real-time event streams.  The intention of these jobs is to connect the Kafka [event log](../event-log) source that is populated with real-time CDC events from Debezium, transform this data into analytics-friendly structures, and stream the resulting data into analytic data repositories that are fast, efficient, and intuitive to query.

### Pre-requisites

This connects with the Kafka-based [event-log](../event-log), and so this must be up-and-running to test out the various jobs in this section.

### Execute jobs

Open any of the files, and execute the job in Intellij.