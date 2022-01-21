FROM flink:1.14.3

# Copy sql-client script
COPY sql-client.sh /opt/sql-client/sql-client.sh
RUN mkdir -p /opt/sql-client/lib

# Download connector libraries
RUN wget -P /opt/sql-client/lib/ https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar; \
    wget -P /opt/sql-client/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-json/1.14.3/flink-json-1.14.3.jar; \
    wget -P /opt/sql-client/lib/ hhttps://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka_2.12/1.14.3/flink-connector-kafka_2.12-1.14.3.jar \
    wget -P /opt/sql-client/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka_2.12/1.14.3/flink-sql-connector-kafka_2.12-1.14.3.jar; \
    wget -P /opt/sql-client/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch7_2.12/1.14.3/flink-sql-connector-elasticsearch7_2.12-1.14.3.jar; \
    wget -P /opt/sql-client/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc_2.12/1.14.3/flink-connector-jdbc_2.12-1.14.3.jar;

# Copy configuration
COPY sql-client-conf.yaml /opt/flink/conf/sql-client-conf.yaml
COPY flink-conf.yaml /opt/flink/conf/flink-conf.yaml

WORKDIR /opt/sql-client
ENV SQL_CLIENT_HOME /opt/sql-client

COPY docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]