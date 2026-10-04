FROM flink:1.20.5

# Kafka SQL connector (matches Flink 1.20.x; connector version latches to the Flink minor)
RUN curl -fSL -o /opt/flink/lib/flink-sql-connector-kafka.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar
