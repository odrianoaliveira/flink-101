FROM flink:2.2.1

# Kafka SQL connector (matches Flink 2.2.x; connector version latches to the Flink minor).
# Includes both the regular ('kafka') and upsert ('upsert-kafka') connectors.
RUN curl -fSL -o /opt/flink/lib/flink-sql-connector-kafka.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar
