FROM cassandra:4.1

# Install PostgreSQL client for YugabyteDB schema init
# and download Kafka CLI tools for topic pre-creation
RUN apt-get update && \
    apt-get install -y --no-install-recommends postgresql-client curl && \
    rm -rf /var/lib/apt/lists/*

# Download and extract Kafka CLI tools (kafka-topics.sh)
ENV KAFKA_VERSION=3.7.1
ENV SCALA_VERSION=2.13
RUN curl -sL "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" \
    | tar -xz -C /opt && \
    ln -s /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka

ENV PATH="/opt/kafka/bin:${PATH}"

COPY init.sh /init.sh
COPY yugabyte.sql /schema/yugabyte.sql
COPY cassandra.cql /schema/cassandra.cql

RUN chmod +x /init.sh

ENTRYPOINT ["/init.sh"]
