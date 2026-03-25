FROM cassandra:4.1

# Install PostgreSQL client for YugabyteDB schema init
# Install kubectl for Strimzi KafkaTopic CRD application
RUN apt-get update && \
    apt-get install -y --no-install-recommends postgresql-client curl ca-certificates && \
    curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/$(dpkg --print-architecture)/kubectl" && \
    install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl && \
    rm kubectl && \
    rm -rf /var/lib/apt/lists/*

COPY init.sh /init.sh
COPY yugabyte.sql /schema/yugabyte.sql
COPY cassandra.cql /schema/cassandra.cql
COPY kafka-topics.yaml /schema/kafka-topics.yaml

RUN chmod +x /init.sh

ENTRYPOINT ["/init.sh"]
