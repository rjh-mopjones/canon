FROM cassandra:4.1

# Install PostgreSQL client for YugabyteDB schema init
RUN apt-get update && \
    apt-get install -y --no-install-recommends postgresql-client && \
    rm -rf /var/lib/apt/lists/*

COPY init.sh /init.sh
COPY yugabyte.sql /schema/yugabyte.sql
COPY cassandra.cql /schema/cassandra.cql

RUN chmod +x /init.sh

ENTRYPOINT ["/init.sh"]
