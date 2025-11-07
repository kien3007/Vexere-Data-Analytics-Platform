FROM trinodb/trino:448

# Copy core configs
COPY config.properties /etc/trino/config.properties
COPY jvm.config       /etc/trino/jvm.config
COPY node.properties  /etc/trino/node.properties

# Catalog: CHỈ dùng Iceberg, không Hive/Delta
COPY lakehouse.properties /etc/trino/catalog/lakehouse.properties
