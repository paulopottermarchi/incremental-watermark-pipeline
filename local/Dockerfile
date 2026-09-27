# local/Dockerfile
# ===========================================================================
# Runtime for the local demo: Python, a JVM, Spark, Delta, dbt-spark and the
# SQL Server JDBC driver.
#
# The driver jar is baked in rather than resolved at runtime so a clone with
# no Maven cache still runs, and the Spark/Delta jars are warmed during the
# build for the same reason — the first `make demo` should not spend two
# minutes silently downloading before it prints anything.
# ===========================================================================

FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        procps \
        make \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# SQL Server JDBC driver
ARG MSSQL_JDBC_VERSION=12.6.1
RUN mkdir -p /opt/jars && \
    curl -fsSL -o /opt/jars/mssql-jdbc.jar \
      "https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/${MSSQL_JDBC_VERSION}.jre11/mssql-jdbc-${MSSQL_JDBC_VERSION}.jre11.jar"
ENV MSSQL_JDBC_JAR=/opt/jars/mssql-jdbc.jar

COPY local/requirements-local.txt /tmp/requirements-local.txt
RUN pip install --no-cache-dir -r /tmp/requirements-local.txt

# Warm the Ivy cache so Delta's jars are already present at first run.
RUN python -c "\
from delta import configure_spark_with_delta_pip; \
from pyspark.sql import SparkSession; \
b = SparkSession.builder.appName('warm').master('local[1]') \
     .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension') \
     .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog'); \
s = configure_spark_with_delta_pip(b).getOrCreate(); s.stop()"

WORKDIR /workspace
ENV PYTHONPATH=/workspace \
    DBT_PROFILES_DIR=/workspace/local

CMD ["bash"]
