# Image Spark pour le job de streaming bronze (Kafka -> Delta Lake sur MinIO).
# Ubuntu 22.04 => Python 3.10, Java 17.
FROM apache/spark:3.5.9-java17-python3

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# delta-spark (API Python) sans dépendances : pyspark est déjà fourni par l'image
RUN pip install --no-cache-dir prometheus-client==0.23.1 && \
    pip install --no-cache-dir --no-deps delta-spark==3.3.2

# JARs : connecteur Kafka, S3A (hadoop-aws 3.3.4 = version Hadoop de Spark 3.5), Delta Lake, JDBC Postgres
ENV SPARK_JARS_DIR=/opt/spark/jars
ARG MAVEN=https://repo1.maven.org/maven2
RUN set -eux; cd ${SPARK_JARS_DIR}; \
    for jar in \
      org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.9/spark-sql-kafka-0-10_2.12-3.5.9.jar \
      org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.9/spark-token-provider-kafka-0-10_2.12-3.5.9.jar \
      org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
      org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
      org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
      io/delta/delta-spark_2.12/3.3.2/delta-spark_2.12-3.3.2.jar \
      io/delta/delta-storage/3.3.2/delta-storage-3.3.2.jar \
      org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar; \
    do curl -fsSL -O "${MAVEN}/${jar}"; done

COPY conf/metrics.properties /opt/spark/conf/metrics.properties

RUN mkdir -p /opt/spark_checkpoints /opt/spark_jobs && chown -R spark:spark /opt/spark_checkpoints /opt/spark_jobs
WORKDIR /opt/spark_jobs
COPY bronze_stream_job.py lake_config.py ./

USER spark
