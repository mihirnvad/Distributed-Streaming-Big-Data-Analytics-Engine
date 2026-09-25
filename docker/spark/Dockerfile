# Spark 4.1 runtime for the master, workers, streaming driver and batch jobs.
#
# Connector JARs (Kafka source, Delta Lake, Postgres JDBC) are resolved at build
# time and copied onto Spark's classpath, so containers start offline and every
# executor already has them - no Ivy resolution or JAR shipping at submit time.
ARG SPARK_IMAGE=apache/spark:4.1.3-scala2.13-java17-python3-ubuntu
FROM ${SPARK_IMAGE}

ARG SPARK_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3,io.delta:delta-spark_2.13:4.3.1,org.postgresql:postgresql:42.7.8"

USER root
ENV TZ=UTC \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/opt/app \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_BREAK_SYSTEM_PACKAGES=1

COPY docker/spark/requirements.txt /tmp/requirements.txt
# The .pth file makes the bundled PySpark importable from plain python3 (pytest,
# REPL), not only through spark-submit.
RUN python3 -m pip install -r /tmp/requirements.txt \
 && site="$(python3 -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')" \
 && printf '%s\n' "${SPARK_HOME}/python" "$(ls "${SPARK_HOME}"/python/lib/py4j-*-src.zip)" > "${site}/spark-bundled.pth"

# Resolve connector packages with Spark's own Ivy resolver, then add only the JARs
# Spark does not already ship (avoids duplicate/conflicting library versions).
COPY docker/spark/install-packages.sh /tmp/install-packages.sh
RUN bash /tmp/install-packages.sh "${SPARK_PACKAGES}" && rm /tmp/install-packages.sh

COPY config/spark-defaults.conf config/fairscheduler.xml config/log4j2.properties /opt/spark/conf/

# Shared lakehouse mount point (named volumes inherit this ownership on first use).
RUN mkdir -p /data/lakehouse /opt/spark/work-dir/worker /opt/app \
 && chown -R spark:spark /data /opt/spark/work-dir /opt/app

WORKDIR /opt/app
COPY --chown=spark:spark common ./common
COPY --chown=spark:spark streaming ./streaming
COPY --chown=spark:spark batch ./batch
# producer/ and dashboard/ are pure Python (no service deps) and are imported by the test-suite.
COPY --chown=spark:spark producer ./producer
COPY --chown=spark:spark dashboard ./dashboard
COPY --chown=spark:spark config ./config
COPY --chown=spark:spark tests ./tests
COPY --chown=spark:spark pyproject.toml ./

USER spark
