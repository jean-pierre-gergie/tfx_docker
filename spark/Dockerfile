
FROM eclipse-temurin:11-jre

# ---- Pinned versions  ----
ARG SPARK_VERSION=3.2.2
ARG HADOOP_VERSION=3.2
ARG SCALA_VERSION=2.12
ARG SPARK_ARCHIVE="spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}"
ARG SPARK_TGZ_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_ARCHIVE}.tgz"
# Official SHA-512 for spark-3.2.2-bin-hadoop3.2.tgz
ARG SPARK_SHA512="a7bdcaf598e9bcf78d7cbd2b8ea08d4363c45a4b0cda0940e168ef7d592459df1dde0c33143049d58b61af15c83e2ea2a93bcf6ec63df46b693a36c978d57182"

# ---- System deps ----
USER root
RUN set -eux; \
    apt-get update; \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      curl ca-certificates tini bash procps gosu; \
    rm -rf /var/lib/apt/lists/*

# ---- Download + verify Spark (checksum fix) ----
WORKDIR /opt
RUN set -eux; \
    curl -fSL --retry 5 --retry-connrefused -o spark.tgz "${SPARK_TGZ_URL}"; \
    echo "${SPARK_SHA512}  spark.tgz" | sha512sum -c -; \
    tar -xzf spark.tgz; \
    rm spark.tgz; \
    ln -s "/opt/${SPARK_ARCHIVE}" /opt/spark

# ---- Create user and dirs ----
RUN set -eux; \
    groupadd -g 185 spark; \
    useradd -u 185 -g spark -m -s /bin/bash spark; \
    # create real dirs on the target, not only via symlink
    mkdir -p "/opt/${SPARK_ARCHIVE}/work" "/opt/${SPARK_ARCHIVE}/logs" /data; \
    # own both the real tree AND the symlink
    chown -R spark:spark "/opt/${SPARK_ARCHIVE}" /opt/spark /data

# ---- Env ----
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
ENV SPARK_NO_DAEMONIZE=true
# Standalone defaults
ENV SPARK_MASTER_HOST=spark-master \
    SPARK_MASTER_PORT=7077 \
    SPARK_MASTER_WEBUI_PORT=8080 \
    SPARK_WORKER_WEBUI_PORT=8081 \
    SPARK_WORKER_PORT=7078 \
    SPARK_WORKER_CORES=2 \
    SPARK_WORKER_MEMORY=2g \
    SPARK_WORKER_DIR=/opt/spark/work 

# ---- Minimal spark-defaults.conf ----
RUN set -eux; \
    mkdir -p "${SPARK_HOME}/conf"; \
    cat > "${SPARK_HOME}/conf/spark-defaults.conf" <<'EOF'
spark.ui.reverseProxy              true
spark.eventLog.enabled             true
spark.eventLog.dir                 file:/opt/spark/logs
spark.history.fs.logDirectory      file:/opt/spark/logs
spark.worker.cleanup.enabled       true
spark.worker.cleanup.appDataTtl    604800
EOF
RUN chown -R spark:spark "${SPARK_HOME}/conf"

# ---- Healthcheck ----
HEALTHCHECK --interval=30s --timeout=5s --retries=10 CMD \
  pgrep -f "org.apache.spark.deploy" >/dev/null || exit 1

# ---- Entrypoint wrapper ----
COPY --chown=spark:spark entrypoint.sh /entrypoint.sh
RUN sed -i 's/\r$//' /entrypoint.sh && chmod +x /entrypoint.sh

USER spark
WORKDIR /home/spark

ENTRYPOINT ["/usr/bin/tini", "--", "/entrypoint.sh"]
# Default to master; use CMD ["worker"] to run a worker
CMD ["master"]
