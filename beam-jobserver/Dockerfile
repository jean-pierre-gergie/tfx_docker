FROM eclipse-temurin:11-jre

# -------- Versions (PINNED) --------
ARG BEAM_VERSION=2.59.0
ARG BEAM_JOBSERVER_ARTIFACT=beam-runners-spark-3-job-server
ARG BEAM_GROUP_PATH=org/apache/beam
ARG JOBSERVER_JAR_NAME=${BEAM_JOBSERVER_ARTIFACT}-${BEAM_VERSION}.jar
ARG JOBSERVER_JAR_URL=https://repo1.maven.org/maven2/${BEAM_GROUP_PATH}/${BEAM_JOBSERVER_ARTIFACT}/${BEAM_VERSION}/${JOBSERVER_JAR_NAME}
ARG JOBSERVER_SHA512_URL=${JOBSERVER_JAR_URL}.sha512

# -------- System deps --------
RUN set -eux; \
    apt-get update; \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      curl ca-certificates bash tini gosu; \
    rm -rf /var/lib/apt/lists/*

# -------- Runtime dirs / user --------
ENV BEAM_HOME=/opt/apache/beam
RUN set -eux; \
    mkdir -p "${BEAM_HOME}/jars" "${BEAM_HOME}/artifacts" "${BEAM_HOME}/logs"; \
    groupadd -g 186 beam || true; \
    useradd -u 186 -g 186 -m -s /bin/bash beam || true; \
    chown -R beam:beam "${BEAM_HOME}"
WORKDIR ${BEAM_HOME}

# -------- Fetch & verify job server jar (format checksum line) --------
RUN set -eux; \
    curl -fSL --retry 5 --retry-connrefused -o "jars/${JOBSERVER_JAR_NAME}" "${JOBSERVER_JAR_URL}"; \
    curl -fSL --retry 5 --retry-connrefused -o "jars/${JOBSERVER_JAR_NAME}.sha512" "${JOBSERVER_SHA512_URL}"; \
    sha="$(cat "jars/${JOBSERVER_JAR_NAME}.sha512")"; \
    echo "${sha}  ${JOBSERVER_JAR_NAME}" | (cd jars && sha512sum -c -); \
    rm "jars/${JOBSERVER_JAR_NAME}.sha512"

# -------- Ports --------
EXPOSE 8099 8098 8097

# -------- Defaults --------
ENV SPARK_MASTER_URL=spark://spark-master:7077
ENV JOB_PORT=8099 \
    ARTIFACT_PORT=8098 \
    EXPANSION_PORT=8097 \
    ARTIFACTS_DIR=/opt/apache/beam/artifacts \
    JAVA_TOOL_OPTIONS="-Xms256m -Xmx1024m"

# -------- Entrypoint --------
COPY --chown=beam:beam entrypoint.sh /entrypoint.sh
RUN sed -i 's/\r$//' /entrypoint.sh && chmod +x /entrypoint.sh

USER beam
ENTRYPOINT ["/usr/bin/tini","--","/entrypoint.sh"]
CMD ["run"]
