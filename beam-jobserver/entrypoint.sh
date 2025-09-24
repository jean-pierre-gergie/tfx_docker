#!/usr/bin/env bash
# beam-jobserver/entrypoint.sh
set -euo pipefail

log() { printf '%(%Y-%m-%d %H:%M:%S)T | %s\n' -1 "$*"; }

# ---- Config (env with sane defaults) ----
: "${SPARK_MASTER_URL:=spark://spark-master:7077}"

: "${JOB_HOST:=0.0.0.0}"   # IMPORTANT: set to "beam-jobserver" via compose
: "${JOB_PORT:=8099}"
: "${ARTIFACT_PORT:=8098}"
: "${EXPANSION_PORT:=8097}"

: "${ARTIFACTS_DIR:=/opt/apache/beam/artifacts}"
: "${BEAM_HOME:=/opt/apache/beam}"
: "${JAVA_TOOL_OPTIONS:=-Xms256m -Xmx1024m}"

# Optional: extra args you want to pass verbatim to the driver
: "${JOB_EXTRA_ARGS:=}"

# ---- Pre-flight ----
log "Picked up JAVA_TOOL_OPTIONS: ${JAVA_TOOL_OPTIONS}"
log "SPARK_MASTER_URL=${SPARK_MASTER_URL}"
log "JOB_HOST=${JOB_HOST}  JOB_PORT=${JOB_PORT}  ARTIFACT_PORT=${ARTIFACT_PORT}  EXPANSION_PORT=${EXPANSION_PORT}"
log "ARTIFACTS_DIR=${ARTIFACTS_DIR}"

mkdir -p "${ARTIFACTS_DIR}" "${BEAM_HOME}/logs" || true

# Ensure the jobserver jar is present
if ! compgen -G "${BEAM_HOME}/jars/beam-runners-spark-3-job-server-*.jar" > /dev/null; then
  echo "FATAL: beam-runners-spark-3-job-server-*.jar not found under ${BEAM_HOME}/jars" >&2
  ls -l "${BEAM_HOME}/jars" || true
  exit 1
fi

# ---- Build classpath ----
CLASSPATH="${BEAM_HOME}/jars/*"

# ---- Run ----
log "Starting SparkJobServerDriver..."
exec java -cp "${CLASSPATH}" \
  org.apache.beam.runners.spark.SparkJobServerDriver \
  --spark-master-url "${SPARK_MASTER_URL}" \
  --job-host "${JOB_HOST}" \
  --job-port "${JOB_PORT}" \
  --artifact-port "${ARTIFACT_PORT}" \
  --expansion-port "${EXPANSION_PORT}" \
  --artifacts-dir "${ARTIFACTS_DIR}" \
  ${JOB_EXTRA_ARGS}
