#!/usr/bin/env bash
set -euo pipefail

CMD="${1:-run}"

JAR="${BEAM_HOME:-/opt/apache/beam}/jars/${JOBSERVER_JAR_NAME:-beam-runners-spark-3-job-server-2.59.0.jar}"

JAVA_BIN="${JAVA_BIN:-java}"

if [[ "$CMD" == "run" ]]; then
    exec ${JAVA_BIN} \
    -jar "${JAR}" \
    --job-host="0.0.0.0" \
    --job-port="${JOB_PORT:-8099}" \
    --artifact-port="${ARTIFACT_PORT:-8098}" \
    --expansion-port="${EXPANSION_PORT:-8097}" \
    --artifacts-dir="${ARTIFACTS_DIR:-/opt/apache/beam/artifacts}" \
    --spark-master-url="${SPARK_MASTER_URL:-spark://spark-master:7077}"
else
  exec "$@"
fi
