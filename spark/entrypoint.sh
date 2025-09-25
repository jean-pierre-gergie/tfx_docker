#!/usr/bin/env bash
set -euo pipefail

mode="${1:-master}"

echo "[spark entrypoint] Effective env:"
echo "  SPARK_MASTER_HOST=${SPARK_MASTER_HOST}"
echo "  SPARK_MASTER_PORT=${SPARK_MASTER_PORT}"
echo "  SPARK_WORKER_PORT=${SPARK_WORKER_PORT}"
echo "  DOCKER_HOST=${DOCKER_HOST-}"
echo "  DOCKER_NETWORK=${DOCKER_NETWORK-}"
echo "  DOCKER_MAC_CONTAINER=${DOCKER_MAC_CONTAINER-}"

if [[ "$mode" == "master" ]]; then
  exec "${SPARK_HOME}/bin/spark-class" \
    org.apache.spark.deploy.master.Master \
    --host "${SPARK_MASTER_HOST}" \
    --port "${SPARK_MASTER_PORT}" \
    --webui-port "${SPARK_MASTER_WEBUI_PORT}"
elif [[ "$mode" == "worker" ]]; then
  : "${SPARK_MASTER_URL:=spark://spark-master:7077}"
  exec "${SPARK_HOME}/bin/spark-class" \
    org.apache.spark.deploy.worker.Worker \
    --webui-port "${SPARK_WORKER_WEBUI_PORT}" \
    --port "${SPARK_WORKER_PORT}" \
    --cores "${SPARK_WORKER_CORES}" \
    --memory "${SPARK_WORKER_MEMORY}" \
    "${SPARK_MASTER_URL}"
else
  exec "$@"
fi
