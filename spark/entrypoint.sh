#!/usr/bin/env bash
set -euo pipefail

mode="${1:-master}"

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
