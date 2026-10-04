#!/usr/bin/env bash
# ==============================================================================
# DOWNLOAD APACHE FLINK DEPENDENCY JARS
# Purpose: Fetch Kafka SQL connector and S3 filesystem plugin for Flink 1.17.1
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

KAFKA_JAR_DIR="${REPO_ROOT}/docker/flink_jars"
S3_PLUGIN_DIR="${REPO_ROOT}/docker/flink_plugins/s3-fs-hadoop"

mkdir -p "${KAFKA_JAR_DIR}" "${S3_PLUGIN_DIR}"

KAFKA_URL="https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.1/flink-sql-connector-kafka-1.17.1.jar"
S3_URL="https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.17.1/flink-s3-fs-hadoop-1.17.1.jar"

KAFKA_TARGET="${KAFKA_JAR_DIR}/flink-sql-connector-kafka-1.17.1.jar"
S3_TARGET="${S3_PLUGIN_DIR}/flink-s3-fs-hadoop-1.17.1.jar"

echo "[1/2] Checking Kafka SQL Connector JAR..."
if [ -f "${KAFKA_TARGET}" ]; then
    echo "  -> Found existing ${KAFKA_TARGET}, skipping download."
else
    echo "  -> Downloading from ${KAFKA_URL}..."
    curl -fSL "${KAFKA_URL}" -o "${KAFKA_TARGET}"
    echo "  -> Download complete: $(ls -lh "${KAFKA_TARGET}")"
fi

echo "[2/2] Checking Flink S3 FS Hadoop Plugin JAR..."
if [ -f "${S3_TARGET}" ]; then
    echo "  -> Found existing ${S3_TARGET}, skipping download."
else
    echo "  -> Downloading from ${S3_URL}..."
    curl -fSL "${S3_URL}" -o "${S3_TARGET}"
    echo "  -> Download complete: $(ls -lh "${S3_TARGET}")"
fi

echo "All required Flink JARs are verified."
