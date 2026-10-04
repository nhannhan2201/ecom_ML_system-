#!/bin/bash
# ==============================================================================
# SCRIPT KHỞI CHẠY FEAST STREAM PUSHER DAEMON (RUBRIC 4.4 & 4.5)
# Dự án: E-Commerce Real-Time Purchase Propensity Prediction System
# ==============================================================================

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${PROJECT_ROOT}/logs"
mkdir -p "${LOG_DIR}"

PYTHON_BIN="/home/nhan/miniconda3/envs/learn_database/bin/python"
if [ ! -f "${PYTHON_BIN}" ]; then
    PYTHON_BIN="$(which python3)"
fi

export MINIO_ENDPOINT="http://localhost:9000"
export REDIS_HOST="localhost"
export REDIS_PORT="6379"
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"

PID_FILE="${LOG_DIR}/stream_pusher.pid"

case "$1" in
    start)
        if [ -f "${PID_FILE}" ] && kill -0 "$(cat "${PID_FILE}")" 2>/dev/null; then
            echo "⚠️  Feast Stream Pusher daemon đang chạy (PID: $(cat "${PID_FILE}"))"
            exit 0
        fi
        echo "🚀 Đang khởi động Feast Stream Pusher daemon (Dual-write Online Redis + Offline MinIO)..."
        nohup "${PYTHON_BIN}" "${PROJECT_ROOT}/feature_store/stream_push_job.py" --target both > "${LOG_DIR}/stream_pusher.log" 2>&1 &
        echo $! > "${PID_FILE}"
        echo "✅ Daemon đã khởi chạy thành công! PID: $(cat "${PID_FILE}")"
        echo "📋 Xem log tại: tail -f ${LOG_DIR}/stream_pusher.log"
        ;;
    stop)
        if [ -f "${PID_FILE}" ]; then
            PID="$(cat "${PID_FILE}")"
            echo "🛑 Đang dừng Feast Stream Pusher daemon (PID: ${PID})..."
            kill "${PID}" 2>/dev/null || true
            rm -f "${PID_FILE}"
            echo "🔒 Daemon đã dừng."
        else
            echo "⚠️  Không tìm thấy PID file. Daemon có thể chưa chạy."
        fi
        ;;
    status)
        if [ -f "${PID_FILE}" ] && kill -0 "$(cat "${PID_FILE}")" 2>/dev/null; then
            echo "🟢 Feast Stream Pusher daemon ĐANG CHẠY (PID: $(cat "${PID_FILE}"))"
        else
            echo "🔴 Feast Stream Pusher daemon ĐÃ DỪNG"
        fi
        ;;
    *)
        echo "Cách sử dụng: $0 {start|stop|status}"
        exit 1
        ;;
esac
