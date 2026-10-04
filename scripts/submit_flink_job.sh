#!/usr/bin/env bash
# ==============================================================================
# SUBMIT_FLINK_JOB.SH - Tiện ích nộp Flink Streaming Job lên Flink Docker Cluster
# ==============================================================================

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_EXEC="${PYTHON_BIN:-python3}"
PYFLINK_BIN="$("$PYTHON_EXEC" -c "import pyflink, os; print(os.path.join(os.path.dirname(pyflink.__file__), 'bin', 'flink'))" 2>/dev/null || which flink || echo "flink")"
JOBMANAGER_REST="${FLINK_JOBMANAGER_HOST:-localhost}:${FLINK_REST_PORT:-8081}"

ACTION="${1:-list}"

case "$ACTION" in
    "baseline")
        echo "🚀 Đang submit Flink Streaming Baseline Job lên Cluster ($JOBMANAGER_REST)..."
        "$PYFLINK_BIN" run -m "$JOBMANAGER_REST" \
            -pyclientexec "$PYTHON_EXEC" \
            -py "$PROJECT_ROOT/src/flink/stream_baseline.py"
        echo "✅ Submit hoàn tất! Xem dashboard tại: http://localhost:8081/#/running-jobs"
        ;;
    "optimized")
        echo "🚀 Đang submit Flink Streaming Optimized Job lên Cluster ($JOBMANAGER_REST)..."
        "$PYFLINK_BIN" run -m "$JOBMANAGER_REST" \
            -pyclientexec "$PYTHON_EXEC" \
            -py "$PROJECT_ROOT/src/flink/stream_optimized.py"
        echo "✅ Submit hoàn tất! Xem dashboard tại: http://localhost:8081/#/running-jobs"
        ;;
    "list")
        echo "📋 Danh sách các Jobs đang chạy trên Flink Cluster:"
        "$PYFLINK_BIN" list -m "$JOBMANAGER_REST"
        ;;
    "cancel")
        JOB_ID="$2"
        if [ -z "$JOB_ID" ]; then
            echo "❌ Lỗi: Vui lòng cung cấp Job ID cần hủy. Ví dụ: ./scripts/submit_flink_job.sh cancel <job_id>"
            exit 1
        fi
        echo "🛑 Đang hủy Job $JOB_ID..."
        "$PYFLINK_BIN" cancel -m "$JOBMANAGER_REST" "$JOB_ID"
        echo "✅ Đã hủy Job thành công."
        ;;
    *)
        echo "Sử dụng:"
        echo "  $0 baseline     # Submit Flink Baseline Job"
        echo "  $0 optimized    # Submit Flink Optimized Job"
        echo "  $0 list         # Xem danh sách jobs trên cluster"
        echo "  $0 cancel <id>  # Hủy một job theo ID"
        ;;
esac
