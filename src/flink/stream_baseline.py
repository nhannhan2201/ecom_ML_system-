"""
================================================================================
SRC/FLINK/STREAM_BASELINE.PY - FLINK STREAMING BASELINE (CHƯA TỐI ƯU)
Dự án: E-Commerce Real-Time Purchase Propensity Prediction System
Mục tiêu Rubric: Flink Baseline without optimization (2.0 điểm)
================================================================================

MỤC ĐÍCH THIẾT KẾ BASELINE:
Tạo ra một Streaming Pipeline ở trạng thái "ngây thơ" (chưa được tối ưu hóa)
nhằm làm bộc lộ rõ ràng 3 vấn đề streaming phổ biến trên Flink Web UI:

1. BỘC LỘ LỖI 1 - BURST TRAFFIC (LƯU LƯỢNG TĂNG ĐỘT BIẾN):
   - Cấu hình 'parallelism = 1' (chỉ sử dụng 1 luồng duy nhất).
   - Topic Kafka có 3 partitions, nhưng chỉ 1 task slot duy nhất gánh toàn bộ.
   - Khi Generator tăng tốc lên hàng nghìn msg/s, TaskManager sẽ bị nghẽn cổ chai:
     -> Quan sát trên Flink UI (Tab Backpressure) sẽ chuyển sang màu ĐỎ (HIGH).

2. BỘC LỘ LỖI 2 - LATE ARRIVAL (DỮ LIỆU ĐẾN TRỄ):
   - Đặt Watermark cực ngắn: 'INTERVAL 2 SECOND' và KHÔNG cho phép trễ (Allowed Lateness = 0).
   - Dữ liệu từ Generator có 5% sự kiện bị trễ 5 - 10 phút (do nghẽn mạng).
   - Flink sẽ thẳng tay vứt bỏ toàn bộ các sự kiện này:
     -> Quan sát trên Flink UI (Metric: 'numLateRecordsDropped') sẽ tăng mạnh (> 0).

3. BỘC LỘ LỖI 3 - STREAMING DUPLICATE (DỮ LIỆU BỊ TRÙNG LẶP):
   - Pipeline KHÔNG sử dụng cơ chế lọc trùng (không có State Deduplication).
   - 1.5% sự kiện bị gửi trùng từ Producer sẽ được Flink tính toán nhiều lần:
     -> Dẫn đến số đếm (COUNT) và tổng doanh thu (SUM) của Window bị thổi phồng sai lệch.

4. XỬ LÝ CỬA SỔ (WINDOW PROCESSING - RUBRIC: 2.0 ĐIỂM):
   - Sử dụng Tumbling Window 15 phút tính toán 4 Real-time Features [D, E] cho từng user_id:
     f_views_15m, f_carts_15m, f_purchases_15m, total_spend_15m.
   - Do không có Deduplication, các sự kiện gửi lặp (duplicate) sẽ bị tính dồn làm sai lệch features!
================================================================================
"""

import os
import sys
from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

# ------------------------------------------------------------------------------
# 1. CẤU HÌNH THÔNG SỐ BASELINE
# ------------------------------------------------------------------------------
# Tự động nhận diện chạy trong Cluster Docker hay chạy Local Terminal:
IS_CLUSTER = os.getenv("FLINK_RUN_MODE", "cluster").lower() == "cluster"

# Kafka bootstrap server:
# - Trong mạng Docker nội bộ Flink Cluster: ecom_kafka:29092
# - Chạy từ máy Host trực tiếp: localhost:9092
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "ecom_kafka:29092" if IS_CLUSTER else "localhost:9092")
KAFKA_TOPIC     = "ecommerce_stream_events"
GROUP_ID        = "flink_stream_baseline_group"

# Cấu hình Cửa sổ Window và Watermark
WINDOW_INTERVAL_MIN = int(os.getenv("FLINK_WINDOW_MIN", "15"))  # Tumbling Window 15 phút (Baseline)
WATERMARK_DELAY_SEC = 2   # [BASELINE] Watermark quá ngắn (chỉ đợi 2 giây -> drop toàn bộ Late Events)
BASELINE_PARALLELISM = 1  # [BASELINE] 1 luồng duy nhất (gây Backpressure khi có Burst)


def build_and_run_baseline_job():
    """Build and execute the baseline PyFlink stream processing job without optimizations.
    
    Demonstrates baseline architectural bottlenecks:
      1. Single thread (Parallelism=1) leading to severe backpressure under burst traffic.
      2. Naive 2-second watermark causing late-arriving events (5-10m) to be dropped.
      3. Absence of state deduplication leading to corrupted feature counts from duplicate events.
      4. Basic tumbling window (15m) aggregation printed to standard output.
    """
    print("=" * 70)
    print("🚀 ĐANG KHỞI CHẠY FLINK STREAMING BASELINE (CHƯA TỐI ƯU)...")
    print(f"📡 Chế độ chạy       : {'Docker Flink Cluster' if IS_CLUSTER else 'Local MiniCluster'}")
    print(f"🔗 Kafka Bootstrap   : {KAFKA_BOOTSTRAP}")
    print(f"📦 Kafka Topic       : {KAFKA_TOPIC}")
    print(f"⚙️  Parallelism       : {BASELINE_PARALLELISM} (Chưa mở rộng song song)")
    print(f"⏳ Watermark Delay   : {WATERMARK_DELAY_SEC} giây (Chưa xử lý Late Arrival)")
    print(f"⏱️  Tumbling Window  : {WINDOW_INTERVAL_MIN} phút (Tính toán Stream Features)")
    print("=" * 70)

    # 1. Khởi tạo Flink Configuration
    config = Configuration()
    if not IS_CLUSTER:
        # Nếu chạy local, tránh xung đột port 8081 của Docker
        config.set_string("rest.bind-port", "8088")

    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(BASELINE_PARALLELISM)

    # 2. Nạp Connector Kafka JAR (cho cả local lẫn cluster)
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    local_jar = os.path.join(project_root, "docker", "flink_jars", "flink-sql-connector-kafka-1.17.1.jar")
    container_jar = "/opt/flink/usrlib/flink-sql-connector-kafka-1.17.1.jar"

    if os.path.exists(container_jar):
        env.add_jars(f"file://{container_jar}")
    elif os.path.exists(local_jar):
        env.add_jars(f"file://{local_jar}")

    # 3. Khởi tạo Table Environment
    t_env = StreamTableEnvironment.create(env)

    # 4. Định nghĩa Bảng Source đọc từ Kafka (KAFKA SOURCE DDL)
    # - Cắt bỏ chuỗi ' UTC' để lấy Event-Time Timestamp
    # - [BASELINE FLAW]: Watermark gán INTERVAL '2' SECOND -> Sự kiện trễ > 2s sẽ bị DROP!
    source_ddl = f"""
        CREATE TABLE kafka_stream_events (
            event_time       STRING,
            event_type       STRING,
            product_id       BIGINT,
            category_id      BIGINT,
            category_code    STRING,
            brand            STRING,
            price            DOUBLE,
            user_id          BIGINT,
            user_session     STRING,
            discount_percent INT,
            row_time         AS TO_TIMESTAMP(SUBSTRING(event_time, 1, 19)),
            WATERMARK FOR row_time AS row_time - INTERVAL '{WATERMARK_DELAY_SEC}' SECOND
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = '{KAFKA_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'properties.group.id'          = '{GROUP_ID}',
            'scan.startup.mode'            = 'earliest-offset',
            'format'                       = 'json',
            'json.ignore-parse-errors'     = 'true'
        )
    """
    t_env.execute_sql(source_ddl)
    print("✅ [1/3] Đã tạo bảng Kafka Source DDL thành công.")

    # 5. Định nghĩa Bảng Sink xuất kết quả (PRINT SINK DDL)
    # Schema khớp 100% với Optimized để so sánh trực diện (Apples-to-Apples Comparison):
    # f_views_15m, f_carts_15m, f_purchases_15m, total_spend_15m
    sink_ddl = """
        CREATE TABLE baseline_features_sink (
            window_start        TIMESTAMP(3),
            window_end          TIMESTAMP(3),
            user_id             BIGINT,
            f_views_15m         BIGINT,
            f_carts_15m         BIGINT,
            f_purchases_15m     BIGINT,
            total_spend_15m     DOUBLE
        ) WITH (
            'connector' = 'print',
            'print-identifier' = '[BASELINE-STREAM-FEAT-15M]'
        )
    """
    t_env.execute_sql(sink_ddl)
    print("✅ [2/3] Đã tạo bảng Sink Table DDL thành công.")

    # 6. Truy vấn Streaming Window Features (CHƯA DEDUPLICATE)
    # - [BASELINE FLAW]: Tính trực tiếp từ kafka_stream_events không qua Deduplication View
    #   -> 1.5% - 5% sự kiện bị duplicate sẽ làm sai lệch, thổi phồng các feature f_views_15m, f_carts_15m, total_spend_15m!
    insert_sql = f"""
        INSERT INTO baseline_features_sink
        SELECT
            TUMBLE_START(row_time, INTERVAL '{WINDOW_INTERVAL_MIN}' MINUTE) AS window_start,
            TUMBLE_END(row_time,   INTERVAL '{WINDOW_INTERVAL_MIN}' MINUTE) AS window_end,
            user_id,
            COUNT(CASE WHEN event_type = 'view'     THEN 1 END) AS f_views_15m,
            COUNT(CASE WHEN event_type = 'cart'     THEN 1 END) AS f_carts_15m,
            COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS f_purchases_15m,
            ROUND(SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0.0 END), 2) AS total_spend_15m
        FROM kafka_stream_events
        GROUP BY
            user_id,
            TUMBLE(row_time, INTERVAL '{WINDOW_INTERVAL_MIN}' MINUTE)
    """

    print("⚡ [3/3] Đang submit truy vấn Streaming SQL vào Flink Engine...")
    result = t_env.execute_sql(insert_sql)
    
    try:
        job_client = result.get_job_client()
        if job_client:
            print(f"🎉 JOB BASELINE ĐÃ SUBMIT THÀNH CÔNG! Job ID: {job_client.get_job_id()}")
            print("👉 Mời bạn truy cập Flink Web UI để quan sát: http://localhost:8081/#/running-jobs")
    except Exception as e:
        print(f"Job đang thực thi (Status: {e})")


if __name__ == "__main__":
    build_and_run_baseline_job()
