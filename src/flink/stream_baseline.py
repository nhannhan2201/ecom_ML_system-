"""Flink Streaming Processing Baseline (Unoptimized).

Runs stream processing without optimizations to demonstrate bottlenecks:
- Parallelism=1 creating backpressure under burst traffic.
- 2-second short watermark dropping 5-10m late-arriving events.
- Absence of deduplication causing inflated window feature counts.
- Tumbling window (15m) without state TTL.
"""

import os
from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

# ------------------------------------------------------------------------------
# 1. CAU HINH THONG SO BASELINE
# ------------------------------------------------------------------------------
IS_CLUSTER = os.getenv("FLINK_RUN_MODE", "cluster").lower() == "cluster"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "ecom_kafka:29092" if IS_CLUSTER else "localhost:9092")
KAFKA_TOPIC = "ecommerce_stream_events"
GROUP_ID = "flink_stream_baseline_group"

WINDOW_INTERVAL_MIN = int(os.getenv("FLINK_WINDOW_MIN", "15"))
WATERMARK_DELAY_SEC = 2
BASELINE_PARALLELISM = 1


def build_and_run_baseline_job():
    """Build and execute the baseline PyFlink stream processing job without optimizations."""
    print("=" * 70)
    print("KHOI CHAY FLINK STREAMING BASELINE (CHUA TOI UU)...")
    print(f"Che do chay       : {'Docker Flink Cluster' if IS_CLUSTER else 'Local MiniCluster'}")
    print(f"Kafka Bootstrap   : {KAFKA_BOOTSTRAP}")
    print(f"Kafka Topic       : {KAFKA_TOPIC}")
    print(f"Parallelism       : {BASELINE_PARALLELISM}")
    print(f"Watermark Delay   : {WATERMARK_DELAY_SEC}s")
    print(f"Tumbling Window   : {WINDOW_INTERVAL_MIN} phut")
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
    print("[OK] [1/3] Da tao bang Kafka Source DDL thanh cong.")

    # 5. Dinh nghia Bang Sink xuat ket qua (PRINT SINK DDL)
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
    print("[OK] [2/3] Da tao bang Sink Table DDL thanh cong.")

    # 6. Truy van Streaming Window Features
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

    print("[INFO] [3/3] Submitting streaming SQL query into Flink Engine...")
    result = t_env.execute_sql(insert_sql)

    try:
        job_client = result.get_job_client()
        if job_client:
            print(f"[OK] BASELINE JOB SUBMITTED SUCCESSFULLY! Job ID: {job_client.get_job_id()}")
            print("Flink Web UI: http://localhost:8081/#/running-jobs")
    except Exception as e:
        print(f"Job is running (Status: {e})")


if __name__ == "__main__":
    build_and_run_baseline_job()
