"""Flink Streaming Processing (Optimized).

Handles streaming data issues on Kafka ecommerce event streams:
- Parallelism=3 and Buffer Debloating to absorb burst traffic.
- 15-minute event-time watermark to tolerate late arrivals (5-10m).
- SQL deduplication via ROW_NUMBER() OVER (...) = 1.
- Hopping window (15m window, 1m slide) calculating 4 real-time features.
Outputs to MinIO staging and Kafka feature topics.
"""

import os
from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.table import StreamTableEnvironment


# ------------------------------------------------------------------------------
# 1. CAU HINH THONG SO TOI UU HOA (OPTIMIZED CONFIGURATION)
# ------------------------------------------------------------------------------
IS_CLUSTER = os.getenv("FLINK_RUN_MODE", "cluster").lower() == "cluster"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "ecom_kafka:29092" if IS_CLUSTER else "localhost:9092")
KAFKA_TOPIC = "ecommerce_stream_events"
GROUP_ID = "flink_stream_optimized_group"
OPTIMIZED_PARALLELISM = 3
WATERMARK_DELAY_MIN = 15
WINDOW_INTERVAL_MIN = int(os.getenv("FLINK_WINDOW_MIN", "15"))
STATE_TTL_HOURS = "1h"


def build_and_run_optimized_job():
    """Build and execute the optimized PyFlink stream processing job."""
    print("=" * 80)
    print("KHOI CHAY FLINK STREAMING OPTIMIZED...")
    print(f"Che do chay          : {'Docker Flink Cluster' if IS_CLUSTER else 'Local MiniCluster'}")
    print(f"Kafka Bootstrap      : {KAFKA_BOOTSTRAP}")
    print(f"Kafka Topic          : {KAFKA_TOPIC}")
    print(f"Parallelism          : {OPTIMIZED_PARALLELISM}")
    print(f"Watermark Delay      : {WATERMARK_DELAY_MIN} phut")
    print(f"Hopping Window       : {WINDOW_INTERVAL_MIN} phut (slide: 1 phut)")
    print(f"State Retention TTL  : {STATE_TTL_HOURS}")
    print("=" * 80)

    # --------------------------------------------------------------------------
    # 2. KHỞI TẠO FLINK CONFIGURATION & STATE BACKEND TỐI ƯU
    # --------------------------------------------------------------------------
    config = Configuration()
    if not IS_CLUSTER:
        config.set_string("rest.bind-port", "8089")

    # [OPTIMIZATION BURST 1]: Kích hoạt Buffer Debloating
    # Giảm kích thước bộ đệm mạng khi có nghẽn để đẩy nhanh tiến độ xử lý và giảm latency
    config.set_string("taskmanager.network.memory.buffer-debloat.enabled", "true")
    config.set_string("taskmanager.network.memory.buffer-debloat.target", "1000ms")

    # [OPTIMIZATION BURST 2]: Cấu hình State TTL trong Table Engine
    config.set_string("table.exec.state.ttl", STATE_TTL_HOURS)

    # [OPTIMIZATION IDLE TIMEOUT]: Ngăn chặn các subtask nhàn rỗi làm tắc nghẽn Watermark
    config.set_string("table.exec.source.idle-timeout", "5000 ms")

    # [OPTIMIZATION S3 MINIO]: Cấu hình kết nối MinIO S3 FileSystem
    minio_endpoint = os.environ.get("MINIO_ENDPOINT") or (
        "http://minio:9000" if IS_CLUSTER else "http://localhost:9000"
    )
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY") or os.environ.get("AWS_ACCESS_KEY_ID")
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY") or os.environ.get("AWS_SECRET_ACCESS_KEY")
    if not minio_access_key:
        raise ValueError("Missing required environment variable: 'MINIO_ACCESS_KEY' (or 'AWS_ACCESS_KEY_ID')")
    if not minio_secret_key:
        raise ValueError("Missing required environment variable: 'MINIO_SECRET_KEY' (or 'AWS_SECRET_ACCESS_KEY')")

    config.set_string("s3.endpoint", minio_endpoint)
    config.set_string("s3.path.style.access", "true")
    config.set_string("s3.access-key", minio_access_key)
    config.set_string("s3.secret-key", minio_secret_key)
    config.set_string("s3.ssl.enabled", "false")
    config.set_string("s3.region", "us-east-1")

    # [OPTIMIZATION BURST 3]: Cấu hình FileSystem Checkpoint Storage (tránh giới hạn 5MB của memory checkpoint)
    config.set_string("state.checkpoints.dir", "file:///tmp/flink/checkpoints")

    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(OPTIMIZED_PARALLELISM)

    # [OPTIMIZATION BURST 3]: Bật Checkpoint định kỳ 10 giây (Exactly-Once Semantics)
    env.enable_checkpointing(10000, CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_checkpoint_timeout(60000)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(5000)
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)

    # --------------------------------------------------------------------------
    # 3. NẠP KAFKA CONNECTOR JAR
    # --------------------------------------------------------------------------
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    local_jar = os.path.join(project_root, "docker", "flink_jars", "flink-sql-connector-kafka-1.17.1.jar")
    container_jar = "/opt/flink/usrlib/flink-sql-connector-kafka-1.17.1.jar"

    if os.path.exists(container_jar):
        env.add_jars(f"file://{container_jar}")
    elif os.path.exists(local_jar):
        env.add_jars(f"file://{local_jar}")

    # Khởi tạo Table Environment
    t_env = StreamTableEnvironment.create(env)

    # --------------------------------------------------------------------------
    # 4. ĐỊNH NGHĨA KAFKA SOURCE VỚI WATERMARK CHUẨN 15 PHÚT (LATE ARRIVAL FIX)
    # --------------------------------------------------------------------------
    # - Schema đầy đủ 10 cột, kế thừa Schema Evolution (cột discount_percent)
    # - row_time: Event-Time trích xuất từ 19 ký tự đầu của chuỗi UTC
    # - WATERMARK: Chờ 15 phút đảm bảo không bỏ rơi bất kỳ event nào bị delay 5 - 10 phút
    # - scan.watermark.idle-timeout: Bỏ qua subtask rảnh rỗi để Watermark liên tục tiến lên
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
            WATERMARK FOR row_time AS row_time - INTERVAL '{WATERMARK_DELAY_MIN}' MINUTE
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
    print("[OK] [1/4] Da tao bang Kafka Source DDL voi Watermark 15 phut thanh cong.")

    # --------------------------------------------------------------------------
    # 5. DINH NGHIA VIEW KHU TRUNG LAP
    # --------------------------------------------------------------------------
    dedup_view_sql = """
        CREATE TEMPORARY VIEW deduped_stream_events AS
        SELECT 
            event_time,
            event_type,
            product_id,
            category_id,
            category_code,
            brand,
            price,
            user_id,
            user_session,
            discount_percent,
            row_time
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY user_id, event_time, product_id, event_type
                    ORDER BY row_time ASC
                ) as row_num
            FROM kafka_stream_events
        )
        WHERE row_num = 1
    """
    t_env.execute_sql(dedup_view_sql)
    print("[OK] [2/4] Da thiet lap View Deduplication thanh cong.")

    # --------------------------------------------------------------------------
    # 6. DINH NGHIA CAC SINK XUAT DU LIEU STREAM
    # --------------------------------------------------------------------------
    # Sink 1: In ra Console de theo doi Realtime Metrics
    print_sink_ddl = """
        CREATE TABLE optimized_features_sink (
            window_start        TIMESTAMP(3),
            window_end          TIMESTAMP(3),
            user_id             BIGINT,
            f_views_15m         BIGINT,
            f_carts_15m         BIGINT,
            f_purchases_15m     BIGINT,
            total_spend_15m     DOUBLE
        ) WITH (
            'connector' = 'print',
            'print-identifier' = '[OPTIMIZED-STREAM-FEAT-15M]'
        )
    """
    t_env.execute_sql(print_sink_ddl)

    # Sink 2: Luu Raw Stream Events vao MinIO Staging (Append-only)
    minio_staging_sink_ddl = """
        CREATE TABLE minio_stream_staging_sink (
            event_time          STRING,
            event_type          STRING,
            product_id          BIGINT,
            category_id         BIGINT,
            category_code       STRING,
            brand               STRING,
            price               DOUBLE,
            user_id             BIGINT,
            user_session        STRING,
            discount_percent    INT
        ) WITH (
            'connector' = 'filesystem',
            'path'      = 's3://ecommerce-raw/staging/stream_events/',
            'format'    = 'json',
            'sink.rolling-policy.rollover-interval' = '1 min',
            'sink.rolling-policy.check-interval' = '10 s'
        )
    """
    t_env.execute_sql(minio_staging_sink_ddl)

    # Sink 3: Day 15m Stream Features vao Kafka Topic phuc vu Feast
    KAFKA_FEATURES_TOPIC = "ecommerce_stream_features_15m"
    kafka_feat_sink_ddl = f"""
        CREATE TABLE kafka_stream_features_sink (
            user_id             BIGINT,
            f_views_15m         BIGINT,
            f_carts_15m         BIGINT,
            f_purchases_15m     BIGINT,
            total_spend_15m     DOUBLE,
            event_timestamp     TIMESTAMP(3),
            created             TIMESTAMP(3)
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = '{KAFKA_FEATURES_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'format'                       = 'json'
        )
    """
    t_env.execute_sql(kafka_feat_sink_ddl)
    print("[OK] [3/4] Da tao cac bang Sink xuat Kafka va MinIO Lakehouse thanh cong.")

    # --------------------------------------------------------------------------
    # 7. THỰC THI STATEMENT SET (XUẤT ĐỒNG THỜI VÀO CÁC SINK TRONG 1 JOB DUY NHẤT)
    # --------------------------------------------------------------------------
    # Sử dụng StatementSet giúp Flink chỉ đọc Kafka 1 lần và xử lý song song các nhánh:
    statement_set = t_env.create_statement_set()

    # Nhánh 1: Ghi các sự kiện stream vào Staging (Append-Only) để Spark gộp vào Bronze Delta Lake
    statement_set.add_insert_sql("""
        INSERT INTO minio_stream_staging_sink
        SELECT
            event_time, event_type, product_id, category_id,
            category_code, brand, price, user_id, user_session, discount_percent
        FROM kafka_stream_events
    """)

    # Nhánh 2a: Tính toán 4 Stream Features (15m window trượt mỗi 1 phút) - In ra Console theo dõi metrics
    statement_set.add_insert_sql("""
        INSERT INTO optimized_features_sink
        SELECT
            HOP_START(row_time, INTERVAL '1' MINUTE, INTERVAL '15' MINUTE) AS window_start,
            HOP_END(row_time,   INTERVAL '1' MINUTE, INTERVAL '15' MINUTE) AS window_end,
            user_id,
            COUNT(CASE WHEN event_type = 'view'     THEN 1 END) AS f_views_15m,
            COUNT(CASE WHEN event_type = 'cart'     THEN 1 END) AS f_carts_15m,
            COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS f_purchases_15m,
            ROUND(SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0.0 END), 2) AS total_spend_15m
        FROM deduped_stream_events
        GROUP BY
            user_id,
            HOP(row_time, INTERVAL '1' MINUTE, INTERVAL '15' MINUTE)
    """)

    # Nhánh 2b: Đẩy Stream Features trực tiếp vào Kafka Topic 'ecommerce_stream_features_15m' để Feast Pusher xử lý
    statement_set.add_insert_sql("""
        INSERT INTO kafka_stream_features_sink
        SELECT
            user_id,
            COUNT(CASE WHEN event_type = 'view'     THEN 1 END) AS f_views_15m,
            COUNT(CASE WHEN event_type = 'cart'     THEN 1 END) AS f_carts_15m,
            COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS f_purchases_15m,
            ROUND(SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0.0 END), 2) AS total_spend_15m,
            HOP_END(row_time, INTERVAL '1' MINUTE, INTERVAL '15' MINUTE) AS event_timestamp,
            CURRENT_TIMESTAMP AS created
        FROM deduped_stream_events
        GROUP BY
            user_id,
            HOP(row_time, INTERVAL '1' MINUTE, INTERVAL '15' MINUTE)
    """)

    print("[INFO] [4/4] Submitting StatementSet Streaming Job into Flink Cluster...")
    result = statement_set.execute()

    try:
        job_client = result.get_job_client()
        if job_client:
            job_id = job_client.get_job_id()
            print("=" * 80)
            print("[OK] FLINK OPTIMIZED JOB SUBMITTED SUCCESSFULLY!")
            print(f"Job ID: {job_id}")
            print(f"Flink Web Dashboard: http://localhost:8081/#/job/{job_id}")
            print("=" * 80)
    except Exception as e:
        print(f"Job is running (Status info: {e})")


if __name__ == "__main__":
    build_and_run_optimized_job()
