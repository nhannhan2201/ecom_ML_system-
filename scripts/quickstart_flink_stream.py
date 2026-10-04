"""
==============================================================================
QUICKSTART_FLINK_STREAM.PY - FLINK STREAMING QUICKSTART SCRIPT
Dự án: E-Commerce Real-Time Purchase Propensity Prediction System
Mô phỏng theo kiến trúc Lab 10: l10_transformation_layer_2 của giảng viên.
==============================================================================

MỤC TIÊU:
1. Kết nối vào Apache Kafka topic 'ecommerce_stream_events'.
2. Trích xuất các trường dữ liệu: event_time, event_type, user_id, product_id, price.
3. Thiết lập Event-Time Watermark (chờ tối đa 5 giây cho dữ liệu bị trễ).
4. Gom nhóm bằng Tumbling Window 1 phút.
5. Tính tổng số lượt view, cart, purchase trong từng phút và in ra màn hình.
==============================================================================
"""

import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

# Cấu hình Kafka Broker và Topic
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC  = "ecommerce_stream_events"
GROUP_ID     = "flink_quickstart_group_v2"
WINDOW_MIN   = 1  # Cửa sổ trượt 1 phút

def run_quickstart():
    print("=" * 60)
    print("🚀 ĐANG KHỞI ĐỘNG FLINK STREAMING QUICKSTART JOB...")
    print(f"📡 Kafka Broker : {KAFKA_BROKER}")
    print(f"📦 Kafka Topic  : {KAFKA_TOPIC}")
    print(f"⏱️  Tumbling Window: {WINDOW_MIN} phút")
    print("=" * 60)

    # 1. Khởi tạo môi trường Flink Execution Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # 2. Nạp Connector Kafka JAR (bắt buộc để Flink đọc được Kafka)
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    jar_path = os.path.join(project_root, "docker", "flink_jars", "flink-sql-connector-kafka-1.17.1.jar")
    print(f"📦 Nạp Kafka Connector JAR: {jar_path}")
    env.add_jars(f"file://{jar_path}")

    # 3. Khởi tạo Table Environment (Flink SQL Engine)
    t_env = StreamTableEnvironment.create(env)

    # 4. Định nghĩa bảng Source đọc từ Kafka
    # Lưu ý chuẩn Lab 10:
    # Cột 'event_time' trong JSON có đuôi ' UTC' (ví dụ: '2019-10-26 00:00:01 UTC').
    # Ta dùng SUBSTRING(event_time, 1, 19) để cắt lấy '2019-10-26 00:00:01'
    # và chuyển sang kiểu TIMESTAMP chuẩn làm Event-Time gán Watermark!
    create_source_table_sql = f"""
        CREATE TABLE ecommerce_events_source (
            event_time      STRING,
            event_type      STRING,
            product_id      INT,
            category_id     BIGINT,
            brand           STRING,
            price           DOUBLE,
            user_id         INT,
            user_session    STRING,
            row_time        AS TO_TIMESTAMP(SUBSTRING(event_time, 1, 19)),
            WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = '{KAFKA_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BROKER}',
            'properties.group.id'          = '{GROUP_ID}',
            'scan.startup.mode'            = 'earliest-offset',
            'format'                       = 'json'
        )
    """
    t_env.execute_sql(create_source_table_sql)
    print("✅ Đã tạo bảng Kafka Source Table thành công.")

    # 5. Viết câu truy vấn Streaming Window Aggregation
    # Gom nhóm theo từng khung 1 phút (TUMBLE) và theo loại sự kiện (view/cart/purchase)
    query_sql = f"""
        SELECT
            TUMBLE_START(row_time, INTERVAL '{WINDOW_MIN}' MINUTE) AS window_start,
            TUMBLE_END(row_time,   INTERVAL '{WINDOW_MIN}' MINUTE) AS window_end,
            event_type,
            COUNT(*)                                                AS event_count,
            COUNT(DISTINCT user_id)                                 AS unique_users,
            ROUND(SUM(price), 2)                                    AS total_value
        FROM ecommerce_events_source
        GROUP BY
            event_type,
            TUMBLE(row_time, INTERVAL '{WINDOW_MIN}' MINUTE)
    """

    print("⚡ Bắt đầu thực thi câu truy vấn Streaming SQL và in kết quả ra màn hình...")
    result_table = t_env.sql_query(query_sql)

    # In kết quả realtime trực tiếp ra Terminal
    result_table.execute().print()

if __name__ == "__main__":
    run_quickstart()
