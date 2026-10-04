"""Feast Real-time Stream Pusher Job.

Consumes real-time 15m feature vectors from Kafka topic 'ecommerce_stream_features_15m'
and pushes to Feast Online Store (Redis) and Offline Store (MinIO Parquet).
"""

import os
import time
import json
import signal
import argparse
from datetime import datetime, timezone
import pandas as pd
from feast import FeatureStore
from feast.data_source import PushMode

# Thiết lập endpoint MinIO & Redis
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
if "ecom_minio" in MINIO_ENDPOINT:
    MINIO_ENDPOINT = MINIO_ENDPOINT.replace("ecom_minio", "minio")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")
os.environ.setdefault("REDIS_CONNECTION_STRING", f"{REDIS_HOST}:{REDIS_PORT}")

access_key = os.getenv("MINIO_ACCESS_KEY") or os.getenv("AWS_ACCESS_KEY_ID")
secret_key = os.getenv("MINIO_SECRET_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY")
if not access_key:
    raise ValueError("Missing required environment variable: 'MINIO_ACCESS_KEY' (or 'AWS_ACCESS_KEY_ID')")
if not secret_key:
    raise ValueError("Missing required environment variable: 'MINIO_SECRET_KEY' (or 'AWS_SECRET_ACCESS_KEY')")

os.environ["AWS_ACCESS_KEY_ID"] = access_key
os.environ["AWS_SECRET_ACCESS_KEY"] = secret_key
os.environ["AWS_ENDPOINT_URL"] = MINIO_ENDPOINT
os.environ["FEAST_S3_ENDPOINT_URL"] = MINIO_ENDPOINT
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["S3_ENDPOINT_URL"] = MINIO_ENDPOINT

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
FEATURES_TOPIC = "ecommerce_stream_features_15m"
PUSH_SOURCE_NAME = "user_stream_push_source"

RUNNING = True


def signal_handler(sig, frame):
    """Handle termination signals (SIGINT, SIGTERM) to stop consumer loop gracefully."""
    global RUNNING
    print("\n[INFO] Termination signal received. Shutting down gracefully...")
    RUNNING = False


def ensure_kafka_topic(topic_name: str, servers: str = BOOTSTRAP_SERVERS):
    """Tự động kiểm tra và tạo Kafka Topic nếu chưa có."""
    try:
        from confluent_kafka.admin import AdminClient, NewTopic

        admin_client = AdminClient({"bootstrap.servers": servers})
        metadata = admin_client.list_topics(timeout=5)
        if topic_name not in metadata.topics:
            print(f"[*] Topic '{topic_name}' does not exist. Creating...")
            new_topic = NewTopic(topic_name, num_partitions=3, replication_factor=1)
            fs = admin_client.create_topics([new_topic])
            for t, f in fs.items():
                f.result()
            print(f"[OK] Created Kafka Topic: '{topic_name}'")
    except Exception as e:
        print(f"[WARN] Kafka topic check: {e}")


def create_sample_seed_dataframe(n: int = 5) -> pd.DataFrame:
    """Tạo tập dữ liệu streaming mẫu với đầy đủ 4 features để kiểm thử nhanh tính năng Push (Seed Mode)."""
    now = datetime.now(timezone.utc)
    sample_users = [489492092, 512364693, 512378423, 512383224, 512436165]
    records = []
    for i in range(min(n, len(sample_users))):
        records.append(
            {
                "user_id": int(sample_users[i]),
                "f_views_15m": int((i + 1) * 3),
                "f_carts_15m": int(i + 1),
                "f_purchases_15m": int(i % 2),
                "total_spend_15m": float((i + 1) * 25.5),
                "event_timestamp": now,
                "created": now,
            }
        )
    df = pd.DataFrame(records)
    df["user_id"] = df["user_id"].astype("int64")
    df["f_views_15m"] = df["f_views_15m"].astype("int64")
    df["f_carts_15m"] = df["f_carts_15m"].astype("int64")
    df["f_purchases_15m"] = df["f_purchases_15m"].astype("int64")
    df["total_spend_15m"] = df["total_spend_15m"].astype("float64")
    return df


def ensure_offline_parquet_exists():
    """Đảm bảo thư mục s3://ecommerce-lakehouse/gold/feat_user_stream/ có file Parquet ban đầu với đầy đủ 4 features."""
    try:
        from pyarrow import fs as pafs
        import pyarrow as pa
        import pyarrow.parquet as pq

        endpoint_host = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
        minio_fs = pafs.S3FileSystem(
            access_key=access_key,
            secret_key=secret_key,
            endpoint_override=endpoint_host,
            scheme="http",
        )
        file_path = "ecommerce-lakehouse/gold/feat_user_stream/stream_features.parquet"
        file_info = minio_fs.get_file_info(file_path)

        schema = pa.schema(
            [
                ("user_id", pa.int64()),
                ("f_views_15m", pa.int64()),
                ("f_carts_15m", pa.int64()),
                ("f_purchases_15m", pa.int64()),
                ("total_spend_15m", pa.float64()),
                ("event_timestamp", pa.timestamp("us", tz="UTC")),
                ("created", pa.timestamp("us", tz="UTC")),
            ]
        )

        if file_info.type == pafs.FileType.NotFound:
            print("[*] Đang khởi tạo file Parquet cấu trúc ban đầu trên MinIO...")
            empty_table = pa.Table.from_batches([], schema=schema)
            pq.write_table(empty_table, file_path, filesystem=minio_fs)
            print(f"[OK] Initialized initial Parquet schema at: s3://{file_path}")
        else:
            existing = pq.read_table(file_path, filesystem=minio_fs)
            if "f_purchases_15m" not in existing.column_names:
                print("[*] Upgrading stream_features.parquet schema with 4 features...")
                df_existing = existing.to_pandas()
                df_existing["f_purchases_15m"] = 0
                df_existing["total_spend_15m"] = 0.0
                df_existing["f_purchases_15m"] = df_existing["f_purchases_15m"].astype("int64")
                df_existing["total_spend_15m"] = df_existing["total_spend_15m"].astype("float64")
                updated_table = pa.Table.from_pandas(df_existing, schema=schema)
                pq.write_table(updated_table, file_path, filesystem=minio_fs)
                print("[OK] Upgraded stream_features.parquet schema (4 features)")
    except Exception as e:
        print(f"[WARN] Offline parquet check: {e}")


def run_push_job(target: str = "online", batch_size: int = 50, seed_count: int = 0):
    """Consume stream feature vectors from Kafka and push into Feast online (Redis) or offline (Parquet).

    Args:
        target: Target destination ('online', 'offline', or 'both').
        batch_size: Number of messages to batch before calling store.push().
        seed_count: If > 0, pushes N simulated synthetic feature records immediately for testing.
    """
    repo_path = os.path.dirname(os.path.abspath(__file__))
    store = FeatureStore(repo_path=repo_path)

    # Đảm bảo Offline Store có file Schema nếu nạp vào Offline
    if target in ("offline", "both"):
        ensure_offline_parquet_exists()

    # Xác định chế độ Push của Feast
    if target == "online":
        push_mode = PushMode.ONLINE
        target_desc = f"ONLINE STORE (RAM Redis - {REDIS_HOST}:{REDIS_PORT})"
    elif target == "offline":
        push_mode = PushMode.OFFLINE
        target_desc = "OFFLINE STORE (MinIO Parquet - s3://ecommerce-lakehouse/gold/feat_user_stream/)"
    else:
        push_mode = PushMode.ONLINE_AND_OFFLINE
        target_desc = "DUAL-WRITE (Redis Online + MinIO Offline)"

    print("=" * 80)
    print("[FEAST STREAM PUSHER] DONG BO DAC TRUNG THOI GIAN THUC (15 PHUT)")
    print("=" * 80)
    print(f"Target                  : {target_desc}")
    print(f"Kafka Topic source      : {FEATURES_TOPIC}")
    print(f"Push Source Feast       : {PUSH_SOURCE_NAME}")
    print(f"Feast Repo              : {repo_path}")
    print("-" * 80)

    # 1. CHẾ ĐỘ SEED DEMO
    if seed_count > 0:
        print(f"[SEED DEMO] Pushing {seed_count} sample records into {target.upper()}...")
        df_seed = create_sample_seed_dataframe(seed_count)

        t0 = time.perf_counter()
        store.push(push_source_name=PUSH_SOURCE_NAME, df=df_seed, to=push_mode)
        latency_ms = (time.perf_counter() - t0) * 1000

        print("Stream Features 15m pushed successfully:")
        print(
            df_seed[
                ["user_id", "f_views_15m", "f_carts_15m", "f_purchases_15m", "total_spend_15m", "event_timestamp"]
            ].to_string(index=False)
        )
        print(f"[OK] [PUSH SUCCESS] Ingestion duration: {latency_ms:.2f} ms")
        print("=" * 80)
        return

    # 2. CHẾ ĐỘ STREAMING LIÊN TỤC (PRODUCTION CONSUMER)
    ensure_kafka_topic(FEATURES_TOPIC)
    group_id = f"feast-pusher-{target}-group"

    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }

    from confluent_kafka import Consumer, KafkaError

    consumer = Consumer(conf)
    consumer.subscribe([FEATURES_TOPIC])
    print(f"[INFO] Connected to Kafka Consumer (Group: {group_id})...")
    print("[INFO] Waiting for incoming events from Apache Flink...")

    batch_records = []
    last_flush_time = time.time()

    try:
        while RUNNING:
            msg = consumer.poll(timeout=1.0)
            now_time = time.time()

            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"[WARN] Kafka error: {msg.error()}")
                    continue

                try:
                    payload = json.loads(msg.value().decode("utf-8"))
                    batch_records.append(
                        {
                            "user_id": int(payload["user_id"]),
                            "f_views_15m": int(payload.get("f_views_15m", 0)),
                            "f_carts_15m": int(payload.get("f_carts_15m", 0)),
                            "f_purchases_15m": int(payload.get("f_purchases_15m", 0)),
                            "total_spend_15m": float(payload.get("total_spend_15m", 0.0)),
                            "event_timestamp": payload.get("event_timestamp", datetime.now(timezone.utc).isoformat()),
                            "created": payload.get("created", datetime.now(timezone.utc).isoformat()),
                        }
                    )
                except Exception as e:
                    print(f"[WARN] Skipped record: {e}")

            # Đẩy batch khi đủ số lượng hoặc sau mỗi 2 giây
            if batch_records and (len(batch_records) >= batch_size or (now_time - last_flush_time) >= 2.0):
                df_batch = pd.DataFrame(batch_records)
                df_batch["user_id"] = df_batch["user_id"].astype("int64")
                df_batch["f_views_15m"] = df_batch["f_views_15m"].astype("int64")
                df_batch["f_carts_15m"] = df_batch["f_carts_15m"].astype("int64")
                df_batch["f_purchases_15m"] = df_batch["f_purchases_15m"].astype("int64")
                df_batch["total_spend_15m"] = df_batch["total_spend_15m"].astype("float64")
                df_batch["event_timestamp"] = pd.to_datetime(df_batch["event_timestamp"], format="mixed", utc=True)
                df_batch["created"] = pd.to_datetime(df_batch["created"], format="mixed", utc=True)

                t0 = time.perf_counter()
                store.push(push_source_name=PUSH_SOURCE_NAME, df=df_batch, to=push_mode)
                dur_ms = (time.perf_counter() - t0) * 1000

                print(
                    f"[OK] [{target.upper()} PUSH SUCCESS] Pushed {len(batch_records)} records into {target.upper()} Store! "
                    f"(Duration: {dur_ms:.2f} ms | Users: {df_batch['user_id'].tolist()[:3]}...)"
                )

                batch_records.clear()
                last_flush_time = now_time

    finally:
        consumer.close()
        print("[INFO] Closed Kafka Consumer cleanly.")


def main():
    """Command-line entry point to launch the Feast stream pusher process."""
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    parser = argparse.ArgumentParser(description="Feast Real-time Stream Pusher Job")
    parser.add_argument(
        "--target",
        choices=["online", "offline", "both"],
        default="online",
        help="Đích nạp đặc trưng: 'online' (Redis), 'offline' (MinIO Parquet), hoặc 'both' (Dual-write)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Kích thước batch gom lại trước khi gọi Feast store.push() (mặc định: 50)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=0,
        help="Chế độ giả lập: Tự động push N records mẫu vào Feast để nghiệm thu ngay lập tức",
    )

    args = parser.parse_args()
    run_push_job(target=args.target, batch_size=args.batch_size, seed_count=args.seed)


if __name__ == "__main__":
    main()
