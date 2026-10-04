"""Online Serving Benchmark for Feast Feature Store (Redis).

Measures low-latency feature retrieval latency across single and batch entity queries
for 9 features: 5 Batch Features (30d) + 4 Stream Features (15m).
"""

import os
import time
import argparse
from feast import FeatureStore
import pandas as pd

# Thiết lập biến môi trường linh hoạt
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
if "ecom_minio" in MINIO_ENDPOINT:
    MINIO_ENDPOINT = MINIO_ENDPOINT.replace("ecom_minio", "minio")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")
os.environ.setdefault("REDIS_CONNECTION_STRING", f"{REDIS_HOST}:{REDIS_PORT}")

access_key = os.getenv("MINIO_ACCESS_KEY") or os.getenv("AWS_ACCESS_KEY_ID") or "minioadmin"
secret_key = os.getenv("MINIO_SECRET_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY") or "minioadmin"

os.environ["AWS_ACCESS_KEY_ID"] = access_key
os.environ["AWS_SECRET_ACCESS_KEY"] = secret_key
os.environ["AWS_ENDPOINT_URL"] = MINIO_ENDPOINT
os.environ["FEAST_S3_ENDPOINT_URL"] = MINIO_ENDPOINT


def get_real_user_ids_from_gold(limit: int = 5) -> list:
    """Doc danh sach User ID tu bang Feast Parquet hoac Gold Lakehouse tren MinIO."""
    try:
        from pyarrow import fs as pafs
        import pyarrow.parquet as pq

        endpoint_host = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
        minio_fs = pafs.S3FileSystem(
            access_key=access_key,
            secret_key=secret_key,
            endpoint_override=endpoint_host,
            scheme="http",
        )
        # Thu doc tu feast/user_batch_features_30d truoc
        try:
            table = pq.read_table(
                "ecommerce-lakehouse/feast/user_batch_features_30d",
                filesystem=minio_fs,
                columns=["user_id"],
            ).slice(0, limit)
            real_ids = table["user_id"].to_pylist()
            if real_ids:
                return real_ids
        except Exception:
            pass

        # Fallback doc tu gold/feat_user_30d
        table = pq.read_table(
            "ecommerce-lakehouse/gold/feat_user_30d",
            filesystem=minio_fs,
            columns=["user_id"],
        ).slice(0, limit)
        real_ids = table["user_id"].to_pylist()
        if real_ids:
            return real_ids
    except Exception as e:
        print(f"[WARN] PyArrow S3 read: {e}")

    return [489492092, 512364693, 512378423, 512383224, 512436165][:limit]


def test_online_serving(custom_user_ids: list = None):
    """Query online feature vectors from the Redis online store for low-latency ML serving.

    Retrieves both 30-day offline batch features (user_batch_features_30d) and 15-minute
    real-time stream features (user_stream_features_15m) using Feast store.get_online_features().

    Args:
        custom_user_ids: Optional list of user IDs to query. If None, samples from MinIO Lakehouse.
    """
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    repo_path = os.path.join(project_root, "feature_store")
    store = FeatureStore(repo_path=repo_path)

    print("=" * 80)
    print("[TEST ONLINE SERVING] Truy van dac trung tu Redis Online Store")
    print("=" * 80)

    # 1. Xac dinh danh sach User IDs thuc te
    if custom_user_ids:
        sample_user_ids = custom_user_ids
    else:
        sample_user_ids = get_real_user_ids_from_gold(limit=5)

    print(f"Danh sach {len(sample_user_ids)} User ID kiem thu:")
    for uid in sample_user_ids:
        print(f"   - User ID: {uid}")
    print("-" * 80)

    # Danh sach day du 9 dac trung (5 Batch 30d + 4 Stream 15m)
    features_to_fetch = [
        # Nhom Offline Batch Features (30 ngay)
        "user_batch_features_30d:f_views_30d",
        "user_batch_features_30d:f_carts_30d",
        "user_batch_features_30d:f_purchases_30d",
        "user_batch_features_30d:f_spend_30d",
        "user_batch_features_30d:f_distinct_categories_30d",
        # Nhom Online Stream Features (15 phut)
        "user_stream_features_15m:f_views_15m",
        "user_stream_features_15m:f_carts_15m",
        "user_stream_features_15m:f_purchases_15m",
        "user_stream_features_15m:total_spend_15m",
    ]

    print(f"Danh sach Feature truy van ({len(features_to_fetch)} features):")
    for feat in features_to_fetch:
        print(f"   - {feat}")
    print("-" * 80)

    # 2. BENCHMARK DO TRE CHO 1 USER (Single Request)
    single_user = [{"user_id": sample_user_ids[0]}]
    _ = store.get_online_features(features=features_to_fetch, entity_rows=single_user)

    latencies = []
    for _ in range(10):
        t0 = time.perf_counter()
        _ = store.get_online_features(features=features_to_fetch, entity_rows=single_user)
        latencies.append((time.perf_counter() - t0) * 1000)

    avg_latency = sum(latencies) / len(latencies)
    min_latency = min(latencies)

    print("[BENCHMARK TOC DO TRUY XUAT CHO 1 KHACH HANG]:")
    print(f"   - User ID                             : {sample_user_ids[0]}")
    print(f"   - Do tre trung binh (Average Latency) : {avg_latency:.2f} ms")
    print(f"   - Do tre tot nhat (Best Latency)       : {min_latency:.2f} ms")
    if avg_latency < 5.0:
        print("   - Danh gia SLA (< 5ms)                : DAT")
    else:
        print(f"   - Danh gia SLA                        : {avg_latency:.2f}ms")
    print("-" * 80)

    # 3. TRUY VAN BATCH NHIEU USER
    print("[KET QUA 9 DAC TRUNG LAY TU RAM REDIS]:")
    batch_users = [{"user_id": uid} for uid in sample_user_ids]
    online_features = store.get_online_features(features=features_to_fetch, entity_rows=batch_users)

    df_features = pd.DataFrame(online_features.to_dict())
    cols = [
        "user_id",
        "f_views_30d",
        "f_carts_30d",
        "f_purchases_30d",
        "f_spend_30d",
        "f_distinct_categories_30d",
        "f_views_15m",
        "f_carts_15m",
        "f_purchases_15m",
        "total_spend_15m",
    ]
    existing_cols = [c for c in cols if c in df_features.columns]
    df_features = df_features[existing_cols]

    print(df_features.to_string(index=False))
    print("=" * 80)
    print("Ket qua: Hoan tat truy van dac trung truc tiep tu Redis Online Store.")


def main():
    """Command-line entry point to test online feature serving from Redis."""
    parser = argparse.ArgumentParser(description="Test Feast Online Serving from Redis")
    parser.add_argument("--user-ids", nargs="+", type=int, default=None, help="Danh sách User ID cần test")
    args = parser.parse_args()
    test_online_serving(custom_user_ids=args.user_ids)


if __name__ == "__main__":
    main()
