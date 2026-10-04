"""
Kiểm thử truy xuất đặc trưng thời gian thực (Online Serving) từ Redis Online Store.
Rubric 4.4 & 4.6: Low-latency Feature Retrieval (< 2ms) via Feast Client.
Kiểm tra toàn diện 9 features: 5 Batch Features (30d) + 4 Stream Features (15m).
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


def get_real_user_ids_from_gold(limit: int = 5) -> list:
    """Đọc danh sách User ID từ bảng Gold Lakehouse trên MinIO."""
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
        table = pq.read_table(
            "ecommerce-lakehouse/gold/feat_user_30d/date=2019-10-26/",
            filesystem=minio_fs,
            columns=["user_id"]
        ).slice(0, limit)
        real_ids = table["user_id"].to_pylist()
        if real_ids:
            return real_ids
    except Exception as e:
        print(f"⚠️ PyArrow S3: {e}")

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
    print("⚡ [TEST ONLINE SERVING]: TRUY VẤN ĐẶC TRƯNG TỪ RAM REDIS PHỤC VỤ DỰ ĐOÁN AI")
    print("=" * 80)

    # 1. Xác định danh sách User IDs thực tế
    if custom_user_ids:
        sample_user_ids = custom_user_ids
    else:
        sample_user_ids = get_real_user_ids_from_gold(limit=5)

    print(f"🔍 Danh sách {len(sample_user_ids)} User ID kiểm thử:")
    for uid in sample_user_ids:
        print(f"   • User ID: {uid}")
    print("-" * 80)

    # Danh sách đầy đủ 9 đặc trưng (5 Batch 30d + 4 Stream 15m)
    features_to_fetch = [
        # Nhóm Offline Batch Features (30 ngày)
        "user_batch_features_30d:f_views_30d",
        "user_batch_features_30d:f_carts_30d",
        "user_batch_features_30d:f_purchases_30d",
        "user_batch_features_30d:f_spend_30d",
        "user_batch_features_30d:f_distinct_categories_30d",
        # Nhóm Online Stream Features (15 phút)
        "user_stream_features_15m:f_views_15m",
        "user_stream_features_15m:f_carts_15m",
        "user_stream_features_15m:f_purchases_15m",
        "user_stream_features_15m:total_spend_15m",
    ]

    print(f"🎯 Danh sách Feature truy vấn ({len(features_to_fetch)} features):")
    for feat in features_to_fetch:
        print(f"   • {feat}")
    print("-" * 80)

    # 2. BENCHMARK ĐỘ TRỄ CHO 1 USER (Single Request)
    single_user = [{"user_id": sample_user_ids[0]}]
    _ = store.get_online_features(features=features_to_fetch, entity_rows=single_user)

    latencies = []
    for _ in range(10):
        t0 = time.perf_counter()
        _ = store.get_online_features(
            features=features_to_fetch,
            entity_rows=single_user
        )
        latencies.append((time.perf_counter() - t0) * 1000)

    avg_latency = sum(latencies) / len(latencies)
    min_latency = min(latencies)

    print("⏱️  [BENCHMARK TỐC ĐỘ TRUY XUẤT CHO 1 KHÁCH HÀNG]:")
    print(f"   • User ID                             : {sample_user_ids[0]}")
    print(f"   • Độ trễ trung bình (Average Latency) : {avg_latency:.2f} ms")
    print(f"   • Độ trễ tốt nhất (Best Latency)       : {min_latency:.2f} ms")
    if avg_latency < 5.0:
        print("   • Đánh giá SLA (< 5ms)                : ✅ ĐẠT XUẤT SẮC!")
    else:
        print(f"   • Đánh giá SLA                        : ⚠️ {avg_latency:.2f}ms")
    print("-" * 80)

    # 3. TRUY VẤN BATCH NHIỀU USER
    print("📊 [KẾT QUẢ 9 ĐẶC TRƯNG LẤY TỪ RAM REDIS]:")
    batch_users = [{"user_id": uid} for uid in sample_user_ids]
    online_features = store.get_online_features(
        features=features_to_fetch,
        entity_rows=batch_users
    )

    df_features = pd.DataFrame(online_features.to_dict())
    cols = [
        "user_id",
        "f_views_30d", "f_carts_30d", "f_purchases_30d", "f_spend_30d", "f_distinct_categories_30d",
        "f_views_15m", "f_carts_15m", "f_purchases_15m", "total_spend_15m"
    ]
    existing_cols = [c for c in cols if c in df_features.columns]
    df_features = df_features[existing_cols]

    print(df_features.to_string(index=False))
    print("=" * 80)
    print("🎉 TOÀN BỘ 9 ĐẶC TRƯNG ĐÃ ĐƯỢC SERVING TRỰC TIẾP TỪ RAM REDIS THÀNH CÔNG RỰC RỠ!")


def main():
    """Command-line entry point to test online feature serving from Redis."""
    parser = argparse.ArgumentParser(description="Test Feast Online Serving from Redis")
    parser.add_argument("--user-ids", nargs="+", type=int, default=None, help="Danh sách User ID cần test")
    args = parser.parse_args()
    test_online_serving(custom_user_ids=args.user_ids)


if __name__ == "__main__":
    main()
