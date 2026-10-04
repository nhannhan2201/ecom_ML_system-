"""Materialize 30-day batch features from MinIO Lakehouse Gold to Redis Online Store.

Supports two execution modes:
  1. Range / Backfill: Loads features across an explicit timestamp window.
  2. Incremental: Automatically uses Feast checkpoint state to sync delta features.
"""

import os
import time
import argparse
from datetime import datetime, timezone

# 1. Thiết lập biến môi trường bắt buộc để Feast & s3fs kết nối MinIO và Redis linh hoạt
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
if "ecom_minio" in MINIO_ENDPOINT:
    MINIO_ENDPOINT = MINIO_ENDPOINT.replace("ecom_minio", "minio")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
os.environ.setdefault("REDIS_CONNECTION_STRING", f"{REDIS_HOST}:{REDIS_PORT}")

access_key = os.getenv("MINIO_ACCESS_KEY") or os.getenv("AWS_ACCESS_KEY_ID") or "minioadmin"
secret_key = os.getenv("MINIO_SECRET_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY") or "minioadmin"

os.environ["AWS_ACCESS_KEY_ID"] = access_key
os.environ["AWS_SECRET_ACCESS_KEY"] = secret_key
os.environ["AWS_ENDPOINT_URL"] = MINIO_ENDPOINT
os.environ["FEAST_S3_ENDPOINT_URL"] = MINIO_ENDPOINT
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["S3_ENDPOINT_URL"] = MINIO_ENDPOINT


from feast import FeatureStore
import redis


def parse_datetime(dt_str: str, is_end_of_day: bool = False) -> datetime:
    """Parse chuoi ngay (YYYY-MM-DD hoac ISO8601) ve datetime UTC."""
    try:
        if len(dt_str.strip()) == 10:
            dt = datetime.strptime(dt_str.strip(), "%Y-%m-%d")
        else:
            dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except ValueError:
        dt = datetime.strptime(dt_str.strip(), "%Y-%m-%d")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    if is_end_of_day:
        return dt.replace(hour=23, minute=59, second=59)
    elif len(dt_str.strip()) == 10:
        return dt.replace(hour=0, minute=0, second=0)
    return dt


def run_materialization(mode: str = "range", start_str: str = None, end_str: str = None, views: list = None):
    """Synchronize offline features from MinIO Lakehouse Gold to the Redis online store.

    Args:
        mode: Synchronization mode ('range' for backfill or 'incremental' for delta updates).
        start_str: Beginning timestamp string (YYYY-MM-DD or ISO8601).
        end_str: Ending timestamp string (YYYY-MM-DD or ISO8601).
        views: List of Feast feature view names to materialize.
    """
    repo_path = os.path.dirname(os.path.abspath(__file__))
    if views is None:
        views = ["user_batch_features_30d"]

    print("=" * 80)
    print("[FEAST MATERIALIZATION] Dong bo dac trung tu MinIO sang Redis Online Store")
    print("=" * 80)
    print(f"Thu muc FeatureStore : {repo_path}")
    print(f"Che do dong bo       : {mode.upper()}")
    print(f"Feature Views        : {', '.join(views)}")
    print(f"Dich luu tru (Redis) : {REDIS_HOST}:{REDIS_PORT} (Database 0)")

    store = FeatureStore(repo_path=repo_path)

    # Do so keys hien tai tren Redis truoc khi nap
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    initial_keys = r.dbsize()
    print(f"So Keys hien tai tren Redis truoc khi nap: {initial_keys:,}")
    print("-" * 80)

    # Dam bao endpoint MinIO phu hop voi moi truong hien tai
    for v_name in views:
        try:
            fv = store.get_feature_view(v_name)
            if hasattr(fv, "batch_source") and hasattr(fv.batch_source, "file_options"):
                if fv.batch_source.file_options.s3_endpoint_override != MINIO_ENDPOINT:
                    fv.batch_source.file_options.s3_endpoint_override = MINIO_ENDPOINT
                    store.registry.apply_feature_view(fv, project=store.project)
                    store.registry.commit()
        except Exception as e:
            print(f"[WARN] Could not dynamic-patch feature view {v_name}: {e}")

    start_time = time.time()

    if mode == "incremental":
        # CHE DO INCREMENTAL: Tu dong tra cuu checkpoint lan sync truoc
        end_date = parse_datetime(end_str, is_end_of_day=True) if end_str else datetime.now(timezone.utc)
        print(f"[INFO] Thuc thi MATERIALIZE INCREMENTAL den moc: {end_date.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print("[INFO] Feast tu dong truy vet checkpoint lan truoc trong registry.db va chi nap delta moi.")

        store.materialize_incremental(end_date=end_date, feature_views=views)
    else:
        # CHE DO RANGE / BACKFILL: Nap dai ngay chi dinh
        start_date = parse_datetime(start_str) if start_str else datetime(2019, 10, 1, 0, 0, 0, tzinfo=timezone.utc)
        end_date = (
            parse_datetime(end_str, is_end_of_day=True)
            if end_str
            else datetime(2019, 10, 26, 23, 59, 59, tzinfo=timezone.utc)
        )

        print(
            f"[INFO] Khoang thoi gian nap: {start_date.strftime('%Y-%m-%d %H:%M:%S UTC')} -> {end_date.strftime('%Y-%m-%d %H:%M:%S UTC')}"
        )
        print("[INFO] Dang doc Parquet tu MinIO va nap len Redis...")

        store.materialize(start_date=start_date, end_date=end_date, feature_views=views)

    duration = time.time() - start_time
    print("-" * 80)
    print(f"[INFO] MATERIALIZE hoan tat trong {duration:.2f} giay")

    # Kiem tra thuc te tren Redis sau khi nap
    print("\n[NGHIEM THU TREN REDIS]:")
    final_keys = r.dbsize()
    added_keys = final_keys - initial_keys
    print(f"  - So Keys truoc khi nap            : {initial_keys:,} keys")
    print(f"  - Tong so Keys hien tai tren Redis : {final_keys:,} keys")
    print(f"  - So Keys tang them (delta)        : +{added_keys:,} keys")

    # Lay 1 key mau de xac nhan du lieu da nam tren RAM
    keys = r.keys(b"*")
    if keys:
        sample_key = keys[0]
        key_type = r.type(sample_key).decode("utf-8")
        print(f"  - Key mau phat hien                : {sample_key[:40]}...")
        print(f"  - Kieu du lieu tren Redis          : {key_type.upper()}")
        if key_type == "hash":
            sample_hash = r.hgetall(sample_key)
            print(f"  - So truong dac trung (Fields)     : {len(sample_hash)} fields")
            field_names = [f.decode("utf-8", errors="ignore") for f in list(sample_hash.keys())[:5]]
            print(f"  - Cac truong mau                   : {field_names}")
        print("  - Trang thai                       : DA NAP THANH CONG TAI REDIS ONLINE STORE")
    print("=" * 80)


def main():
    """Command-line entry point to run Feast materialization."""
    parser = argparse.ArgumentParser(description="Feast Materialization Pipeline to Redis Online Store")
    parser.add_argument(
        "--mode",
        choices=["range", "incremental"],
        default="range",
        help="Chế độ nạp: 'range' (Backfill dải ngày) hoặc 'incremental' (Tự động tăng dần theo checkpoint)",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Ngày bắt đầu (YYYY-MM-DD hoặc ISO). Mặc định cho mode range: 2019-10-01",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="Ngày kết thúc (YYYY-MM-DD hoặc ISO). Mặc định: 2019-10-26 (range) hoặc now() (incremental)",
    )
    parser.add_argument(
        "--views",
        nargs="+",
        default=["user_batch_features_30d"],
        help="Danh sách Feature Views cần nạp (mặc định: user_batch_features_30d)",
    )

    args = parser.parse_args()
    run_materialization(mode=args.mode, start_str=args.start_date, end_str=args.end_date, views=args.views)


if __name__ == "__main__":
    main()
