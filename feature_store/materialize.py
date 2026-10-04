"""
Materialize 30-day batch features from MinIO Lakehouse Gold to Redis Online Store.
Rubric 4.4: Airflow Incremental Materialize Pipeline & Feast Online Store Sync.

Hỗ trợ 2 chế độ:
  1. Range / Backfill (Mặc định): Nạp một khoảng thời gian cụ thể (mặc định: 2019-10-01 -> 2019-10-26).
  2. Incremental (Tự động tăng dần): Tự động tìm checkpoint lần sync trước và đồng bộ delta mới đến mốc end_date.
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


from feast import FeatureStore
import redis

def parse_datetime(dt_str: str, is_end_of_day: bool = False) -> datetime:
    """Parse chuỗi ngày (YYYY-MM-DD hoặc ISO8601) về datetime UTC."""
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        dt = datetime.strptime(dt_str, "%Y-%m-%d")
        if is_end_of_day:
            return dt.replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
        return dt.replace(hour=0, minute=0, second=0, tzinfo=timezone.utc)

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
    print("🚀 [FEAST MATERIALIZATION]: ĐỒNG BỘ ĐẶC TRƯNG TỪ MINIO SANG REDIS ONLINE STORE")
    print("=" * 80)
    print(f"📁 Thư mục FeatureStore : {repo_path}")
    print(f"⚙️  Chế độ đồng bộ      : {mode.upper()}")
    print(f"🎯 Feature Views        : {', '.join(views)}")
    print(f"📤 Đích lưu trữ (Redis) : {REDIS_HOST}:{REDIS_PORT} (Database 0)")

    store = FeatureStore(repo_path=repo_path)

    # Đo số keys hiện tại trên Redis trước khi nạp
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    initial_keys = r.dbsize()
    print(f"📊 Số Keys hiện tại trên Redis trước khi nạp: {initial_keys:,}")
    print("-" * 80)

    # Đảm bảo endpoint MinIO phù hợp với môi trường hiện tại (Docker vs Localhost)
    for v_name in views:
        try:
            fv = store.get_feature_view(v_name)
            if hasattr(fv, "batch_source") and hasattr(fv.batch_source, "file_options"):
                if fv.batch_source.file_options.s3_endpoint_override != MINIO_ENDPOINT:
                    fv.batch_source.file_options.s3_endpoint_override = MINIO_ENDPOINT
                    store.registry.apply_feature_view(fv, project=store.project)
                    store.registry.commit()
        except Exception as e:
            print(f"⚠️ Could not dynamic-patch feature view {v_name}: {e}")

    start_time = time.time()

    if mode == "incremental":
        # CHẾ ĐỘ INCREMENTAL: Tự động tra cứu checkpoint lần sync trước
        end_date = parse_datetime(end_str, is_end_of_day=True) if end_str else datetime.now(timezone.utc)
        print(f"🔄 Đang thực thi MATERIALIZE INCREMENTAL đến mốc: {end_date.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print("💡 Feast tự động truy vết checkpoint lần trước trong registry.db và chỉ nạp delta mới.")

        store.materialize_incremental(
            end_date=end_date,
            feature_views=views
        )
    else:
        # CHẾ ĐỘ RANGE / BACKFILL: Nạp dải ngày chỉ định (Mặc định cho dataset demo 2019)
        start_date = parse_datetime(start_str) if start_str else datetime(2019, 10, 1, 0, 0, 0, tzinfo=timezone.utc)
        end_date = parse_datetime(end_str, is_end_of_day=True) if end_str else datetime(2019, 10, 26, 23, 59, 59, tzinfo=timezone.utc)

        print(f"📅 Khoảng thời gian nạp : {start_date.strftime('%Y-%m-%d %H:%M:%S UTC')} ➔ {end_date.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print("⏳ Đang tiến hành đọc Parquet từ MinIO và ghi nạp lên RAM Redis...")

        store.materialize(
            start_date=start_date,
            end_date=end_date,
            feature_views=views
        )

    duration = time.time() - start_time
    print("-" * 80)
    print(f"✅ [MATERIALIZE HOÀN TẤT XUẤT SẮC] Thời gian thực thi: {duration:.2f} giây")

    # Kiểm tra thực tế trên Redis sau khi nạp
    print("\n🔍 [BƯỚC NGHIỆM THU TRÊN REDIS]:")
    final_keys = r.dbsize()
    print(f"  • Số Keys trước khi nạp            : {initial_keys:,} keys")
    print(f"  • Tổng số Keys hiện tại trên Redis : {final_keys:,} keys")
    print(f"  • Số Keys tăng thêm                : +{final_keys - initial_keys:,} keys")

    # Lấy 1 key ngẫu nhiên để xác nhận dữ liệu đã nằm trên RAM
    keys = r.keys(b"*")
    if keys:
        sample_key = keys[0]
        key_type = r.type(sample_key).decode("utf-8")
        print(f"  • Key mẫu phát hiện                : {sample_key[:40]}...")
        print(f"  • Kiểu dữ liệu trên Redis          : {key_type.upper()}")
        if key_type == "hash":
            sample_hash = r.hgetall(sample_key)
            print(f"  • Số trường đặc trưng (Fields)     : {len(sample_hash)} fields")
            field_names = [f.decode("utf-8", errors="ignore") for f in list(sample_hash.keys())[:5]]
            print(f"  • Các trường mẫu                   : {field_names}")
        print("  • Trạng thái                        : ĐÃ NẠP THÀNH CÔNG LÊN RAM REDIS!")
    print("=" * 80)

def main():
    """Command-line entry point to run Feast materialization."""
    parser = argparse.ArgumentParser(description="Feast Materialization Pipeline to Redis Online Store")
    parser.add_argument(
        "--mode",
        choices=["range", "incremental"],
        default="range",
        help="Chế độ nạp: 'range' (Backfill dải ngày) hoặc 'incremental' (Tự động tăng dần theo checkpoint)"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Ngày bắt đầu (YYYY-MM-DD hoặc ISO). Mặc định cho mode range: 2019-10-01"
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="Ngày kết thúc (YYYY-MM-DD hoặc ISO). Mặc định: 2019-10-26 (range) hoặc now() (incremental)"
    )
    parser.add_argument(
        "--views",
        nargs="+",
        default=["user_batch_features_30d"],
        help="Danh sách Feature Views cần nạp (mặc định: user_batch_features_30d)"
    )

    args = parser.parse_args()
    run_materialization(
        mode=args.mode,
        start_str=args.start_date,
        end_str=args.end_date,
        views=args.views
    )

if __name__ == "__main__":
    main()
