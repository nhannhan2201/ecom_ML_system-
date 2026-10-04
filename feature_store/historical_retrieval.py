"""
Feast Historical Retrieval Pipeline (Point-in-Time AS-OF Join)
Dự án: E-Commerce Real-Time Purchase Propensity Prediction System
Mục tiêu Rubric Final ML: Kéo dữ liệu từ offline store thông qua Feast ghép với bảng nhãn để tạo ra dataset huấn luyện.

Các nguyên tắc quan trọng:
1. Point-in-Time Correctness: Không để xảy ra Data Leakage (đặc trưng chỉ được tính trước prediction_timestamp).
2. Đầy đủ 9 đặc trưng: 5 Batch Features (30d) + 4 Stream Features (15m).
3. Hỗ trợ xuất trực tiếp ra file Parquet để mô hình Machine Learning (XGBoost/LightGBM) sử dụng.
"""

import os
import sys
import time
import argparse
import pandas as pd
import pyarrow.parquet as pq
import s3fs
from datetime import datetime, timezone
from feast import FeatureStore

# Thiết lập biến môi trường kết nối MinIO & Redis
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
if "ecom_minio" in MINIO_ENDPOINT:
    MINIO_ENDPOINT = MINIO_ENDPOINT.replace("ecom_minio", "minio")

os.environ["AWS_ACCESS_KEY_ID"] = "minioadmin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minioadmin"
os.environ["AWS_ENDPOINT_URL"] = MINIO_ENDPOINT
os.environ["FEAST_S3_ENDPOINT_URL"] = MINIO_ENDPOINT
os.environ["S3_ENDPOINT_URL"] = MINIO_ENDPOINT
os.environ.setdefault("REDIS_CONNECTION_STRING", "localhost:6379")


def get_s3_filesystem():
    """Khởi tạo s3fs client kết nối MinIO."""
    return s3fs.S3FileSystem(
        key="minioadmin",
        secret="minioadmin",
        client_kwargs={"endpoint_url": MINIO_ENDPOINT},
    )


def load_user_labels(limit: int = None, sample_fraction: float = None) -> pd.DataFrame:
    """Đọc bảng nhãn Ground Truth từ MinIO Gold Lakehouse."""
    fs = get_s3_filesystem()
    label_path = "ecommerce-lakehouse/gold/user_labels/date=2019-10-26/"
    print(f"📖 Đang đọc bảng nhãn Ground Truth từ: s3://{label_path}")
    
    table = pq.read_table(label_path, filesystem=fs)
    df_labels = table.to_pandas()
    
    if sample_fraction and 0 < sample_fraction < 1.0:
        df_labels = df_labels.sample(frac=sample_fraction, random_state=42)
    elif limit and limit > 0:
        df_labels = df_labels.head(limit)
        
    # Chuẩn hóa cột thời gian cho Feast AS-OF Join
    df_labels["prediction_timestamp"] = pd.to_datetime(df_labels["prediction_timestamp"], utc=True)
    df_labels["event_timestamp"] = df_labels["prediction_timestamp"]
    df_labels["user_id"] = df_labels["user_id"].astype("int64")
    df_labels["target_purchase_1h"] = df_labels["target_purchase_1h"].astype("int32")
    
    print(f"✅ Đã tải {len(df_labels):,} dòng nhãn (Positive rate: {(df_labels['target_purchase_1h'] == 1).mean():.2%})")
    return df_labels


def build_training_dataset(limit: int = 5000, output_path: str = None) -> pd.DataFrame:
    """Thực hiện AS-OF Join giữa bảng nhãn và Feature Store để sinh Training Dataset."""
    repo_path = os.path.dirname(os.path.abspath(__file__))
    store = FeatureStore(repo_path=repo_path)
    
    print("=" * 80)
    print("🚀 [FEAST HISTORICAL RETRIEVAL]: AS-OF POINT-IN-TIME JOIN CHO HUẤN LUYỆN MODEL")
    print("=" * 80)
    print(f"📁 Feast Repo : {repo_path}")
    print(f"🎯 Target     : Ghép bảng nhãn với 9 Features (5 Batch + 4 Stream)")
    print("-" * 80)
    
    # 1. Đọc bảng nhãn
    df_labels = load_user_labels(limit=limit)
    
    # 2. Danh sách features cần ghép
    features_to_fetch = [
        # Nhóm Offline Batch Features (30 ngày)
        "user_batch_features_30d:f_views_30d",
        "user_batch_features_30d:f_carts_30d",
        "user_batch_features_30d:f_purchases_30d",
        "user_batch_features_30d:f_spend_30d",
        "user_batch_features_30d:f_distinct_categories_30d",
        # Nhóm Stream Features (15 phút)
        "user_stream_features_15m:f_views_15m",
        "user_stream_features_15m:f_carts_15m",
        "user_stream_features_15m:f_purchases_15m",
        "user_stream_features_15m:total_spend_15m",
    ]
    
    # 3. Thực thi Point-in-Time Join
    print(f"⏳ Đang thực hiện AS-OF Join trên Feast Offline Store...")
    start_time = time.time()
    
    training_data = store.get_historical_features(
        entity_df=df_labels,
        features=features_to_fetch,
    )
    df_train = training_data.to_df()
    duration = time.time() - start_time
    print(f"✅ AS-OF Join hoàn tất trong: {duration:.2f} giây! Kích thước: {df_train.shape}")
    print("-" * 80)
    
    # 4. Xử lý giá trị Missing (Imputation)
    feature_cols = [
        "f_views_30d", "f_carts_30d", "f_purchases_30d", "f_spend_30d", "f_distinct_categories_30d",
        "f_views_15m", "f_carts_15m", "f_purchases_15m", "total_spend_15m"
    ]
    for col in feature_cols:
        if col in df_train.columns:
            df_train[col] = df_train[col].fillna(0)
            
    # 5. Đánh giá chất lượng Dataset
    pos_count = (df_train["target_purchase_1h"] == 1).sum()
    neg_count = (df_train["target_purchase_1h"] == 0).sum()
    print("📊 [THỐNG KÊ DATASET HUẤN LUYỆN (TRAINING DATASET)]:")
    print(f"   • Tổng số mẫu (Observations) : {len(df_train):,}")
    print(f"   • Số mẫu Positive (Chốt đơn) : {pos_count:,} ({pos_count / len(df_train):.2%})")
    print(f"   • Số mẫu Negative (Bỏ giỏ)   : {neg_count:,} ({neg_count / len(df_train):.2%})")
    print(f"   • Số lượng đặc trưng          : {len(feature_cols)} features")
    print("\n🔍 5 dòng mẫu đầu tiên:")
    display_cols = ["user_id", "prediction_timestamp", "target_purchase_1h"] + feature_cols[:5]
    print(df_train[display_cols].head(5).to_string(index=False))
    print("-" * 80)
    
    # 6. Lưu file Parquet nếu có yêu cầu
    if output_path:
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        df_train.to_parquet(output_path, index=False)
        print(f"💾 Đã lưu Training Dataset vào: {output_path}")
        
    print("=" * 80)
    return df_train


def main():
    """Command-line entry point to generate a point-in-time correct training dataset via Feast."""
    parser = argparse.ArgumentParser(description="Feast Point-in-Time Historical Join for Training")
    parser.add_argument("--limit", type=int, default=5000, help="Số lượng dòng nhãn cần join (mặc định: 5000, 0 = toàn bộ)")
    parser.add_argument("--output", type=str, default="feature_store/data/training_dataset.parquet", help="Đường dẫn lưu file Parquet")
    args = parser.parse_args()
    
    build_training_dataset(limit=args.limit if args.limit > 0 else None, output_path=args.output)


if __name__ == "__main__":
    main()
