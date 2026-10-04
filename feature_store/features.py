"""
Feast Feature Store Definitions for E-Commerce Purchase Propensity Prediction System.
Defines:
- Entity: user_id
- BatchFeatureView: user_batch_features_30d (5 features, 30d TTL, sourced from MinIO Delta/Parquet Gold)
- StreamFeatureView: user_stream_features_15m (4 features, 2h TTL, sourced from Flink PushSource)
- FeatureService: ecom_propensity_v1 (Serving contract for XGBoost Purchase Propensity Model)
"""

import os
from datetime import timedelta
from feast import (
    Entity,
    Field,
    FeatureView,
    FileSource,
    PushSource,
    FeatureService,
    ValueType,
)
from feast.types import Int64, Float64

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")

# ==============================================================================
# 1. ENTITY DEFINITION (Khóa định danh đối tượng)
# ==============================================================================
user_entity = Entity(
    name="user_id",
    value_type=ValueType.INT64,
    join_keys=["user_id"],
    description="Mã định danh khách hàng e-commerce (Distinct User ID)",
)

# ==============================================================================
# 2. BATCH DATA SOURCE & FEATURE VIEW (Đặc trưng 30 ngày - Spark Offline)
# ==============================================================================
# Đọc trực tiếp từ bảng Parquet/Delta đã qua tối ưu hóa Z-Order trên MinIO
batch_source_30d = FileSource(
    name="user_batch_features_30d_source",
    path="s3://ecommerce-lakehouse/gold/feat_user_30d/date=2019-10-26/",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
    s3_endpoint_override=MINIO_ENDPOINT,
)

user_batch_features_30d = FeatureView(
    name="user_batch_features_30d",
    entities=[user_entity],
    ttl=timedelta(days=30),  # Rubric 4.7: Thói quen tiêu dùng có giá trị trong 30 ngày
    schema=[
        Field(name="f_views_30d", dtype=Int64, description="Số lượt xem hàng trong 30 ngày qua [A]"),
        Field(name="f_carts_30d", dtype=Int64, description="Số lượt thêm vào giỏ trong 30 ngày qua [B]"),
        Field(name="f_purchases_30d", dtype=Int64, description="Số lần mua hàng thành công trong 30 ngày qua"),
        Field(name="f_spend_30d", dtype=Float64, description="Tổng số tiền đã chi tiêu trong 30 ngày qua [C]"),
        Field(name="f_distinct_categories_30d", dtype=Int64, description="Số ngành hàng khác nhau đã tương tác"),
    ],
    online=True,
    source=batch_source_30d,
    tags={"team": "data_engineering", "tier": "gold", "type": "batch"},
)

# ==============================================================================
# 3. STREAM DATA SOURCE & FEATURE VIEW (Đặc trưng 15 phút - Flink Real-time)
# ==============================================================================
# Nguồn đẩy thời gian thực từ Apache Flink qua Dual-write (Online Redis + Offline MinIO)
# Khớp 100% với 4 stream features từ Flink (f_views_15m, f_carts_15m, f_purchases_15m, total_spend_15m)
stream_push_source = PushSource(
    name="user_stream_push_source",
    batch_source=FileSource(
        name="user_stream_batch_source",
        path="s3://ecommerce-lakehouse/gold/feat_user_stream/stream_features.parquet",
        timestamp_field="event_timestamp",
        created_timestamp_column="created",
        s3_endpoint_override=MINIO_ENDPOINT,
    ),
)

user_stream_features_15m = FeatureView(
    name="user_stream_features_15m",
    entities=[user_entity],
    ttl=timedelta(hours=2),  # Rubric 4.7: Ý định mua hàng tức thời có giá trị trong 2 giờ
    schema=[
        Field(name="f_views_15m", dtype=Int64, description="Số lượt xem sản phẩm trong 15 phút qua [D]"),
        Field(name="f_carts_15m", dtype=Int64, description="Số lượt thêm vào giỏ trong 15 phút qua [E]"),
        Field(name="f_purchases_15m", dtype=Int64, description="Số lượt chốt đơn trong 15 phút qua"),
        Field(name="total_spend_15m", dtype=Float64, description="Tổng chi tiêu trong 15 phút qua"),
    ],
    online=True,
    source=stream_push_source,
    tags={"team": "data_engineering", "tier": "gold", "type": "stream"},
)

# ==============================================================================
# 4. FEATURE SERVICE (Hợp đồng Feature trọn gói cho Model Training & Inference)
# ==============================================================================
ecom_propensity_v1 = FeatureService(
    name="ecom_propensity_v1",
    features=[
        user_batch_features_30d,
        user_stream_features_15m,
    ],
    tags={"model": "xgboost", "task": "purchase_propensity"},
)
