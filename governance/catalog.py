"""
Declarative Data Governance Catalog (Metadata as Code)
Dự án: E-Commerce Real-Time Purchase Propensity Prediction System
Mô hình chuẩn: DataHub Governance Architecture (tương đồng với RecSys-MLops)

Nguyên tắc:
1. Pure Declarative: 100% không chứa network code, không phụ thuộc Spark hay MinIO.
2. Quản lý toàn bộ 4 tầng Medallion + Feature Store (Raw -> Bronze -> Silver -> Gold -> Feast Redis).
3. Khai báo tường minh Upstream Lineage, Field Schemas, Assertions và Data Contracts.
"""

from dataclasses import dataclass
from typing import Dict, Optional, Tuple

ENV = "PROD"


@dataclass(frozen=True)
class SchemaFieldSpec:
    """Specification of an individual dataset column/field in the DataHub catalog.
    
    Attributes:
        name: Column name matching the physical storage column.
        type_name: Abstract DataHub type ('string', 'number', 'time', 'boolean').
        description: Business and technical documentation for the column.
        nullable: Whether NULL values are permitted by the contract.
        is_primary_key: True if this column forms part of the table's primary key.
    """
    name: str
    type_name: str  # "string", "number", "time", "boolean"
    description: str
    nullable: bool = True
    is_primary_key: bool = False


@dataclass(frozen=True)
class AssertionSpec:
    """Specification of an automated data quality check or contract assertion.
    
    Attributes:
        urn: Unique DataHub assertion URN.
        name: Short human-readable title for the assertion.
        description: Detailed explanation of the condition being checked.
        logic: SQL/logical condition defining expected valid state.
        metric_name: Quality metric identifier tracked by DataHub.
    """
    urn: str
    name: str
    description: str
    logic: str
    metric_name: str


@dataclass(frozen=True)
class DataContractSpec:
    """Specification for a collection of data quality assertions forming a Data Contract.
    
    Attributes:
        urn: Unique DataHub data contract URN.
        description: Objective and scope of this data contract.
        assertions: Tuple of AssertionSpec items evaluated against the dataset.
    """
    urn: str
    description: str
    assertions: Tuple[AssertionSpec, ...] = ()


@dataclass(frozen=True)
class DatasetSpec:
    """Specification of a physical dataset in the lakehouse or streaming platform.
    
    Attributes:
        key: Internal registry lookup key.
        platform: Data platform identifier (e.g. 's3', 'kafka', 'delta').
        name: Canonical human-readable table or topic name.
        urn: Unique DataHub dataset URN.
        description: Business purpose, retention, and storage characteristics.
        domain: Organizational domain (e.g. 'ecommerce', 'feature_store').
        tags: List of categorization tags for filtering in DataHub UI.
        schema_fields: Tuple of SchemaFieldSpec defining the authoritative schema.
        upstreams: List of upstream dataset URNs for lineage graph rendering.
        contract: Optional DataContractSpec containing quality assertions.
    """
    key: str
    platform: str
    name: str
    urn: str
    description: str
    domain: str
    tags: Tuple[str, ...]
    schema_fields: Tuple[SchemaFieldSpec, ...]
    upstreams: Tuple[str, ...] = ()
    contract: Optional[DataContractSpec] = None


@dataclass(frozen=True)
class DataProductSpec:
    """Specification of a logical Data Product grouping related analytical datasets.
    
    Attributes:
        id: Unique identifier for the data product.
        name: Display name in DataHub UI.
        description: Value proposition and consumer audience for the product.
        dataset_keys: Tuple of dataset keys belonging to this data product.
    """
    id: str
    name: str
    description: str
    dataset_keys: Tuple[str, ...]


# ==============================================================================
# 1. HÀM TẠO URN CHUẨN DATAHUB
# ==============================================================================
def make_dataset_urn(platform: str, path: str, env: str = ENV) -> str:
    """Construct a standardized DataHub dataset URN string.
    
    Args:
        platform: Platform type identifier (e.g. 's3', 'kafka', 'delta', 'postgres').
        path: Path or topic identifier relative to the storage engine.
        env: Target environment ('PROD', 'DEV', etc.). Defaults to ENV.
        
    Returns:
        Canonical URN string format: 'urn:li:dataset:(urn:li:dataPlatform:{platform},{path},{env})'
    """
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{path},{env})"


# ==============================================================================
# 2. KHAI BÁO CÁC DATASETS THEO MÔ HÌNH MEDALLION (DATA CATALOG SPEC)
# ==============================================================================
RAW_BATCH_URN = make_dataset_urn("s3", "ecommerce-raw/batch")
RAW_STREAM_URN = make_dataset_urn("kafka", "ecommerce_stream_events")
RAW_STAGING_STREAM_URN = make_dataset_urn("s3", "ecommerce-raw/staging/stream_events")
BRONZE_URN = make_dataset_urn("delta", "ecommerce-lakehouse/bronze/raw_events")
SILVER_URN = make_dataset_urn("delta", "ecommerce-lakehouse/silver/stg_events")
GOLD_DIM_PROD_URN = make_dataset_urn("delta", "ecommerce-lakehouse/gold/dim_product")
GOLD_DIM_USER_URN = make_dataset_urn("delta", "ecommerce-lakehouse/gold/dim_user")
GOLD_FACT_EVENTS_URN = make_dataset_urn("delta", "ecommerce-lakehouse/gold/fact_user_events")
GOLD_FEAT_30D_URN = make_dataset_urn("delta", "ecommerce-lakehouse/gold/feat_user_30d")
GOLD_FEAT_STREAM_URN = make_dataset_urn("s3", "ecommerce-lakehouse/gold/feat_user_stream/stream_features.parquet")
GOLD_USER_LABELS_URN = make_dataset_urn("delta", "ecommerce-lakehouse/gold/user_labels")
REDIS_ONLINE_URN = make_dataset_urn("redis", "ecommerce-feature-store/online_redis")


DATASETS: Dict[str, DatasetSpec] = {
    # --------------------------------------------------------------------------
    # 0. TẦNG RAW (SOURCES)
    # --------------------------------------------------------------------------
    "raw_batch": DatasetSpec(
        key="raw_batch",
        platform="s3",
        name="ecommerce-raw/batch",
        urn=RAW_BATCH_URN,
        description="Nguồn dữ liệu thô dạng file CSV lịch sử (Part 1 cũ + Part 2 Schema Evolution)",
        domain="urn:li:domain:bronze_lakehouse",
        tags=("Source", "S3", "CSV", "Raw"),
        schema_fields=(
            SchemaFieldSpec("event_time", "time", "Thời điểm phát sinh sự kiện"),
            SchemaFieldSpec("event_type", "string", "Loại hành vi (view, cart, purchase)"),
            SchemaFieldSpec("product_id", "number", "Mã định danh sản phẩm"),
            SchemaFieldSpec("category_id", "number", "Mã danh mục sản phẩm"),
            SchemaFieldSpec("category_code", "string", "Tên phân cấp danh mục"),
            SchemaFieldSpec("brand", "string", "Thương hiệu sản phẩm"),
            SchemaFieldSpec("price", "number", "Giá bán niêm yết (USD)"),
            SchemaFieldSpec("user_id", "number", "Mã khách hàng"),
            SchemaFieldSpec("user_session", "string", "Mã phiên làm việc UUID"),
            SchemaFieldSpec("discount_percent", "number", "Tỉ lệ giảm giá tiêm từ ngày 16/10 (Schema Evolution)"),
        ),
        upstreams=(),
    ),
    "raw_stream": DatasetSpec(
        key="raw_stream",
        platform="kafka",
        name="ecommerce_stream_events",
        urn=RAW_STREAM_URN,
        description="Kafka Topic nhận luồng sự kiện thời gian thực (Burst, Late Arrival, Duplicate)",
        domain="urn:li:domain:bronze_lakehouse",
        tags=("Source", "Kafka", "Streaming", "Realtime"),
        schema_fields=(
            SchemaFieldSpec("event_time", "time", "Thời gian sự kiện"),
            SchemaFieldSpec("event_type", "string", "Loại hành vi (view/cart/purchase)"),
            SchemaFieldSpec("product_id", "number", "Mã sản phẩm"),
            SchemaFieldSpec("category_id", "number", "Mã danh mục sản phẩm"),
            SchemaFieldSpec("category_code", "string", "Tên phân cấp danh mục"),
            SchemaFieldSpec("brand", "string", "Thương hiệu sản phẩm"),
            SchemaFieldSpec("price", "number", "Giá niêm yết"),
            SchemaFieldSpec("user_id", "number", "Mã khách hàng (Partition Key)", is_primary_key=True),
            SchemaFieldSpec("user_session", "string", "Mã phiên làm việc UUID"),
            SchemaFieldSpec("discount_percent", "number", "Chiết khấu khuyến mãi (Schema Evolution)"),
        ),
        upstreams=(),
    ),
    "raw_staging_stream": DatasetSpec(
        key="raw_staging_stream",
        platform="s3",
        name="ecommerce-raw/staging/stream_events",
        urn=RAW_STAGING_STREAM_URN,
        description="MinIO S3 Stream Staging: Tệp JSON do Flink staging job ghi ra từ Kafka, phục vụ DP1 nạp định kỳ vào Bronze.",
        domain="urn:li:domain:bronze_lakehouse",
        tags=("Source", "S3", "JSON", "Streaming_Staging"),
        schema_fields=(
            SchemaFieldSpec("event_time", "time", "Thời gian sự kiện"),
            SchemaFieldSpec("event_type", "string", "Loại hành vi"),
            SchemaFieldSpec("product_id", "number", "Mã sản phẩm"),
            SchemaFieldSpec("category_id", "number", "Mã danh mục"),
            SchemaFieldSpec("category_code", "string", "Tên phân cấp danh mục"),
            SchemaFieldSpec("brand", "string", "Thương hiệu"),
            SchemaFieldSpec("price", "number", "Giá bán"),
            SchemaFieldSpec("user_id", "number", "Mã khách hàng"),
            SchemaFieldSpec("user_session", "string", "Phiên truy cập"),
            SchemaFieldSpec("discount_percent", "number", "Tỉ lệ giảm giá"),
        ),
        upstreams=(RAW_STREAM_URN,),
    ),

    # --------------------------------------------------------------------------
    # 1. TẦNG BRONZE LAKEHOUSE
    # --------------------------------------------------------------------------
    "bronze_raw_events": DatasetSpec(
        key="bronze_raw_events",
        platform="delta",
        name="ecommerce-lakehouse/bronze/raw_events",
        urn=BRONZE_URN,
        description="Bronze Lakehouse: Lưu trữ sự kiện thô nguyên trạng chuẩn ACID Delta Lake từ Batch CSV và Stream Staging.",
        domain="urn:li:domain:bronze_lakehouse",
        tags=("Bronze", "DeltaLake", "Append_Only", "Schema_Evolution"),
        schema_fields=(
            SchemaFieldSpec("event_time", "time", "Thời điểm tương tác"),
            SchemaFieldSpec("event_type", "string", "Hành vi (view/cart/purchase)"),
            SchemaFieldSpec("product_id", "number", "ID sản phẩm"),
            SchemaFieldSpec("category_id", "number", "ID danh mục"),
            SchemaFieldSpec("category_code", "string", "Phân loại ngành hàng"),
            SchemaFieldSpec("brand", "string", "Thương hiệu"),
            SchemaFieldSpec("price", "number", "Giá bán"),
            SchemaFieldSpec("user_id", "number", "ID khách hàng"),
            SchemaFieldSpec("user_session", "string", "Phiên truy cập"),
            SchemaFieldSpec("discount_percent", "number", "Chiết khấu khuyến mãi"),
            SchemaFieldSpec("ingestion_time", "time", "Thời điểm nạp vào Bronze Lakehouse"),
        ),
        upstreams=(RAW_BATCH_URN, RAW_STAGING_STREAM_URN),
        contract=DataContractSpec(
            urn="urn:li:dataContract:contract_bronze_lakehouse",
            description="Bronze Quality Contract: Đảm bảo không có user_id null trên tập mẫu (Sample-based 50k rows)",
            assertions=(
                AssertionSpec(
                    urn="urn:li:assertion:dp1_bronze_null_check",
                    name="Bronze Sample Zero Null User Check",
                    description="Kiểm tra không có bản ghi nào bị khuyết user_id trên mẫu 50,000 dòng",
                    logic="sample_count(user_id IS NULL) == 0 (sample_size = 50,000 rows)",
                    metric_name="sample_null_user_count",
                ),
            ),
        ),
    ),

    # --------------------------------------------------------------------------
    # 2. TẦNG SILVER LAKEHOUSE
    # --------------------------------------------------------------------------
    "silver_stg_events": DatasetSpec(
        key="silver_stg_events",
        platform="delta",
        name="ecommerce-lakehouse/silver/stg_events",
        urn=SILVER_URN,
        description="Silver Lakehouse: Dữ liệu sự kiện đã khử trùng lặp bằng Window Top-1, xử lý skew và phân vùng theo ngày.",
        domain="urn:li:domain:silver_curated",
        tags=("Silver", "DeltaLake", "Deduplicated", "Cleaned", "Partitioned"),
        schema_fields=(
            SchemaFieldSpec("event_time", "time", "Thời gian sự kiện chuẩn hóa UTC"),
            SchemaFieldSpec("event_type", "string", "Loại hành vi"),
            SchemaFieldSpec("product_id", "number", "ID sản phẩm"),
            SchemaFieldSpec("category_id", "number", "ID danh mục"),
            SchemaFieldSpec("category_level1", "string", "Ngành hàng cấp 1 rút gọn (High-Cardinality handling)"),
            SchemaFieldSpec("brand", "string", "Thương hiệu chuẩn hóa"),
            SchemaFieldSpec("price", "number", "Giá bán (USD)"),
            SchemaFieldSpec("user_id", "number", "ID người dùng"),
            SchemaFieldSpec("user_session", "string", "Mã phiên làm việc"),
            SchemaFieldSpec("discount_percent", "number", "Tỉ lệ giảm giá"),
            SchemaFieldSpec("date", "time", "Cột phân vùng vật lý (Partition Key)"),
        ),
        upstreams=(BRONZE_URN,),
        contract=DataContractSpec(
            urn="urn:li:dataContract:contract_silver_curated",
            description="Silver Quality Contract: Khử trùng lặp trên bộ khóa (user_id, event_time, product_id, event_type) trên tập mẫu",
            assertions=(
                AssertionSpec(
                    urn="urn:li:assertion:dp2_silver_dedup_check",
                    name="Silver Sample Deduplication Check",
                    description="Kiểm tra tính duy nhất trên tập mẫu 50,000 dòng theo bộ khóa Spark deduplication",
                    logic="sample_duplicate_count == 0 on (user_id, event_time, product_id, event_type) (sample_size = 50,000 rows)",
                    metric_name="sample_silver_duplicate_count",
                ),
            ),
        ),
    ),

    # --------------------------------------------------------------------------
    # 3. TẦNG GOLD DATA WAREHOUSE (STAR SCHEMA & SCD2 / SNAPSHOT)
    # --------------------------------------------------------------------------
    "gold_dim_product": DatasetSpec(
        key="gold_dim_product",
        platform="delta",
        name="ecommerce-lakehouse/gold/dim_product",
        urn=GOLD_DIM_PROD_URN,
        description="Gold DWH: Chiều sản phẩm áp dụng SCD-like Type 2 dựa trên các trạng thái thuộc tính quan sát được (valid_from_ts, valid_to_ts, is_current).",
        domain="urn:li:domain:gold_dwh",
        tags=("Gold", "DeltaLake", "Star_Schema", "SCD_Type_2", "Dimension"),
        schema_fields=(
            SchemaFieldSpec("product_sk", "string", "Khóa thay thế Surrogate Key duy nhất (MD5 hash)", is_primary_key=True),
            SchemaFieldSpec("product_id", "number", "Mã tự nhiên Natural Key"),
            SchemaFieldSpec("category_id", "number", "ID danh mục"),
            SchemaFieldSpec("category_level1", "string", "Ngành hàng cấp 1 rút gọn"),
            SchemaFieldSpec("brand", "string", "Thương hiệu"),
            SchemaFieldSpec("price", "number", "Giá bán tại phiên bản quan sát"),
            SchemaFieldSpec("discount_percent", "number", "Tỉ lệ giảm giá"),
            SchemaFieldSpec("valid_from_ts", "time", "Thời điểm phiên bản bắt đầu có hiệu lực"),
            SchemaFieldSpec("valid_to_ts", "time", "Thời điểm phiên bản hết hạn (NULL nếu là bản ghi hiện tại)"),
            SchemaFieldSpec("is_current", "boolean", "Cờ đánh dấu phiên bản đang hiệu lực (TRUE/FALSE)"),
        ),
        upstreams=(SILVER_URN,),
        contract=DataContractSpec(
            urn="urn:li:dataContract:contract_gold_dim_product",
            description="SCD Type 2 Contract: product_sk là khóa chính, valid_from_ts <= valid_to_ts (Sample-based 50k rows)",
            assertions=(
                AssertionSpec(
                    urn="urn:li:assertion:dp2_scd2_integrity_check",
                    name="SCD Type 2 Sample Integrity Check",
                    description="Bảo đảm tính toàn vẹn phiên bản lịch sử chiều sản phẩm trên tập mẫu 50,000 dòng",
                    logic="sample_count(product_sk IS NULL) == 0 and sample_count(valid_from_ts > valid_to_ts) == 0 (sample_size = 50,000 rows)",
                    metric_name="sample_scd2_invalid_count",
                ),
            ),
        ),
    ),
    "gold_dim_user": DatasetSpec(
        key="gold_dim_user",
        platform="delta",
        name="ecommerce-lakehouse/gold/dim_user",
        urn=GOLD_DIM_USER_URN,
        description="Gold DWH: Chiều khách hàng dạng current-state snapshot dimension tổng hợp hành vi trọn đời.",
        domain="urn:li:domain:gold_dwh",
        tags=("Gold", "DeltaLake", "Star_Schema", "Dimension", "Snapshot_Dimension"),
        schema_fields=(
            SchemaFieldSpec("user_id", "number", "Mã định danh khách hàng", is_primary_key=True),
            SchemaFieldSpec("first_seen", "time", "Thời điểm tương tác đầu tiên"),
            SchemaFieldSpec("last_seen", "time", "Thời điểm tương tác gần nhất"),
            SchemaFieldSpec("total_lifetime_events", "number", "Tổng số tương tác tích lũy"),
            SchemaFieldSpec("is_active", "boolean", "Trạng thái hoạt động"),
            SchemaFieldSpec("valid_from_ts", "time", "Mốc thời gian bắt đầu có hiệu lực snapshot"),
            SchemaFieldSpec("valid_to_ts", "time", "Mốc thời gian hết hiệu lực snapshot"),
            SchemaFieldSpec("is_current", "boolean", "Cờ trạng thái hiện tại"),
        ),
        upstreams=(SILVER_URN,),
    ),
    "gold_fact_user_events": DatasetSpec(
        key="gold_fact_user_events",
        platform="delta",
        name="ecommerce-lakehouse/gold/fact_user_events",
        urn=GOLD_FACT_EVENTS_URN,
        description="Gold DWH: Bảng sự kiện giao dịch thuần túy (Pure Fact Table) liên kết với dim_product qua product_sk (không chứa product_id).",
        domain="urn:li:domain:gold_dwh",
        tags=("Gold", "DeltaLake", "Star_Schema", "Pure_Fact", "Z_Order_Optimized"),
        schema_fields=(
            SchemaFieldSpec("event_id", "string", "Khóa định danh sự kiện duy nhất (MD5 hash)", is_primary_key=True),
            SchemaFieldSpec("event_timestamp", "time", "Thời điểm sự kiện dạng timestamp chuẩn"),
            SchemaFieldSpec("event_time", "string", "Thời điểm sự kiện dạng chuỗi gốc"),
            SchemaFieldSpec("date", "time", "Cột phân vùng vật lý (Partition Key)"),
            SchemaFieldSpec("user_id", "number", "Khóa ngoại tới dim_user"),
            SchemaFieldSpec("product_sk", "string", "Khóa ngoại Surrogate Key tới dim_product"),
            SchemaFieldSpec("category_level1", "string", "Ngành hàng cấp 1 rút gọn"),
            SchemaFieldSpec("brand", "string", "Thương hiệu"),
            SchemaFieldSpec("price", "number", "Giá bán giao dịch (USD)"),
            SchemaFieldSpec("discount_percent", "number", "Tỉ lệ chiết khấu"),
            SchemaFieldSpec("event_type", "string", "Loại hành vi (view, cart, purchase)"),
            SchemaFieldSpec("user_session", "string", "Phiên làm việc UUID"),
        ),
        upstreams=(SILVER_URN, GOLD_DIM_PROD_URN, GOLD_DIM_USER_URN),
    ),

    # --------------------------------------------------------------------------
    # 4. TẦNG GOLD FEATURE STORE & LABELS (FEAST & ML TRAINING)
    # --------------------------------------------------------------------------
    "gold_feat_user_30d": DatasetSpec(
        key="gold_feat_user_30d",
        platform="delta",
        name="ecommerce-lakehouse/gold/feat_user_30d",
        urn=GOLD_FEAT_30D_URN,
        description="Feast Offline Store: 5 đặc trưng tổng hợp hành vi 30 ngày chốt theo chuẩn Feast (event_timestamp, created) và phân vùng theo date.",
        domain="urn:li:domain:mlops_feature_store",
        tags=("Feast", "Feature_Store", "Offline_Store", "Gold", "Z_Order"),
        schema_fields=(
            SchemaFieldSpec("user_id", "number", "Mã khách hàng", is_primary_key=True),
            SchemaFieldSpec("f_views_30d", "number", "Số lượt xem hàng 30 ngày"),
            SchemaFieldSpec("f_carts_30d", "number", "Số lượt thêm giỏ 30 ngày"),
            SchemaFieldSpec("f_purchases_30d", "number", "Số lần mua hàng 30 ngày"),
            SchemaFieldSpec("f_spend_30d", "number", "Tổng chi tiêu 30 ngày"),
            SchemaFieldSpec("f_distinct_categories_30d", "number", "Số ngành hàng đã tương tác"),
            SchemaFieldSpec("event_timestamp", "time", "Cột timestamp chuẩn hóa Feast"),
            SchemaFieldSpec("created", "time", "Thời điểm sinh bản ghi Feast"),
            SchemaFieldSpec("date", "time", "Cột phân vùng vật lý (Partition Key)"),
        ),
        upstreams=(SILVER_URN,),
        contract=DataContractSpec(
            urn="urn:li:dataContract:contract_feat_user_30d",
            description="Feast Offline Contract: Tuân thủ chuẩn event_timestamp và created, không null (Sample-based 50k rows)",
            assertions=(
                AssertionSpec(
                    urn="urn:li:assertion:dp3_feast_schema_contract",
                    name="Feast Schema Contract Sample Check",
                    description="Bảo đảm có cột event_timestamp và created trên tập mẫu 50,000 dòng",
                    logic="sample_count(event_timestamp IS NULL) == 0 and sample_count(created IS NULL) == 0 (sample_size = 50,000 rows)",
                    metric_name="sample_feast_timestamp_null_count",
                ),
            ),
        ),
    ),
    "gold_feat_user_stream": DatasetSpec(
        key="gold_feat_user_stream",
        platform="s3",
        name="ecommerce-lakehouse/gold/feat_user_stream/stream_features.parquet",
        urn=GOLD_FEAT_STREAM_URN,
        description="Feast Stream Dual-Write: Đặc trưng cửa sổ trượt 15 phút do stream_push_job nạp vào MinIO Parquet (FileSource) phục vụ offline training và as-of join.",
        domain="urn:li:domain:mlops_feature_store",
        tags=("Feast", "Feature_Store", "Stream_Features", "Dual_Write", "Parquet"),
        schema_fields=(
            SchemaFieldSpec("user_id", "number", "Mã khách hàng", is_primary_key=True),
            SchemaFieldSpec("f_views_15m", "number", "Số lượt xem 15 phút"),
            SchemaFieldSpec("f_carts_15m", "number", "Số lượt thêm giỏ 15 phút"),
            SchemaFieldSpec("f_purchases_15m", "number", "Số lượt mua hàng 15 phút"),
            SchemaFieldSpec("total_spend_15m", "number", "Tổng tiền chi tiêu 15 phút"),
            SchemaFieldSpec("event_timestamp", "time", "Timestamp sự kiện Flink"),
            SchemaFieldSpec("created", "time", "Thời điểm ghi Parquet"),
        ),
        upstreams=(RAW_STREAM_URN,),
    ),
    "gold_user_labels": DatasetSpec(
        key="gold_user_labels",
        platform="delta",
        name="ecommerce-lakehouse/gold/user_labels",
        urn=GOLD_USER_LABELS_URN,
        description="Ground Truth Labels: Bảng nhãn phục vụ huấn luyện máy học (target_purchase_1h thuộc [0, 1]) phân vùng theo date.",
        domain="urn:li:domain:mlops_feature_store",
        tags=("ML", "Ground_Truth", "Labels", "Binary_Classification"),
        schema_fields=(
            SchemaFieldSpec("user_id", "number", "Mã khách hàng", is_primary_key=True),
            SchemaFieldSpec("prediction_timestamp", "time", "Thời điểm ra quyết định dự đoán"),
            SchemaFieldSpec("target_purchase_1h", "number", "Nhãn nhị phân: 1 nếu mua trong 1h, 0 nếu bỏ giỏ"),
            SchemaFieldSpec("date", "time", "Cột phân vùng vật lý (Partition Key)"),
        ),
        upstreams=(SILVER_URN,),
        contract=DataContractSpec(
            urn="urn:li:dataContract:contract_user_labels",
            description="ML Target Contract: Nhãn nằm trong tập [0, 1] trên tập mẫu (Sample-based 50k rows)",
            assertions=(
                AssertionSpec(
                    urn="urn:li:assertion:dp3_binary_labels_contract",
                    name="Binary Label Sample Integrity Check",
                    description="Kiểm tra nhãn target_purchase_1h chỉ nhận giá trị 0 hoặc 1 trên mẫu 50,000 dòng",
                    logic="sample_count(target_purchase_1h NOT IN (0, 1)) == 0 and sample_count(user_id IS NULL) == 0 (sample_size = 50,000 rows)",
                    metric_name="sample_invalid_label_count",
                ),
            ),
        ),
    ),

    # --------------------------------------------------------------------------
    # 5. TẦNG FEAST ONLINE STORE (REDIS)
    # --------------------------------------------------------------------------
    "redis_online": DatasetSpec(
        key="redis_online",
        platform="redis",
        name="ecommerce-feature-store/online_redis",
        urn=REDIS_ONLINE_URN,
        description="Feast Online Store: Bộ nhớ RAM Redis lưu trữ vector đặc trưng phục vụ truy xuất độ trễ thấp cho Model Inference.",
        domain="urn:li:domain:mlops_feature_store",
        tags=("Feast", "Online_Store", "Redis", "Low_Latency", "RAM"),
        schema_fields=(
            SchemaFieldSpec("user_id", "number", "Redis Key định danh khách hàng (Join Key)", is_primary_key=True),
            # 5 Batch Features (30 ngày)
            SchemaFieldSpec("f_views_30d", "number", "Số lượt xem sản phẩm 30 ngày qua (Feast Batch)"),
            SchemaFieldSpec("f_carts_30d", "number", "Số lượt thêm giỏ 30 ngày qua (Feast Batch)"),
            SchemaFieldSpec("f_purchases_30d", "number", "Số lần mua hàng 30 ngày qua (Feast Batch)"),
            SchemaFieldSpec("f_spend_30d", "number", "Tổng tiền chi tiêu 30 ngày qua (Feast Batch)"),
            SchemaFieldSpec("f_distinct_categories_30d", "number", "Số danh mục đã tương tác 30 ngày (Feast Batch)"),
            # 4 Stream Features (15 phút)
            SchemaFieldSpec("f_views_15m", "number", "Số lượt xem trong 15 phút (Flink Stream Dual-write)"),
            SchemaFieldSpec("f_carts_15m", "number", "Số lượt thêm giỏ 15 phút (Flink Stream Dual-write)"),
            SchemaFieldSpec("f_purchases_15m", "number", "Số lần mua hàng 15 phút (Flink Stream Dual-write)"),
            SchemaFieldSpec("total_spend_15m", "number", "Tổng tiền chi tiêu 15 phút (Flink Stream Dual-write)"),
        ),
        upstreams=(GOLD_FEAT_30D_URN, GOLD_FEAT_STREAM_URN),
    ),
}


# ==============================================================================
# 3. KHAI BÁO CÁC DATA PRODUCTS (GÓI SẢN PHẨM DỮ LIỆU)
# ==============================================================================
DATA_PRODUCTS: Tuple[DataProductSpec, ...] = (
    DataProductSpec(
        id="dp1_raw_to_bronze",
        name="DP1: Ingestion & Raw Lakehouse",
        description="Pipeline nạp dữ liệu từ S3 CSV và Flink Stream Staging vào Bronze Lakehouse Delta",
        dataset_keys=("raw_batch", "raw_stream", "raw_staging_stream", "bronze_raw_events"),
    ),
    DataProductSpec(
        id="dp2_lakehouse_dwh",
        name="DP2: Curated Lakehouse & Star Schema",
        description="Pipeline làm sạch Bronze, khử trùng lặp và dựng Star Schema DWH (dim_product SCD2, dim_user snapshot, fact_user_events pure fact)",
        dataset_keys=("silver_stg_events", "gold_dim_product", "gold_dim_user", "gold_fact_user_events"),
    ),
    DataProductSpec(
        id="dp3_feature_store_ml",
        name="DP3: Feature Store & Training Pipeline",
        description="Pipeline tính toán offline features 30 ngày, dual-write streaming và đồng bộ Redis",
        dataset_keys=("gold_feat_user_30d", "gold_feat_user_stream", "gold_user_labels", "redis_online"),
    ),
)
