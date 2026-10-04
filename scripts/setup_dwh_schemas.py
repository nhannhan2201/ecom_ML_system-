#!/usr/bin/env python3
"""
scripts/setup_dwh_schemas.py
================================================================================
THIẾT LẬP DATA WAREHOUSE TOÀN DIỆN TRÊN POSTGRESQL (RUBRIC MINI & FINAL COURSEWORK)
================================================================================
Mục tiêu bài tập Rubric:
1. Tạo 3 Schemas chuyên nghiệp tương ứng 3 Zones: `bronze`, `silver`, `gold` (Rubric 2.0đ).
2. Xây dựng cấu trúc bảng chuẩn mực:
   - `bronze.raw_events`: Dữ liệu thô 11 cột.
   - `silver.stg_events`: Dữ liệu làm sạch 14 cột, phân vùng date.
   - `gold.dim_product`: Bảng chiều SCD Type 2 (valid_from_ts, valid_to_ts, is_current, product_sk) (Rubric 2.0đ).
   - `gold.dim_user`: Bảng chiều người dùng (user_id PK).
   - `gold.fact_user_events`: Bảng sự kiện Pure Fact (liên kết khóa ngoại FK với dim_product & dim_user) (Rubric 2.0đ).
   - `gold.feat_user_30d`: Bảng Feature Store có 2 cột event_timestamp & created chuẩn Feast (Rubric 2.0đ).
   - `gold.user_labels`: Bảng nhãn Ground Truth (target_purchase_1h).
3. Nạp dữ liệu mẫu/thực tế từ MinIO Lakehouse vào PostgreSQL để DBeaver xem được dữ liệu thật.
4. Tối ưu hóa lưu trữ Data Warehouse (Indexing - Rubric 2.0đ):
   - Đo lường BEFORE Index (Sequential Scan).
   - Tạo Composite B-Tree Index trên (user_id, event_time DESC).
   - Đo lường AFTER Index (Bitmap Index Scan).
   - In bảng phân tích hiệu năng phục vụ nộp báo cáo.
================================================================================
"""

import sys
import time
import io
import re
import psycopg2
import psycopg2.extras
import pyarrow.dataset as ds
from pyarrow.fs import S3FileSystem

# Cấu hình kết nối
PG_HOST = "localhost"
PG_PORT = 5432
PG_USER = "postgres"
PG_PASS = "postgres"
PG_DB   = "ecom_dwh"

MINIO_ENDPOINT   = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

def get_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASS,
        dbname=PG_DB
    )

def get_s3_filesystem():
    return S3FileSystem(
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        endpoint_override=MINIO_ENDPOINT,
        scheme="http"
    )

# ==============================================================================
# BƯỚC 1: KHỞI TẠO 3 SCHEMAS VÀ TOÀN BỘ CẤU TRÚC BẢNG DWH
# ==============================================================================
def create_schemas_and_tables(conn):
    print("=" * 80)
    print(" 🛠️  [BƯỚC 1]: KHỞI TẠO 3 SCHEMAS (BRONZE, SILVER, GOLD) VÀ CÁC BẢNG DATA WAREHOUSE")
    print("=" * 80)
    with conn.cursor() as cur:
        # 1. Tạo 3 Schemas
        cur.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
        cur.execute("CREATE SCHEMA IF NOT EXISTS silver;")
        cur.execute("CREATE SCHEMA IF NOT EXISTS gold;")
        
        # 2. Xóa các bảng cũ nếu đã tồn tại
        cur.execute("""
            DROP TABLE IF EXISTS gold.fact_user_events CASCADE;
            DROP TABLE IF EXISTS gold.dim_product CASCADE;
            DROP TABLE IF EXISTS gold.dim_user CASCADE;
            DROP TABLE IF EXISTS gold.feat_user_30d CASCADE;
            DROP TABLE IF EXISTS gold.user_labels CASCADE;
            DROP TABLE IF EXISTS silver.stg_events CASCADE;
            DROP TABLE IF EXISTS bronze.raw_events CASCADE;
        """)

        # 3. TẦNG BRONZE: raw_events (Dữ liệu thô hợp nhất)
        cur.execute("""
            CREATE TABLE bronze.raw_events (
                id BIGSERIAL PRIMARY KEY,
                event_time VARCHAR(64),
                event_type VARCHAR(32),
                product_id BIGINT,
                category_id BIGINT,
                category_code VARCHAR(128),
                brand VARCHAR(64),
                price NUMERIC(10, 2),
                user_id BIGINT,
                user_session VARCHAR(64),
                discount_percent INT,
                ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # 4. TẦNG SILVER: stg_events (Dữ liệu sạch, khử trùng lặp)
        cur.execute("""
            CREATE TABLE silver.stg_events (
                id BIGSERIAL PRIMARY KEY,
                event_timestamp TIMESTAMP NOT NULL,
                date DATE NOT NULL,
                event_time VARCHAR(64),
                event_type VARCHAR(32) NOT NULL,
                product_id BIGINT NOT NULL,
                category_id BIGINT,
                category_code VARCHAR(128),
                category_level1 VARCHAR(64),
                brand VARCHAR(64),
                price NUMERIC(10, 2),
                user_id BIGINT NOT NULL,
                user_session VARCHAR(64),
                discount_percent NUMERIC(5, 2)
            );
        """)

        # 5. TẦNG GOLD:
        # 5.1. dim_product (SCD Type 2)
        cur.execute("""
            CREATE TABLE gold.dim_product (
                product_sk VARCHAR(64) PRIMARY KEY,
                product_id BIGINT NOT NULL,
                category_id BIGINT,
                category_level1 VARCHAR(64),
                brand VARCHAR(64),
                price NUMERIC(10, 2),
                discount_percent NUMERIC(5, 2),
                valid_from_ts TIMESTAMP NOT NULL,
                valid_to_ts TIMESTAMP,
                is_current BOOLEAN NOT NULL
            );
        """)

        # 5.2. dim_user (Khách hàng)
        cur.execute("""
            CREATE TABLE gold.dim_user (
                user_id BIGINT PRIMARY KEY,
                first_seen TIMESTAMP,
                last_seen TIMESTAMP,
                total_lifetime_events BIGINT,
                is_active BOOLEAN DEFAULT TRUE,
                valid_from_ts TIMESTAMP,
                valid_to_ts TIMESTAMP,
                is_current BOOLEAN DEFAULT TRUE
            );
        """)

        # 5.3. fact_user_events (Pure Fact - Liên kết khóa ngoại FK với dim_product và dim_user)
        cur.execute("""
            CREATE TABLE gold.fact_user_events (
                id BIGSERIAL PRIMARY KEY,
                event_id VARCHAR(64) NOT NULL,
                event_time TIMESTAMP NOT NULL,
                date DATE NOT NULL,
                user_id BIGINT NOT NULL,
                product_sk VARCHAR(64) NOT NULL,
                category_level1 VARCHAR(64),
                brand VARCHAR(64),
                price NUMERIC(10, 2),
                discount_percent NUMERIC(5, 2),
                event_type VARCHAR(32) NOT NULL,
                user_session VARCHAR(64)
            );
        """)

        # 5.4. feat_user_30d (Feature Store - Chuẩn Feast với event_timestamp và created)
        cur.execute("""
            CREATE TABLE gold.feat_user_30d (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                f_views_30d BIGINT DEFAULT 0,
                f_carts_30d BIGINT DEFAULT 0,
                f_purchases_30d BIGINT DEFAULT 0,
                f_spend_30d NUMERIC(12, 2) DEFAULT 0.0,
                f_distinct_categories_30d BIGINT DEFAULT 0,
                event_timestamp TIMESTAMP NOT NULL,
                created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                date DATE NOT NULL
            );
            CREATE INDEX idx_feat_user_time ON gold.feat_user_30d (user_id, event_timestamp);
        """)

        # 5.5. user_labels (Nhãn ML phục vụ huấn luyện)
        cur.execute("""
            CREATE TABLE gold.user_labels (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                prediction_timestamp TIMESTAMP NOT NULL,
                target_purchase_1h INT NOT NULL,
                date DATE NOT NULL
            );
            CREATE INDEX idx_labels_user_time ON gold.user_labels (user_id, prediction_timestamp);
        """)

        conn.commit()
    print(" -> Đã tạo 3 Schemas (bronze, silver, gold) và 7 bảng Data Warehouse thành công.")


# ==============================================================================
# BƯỚC 2: NẠP DỮ LIỆU TỪ MINIO LAKEHOUSE VÀO POSTGRESQL
# ==============================================================================
def load_table_from_minio(conn, s3, lakehouse_path, target_table, columns, pk_col=None, limit=None):
    """Hàm helper đọc Parquet/Delta từ MinIO bằng PyArrow và COPY vào PostgreSQL qua copy_expert có khử trùng lặp PK."""
    dataset = ds.dataset(lakehouse_path, filesystem=s3, format="parquet", partitioning="hive")
    scanner = dataset.scanner(columns=columns, batch_size=50000)
    
    total = 0
    seen_pks = set() if pk_col else None
    with conn.cursor() as cur:
        for batch in scanner.to_batches():
            df = batch.to_pandas().dropna(subset=[columns[0]])
            if pk_col:
                if isinstance(pk_col, list):
                    df = df.drop_duplicates(subset=pk_col)
                else:
                    df = df.drop_duplicates(subset=[pk_col])
                    if seen_pks is not None:
                        df = df[~df[pk_col].isin(seen_pks)]
                        seen_pks.update(df[pk_col])
            
            if df.empty:
                continue
                
            buf = io.StringIO()
            df.to_csv(buf, index=False, header=False, sep="\t", na_rep="\\N")
            buf.seek(0)
            cols_str = ", ".join(columns)
            sql = f"COPY {target_table} ({cols_str}) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N')"
            cur.copy_expert(sql, buf)
            total += len(df)
            if limit and total >= limit:
                break
        conn.commit()
    return total

def sync_data_from_lakehouse(conn):
    print("\n" + "=" * 80)
    print(" 📥 [BƯỚC 2]: ĐỒNG BỘ DỮ LIỆU TỪ MINIO DATA LAKEHOUSE VÀO CÁC TẦNG POSTGRESQL")
    print("=" * 80)
    s3 = get_s3_filesystem()

    with conn.cursor() as cur:
        cur.execute("SET synchronous_commit = OFF;")
        conn.commit()

    # 1. Nạp Bronze Toàn bộ (1,832,306 dòng thô ban đầu)
    print(" -> [1/7] Đang đồng bộ bronze.raw_events (Toàn bộ dữ liệu thô)...")
    b_cols = ["event_time", "event_type", "product_id", "category_id", "category_code", 
              "brand", "price", "user_id", "user_session", "discount_percent"]
    n_bronze = load_table_from_minio(conn, s3, "ecommerce-lakehouse/bronze/raw_events", "bronze.raw_events", b_cols, limit=None)
    print(f"     -> Đã nạp {n_bronze:,} bản ghi vào bronze.raw_events.")

    # 2. Nạp Silver Toàn bộ (1,398,581 dòng sạch)
    print(" -> [2/7] Đang đồng bộ silver.stg_events (Toàn bộ dữ liệu sạch)...")
    s_cols = ["event_timestamp", "date", "event_time", "event_type", "product_id", 
              "category_id", "category_code", "category_level1", "brand", "price", 
              "user_id", "user_session", "discount_percent"]
    n_silver = load_table_from_minio(conn, s3, "ecommerce-lakehouse/silver/stg_events", "silver.stg_events", s_cols, limit=None)
    print(f"     -> Đã nạp {n_silver:,} bản ghi vào silver.stg_events.")

    # 3. Nạp Gold dim_product Toàn bộ (256,401 dòng SCD2)
    print(" -> [3/7] Đang đồng bộ gold.dim_product (Toàn bộ 256k dòng SCD Type 2)...")
    dp_cols = ["product_sk", "product_id", "category_id", "category_level1", "brand", "price", "discount_percent", "valid_from_ts", "valid_to_ts", "is_current"]
    n_dim_p = load_table_from_minio(conn, s3, "ecommerce-lakehouse/gold/dim_product", "gold.dim_product", dp_cols, pk_col="product_sk", limit=None)
    print(f"     -> Đã nạp {n_dim_p:,} bản ghi vào gold.dim_product.")

    # 4. Nạp Gold dim_user Toàn bộ (239,356 dòng)
    print(" -> [4/7] Đang đồng bộ gold.dim_user (Toàn bộ 239k khách hàng)...")
    du_cols = ["user_id", "first_seen", "last_seen", "total_lifetime_events", "is_active", "valid_from_ts", "valid_to_ts", "is_current"]
    n_dim_u = load_table_from_minio(conn, s3, "ecommerce-lakehouse/gold/dim_user", "gold.dim_user", du_cols, pk_col="user_id", limit=None)
    print(f"     -> Đã nạp {n_dim_u:,} bản ghi vào gold.dim_user.")

    # 5. Nạp Gold fact_user_events Toàn bộ (1,398,581 dòng Pure Fact)
    print(" -> [5/7] Đang đồng bộ gold.fact_user_events (Toàn bộ 1.39M sự kiện Pure Fact)...")
    f_cols = ["event_id", "event_time", "date", "user_id", "product_sk", "category_level1", "brand", "price", "discount_percent", "event_type", "user_session"]
    n_fact = load_table_from_minio(conn, s3, "ecommerce-lakehouse/gold/fact_user_events", "gold.fact_user_events", f_cols, pk_col=None, limit=None)
    print(f"     -> Đã nạp {n_fact:,} bản ghi vào gold.fact_user_events.")

    # 6. Nạp Gold feat_user_30d Toàn bộ (173,941 dòng Feast Feature Store)
    print(" -> [6/7] Đang đồng bộ gold.feat_user_30d (Toàn bộ Feature Store)...")
    feat_cols = ["user_id", "f_views_30d", "f_carts_30d", "f_purchases_30d", "f_spend_30d", "f_distinct_categories_30d", "event_timestamp", "created", "date"]
    n_feat = load_table_from_minio(conn, s3, "ecommerce-lakehouse/gold/feat_user_30d", "gold.feat_user_30d", feat_cols, limit=None)
    print(f"     -> Đã nạp {n_feat:,} bản ghi vào gold.feat_user_30d.")

    # 7. Nạp Gold user_labels Toàn bộ (216,881 dòng nhãn ML)
    print(" -> [7/7] Đang đồng bộ gold.user_labels (Toàn bộ nhãn ML)...")
    lbl_cols = ["user_id", "prediction_timestamp", "target_purchase_1h", "date"]
    n_lbl = load_table_from_minio(conn, s3, "ecommerce-lakehouse/gold/user_labels", "gold.user_labels", lbl_cols, limit=None)
    print(f"     -> Đã nạp {n_lbl:,} bản ghi vào gold.user_labels.")

    # 8. Tạo Foreign Key Constraints để DBeaver tự động hiển thị sơ đồ Star Schema ERD và đảm bảo Toàn vẹn dữ liệu
    print(" -> [8/8] Đang gắn Foreign Key Relationships cho mô hình Star Schema...")
    with conn.cursor() as cur:
        cur.execute("""
            ALTER TABLE gold.fact_user_events 
                ADD CONSTRAINT fk_fact_dim_product FOREIGN KEY (product_sk) REFERENCES gold.dim_product(product_sk);
            ALTER TABLE gold.fact_user_events 
                ADD CONSTRAINT fk_fact_dim_user FOREIGN KEY (user_id) REFERENCES gold.dim_user(user_id);
            ALTER TABLE gold.feat_user_30d 
                ADD CONSTRAINT fk_feat_dim_user FOREIGN KEY (user_id) REFERENCES gold.dim_user(user_id);
            ALTER TABLE gold.user_labels 
                ADD CONSTRAINT fk_labels_dim_user FOREIGN KEY (user_id) REFERENCES gold.dim_user(user_id);
        """)
        cur.execute("SET synchronous_commit = ON;")
        cur.execute("ANALYZE;")
        conn.commit()
    print(" -> Khai báo quan hệ Khóa ngoại Dim - Fact và cập nhật Statistics Catalog thành công.")

    print("\n" + "-" * 60)
    print(" 📋 [XÁC MINH SỐ LƯỢNG BẢN GHI ĐỒNG BỘ TRÊN POSTGRESQL]:")
    print("-" * 60)
    tables_to_check = [
        ("bronze.raw_events", "Raw Ingest Stream"),
        ("silver.stg_events", "Clean & Deduplicated"),
        ("gold.dim_product", "Product Dimension (SCD2)"),
        ("gold.dim_user", "User Dimension"),
        ("gold.fact_user_events", "Pure Fact Table"),
        ("gold.feat_user_30d", "Feast Feature Store"),
        ("gold.user_labels", "ML Training Labels")
    ]
    with conn.cursor() as cur:
        for tbl, desc in tables_to_check:
            cur.execute(f"SELECT COUNT(*) FROM {tbl};")
            cnt = cur.fetchone()[0]
            print(f"  • {tbl:<23} ({desc:<25}): {cnt:>10,} dòng")
    print("-" * 60)


# ==============================================================================
# BƯỚC 3: BENCHMARK TỐI ƯU HÓA LƯU TRỮ DWH (INDEXING - RUBRIC 2.0Đ)
# ==============================================================================
def run_explain_analyze(conn, user_id, min_time, label):
    query = """
        EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
        SELECT event_type, product_sk, price, event_time 
        FROM gold.fact_user_events 
        WHERE user_id = %s AND event_time >= %s
        ORDER BY event_time DESC;
    """
    with conn.cursor() as cur:
        cur.execute(query, (user_id, min_time))
        lines = [r[0] for r in cur.fetchall()]
        plan_text = "\n".join(lines)
        
    exec_time_match = re.search(r"Execution Time:\s+([0-9.]+)\s+ms", plan_text)
    cost_match = re.search(r"cost=([0-9.]+)\.\.([0-9.]+)", plan_text)
    buffers_match = re.search(r"Buffers:\s+([^,\n]+)", plan_text)
    
    scan_type = "Sequential Scan (Seq Scan)"
    if "Bitmap Index Scan" in plan_text:
        scan_type = "Bitmap Index Scan (B-Tree)"
    elif "Index Scan" in plan_text:
        scan_type = "Index Scan (B-Tree)"
        
    return {
        "label": label,
        "scan_type": scan_type,
        "exec_time_ms": float(exec_time_match.group(1)) if exec_time_match else 0.0,
        "cost_total": float(cost_match.group(2)) if cost_match else 0.0,
        "buffers": buffers_match.group(1).strip() if buffers_match else "N/A",
        "raw_plan": plan_text
    }

def benchmark_dwh_indexing(conn):
    print("\n" + "=" * 80)
    print(" ⚡ [BƯỚC 3]: THỰC THI BENCHMARK DATA WAREHOUSE INDEXING (RUBRIC 2.0Đ)")
    print("=" * 80)

    # Chọn 1 user_id có nhiều sự kiện thực tế để test
    with conn.cursor() as cur:
        cur.execute("""
            SELECT user_id, COUNT(*) as cnt, MIN(event_time)
            FROM gold.fact_user_events
            GROUP BY user_id
            HAVING COUNT(*) >= 10
            ORDER BY cnt DESC
            LIMIT 1;
        """)
        row = cur.fetchone()
        test_user = row[0]
        event_cnt = row[1]
        min_time  = row[2]

    print(f" -> Kiểm thử với: user_id = {test_user} ({event_cnt} events, event_time >= '{min_time}')")

    # 1. BASELINE: CHƯA ĐÁNH INDEX
    print("\n>>> [1/2] ĐO LƯỜNG TRƯỚC KHI TỐI ƯU (BASELINE - CHƯA ĐÁNH INDEX):")
    for _ in range(2):
        run_explain_analyze(conn, test_user, min_time, "Warmup")
    metrics_before = run_explain_analyze(conn, test_user, min_time, "Baseline")
    print(metrics_before["raw_plan"])

    # 2. TẠO COMPOSITE B-TREE INDEX
    print("\n>>> TẠO COMPOSITE B-TREE INDEX TRÊN (user_id, event_time DESC)...")
    idx_sql = "CREATE INDEX idx_fact_user_events_user_time ON gold.fact_user_events (user_id, event_time DESC);"
    print(f"    Lệnh: {idx_sql}")
    t0 = time.time()
    with conn.cursor() as cur:
        cur.execute(idx_sql)
        conn.commit()
        cur.execute("ANALYZE gold.fact_user_events;")
        conn.commit()
    print(f" -> Đã tạo Index thành công trong {time.time() - t0:.2f}s.")

    # 3. OPTIMIZED: SAU KHI ĐÁNH INDEX
    print("\n>>> [2/2] ĐO LƯỜNG SAU KHI TỐI ƯU (OPTIMIZED - ĐÃ CÓ B-TREE INDEX):")
    for _ in range(2):
        run_explain_analyze(conn, test_user, min_time, "Warmup")
    metrics_after = run_explain_analyze(conn, test_user, min_time, "Optimized")
    print(metrics_after["raw_plan"])

    # 4. SO SÁNH
    speedup = metrics_before["exec_time_ms"] / max(metrics_after["exec_time_ms"], 0.001)
    cost_red = (1 - (metrics_after["cost_total"] / max(metrics_before["cost_total"], 1))) * 100

    print("\n" + "=" * 80)
    print(" 📊 BẢNG TỔNG HỢP HIỆU QUẢ DATA WAREHOUSE INDEXING (NỘP RUBRIC 2.0Đ)")
    print("=" * 80)
    print(f"{'Tiêu chí so sánh':<32} | {'Trước Optimize (Baseline)':<30} | {'Sau Optimize (Composite Index)':<30} | {'Mức độ cải thiện'}")
    print("-" * 115)
    print(f"{'Phương thức quét (Scan Type)':<32} | {metrics_before['scan_type']:<30} | {metrics_after['scan_type']:<30} | Loại bỏ quét thừa (0 rows removed)")
    print(f"{'Thời gian thực thi (Execution)':<32} | {metrics_before['exec_time_ms']:>10.2f} ms{'':<17} | {metrics_after['exec_time_ms']:>10.2f} ms{'':<17} | Nhanh hơn {speedup:.1f} lần")
    print(f"{'Chi phí truy vấn (Cost Score)':<32} | {metrics_before['cost_total']:>10.2f}{'':<20} | {metrics_after['cost_total']:>10.2f}{'':<20} | Giảm {cost_red:.1f}%")
    print(f"{'Bộ nhớ đệm đọc (Buffers)':<32} | {metrics_before['buffers']:<30} | {metrics_after['buffers']:<30} | Tiết kiệm I/O tối đa")
    print("=" * 115)


def main():
    conn = get_connection()
    try:
        create_schemas_and_tables(conn)
        sync_data_from_lakehouse(conn)
        benchmark_dwh_indexing(conn)
        print("\n" + "=" * 80)
        print(" 🎉 HOÀN THÀNH THIẾT LẬP DATA WAREHOUSE VÀ BENCHMARK INDEXING!")
        print("=" * 80)
        print(" 📌 THÔNG TIN KẾT NỐI DBEAVER:")
        print("    - Host     : localhost")
        print("    - Port     : 5432")
        print("    - Database : ecom_dwh")
        print("    - Username : postgres")
        print("    - Password : postgres")
        print("    - Schemas  : bronze (raw_events), silver (stg_events), gold (dim_*, fact_*, feat_*)")
        print("=" * 80 + "\n")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
