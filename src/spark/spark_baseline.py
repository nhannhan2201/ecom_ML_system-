"""Spark Batch Processing Baseline (Unoptimized).

Executes batch processing without optimizations to demonstrate bottlenecks:
- Disables AQE, Skew Join handling, and Broadcast Joins (forces Sort-Merge Join).
- Skips deduplication, high-cardinality approximation, and schema enforcement.
- Serves as the baseline comparison point for Spark optimization experiments.
"""

import os
import sys
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [SPARK-BASELINE] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("SparkBaseline")


def create_baseline_spark_session() -> SparkSession:
    """
    Khởi tạo SparkSession ở chế độ BASELINE (Tắt toàn bộ cơ chế tối ưu).
    """
    logger.info(">>> Đang khởi tạo SparkSession chế độ BASELINE (TẮT TẤT CẢ TỐI ƯU)...")

    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY") or os.environ.get("AWS_ACCESS_KEY_ID")
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY") or os.environ.get("AWS_SECRET_ACCESS_KEY")
    if not minio_access_key:
        raise ValueError("Missing required environment variable: 'MINIO_ACCESS_KEY' (or 'AWS_ACCESS_KEY_ID')")
    if not minio_secret_key:
        raise ValueError("Missing required environment variable: 'MINIO_SECRET_KEY' (or 'AWS_SECRET_ACCESS_KEY')")

    spark = (
        SparkSession.builder.appName("ECom-Spark-Offline-Baseline")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "io.delta:delta-spark_2.12:3.0.0",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # [BASELINE ANTI-PATTERNS]:
        .config("spark.sql.adaptive.enabled", "false")  # Tắt AQE
        .config("spark.sql.adaptive.skewJoin.enabled", "false")  # Tắt xử lý Skew
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")  # Tắt Broadcast Join
        .config("spark.sql.shuffle.partitions", "200")  # Giữ mặc định 200 partitions
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info(" SparkSession Baseline đã khởi tạo thành công.")
    logger.info(" Spark UI đang lắng nghe tại: http://localhost:4040")
    return spark


def run_baseline_pipeline():
    """
    Thực thi toàn bộ luồng xử lý Baseline và ghi nhận các điểm nghẽn hiệu năng.
    """
    total_start_time = time.time()
    spark = create_baseline_spark_session()

    minio_raw_base = "s3a://ecommerce-raw/batch"
    minio_output_baseline = "s3a://ecommerce-lakehouse/baseline"

    print("\n" + "=" * 80)
    print(" BẮT ĐẦU CHẠY SPARK OFFLINE PIPELINE - BASELINE (WITHOUT OPTIMIZATION)")
    print("=" * 80)

    # --------------------------------------------------------------------------
    # BƯỚC 1: ĐỌC DỮ LIỆU THÔ & MINH HỌA VẤN ĐỀ SCHEMA EVOLUTION
    # --------------------------------------------------------------------------
    logger.info("[BƯỚC 1/5]: Đọc dữ liệu batch thô từ MinIO và kiểm tra Schema...")
    step1_start = time.time()

    # Cách đọc ngây thơ (Baseline): đọc riêng rẽ từng file bằng spark.read.csv() thông thường
    old_csv_path = f"{minio_raw_base}/raw_events_old*.csv"
    new_csv_path = f"{minio_raw_base}/raw_events_new*.csv"

    df_old = spark.read.csv(old_csv_path, header=True, inferSchema=True)
    df_new = spark.read.csv(new_csv_path, header=True, inferSchema=True)

    old_cols = set(df_old.columns)
    new_cols = set(df_new.columns)
    diff_cols = new_cols - old_cols

    logger.warning(" [VẤN ĐỀ SCHEMA EVOLUTION PHÁT HIỆN]:")
    logger.warning(f"  - Tập dữ liệu cũ (01/10-15/10) có {len(old_cols)} cột: {sorted(list(old_cols))}")
    logger.warning(f"  - Tập dữ liệu mới (16/10-25/10) có {len(new_cols)} cột: {sorted(list(new_cols))}")
    logger.warning(f"  - Cột mới xuất hiện thêm trong schema: {diff_cols}")

    # Trong Baseline: Không dùng Delta Lake mergeSchema, mà dùng unionByName(allowMissingColumns=True)
    # hoặc gộp cưỡng bức khiến cột mới có giá trị null ở toàn bộ dữ liệu cũ mà không có metadata tracking.
    df_raw = df_old.unionByName(df_new, allowMissingColumns=True)
    raw_count = df_raw.count()
    logger.info(f"-> Tổng số dòng đọc được từ 2 file: {raw_count:,} dòng (Thời gian: {time.time() - step1_start:.2f}s)")

    # --------------------------------------------------------------------------
    # BƯỚC 2: MINH HỌA VẤN ĐỀ OFFLINE DUPLICATE (2% RÁC KHÔNG ĐƯỢC LÀM SẠCH)
    # --------------------------------------------------------------------------
    logger.info("[BƯỚC 2/5]: Kiểm tra vấn đề Offline Duplicate...")
    step2_start = time.time()

    # Trong Baseline: HOÀN TOÀN BỎ QUA BƯỚC DEDUPLICATION
    approx_distinct_events = df_raw.select("user_id", "event_time", "product_id", "event_type").distinct().count()
    duplicate_count = raw_count - approx_distinct_events
    duplicate_rate = (duplicate_count / raw_count) * 100

    logger.warning(" [VẤN ĐỀ OFFLINE DUPLICATE PHÁT HIỆN]:")
    logger.warning(f"  - Tổng số bản ghi thô: {raw_count:,}")
    logger.warning(f"  - Số bản ghi duy nhất: {approx_distinct_events:,}")
    logger.warning(f"  - Số bản ghi trùng lặp (Duplicate rác): {duplicate_count:,} (~{duplicate_rate:.2f}%)")
    logger.warning("  - BASELINE ACTION: Giữ nguyên toàn bộ duplicate trong pipeline, không lọc rác!")
    logger.info(f"  - Thời gian kiểm tra: {time.time() - step2_start:.2f}s")

    # --------------------------------------------------------------------------
    # BƯỚC 3: MINH HỌA VẤN ĐỀ HIGH CARDINALITY & SHUFFLE SPILL (DISK/MEMORY)
    # --------------------------------------------------------------------------
    logger.info("[BƯỚC 3/5]: Thực hiện gom nhóm trên cột High Cardinality (category_id 4 cấp)...")
    step3_start = time.time()

    # Anti-Pattern trong Baseline:
    # Cột `category_id` sâu 4 cấp (appliances.kitchen.refrigerators...) có hàng chục ngàn giá trị rời rạc.
    # Baseline thực hiện COUNT(DISTINCT category_id) và COUNT(DISTINCT user_session) trực tiếp
    # mà không rút gọn cấp 1 (level 1) và không dùng HyperLogLog (approx_count_distinct).
    # Với RAM 1GB và shuffle partitions 200, Spark buộc phải SPILL hash-table ra Disk!

    logger.info(" Đang thực thi phép gom nhóm High Cardinality nặng (gây Shuffle Spill trên Spark UI)...")

    high_card_agg = df_raw.groupBy("user_id").agg(
        F.count("event_type").alias("total_events"),
        F.countDistinct("category_id").alias("n_distinct_deep_categories"),
        F.countDistinct("user_session").alias("n_distinct_sessions"),
        F.sum("price").alias("gross_spend"),
    )

    # Trigger action để Spark thực hiện shuffle toàn diện
    high_card_count = high_card_agg.count()
    step3_duration = time.time() - step3_start
    logger.info(
        f"-> Hoàn thành gom nhóm High Cardinality cho {high_card_count:,} users. Thời gian: {step3_duration:.2f}s"
    )
    logger.warning(
        " [KIỂM TRA SPARK UI]: Vào Stage Details của Stage vừa xong -> Cột 'Shuffle Spill (Memory)' & 'Shuffle Spill (Disk)'"
    )

    # --------------------------------------------------------------------------
    # BƯỚC 4: MINH HỌA VẤN ĐỀ DATA SKEW & TASK STRAGGLER
    # --------------------------------------------------------------------------
    logger.info("[BƯỚC 4/5]: Thực hiện Join dữ liệu bị Skew nặng (Tạo Task Straggler trên Spark UI)...")
    step4_start = time.time()

    # Dữ liệu REES46 bị skew cực nặng: ngành hàng 'electronics.smartphone' chiếm ~40% tổng dữ liệu.
    # Trong Baseline:
    # 1. Tắt Broadcast Join -> Buộc Spark phải Shuffle Hash Join toàn bộ dữ liệu.
    # 2. Tắt AQE Skew Join -> Spark không tự động chia nhỏ partition bị skew.
    # 3. Không có Salting Key -> Partition chứa key hot 'electronics.smartphone' phải gánh
    #    khối lượng công việc gấp hàng chục lần các partition khác!

    # Tạo một dimension table giả lập từ chính danh mục sản phẩm (Product Catalog Metadata)
    dim_categories = (
        df_raw.select("category_code")
        .filter(F.col("category_code").isNotNull())
        .distinct()
        .withColumn("category_tax_rate", F.when(F.col("category_code").like("electronics%"), 0.10).otherwise(0.05))
        .withColumn(
            "category_priority", F.when(F.col("category_code").like("electronics%"), "HIGH").otherwise("NORMAL")
        )
    )

    logger.info(" Đang thực hiện Shuffle Join trên key bị Skew (category_code) mà KHÔNG có AQE / Salting...")

    # Thực hiện phép SortMergeJoin / ShuffleHashJoin trên category_code
    skewed_joined_df = df_raw.join(dim_categories, on="category_code", how="inner")

    # Tính toán tổng hợp nặng trên từng ngành hàng để bộc lộ rõ Task Straggler
    category_summary = skewed_joined_df.groupBy("category_code", "category_priority").agg(
        F.count("*").alias("event_count"), F.sum("price").alias("total_revenue"), F.avg("price").alias("avg_price")
    )

    # Trigger action để Spark ghi nhận Stage Timeline bị lệch (Straggler task kéo dài)
    category_summary.write.mode("overwrite").format("parquet").save(f"{minio_output_baseline}/category_summary/")
    step4_duration = time.time() - step4_start
    logger.info(f"-> Hoàn thành Skewed Shuffle Join. Thời gian: {step4_duration:.2f}s")
    logger.warning(
        " [KIỂM TRA SPARK UI]: Vào Stage Details -> Mở tab 'Event Timeline' để thấy Task Straggler bị lệch thời gian!"
    )

    # --------------------------------------------------------------------------
    # BƯỚC 5: TÍNH TOÁN BẢNG ROLLING WINDOW 30 NGÀY (FEAT_USER_30D) CHƯA TỐI ƯU
    # --------------------------------------------------------------------------
    logger.info("[BƯỚC 5/5]: Tính toán bảng Batch Features 30 ngày (feat_user_30d) theo cách Baseline...")
    step5_start = time.time()

    prediction_date = "2019-10-15"

    # Tạo View tạm để query
    df_raw.createOrReplaceTempView("raw_events_baseline")

    # SQL tính toán 5 batch feature theo đúng định dạng Rubric, nhưng tính trên dữ liệu CÓ DUPLICATE và CHƯA RÚT GỌN CẤP
    feat_user_30d_baseline = spark.sql(f"""
        SELECT
            user_id,
            COUNT(CASE WHEN event_type = 'view' THEN 1 END) AS f_views_30d,
            COUNT(CASE WHEN event_type = 'cart' THEN 1 END) AS f_carts_30d,
            COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS f_purchases_30d,
            SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) AS f_spend_30d,
            COUNT(DISTINCT category_id) AS f_distinct_categories_30d,
            CAST('{prediction_date}' AS TIMESTAMP) AS event_timestamp,
            current_timestamp() AS created
        FROM raw_events_baseline
        WHERE event_time >= date_sub(CAST('{prediction_date}' AS DATE), 30)
          AND event_time < CAST('{prediction_date}' AS DATE)
        GROUP BY user_id
    """)

    # Ghi ra MinIO Lakehouse Baseline (Không tối ưu partition, không nén, không Delta Log optimize)
    feat_output_path = f"{minio_output_baseline}/feat_user_30d/"
    feat_user_30d_baseline.write.mode("overwrite").parquet(feat_output_path)
    step5_duration = time.time() - step5_start
    logger.info(f"-> Đã ghi feat_user_30d (Baseline) ra {feat_output_path}. Thời gian: {step5_duration:.2f}s")

    total_duration = time.time() - total_start_time

    # --------------------------------------------------------------------------
    # TỔNG HỢP KẾT QUẢ VÀ HƯỚNG DẪN CHỤP MINH CHỨNG SPARK UI
    # --------------------------------------------------------------------------
    print("\n" + "=" * 80)
    print(" BẢNG TỔNG HỢP KẾT QUẢ CHẠY SPARK BASELINE (WITHOUT OPTIMIZATION)")
    print("=" * 80)
    print(f" 1. Tổng thời gian chạy toàn bộ Baseline Job: {total_duration:.2f} giây")
    print(f" 2. Dữ liệu đầu vào: {raw_count:,} dòng (bao gồm {duplicate_count:,} dòng rác trùng lặp)")
    print(f" 3. Vấn đề Duplicate: TỶ LỆ TRÙNG LẶP = {duplicate_rate:.2f}% (Chưa được xử lý)")
    print(" 4. Vấn đề Schema Evolution: Cột 'discount_percent' bị thiếu trong nửa đầu dữ liệu")
    print(" 5. Vấn đề High Cardinality: COUNT(DISTINCT category_id) gây Shuffle Spill Memory & Disk")
    print(" 6. Vấn đề Data Skew: Key 'electronics.smartphone' gây Task Straggler nghẽn Stage")
    print("=" * 80)
    print("\n HUONG DAN KIEM TRA SPARK UI (PORT 4040):")
    print("  * Bước 1: Mở trình duyệt truy cập: http://localhost:4040")
    print("  * Bước 2: Vào tab 'Jobs' -> Chụp lại danh sách các Completed Jobs và thời gian.")
    print("  * Bước 3: Vào tab 'Stages' -> Nhấp vào Stage có Shuffle Read lớn nhất.")
    print("  * Bước 4: Mở rộng mục 'Event Timeline':")
    print("    - Xem thanh tiến trình của các Task: Các task bình thường kết thúc rất sớm,")
    print("      trong khi Task xử lý key Skew (Straggler) kéo dài tới cuối Stage.")
    print("  * Bước 5: Xem bảng 'Summary Metrics':")
    print("    - Cột Max vs 75th percentile của Duration và Shuffle Read Size có độ lệch cực lớn.")
    print("    - Xem các cột 'Shuffle Spill (Memory)' và 'Shuffle Spill (Disk)' có xuất hiện dung lượng.")
    print("=" * 80 + "\n")

    # Giữ SparkSession sống để người dùng trực tiếp mở trình duyệt kiểm tra Spark UI
    print("\n" + "=" * 80)
    print(" SPARK UI ĐANG CHẠY TẠI: http://localhost:4040")
    print(" Anh hãy mở trình duyệt vào http://localhost:4040 để xem các Stage, Task Straggler và Shuffle Spill.")
    print(" Sau khi xem và chụp ảnh xong, hãy quay lại đây nhấn [ENTER] để kết thúc chương trình.")
    print("=" * 80 + "\n")
    try:
        input(">>> Nhấn [ENTER] trên bàn phím để dừng SparkSession...")
    except (EOFError, KeyboardInterrupt):
        logger.info("Đang dừng SparkSession...")

    spark.stop()
    logger.info(" Đã dừng SparkSession Baseline an toàn.")


if __name__ == "__main__":
    run_baseline_pipeline()
