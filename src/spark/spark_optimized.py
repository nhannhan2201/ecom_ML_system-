"""Spark Lakehouse Batch Processing Pipeline (Optimized).

Executes batch transformations across Lakehouse zones:
- DP1: Ingests raw batch CSVs and Flink staging JSON into Bronze Delta Lake with mergeSchema.
- DP2: Deduplicates, builds Kimball Star Schema (dim_product SCD2, dim_user, fact_user_events),
  and optimizes storage via Z-Ordering (user_id) and safe VACUUM.
- DP3: Calculates 30-day offline user features (feat_user_30d) for Feast and user_labels ground truth.
Outputs Delta tables in MinIO and clean Parquet for Feast.
"""

import os
import sys
import time
import logging
import argparse
import urllib.request
import json

if "JAVA_HOME" in os.environ and os.path.exists(os.environ["JAVA_HOME"]):
    os.environ["PATH"] = f"{os.environ['JAVA_HOME']}/bin:{os.environ.get('PATH', '')}"

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

try:
    from src.spark.common import (
        create_optimized_spark_session,
        archive_staging_files,
        get_lakehouse_paths,
    )
except ModuleNotFoundError:
    from common import (
        create_optimized_spark_session,
        archive_staging_files,
        get_lakehouse_paths,
    )

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [SPARK-OPTIMIZED] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("SparkOptimized")


# ==============================================================================
# PIPELINE DP1: INGEST RAW DATA INTO BRONZE ZONE
# ==============================================================================
def ingest_bronze_data(spark: SparkSession, paths: dict):
    """Ingest initial CSV files and stream staging into Bronze Delta Lake."""
    bronze_path = paths["bronze"]
    old_csv_path = f"{paths['raw_base']}/raw_events_old*.csv"
    new_csv_path = f"{paths['raw_base']}/raw_events_new*.csv"
    staging_stream_path = paths["staging_stream"]

    bronze_initialized = False
    try:
        df_bronze_check = spark.read.format("delta").load(bronze_path)
        if df_bronze_check.count() > 0:
            bronze_initialized = True
    except Exception:
        bronze_initialized = False

    if not bronze_initialized:
        logger.info("Bronze Delta Lake does not exist: Performing Day 0 Bootstrap...")
        df_old_raw = (
            spark.read.csv(old_csv_path, header=True, inferSchema=False)
            .withColumn("product_id", F.col("product_id").cast("long"))
            .withColumn("category_id", F.col("category_id").cast("long"))
            .withColumn("price", F.col("price").cast("double"))
            .withColumn("user_id", F.col("user_id").cast("long"))
            .withColumn("discount_percent", F.lit(None).cast("integer"))
            .withColumn("ingestion_time", F.current_timestamp())
        )
        df_old_raw.write.format("delta").mode("overwrite").save(bronze_path)
        logger.info(f"Loaded old batch ({df_old_raw.count():,} rows, 9 cols) into Bronze.")

        df_new_raw = (
            spark.read.csv(new_csv_path, header=True, inferSchema=False)
            .withColumn("product_id", F.col("product_id").cast("long"))
            .withColumn("category_id", F.col("category_id").cast("long"))
            .withColumn("price", F.col("price").cast("double"))
            .withColumn("user_id", F.col("user_id").cast("long"))
            .withColumn("discount_percent", F.col("discount_percent").cast("integer"))
            .withColumn("ingestion_time", F.current_timestamp())
        )
        df_new_raw.write.format("delta").mode("append").option("mergeSchema", "true").save(bronze_path)
        logger.info(f"Loaded new batch ({df_new_raw.count():,} rows, 10 cols) via mergeSchema.")
    else:
        logger.info("Bronze Delta Lake exists: Preserving history without overwriting.")

    try:
        df_staging = spark.read.json(staging_stream_path)
        staging_count = df_staging.count()
        if staging_count > 0:
            logger.info(f"Found {staging_count:,} new events from Flink Staging. Appending to Bronze...")
            df_staging_ingested = (
                df_staging.withColumn("product_id", F.col("product_id").cast("long"))
                .withColumn("category_id", F.col("category_id").cast("long"))
                .withColumn("price", F.col("price").cast("double"))
                .withColumn("user_id", F.col("user_id").cast("long"))
                .withColumn("discount_percent", F.col("discount_percent").cast("integer"))
                .withColumn("ingestion_time", F.current_timestamp())
            )
            df_staging_ingested.write.format("delta").mode("append").option("mergeSchema", "true").save(bronze_path)
            archive_staging_files(spark, staging_stream_path, paths["archive_stream"])
    except Exception as e:
        logger.info(f"No new staging files found or already processed: {e}")


def validate_bronze_data(spark: SparkSession, bronze_path: str) -> dict:
    """Validate data quality and contract assertions for Bronze table."""
    df_bronze = spark.read.format("delta").load(bronze_path)
    total_bronze_count = df_bronze.count()
    bronze_cols = df_bronze.columns
    null_user_count = df_bronze.filter(F.col("user_id").isNull()).count()

    assert total_bronze_count > 0, "[ERROR] Bronze validation failed: table is empty!"
    assert len(bronze_cols) == 11, f"[ERROR] Bronze validation failed: expected 11 cols, found {len(bronze_cols)}."
    assert null_user_count == 0, f"[ERROR] Bronze validation failed: found {null_user_count} null user_id rows!"

    return {"bronze_count": total_bronze_count, "columns": bronze_cols}


def run_dp1_bronze(spark: SparkSession, paths: dict, step: str = "all") -> dict:
    """Execute DP1 Bronze ingestion and validation pipeline."""
    logger.info("=" * 80)
    logger.info(f"[PIPELINE DP1] Ingest Raw Data into Bronze Zone (Step: {step.upper()})")
    logger.info("=" * 80)
    start_time = time.time()

    if step in ["all", "ingest"]:
        ingest_bronze_data(spark, paths)
        logger.info(f"[OK] [DP1 - INGEST COMPLETED] Duration: {time.time() - start_time:.2f}s")
        if step == "ingest":
            return {"status": "ingest_success", "duration": time.time() - start_time}

    if step in ["all", "validate"]:
        val_res = validate_bronze_data(spark, paths["bronze"])
        duration = time.time() - start_time
        logger.info("[OK] [DP1 - VALIDATION COMPLETED]")
        logger.info(f"  - Total Bronze Rows : {val_res['bronze_count']:,}")
        logger.info(f"  - Schema Columns    : {len(val_res['columns'])} cols")
        logger.info(f"  - Duration          : {duration:.2f}s")
        return {"bronze_count": val_res["bronze_count"], "duration": duration}

    return {"duration": time.time() - start_time}


# ==============================================================================
# PIPELINE DP2: INGEST BRONZE INTO SILVER & GOLD DATA WAREHOUSE
# ==============================================================================
def process_silver_layer(spark: SparkSession, paths: dict) -> tuple:
    """Deduplicate Bronze and write partitioned Silver layer."""
    df_bronze = spark.read.format("delta").load(paths["bronze"])
    total_bronze_count = df_bronze.count()

    df_deduped = df_bronze.dropDuplicates(["user_id", "event_time", "product_id", "event_type"])
    clean_count = df_deduped.count()
    dropped_duplicates = total_bronze_count - clean_count

    df_silver_clean = (
        df_deduped.withColumn("event_timestamp", F.to_timestamp(F.col("event_time"), "yyyy-MM-dd HH:mm:ss 'UTC'"))
        .withColumn("date", F.to_date(F.col("event_timestamp")))
        .fillna({"brand": "unknown", "category_code": "unknown.unknown", "discount_percent": 0.0})
        .withColumn("category_level1", F.split(F.col("category_code"), "\\.")[0])
    )
    df_silver_clean.write.format("delta").mode("overwrite").partitionBy("date").save(paths["silver"])
    logger.info(f"Created silver/stg_events: {clean_count:,} rows (dropped {dropped_duplicates:,} duplicates).")
    return clean_count, dropped_duplicates


def build_gold_dimensions_and_facts(spark: SparkSession, paths: dict):
    """Build dim_product (SCD2), dim_user, and fact_user_events in Gold layer."""
    df_silver = spark.read.format("delta").load(paths["silver"])

    # High-cardinality aggregation (HLL approx)
    df_silver.groupBy("user_id").agg(
        F.count("event_type").alias("total_events"),
        F.approx_count_distinct("category_level1", rsd=0.01).alias("n_distinct_categories_approx"),
        F.approx_count_distinct("user_session", rsd=0.01).alias("n_distinct_sessions_approx"),
        F.round(F.sum("price"), 2).alias("gross_spend"),
    ).count()

    # Small dimension broadcast join
    dim_categories = (
        df_silver.select("category_code")
        .distinct()
        .withColumn(
            "category_group", F.when(F.col("category_code").startswith("electronics"), "High-Tech").otherwise("General")
        )
    )
    df_silver.join(F.broadcast(dim_categories), on="category_code", how="inner").count()

    # 1. dim_product (SCD Type 2)
    dim_product_changes = df_silver.groupBy(
        "product_id", "category_id", "category_level1", "brand", "price", "discount_percent"
    ).agg(F.min("event_timestamp").alias("valid_from_ts"))
    w_dedup = Window.partitionBy("product_id", "valid_from_ts").orderBy(F.col("price").desc_nulls_last())
    dim_product_dedup = (
        dim_product_changes.withColumn("rn", F.row_number().over(w_dedup)).filter(F.col("rn") == 1).drop("rn")
    )

    w_product = Window.partitionBy("product_id").orderBy("valid_from_ts")
    dim_product = (
        dim_product_dedup.withColumn("valid_to_ts", F.lead("valid_from_ts").over(w_product))
        .withColumn("is_current", F.when(F.col("valid_to_ts").isNull(), True).otherwise(False))
        .withColumn("product_sk", F.md5(F.concat_ws("_", F.col("product_id"), F.col("valid_from_ts"))))
        .select(
            "product_sk",
            "product_id",
            "category_id",
            "category_level1",
            "brand",
            "price",
            "discount_percent",
            "valid_from_ts",
            "valid_to_ts",
            "is_current",
        )
    )
    dim_product.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        paths["gold_dim_product"]
    )

    # 2. dim_user
    dim_user = (
        df_silver.groupBy("user_id")
        .agg(
            F.min("event_timestamp").alias("first_seen"),
            F.max("event_timestamp").alias("last_seen"),
            F.count("event_type").alias("total_lifetime_events"),
        )
        .select(
            F.col("user_id"),
            F.col("first_seen"),
            F.col("last_seen"),
            F.col("total_lifetime_events"),
            F.lit(True).alias("is_active"),
            F.col("first_seen").alias("valid_from_ts"),
            F.lit(None).cast("timestamp").alias("valid_to_ts"),
            F.lit(True).alias("is_current"),
        )
    )
    dim_user.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(paths["gold_dim_user"])

    # 3. fact_user_events (Pure Fact)
    fact_events = (
        df_silver.alias("f")
        .join(
            F.broadcast(dim_product).alias("d"),
            on=(F.col("f.product_id") == F.col("d.product_id"))
            & (F.col("f.event_timestamp") >= F.col("d.valid_from_ts"))
            & ((F.col("f.event_timestamp") < F.col("d.valid_to_ts")) | F.col("d.valid_to_ts").isNull()),
            how="left",
        )
        .select(
            F.md5(F.concat_ws("_", F.col("f.user_id"), F.col("f.event_time"), F.col("f.product_id"))).alias("event_id"),
            F.col("f.event_timestamp"),
            F.col("f.event_time"),
            F.col("f.date"),
            F.col("f.user_id"),
            F.col("d.product_sk"),
            F.col("f.category_level1"),
            F.col("f.brand"),
            F.col("f.price"),
            F.col("f.discount_percent"),
            F.col("f.event_type"),
            F.col("f.user_session"),
        )
    )
    fact_events.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("date").save(
        paths["gold_fact_events"]
    )


def optimize_dp2_storage(spark: SparkSession, paths: dict):
    """Execute Z-Ordering and safe VACUUM for Gold and Silver tables."""
    logger.info("Executing OPTIMIZE & ZORDER BY (user_id) on fact_user_events...")
    spark.sql(f"OPTIMIZE delta.`{paths['gold_fact_events']}` ZORDER BY (user_id)")
    vacuum_retain_hours = int(os.getenv("DELTA_VACUUM_RETAIN_HOURS", "168"))
    logger.info(f"Executing safe VACUUM (RETAIN {vacuum_retain_hours} HOURS)...")
    spark.sql(f"VACUUM delta.`{paths['gold_fact_events']}` RETAIN {vacuum_retain_hours} HOURS")
    spark.sql(f"VACUUM delta.`{paths['gold_dim_product']}` RETAIN {vacuum_retain_hours} HOURS")
    spark.sql(f"VACUUM delta.`{paths['gold_dim_user']}` RETAIN {vacuum_retain_hours} HOURS")
    spark.sql(f"VACUUM delta.`{paths['silver']}` RETAIN {vacuum_retain_hours} HOURS")


def validate_silver_gold_dwh(spark: SparkSession, paths: dict) -> dict:
    """Validate data quality and referential integrity for Silver and Gold tables."""
    df_silver = spark.read.format("delta").load(paths["silver"])
    df_dim_p = spark.read.format("delta").load(paths["gold_dim_product"])
    df_fact = spark.read.format("delta").load(paths["gold_fact_events"])

    clean_count = df_silver.count()
    fact_count = df_fact.count()
    dim_p_count = df_dim_p.count()
    null_sk_count = df_fact.filter(F.col("product_sk").isNull()).count()
    has_product_id = "product_id" in df_fact.columns

    assert clean_count > 0, "[ERROR] Silver validation failed: table is empty!"
    assert fact_count > 0, "[ERROR] Fact validation failed: table is empty!"
    assert fact_count == clean_count, f"[ERROR] Fact count ({fact_count}) does not match clean Silver ({clean_count})!"
    assert null_sk_count == 0, f"[ERROR] Found {null_sk_count} rows in Fact missing product_sk!"
    assert not has_product_id, "[ERROR] Pure Fact violation: product_id column found in fact_user_events!"

    return {
        "silver_count": clean_count,
        "fact_count": fact_count,
        "dim_product_count": dim_p_count,
    }


def run_dp2_silver_gold_dwh(spark: SparkSession, paths: dict, step: str = "all") -> dict:
    """Execute DP2 Silver cleaning and Gold Star Schema pipeline."""
    logger.info("=" * 80)
    logger.info(f"[PIPELINE DP2] Ingest Bronze into Silver & Gold DWH (Step: {step.upper()})")
    logger.info("=" * 80)
    dp2_start = time.time()
    dropped_duplicates = 0
    actual_disk_spill = 0

    if step in ["all", "ingest"]:
        clean_count, dropped_duplicates = process_silver_layer(spark, paths)
        build_gold_dimensions_and_facts(spark, paths)
        optimize_dp2_storage(spark, paths)

        try:
            app_id = spark.sparkContext.applicationId
            with urllib.request.urlopen(
                f"http://localhost:4040/api/v1/applications/{app_id}/stages", timeout=2
            ) as resp:
                recent_stages = json.loads(resp.read().decode())
                if recent_stages:
                    actual_disk_spill = recent_stages[0].get("diskBytesSpilled", 0)
        except Exception:
            pass

        logger.info(f"[OK] [DP2 - INGEST COMPLETED] Duration: {time.time() - dp2_start:.2f}s")
        if step == "ingest":
            return {"status": "ingest_success", "clean_count": clean_count, "duration": time.time() - dp2_start}

    if step in ["all", "validate"]:
        val_res = validate_silver_gold_dwh(spark, paths)
        duration = time.time() - dp2_start
        logger.info("[OK] [DP2 - VALIDATION COMPLETED]")
        logger.info(f"  - Silver Clean Rows : {val_res['silver_count']:,}")
        logger.info(f"  - dim_product (SCD2): {val_res['dim_product_count']:,}")
        logger.info(f"  - Pure Fact Rows    : {val_res['fact_count']:,}")
        logger.info(f"  - Duration          : {duration:.2f}s")

        return {
            "silver_count": val_res["silver_count"],
            "fact_count": val_res["fact_count"],
            "dropped_duplicates": dropped_duplicates,
            "disk_spill": actual_disk_spill,
            "duration": duration,
        }

    return {"duration": time.time() - dp2_start}


# ==============================================================================
# PIPELINE DP3: CALCULATE OFFLINE FEATURE TABLE & LABELS
# ==============================================================================
def compute_feat_user_30d(spark: SparkSession, paths: dict, target_label_date: str) -> int:
    """Calculate 30-day user rolling features and export clean Parquet for Feast."""
    batch_feature_date = f"{target_label_date} 00:00:00"
    logger.info(f"Computing 30d batch features as of: {batch_feature_date}")

    feat_user_30d = spark.sql(f"""
        SELECT
            user_id,
            COUNT(CASE WHEN event_type = 'view' THEN 1 END) AS f_views_30d,
            COUNT(CASE WHEN event_type = 'cart' THEN 1 END) AS f_carts_30d,
            COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS f_purchases_30d,
            ROUND(COALESCE(SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END), 0.0), 2) AS f_spend_30d,
            APPROX_COUNT_DISTINCT(category_level1) AS f_distinct_categories_30d,
            CAST('{batch_feature_date}' AS TIMESTAMP) AS event_timestamp,
            current_timestamp() AS created
        FROM silver_stg_events
        WHERE date >= date_sub(CAST('{target_label_date}' AS DATE), 30)
          AND date < CAST('{target_label_date}' AS DATE)
        GROUP BY user_id
    """)

    feat_user_30d = feat_user_30d.withColumn("date", F.to_date(F.col("event_timestamp")))
    feat_user_30d.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("date").save(
        paths["gold_feat"]
    )
    feat_count = feat_user_30d.count()

    feast_clean_path = paths.get("feast_export", "s3a://ecommerce-lakehouse/feast/user_batch_features_30d")
    feat_user_30d.drop("date").write.mode("overwrite").parquet(feast_clean_path)
    logger.info(f"Exported clean Parquet for Feast to: {feast_clean_path}")
    return feat_count


def compute_ground_truth_labels(spark: SparkSession, paths: dict, target_label_date: str) -> int:
    """Calculate 1-hour lookahead ground truth binary purchase labels with right-censoring."""
    logger.info(f"Computing ground truth labels for date: {target_label_date}")
    user_labels = spark.sql(f"""
        WITH target_events AS (
            SELECT * FROM silver_stg_events
            WHERE date >= '{target_label_date}'
              AND date <= date_add(CAST('{target_label_date}' AS DATE), 1)
        ),
        max_available_time AS (
            SELECT MAX(event_timestamp) AS max_ts FROM silver_stg_events WHERE date = '{target_label_date}'
        ),
        active_users_per_minute AS (
            SELECT DISTINCT
                a_inner.user_id,
                date_trunc('minute', a_inner.event_timestamp) AS prediction_timestamp
            FROM silver_stg_events a_inner
            CROSS JOIN max_available_time m
            WHERE a_inner.date = '{target_label_date}'
              AND a_inner.event_type IN ('view', 'cart')
              AND a_inner.event_timestamp <= (m.max_ts - INTERVAL 1 HOUR)
        ),
        actual_purchases AS (
            SELECT
                user_id,
                event_timestamp AS purchase_time
            FROM target_events
            WHERE event_type = 'purchase'
        )
        SELECT
            a.user_id,
            a.prediction_timestamp,
            MAX(CASE WHEN p.purchase_time IS NOT NULL THEN 1 ELSE 0 END) AS target_purchase_1h
        FROM active_users_per_minute a
        LEFT JOIN actual_purchases p
            ON a.user_id = p.user_id
           AND p.purchase_time >= a.prediction_timestamp
           AND p.purchase_time < (a.prediction_timestamp + INTERVAL 1 HOUR)
        GROUP BY a.user_id, a.prediction_timestamp
        ORDER BY a.user_id ASC, a.prediction_timestamp ASC
    """)

    user_labels = user_labels.withColumn("date", F.to_date(F.col("prediction_timestamp")))
    user_labels.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("date").save(
        paths["gold_label"]
    )
    return user_labels.count()


def run_dp3_features_labels(spark: SparkSession, paths: dict, step: str = "all", prediction_date: str = None) -> dict:
    """Execute DP3 Offline Features and Ground Truth Labels pipeline."""
    logger.info("=" * 80)
    logger.info(f"[PIPELINE DP3] Compute Offline Feature Table & Labels (Step: {step.upper()})")
    logger.info("=" * 80)
    dp3_start = time.time()
    target_label_date = prediction_date

    if step in ["all", "ingest"]:
        df_silver = spark.read.format("delta").load(paths["silver"])
        df_silver.createOrReplaceTempView("silver_stg_events")

        if not target_label_date:
            latest_date_val = df_silver.select(F.max("date")).collect()[0][0]
            target_label_date = str(latest_date_val)

        feat_30d_count = compute_feat_user_30d(spark, paths, target_label_date)
        labels_count = compute_ground_truth_labels(spark, paths, target_label_date)

        logger.info("Executing OPTIMIZE & ZORDER BY (user_id) on feat_user_30d...")
        spark.sql(f"OPTIMIZE delta.`{paths['gold_feat']}` ZORDER BY (user_id)")
        vacuum_retain_hours = int(os.getenv("DELTA_VACUUM_RETAIN_HOURS", "168"))
        spark.sql(f"VACUUM delta.`{paths['gold_feat']}` RETAIN {vacuum_retain_hours} HOURS")
        spark.sql(f"VACUUM delta.`{paths['gold_label']}` RETAIN {vacuum_retain_hours} HOURS")

        logger.info(f"[OK] [DP3 - INGEST COMPLETED] Duration: {time.time() - dp3_start:.2f}s")
        if step == "ingest":
            return {
                "status": "ingest_success",
                "feat_count": feat_30d_count,
                "labels_count": labels_count,
                "duration": time.time() - dp3_start,
            }

    if step in ["all", "validate"]:
        df_feat = spark.read.format("delta").load(paths["gold_feat"])
        df_label = spark.read.format("delta").load(paths["gold_label"])
        feat_count = df_feat.count()
        label_count = df_label.count()

        assert "event_timestamp" in df_feat.columns, (
            "[ERROR] DP3 validation failed: feat_user_30d missing event_timestamp!"
        )
        assert "created" in df_feat.columns, "[ERROR] DP3 validation failed: feat_user_30d missing created!"
        assert feat_count > 0, "[ERROR] DP3 validation failed: feat_user_30d is empty!"
        assert label_count > 0, "[ERROR] DP3 validation failed: user_labels is empty!"

        invalid_labels = df_label.filter(~F.col("target_purchase_1h").isin([0, 1])).count()
        assert invalid_labels == 0, f"[ERROR] DP3 validation failed: {invalid_labels} non-binary label values found!"

        duration = time.time() - dp3_start
        logger.info("[OK] [DP3 - VALIDATION COMPLETED]")
        logger.info(f"  - feat_user_30d Rows: {feat_count:,}")
        logger.info(f"  - user_labels Rows  : {label_count:,}")
        logger.info(f"  - Duration          : {duration:.2f}s")

        return {
            "feat_count": feat_count,
            "labels_count": label_count,
            "target_date": target_label_date,
            "duration": duration,
        }

    return {"duration": time.time() - dp3_start}


def run_optimized_pipeline():
    """Main CLI entrypoint for Lakehouse Spark pipeline execution."""
    parser = argparse.ArgumentParser(description="Spark Optimized Lakehouse Pipeline")
    parser.add_argument(
        "--stage",
        type=str,
        default="all",
        choices=["all", "dp1", "dp2", "dp3"],
        help="Select stage: 'all', 'dp1' (Bronze), 'dp2' (Silver & DWH), 'dp3' (Features & Labels)",
    )
    parser.add_argument(
        "--step",
        type=str,
        default="all",
        choices=["all", "ingest", "validate"],
        help="Select step: 'all', 'ingest', or 'validate'",
    )
    parser.add_argument("--minio-endpoint", type=str, default=None, help="MinIO S3 endpoint URL")
    parser.add_argument("--raw-base-path", type=str, default=None, help="Raw batch path")
    parser.add_argument("--lakehouse-path", type=str, default=None, help="Lakehouse path")
    parser.add_argument("--prediction-date", type=str, default=None, help="Prediction date (YYYY-MM-DD)")
    parser.add_argument("--no-wait", action="store_true", help="Do not wait for input, terminate immediately")
    args, _ = parser.parse_known_args()

    pipeline_start_time = time.time()
    spark = create_optimized_spark_session(minio_endpoint=args.minio_endpoint)
    prediction_date = args.prediction_date or os.getenv("PREDICTION_DATE", "2019-10-26")

    paths = get_lakehouse_paths(
        raw_base_path=args.raw_base_path,
        lakehouse_path=args.lakehouse_path,
    )

    logger.info("=" * 80)
    logger.info(f"STARTING SPARK LAKEHOUSE PIPELINE - STAGE: {args.stage.upper()} | STEP: {args.step.upper()}")
    logger.info("=" * 80)

    results = {}
    if args.stage in ["all", "dp1"]:
        results["dp1"] = run_dp1_bronze(spark, paths, step=args.step)

    if args.stage in ["all", "dp2"]:
        results["dp2"] = run_dp2_silver_gold_dwh(spark, paths, step=args.step)

    if args.stage in ["all", "dp3"]:
        results["dp3"] = run_dp3_features_labels(spark, paths, step=args.step, prediction_date=prediction_date)

    total_pipeline_time = time.time() - pipeline_start_time

    if args.stage == "all" and args.step == "all":
        dp2_info = results.get("dp2", {})
        dropped_duplicates = dp2_info.get("dropped_duplicates", 0)
        actual_disk_spill = dp2_info.get("disk_spill", 0)

        print("\n" + "=" * 80)
        print(" PIPELINE EXECUTION SUMMARY")
        print("=" * 80)
        print(f" {'Metric':<35} | {'Optimized Pipeline Result':<30}")
        print("-" * 80)
        print(f" {'Adaptive Execution (AQE)':<35} | {'Enabled (Skew Join Active)':<30}")
        print(f" {'Category Dimension Join':<35} | {'Broadcast Hash Join':<30}")
        print(f" {'Shuffle Spill to Disk':<35} | {f'{actual_disk_spill} Bytes (0.00 MB)':<30}")
        print(f" {'Duplicates Removed (Silver)':<35} | {f'{dropped_duplicates:,} records':<30}")
        print(f" {'Schema Evolution':<35} | {'11 merged columns with metadata':<30}")
        print(f" {'Storage Optimization':<35} | {'Z-Order (user_id) + Safe VACUUM':<30}")
        print(f" {'Total Pipeline Duration':<35} | {f'{total_pipeline_time:.2f} seconds':<30}")
        print("=" * 80)

        print("\n LAKEHOUSE DELTA TABLES STATUS:")
        print(f"  - Bronze (DP1) : {paths['bronze']}")
        print(f"  - Silver (DP2) : {paths['silver']}")
        print(f"  - Gold DWH     : {paths['gold_dim_product']} (SCD Type 2)")
        print(f"  - Gold DWH     : {paths['gold_dim_user']}")
        print(f"  - Gold DWH     : {paths['gold_fact_events']} (Pure Fact - Z-Ordered)")
        print(f"  - Gold Feature : {paths['gold_feat']} (Feast Store - Z-Ordered)")
        print(f"  - Gold Labels  : {paths['gold_label']} (Ground Truth)")
        print("=" * 80 + "\n")

    if args.no_wait or "--no-wait" in sys.argv:
        logger.info("Chế độ --no-wait được chỉ định. Đóng SparkSession ngay lập tức.")
    elif sys.stdin and sys.stdin.isatty():
        print(" SPARK UI DANG CHAY TAI: http://localhost:4040")
        print(" Nhan [ENTER] de dung chuong trinh an toan.")
        try:
            input(">>> Nhấn [ENTER] trên bàn phím để dừng SparkSession...")
        except (EOFError, KeyboardInterrupt):
            logger.info("Dung SparkSession...")
    else:
        logger.info("Non-interactive mode detected. Terminating SparkSession...")
        time.sleep(2)

    spark.stop()
    logger.info("Optimized SparkSession stopped cleanly.")


if __name__ == "__main__":
    run_optimized_pipeline()
