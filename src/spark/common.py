"""Common Spark Utilities and Session Builder.

Provides reusable SparkSession creation, Lakehouse path resolution,
and Staging file archival helpers for Lakehouse pipelines.
"""

import os
import time
import logging
import urllib.parse
import socket
from pyspark.sql import SparkSession

logger = logging.getLogger("SparkCommon")


def resolve_minio_endpoint(endpoint: str = None) -> str:
    """Resolve MinIO endpoint, handling docker internal network names and underscores."""
    if not endpoint:
        endpoint = os.getenv("MINIO_ENDPOINT")
    if not endpoint:
        try:
            socket.gethostbyname("ecom_minio")
            endpoint = "http://ecom_minio:9000"
        except Exception:
            endpoint = "http://localhost:9000"

    try:
        parsed = urllib.parse.urlparse(endpoint)
        if parsed.hostname and "_" in parsed.hostname:
            resolved_ip = socket.gethostbyname(parsed.hostname)
            endpoint = endpoint.replace(parsed.hostname, resolved_ip)
    except Exception as e:
        logger.warning(f"Could not resolve hostname for {endpoint}: {e}")

    return endpoint


def create_optimized_spark_session(
    app_name: str = "ECom-Lakehouse-Pipeline-Optimized",
    minio_endpoint: str = None,
    master: str = "local[*]",
    driver_memory: str = "3g",
    executor_memory: str = "3g",
) -> SparkSession:
    """Create SparkSession configured for Lakehouse pipelines with Delta Lake and MinIO S3A."""
    resolved_endpoint = resolve_minio_endpoint(minio_endpoint)
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY") or os.environ.get("AWS_ACCESS_KEY_ID") or "minioadmin"
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY") or os.environ.get("AWS_SECRET_ACCESS_KEY") or "minioadmin"

    logger.info(f"Initializing optimized SparkSession (Endpoint: {resolved_endpoint})...")

    spark = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.driver.memory", driver_memory)
        .config("spark.executor.memory", executor_memory)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "2")
        .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "16MB")
        .config("spark.sql.shuffle.partitions", "12")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "50")
        .config("spark.sql.autoBroadcastJoinThreshold", "64MB")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", resolved_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.databricks.delta.vacuum.parallelDelete.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info("Optimized SparkSession initialized successfully.")
    return spark


def get_lakehouse_paths(raw_base_path: str = None, lakehouse_path: str = None, staging_stream_path: str = None) -> dict:
    """Return dictionary of standardized Lakehouse paths across raw, bronze, silver, gold, feast."""
    raw_base = raw_base_path or os.getenv("RAW_BASE_PATH", "s3a://ecommerce-raw/batch")
    lakehouse = lakehouse_path or os.getenv("LAKEHOUSE_BUCKET_PATH", "s3a://ecommerce-lakehouse")
    staging_stream = staging_stream_path or os.getenv(
        "STAGING_STREAM_PATH", "s3a://ecommerce-raw/staging/stream_events"
    )

    return {
        "raw_base": raw_base,
        "staging_stream": staging_stream,
        "archive_stream": "s3a://ecommerce-raw/archive/stream_events",
        "bronze": f"{lakehouse}/bronze/raw_events",
        "silver": f"{lakehouse}/silver/stg_events",
        "gold_dim_product": f"{lakehouse}/gold/dim_product",
        "gold_dim_user": f"{lakehouse}/gold/dim_user",
        "gold_fact_events": f"{lakehouse}/gold/fact_user_events",
        "gold_feat": f"{lakehouse}/gold/feat_user_30d",
        "gold_label": f"{lakehouse}/gold/user_labels",
        "feast_export": f"{lakehouse}/feast/user_batch_features_30d",
    }


def archive_staging_files(spark: SparkSession, staging_path: str, archive_base_path: str):
    """Move processed JSON files from Staging to Archive directory to prevent re-ingestion."""
    try:
        conf = spark.sparkContext._jsc.hadoopConfiguration()
        Path = spark._jvm.org.apache.hadoop.fs.Path
        staging_hadoop_path = Path(staging_path)
        fs = staging_hadoop_path.getFileSystem(conf)

        if fs.exists(staging_hadoop_path):
            file_statuses = fs.listStatus(staging_hadoop_path)
            archived_count = 0
            archive_dir = Path(f"{archive_base_path}/{int(time.time())}")

            for status in file_statuses:
                file_path = status.getPath()
                name = file_path.getName()
                if not name.startswith("_") and not name.startswith("."):
                    if not fs.exists(archive_dir):
                        fs.mkdirs(archive_dir)
                    target_path = Path(f"{archive_dir}/{name}")
                    fs.rename(file_path, target_path)
                    archived_count += 1
            if archived_count > 0:
                logger.info(f"[ARCHIVE STAGING] Moved {archived_count} files to {archive_dir}")
    except Exception as e:
        logger.warning(f"Failed to archive staging files (continuing): {e}")
