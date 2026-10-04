"""
================================================================================
MODULE: BATCH DATA GENERATOR (OFFLINE DATA FEEDER & BENCHMARK SCALER)
Project: E-Commerce Real-Time Purchase Propensity Prediction System
Author: Hoang Minh Nhan

Design objectives:
1. Simulate Skew & High Cardinality (inheriting natural REES46 distributions + synthetic hot keys)
2. Simulate Schema Evolution:
   - Part 1 (01/10 -> 15/10): Exactly 9 canonical columns (no discount_percent)
   - Part 2 (16/10 -> 25/10): Exactly 10 canonical columns (with discount_percent)
3. Simulate Offline Data Quality Issues: Inject ~2% duplicate records into both parts
4. Generator Configuration: Read parameters from config/generator_config.yaml
5. Store Data into MinIO: Upload to MinIO bucket 'ecommerce-raw'
   - Small mode: upload raw_events_old.csv & raw_events_new.csv
   - Medium / Full mode: chunked scaling with deterministic replay up to target GB
     without holding the entire dataset in RAM (Zero-OOM streaming architecture).
6. High-Cardinality Scaling:
   - Deterministic user_id mapping for replica > 0 based on user hash (new_user_pct).
   - Deterministic product price jitter (+-price_jitter_pct) for replica > 0 to support SCD2.
   - Deterministic seed sequence per (base_seed, replica_idx, chunk_idx, part_idx).
================================================================================
"""

import os
import sys
import io
import gc
import time
import json
import shutil
import logging
import argparse
from datetime import datetime, timedelta, timezone
import yaml
import pandas as pd
import numpy as np
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [BatchDataGenerator] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("BatchDataGenerator")

CANONICAL_9_COLUMNS = [
    "event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "user_session",
]
CANONICAL_10_COLUMNS = CANONICAL_9_COLUMNS + ["discount_percent"]


class BatchDataGenerator:
    """
    Controller for offline batch data generation, fault injection, and benchmark scaling.
    Supports small (1M sample), medium (5GB), and full (>=100GB benchmark scale) modes.
    """

    def __init__(
        self,
        config_path: str = "config/generator_config.yaml",
        mode: str = None,
        sample_size: int = None,
        target_size_gb: float = None,
        skew_override: bool = False,
        dry_run: bool = False,
        stats_only: bool = False,
    ):
        self.config_path = config_path
        self.config = self._load_config()
        self.batch_cfg = self.config.get("batch_generator", {})
        self.minio_cfg = self.batch_cfg.get("minio", {})
        self.dry_run = dry_run
        self.stats_only = stats_only

        # Randomness & seed configuration
        self.base_seed = int(self.batch_cfg.get("base_seed", 42))

        # Replica configuration for scaling cardinality and SCD2 attributes
        replica_cfg = self.batch_cfg.get("replica", {})
        self.new_user_pct = int(replica_cfg.get("new_user_pct", 30))
        self.id_offset = int(replica_cfg.get("id_offset", 1_000_000_000))
        self.price_jitter_pct = float(replica_cfg.get("price_jitter_pct", 5))

        # Fault injection configuration
        fault_inj = self.batch_cfg.get("fault_injection", {})
        self.skew_cfg = dict(fault_inj.get("skew", {"enabled": False}))
        if skew_override:
            self.skew_cfg["enabled"] = True
        self.sessions_per_key = int(self.skew_cfg.get("sessions_per_key", 50))
        self.drift_cfg = dict(fault_inj.get("drift", {"enabled": False}))

        # Mode determination
        self.mode = mode or self.batch_cfg.get("mode", "small")
        modes_def = self.batch_cfg.get("modes_definition", {})

        self.chunk_size = self.batch_cfg.get("chunk_size", 250000)
        self.part_size_mb = self.batch_cfg.get("part_size_mb", 500)
        self.time_shift_days = 30

        if self.mode == "small":
            small_def = modes_def.get("small", {})
            self.sample_size = sample_size or (
                small_def.get("sample_size", 1000000) if isinstance(small_def, dict) else small_def
            )
            self.target_size_gb = None
        elif self.mode == "medium":
            medium_def = modes_def.get("medium", {})
            if isinstance(medium_def, dict) and "target_size_gb" in medium_def:
                self.target_size_gb = target_size_gb or medium_def.get("target_size_gb", 5)
                self.sample_size = None
                self.time_shift_days = medium_def.get("time_shift_days", 30)
                self.part_size_mb = medium_def.get("part_size_mb", 500)
            else:
                self.sample_size = sample_size or 5000000
                self.target_size_gb = target_size_gb
        elif self.mode == "full":
            self.sample_size = None
            full_def = modes_def.get("full", {})
            if isinstance(full_def, dict):
                self.target_size_gb = target_size_gb or full_def.get("target_size_gb", 100)
                self.time_shift_days = full_def.get("time_shift_days", 30)
                self.part_size_mb = full_def.get("part_size_mb", 500)
            else:
                self.target_size_gb = target_size_gb or 100
        else:
            self.sample_size = sample_size or self.batch_cfg.get("sample_size", 1000000)
            self.target_size_gb = target_size_gb

        self.input_csv = (
            self._resolve_input_csv() if not self.dry_run else self.batch_cfg.get("input_csv", "2019-Oct.csv")
        )
        self.s3_client = self._init_s3_client() if (not self.dry_run and not self.stats_only) else None

        logger.info(
            f"Init BatchGenerator: mode='{self.mode}', dry_run={self.dry_run}, stats_only={self.stats_only}, "
            f"skew_enabled={self.skew_cfg.get('enabled', False)}, base_seed={self.base_seed}, "
            f"sample_size={f'{self.sample_size:,}' if self.sample_size else 'N/A'}, "
            f"target_size_gb={self.target_size_gb or 'N/A'}, chunk_size={self.chunk_size:,}"
        )

    def _load_config(self) -> dict:
        """Load and parse YAML configuration."""
        if not os.path.exists(self.config_path):
            alt_path = os.path.join("..", self.config_path)
            if os.path.exists(alt_path):
                self.config_path = alt_path
            else:
                raise FileNotFoundError(f"Config file not found at: {self.config_path}")

        logger.info(f"Loading configuration from: {self.config_path}")
        with open(self.config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _resolve_input_csv(self) -> str:
        """Locate raw REES46 CSV file (2019-Oct.csv)."""
        csv_name = self.batch_cfg.get("input_csv", "2019-Oct.csv")
        if os.path.exists(csv_name):
            return csv_name
        alt = os.path.join("..", csv_name)
        if os.path.exists(alt):
            return alt
        raise FileNotFoundError(f"Source file '{csv_name}' not found. Please place '2019-Oct.csv' in the project root.")

    def _init_s3_client(self):
        """Initialize Boto3 S3 Client for MinIO."""
        endpoint = os.environ.get("MINIO_ENDPOINT") or self.minio_cfg.get("endpoint_url", "http://localhost:9000")
        access_key = os.environ.get("MINIO_ACCESS_KEY") or os.environ.get("AWS_ACCESS_KEY_ID")
        secret_key = os.environ.get("MINIO_SECRET_KEY") or os.environ.get("AWS_SECRET_ACCESS_KEY")
        if not access_key or not secret_key:
            logger.warning("MinIO credentials not set in environment. S3 client initialized with fallback.")
            access_key = access_key or "minioadmin"
            secret_key = secret_key or "minioadmin"

        return boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="us-east-1",
        )

    def _ensure_bucket_exists(self, bucket_name: str):
        """Ensure destination MinIO bucket exists."""
        if not self.s3_client:
            return
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
        except ClientError:
            logger.info(f"Bucket '{bucket_name}' does not exist. Creating...")
            self.s3_client.create_bucket(Bucket=bucket_name)

    def _transform_chunk(
        self,
        df_chunk: pd.DataFrame,
        is_part2: bool,
        replica_idx: int = 0,
        chunk_idx: int = 0,
        part_idx: int = 0,
        duplicate_rate: float = 0.02,
    ) -> tuple[pd.DataFrame, int]:
        """
        Process a single data chunk:
        1. Initialize deterministic RNG via SeedSequence(base_seed, replica_idx, chunk_idx, part_idx).
        2. Time shift, user ID offset mapping, price jitter, session suffix for replica > 0.
        3. Schema Evolution (Part 1: 9 cols, Part 2: 10 cols with discount_percent).
        4. Skew injection with consistent user_session mapping.
        5. Duplicate injection (~2%).
        """
        df = df_chunk.copy()
        rng = np.random.default_rng(np.random.SeedSequence([self.base_seed, replica_idx, chunk_idx, part_idx]))

        # 1. Deterministic Replay / Scaling (replica > 0)
        if replica_idx > 0:
            # 1a. Time shift
            shift_delta = timedelta(days=replica_idx * self.time_shift_days)
            try:
                raw_time = df["event_time"].astype(str).str.replace(" UTC", "")
                dt_series = pd.to_datetime(raw_time, format="%Y-%m-%d %H:%M:%S", errors="coerce")
                dt_shifted = dt_series + shift_delta
                df["event_time"] = dt_shifted.dt.strftime("%Y-%m-%d %H:%M:%S UTC")
            except Exception as e:
                logger.warning(f"Error shifting timestamp for replica {replica_idx}: {e}")

            # 1b. Deterministic User ID Mapping (Vectorized 64-bit numpy hash)
            # If hash(user_id, replica) % 100 < new_user_pct: new ID = user_id + replica * id_offset
            # Consistent across all rows for the same user.
            u_arr = df["user_id"].fillna(0).astype(np.int64).values
            u_uint = u_arr.astype(np.uint64)
            r_uint = np.uint64(replica_idx)
            with np.errstate(over="ignore"):
                h_user = (u_uint ^ (r_uint * np.uint64(0x9E3779B97F4A7C15))) * np.uint64(0xBF58476D1CE4E5B9)
                h_user = (h_user ^ (h_user >> np.uint64(30))) * np.uint64(0x94D049BB133111EB)
                h_user = h_user ^ (h_user >> np.uint64(31))
            mask_new_user = (h_user % np.uint64(100)) < np.uint64(self.new_user_pct)
            new_uid = u_arr + (replica_idx * self.id_offset)
            df["user_id"] = np.where(mask_new_user, new_uid, u_arr)

            # 1c. Deterministic Product Price Jitter (+-price_jitter_pct)
            # price' = round(price * (1 + j), 2) for SCD2 price history simulation
            if "price" in df.columns and "product_id" in df.columns:
                p_arr = df["product_id"].fillna(0).astype(np.int64).values
                p_uint = p_arr.astype(np.uint64)
                with np.errstate(over="ignore"):
                    h_prod = (p_uint ^ (r_uint * np.uint64(0x85EBCA6B))) * np.uint64(0xC2B2AE35)
                    h_prod = (h_prod ^ (h_prod >> np.uint64(16))) * np.uint64(0x27D4EB2F)
                    h_prod = h_prod ^ (h_prod >> np.uint64(15))
                # Ratio in [-1.0, 1.0]
                ratio = ((h_prod % np.uint64(10001)).astype(np.float64) / 5000.0 - 1.0) * (
                    self.price_jitter_pct / 100.0
                )
                df["price"] = np.round(df["price"].values * (1.0 + ratio), 2)

            # 1d. Distinct session suffix
            df["user_session"] = df["user_session"].astype(str) + f"-r{replica_idx}"

        # 2. Schema Evolution
        se_cfg = self.batch_cfg.get("fault_injection", {}).get("schema_evolution", {})
        discount_vals = se_cfg.get("discount_values", [4, 5, 8, 10, 12])
        col_name = se_cfg.get("new_column", "discount_percent")

        if not is_part2:
            # Part 1: Canonical 9 columns (no discount_percent)
            if col_name in df.columns:
                df = df.drop(columns=[col_name])
            df = df[[c for c in CANONICAL_9_COLUMNS if c in df.columns]]
        else:
            # Part 2: Canonical 10 columns (with discount_percent)
            df[col_name] = rng.choice(discount_vals, size=len(df))
            cols_order = [c for c in CANONICAL_10_COLUMNS if c in df.columns]
            df = df[cols_order]

        # 3. Skew Injection (Synthetic hot keys with consistent sessions)
        if self.skew_cfg.get("enabled", False):
            skew_col = self.skew_cfg.get("column", "user_id")
            hot_keys = self.skew_cfg.get("top_k_keys", [999999999, 888888888, 777777777])
            hot_ratio = float(self.skew_cfg.get("hot_ratio", 0.30))
            if skew_col in df.columns and hot_keys and hot_ratio > 0:
                n_skew = int(len(df) * hot_ratio)
                if n_skew > 0:
                    skew_indices = rng.choice(df.index, size=n_skew, replace=False)
                    chosen_hot_keys = rng.choice(hot_keys, size=n_skew)
                    chosen_session_ids = rng.integers(0, self.sessions_per_key, size=n_skew)
                    df.loc[skew_indices, skew_col] = chosen_hot_keys
                    hot_sessions = [f"hot-{k}-{s}" for k, s in zip(chosen_hot_keys, chosen_session_ids)]
                    df.loc[skew_indices, "user_session"] = hot_sessions

        # 4. Concept / Data Drift (Opt-in)
        if self.drift_cfg.get("enabled", False) and is_part2:
            drift_col = self.drift_cfg.get("column", "price")
            drift_factor = float(self.drift_cfg.get("drift_factor", 1.5))
            if drift_col in df.columns:
                df[drift_col] = (df[drift_col] * drift_factor).round(2)

        # 5. Duplicate Injection (~2%)
        n_dup = 0
        if duplicate_rate > 0:
            n_dup = int(len(df) * duplicate_rate)
            if n_dup > 0:
                dup_indices = rng.choice(len(df), size=n_dup, replace=True)
                dup_sample = df.iloc[dup_indices]
                df = pd.concat([df, dup_sample], ignore_index=True)
                # Shuffle deterministically
                shuffle_indices = rng.permutation(len(df))
                df = df.iloc[shuffle_indices].reset_index(drop=True)

        return df, n_dup

    def _upload_bytes_to_minio(self, data_bytes: bytes, object_name: str, bucket: str) -> bool:
        """Upload raw bytes to MinIO."""
        if self.stats_only or not self.s3_client:
            return False
        self._ensure_bucket_exists(bucket)
        bio = io.BytesIO(data_bytes)
        self.s3_client.upload_fileobj(bio, bucket, object_name)
        return True

    def _save_manifest(self, manifest_data: dict):
        """Save generation manifest locally and upload to MinIO."""
        os.makedirs("data", exist_ok=True)
        local_path = "data/generation_manifest.json"
        with open(local_path, "w", encoding="utf-8") as f:
            json.dump(manifest_data, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved generation manifest locally at: {local_path}")

        if self.s3_client and not self.stats_only:
            bucket = self.minio_cfg.get("bucket_name", "ecommerce-raw")
            manifest_obj = "batch/generation_manifest.json"
            try:
                self._upload_bytes_to_minio(
                    json.dumps(manifest_data, indent=2, ensure_ascii=False).encode("utf-8"), manifest_obj, bucket
                )
                logger.info(f"Uploaded manifest to s3://{bucket}/{manifest_obj}")
            except Exception as e:
                logger.warning(f"Failed to upload manifest to MinIO: {e}")

    def run_dry_run_estimation(self):
        """
        Estimate metrics for benchmark scale and verify local disk space.
        """
        target_gb = self.target_size_gb or 100.0
        target_bytes = int(target_gb * 1024 * 1024 * 1024)
        avg_row_bytes = 131.5
        estimated_rows = int(target_bytes / avg_row_bytes)
        part_size_mb = self.part_size_mb or 500
        estimated_parts = int((target_gb * 1024) / part_size_mb)

        # Cardinality estimation
        # REES46 Oct has ~3.02M unique users in 42.4M rows
        base_unique_users = 3022290
        raw_dataset_gb = 5.57
        replicas_count = max(1, int(np.ceil(target_gb / raw_dataset_gb)))
        if replicas_count > 1:
            est_unique_users = int(base_unique_users * (1.0 + (replicas_count - 1) * (self.new_user_pct / 100.0)))
        else:
            est_unique_users = int(base_unique_users * min(1.0, (self.sample_size or estimated_rows) / 42400000))

        total_disk, used_disk, free_disk = shutil.disk_usage(".")
        free_disk_gb = free_disk / (1024**3)
        required_disk_gb = target_gb * 1.3

        print("\n" + "=" * 85)
        print(f"DRY-RUN ESTIMATION REPORT: BENCHMARK MODE ({target_gb:.1f} GB)")
        print("=" * 85)
        print("1. ESTIMATED DATA SCALE:")
        print(f"   - Target volume:                 {target_gb:.1f} GB ({target_bytes:,} bytes)")
        print(f"   - Estimated total records:       ~{estimated_rows:,} rows")
        print(f"   - Part 1 (raw_events_old, 9 cols): ~{estimated_rows // 2:,} rows (~{target_gb / 2:.1f} GB)")
        print(f"   - Part 2 (raw_events_new, 10 cols): ~{estimated_rows // 2:,} rows (~{target_gb / 2:.1f} GB)")
        print(f"   - Part file size:                {part_size_mb} MB")
        print(f"   - Expected part files:           ~{estimated_parts} files")
        print("-" * 85)
        print("2. CARDINALITY & REPLICA PROJECTION:")
        print(f"   - Estimated Replicas:            {replicas_count}")
        print(f"   - New User ID Ratio per Replica: {self.new_user_pct}% (Offset: {self.id_offset:,})")
        print(f"   - Estimated Unique Users:        ~{est_unique_users:,} unique user_ids")
        print(
            f"   - Product Price Jitter:          +-{self.price_jitter_pct}% (SCD2 version multiplier: {replicas_count}x)"
        )
        print("-" * 85)
        print("3. DISK SPACE REQUIREMENT CHECK:")
        print(f"   - Available disk space:          {free_disk_gb:.2f} GB")
        print(f"   - Minimum safe requirement:      {required_disk_gb:.2f} GB (1.3x target)")
        if free_disk_gb < required_disk_gb:
            print(f"   WARNING: Available disk ({free_disk_gb:.2f} GB) < 1.3x target ({required_disk_gb:.2f} GB)!")
            print("   Recommendation: Use --stats-only to measure cardinality without writing data.")
        else:
            print("   Status: Sufficient disk space available (> 1.3x target).")
        print("=" * 85 + "\n")

    def run_sample_mode(self):
        """
        Execute sample generation (small: 1M or medium sample) with chunked reading.
        """
        sample_size = self.sample_size or 1000000
        half_sample = sample_size // 2
        effective_date = (
            self.batch_cfg.get("fault_injection", {}).get("schema_evolution", {}).get("effective_date", "2019-10-16")
        )
        skip_start = 20500000
        bucket = self.minio_cfg.get("bucket_name", "ecommerce-raw")
        p1_obj = self.minio_cfg.get("part1_object_name", "batch/raw_events_old.csv")
        p2_obj = self.minio_cfg.get("part2_object_name", "batch/raw_events_new.csv")
        dup_rate = self.batch_cfg.get("fault_injection", {}).get("duplicate", {}).get("rate", 0.02)

        start_time = time.time()
        logger.info(f"Starting {self.mode.upper()} mode ({sample_size:,} rows, chunk_size={self.chunk_size:,})...")

        # Part 1: 01/10 -> 15/10 (9 columns)
        logger.info(f"[Part 1]: Reading & processing {half_sample:,} rows (Pre-{effective_date})...")
        p1_chunks = []
        rows_read_p1 = 0
        p1_dups = 0

        for chunk_idx, chunk in enumerate(pd.read_csv(self.input_csv, chunksize=self.chunk_size)):
            needed = half_sample - rows_read_p1
            if needed <= 0:
                break
            if len(chunk) > needed:
                chunk = chunk.iloc[:needed]

            processed_chunk, n_dup = self._transform_chunk(
                chunk, is_part2=False, replica_idx=0, chunk_idx=chunk_idx, part_idx=0, duplicate_rate=dup_rate
            )
            p1_chunks.append(processed_chunk)
            rows_read_p1 += len(chunk)
            p1_dups += n_dup
            del chunk

        df_p1 = pd.concat(p1_chunks, ignore_index=True)
        del p1_chunks
        gc.collect()

        p1_csv_bytes = df_p1.to_csv(index=False, encoding="utf-8").encode("utf-8")
        p1_size_mb = len(p1_csv_bytes) / (1024 * 1024)
        if not self.stats_only:
            logger.info(f"[Part 1]: Uploading {len(df_p1):,} rows ({df_p1.shape[1]} cols) to s3://{bucket}/{p1_obj}...")
            self._upload_bytes_to_minio(p1_csv_bytes, p1_obj, bucket)
            # Also save local copies for quick offline access
            os.makedirs("data", exist_ok=True)
            with open("data/raw_events_old.csv", "wb") as f_local:
                f_local.write(p1_csv_bytes)
        del p1_csv_bytes

        # Part 2: 16/10 -> 25/10 (10 columns with discount_percent)
        logger.info(
            f"[Part 2]: Reading & processing {half_sample:,} rows from {effective_date} (skip {skip_start:,} rows)..."
        )
        p2_chunks = []
        rows_read_p2 = 0
        p2_dups = 0

        for chunk_idx, chunk in enumerate(
            pd.read_csv(self.input_csv, skiprows=range(1, skip_start), chunksize=self.chunk_size)
        ):
            needed = half_sample - rows_read_p2
            if needed <= 0:
                break
            if len(chunk) > needed:
                chunk = chunk.iloc[:needed]

            processed_chunk, n_dup = self._transform_chunk(
                chunk, is_part2=True, replica_idx=0, chunk_idx=chunk_idx, part_idx=0, duplicate_rate=dup_rate
            )
            p2_chunks.append(processed_chunk)
            rows_read_p2 += len(chunk)
            p2_dups += n_dup
            del chunk

        df_p2 = pd.concat(p2_chunks, ignore_index=True)
        del p2_chunks
        gc.collect()

        p2_csv_bytes = df_p2.to_csv(index=False, encoding="utf-8").encode("utf-8")
        p2_size_mb = len(p2_csv_bytes) / (1024 * 1024)
        if not self.stats_only:
            logger.info(f"[Part 2]: Uploading {len(df_p2):,} rows ({df_p2.shape[1]} cols) to s3://{bucket}/{p2_obj}...")
            self._upload_bytes_to_minio(p2_csv_bytes, p2_obj, bucket)
            with open("data/raw_events_new.csv", "wb") as f_local:
                f_local.write(p2_csv_bytes)
        del p2_csv_bytes

        total_time = time.time() - start_time
        total_rows = len(df_p1) + len(df_p2)
        total_dups = p1_dups + p2_dups
        actual_dup_rate = (total_dups / total_rows) * 100 if total_rows > 0 else 0.0

        # Calculate actual skew if enabled
        actual_skew_stats = {}
        if self.skew_cfg.get("enabled", False):
            skew_col = self.skew_cfg.get("column", "user_id")
            hot_keys = self.skew_cfg.get("top_k_keys", [])
            comb_users = pd.concat([df_p1[skew_col], df_p2[skew_col]], ignore_index=True)
            hot_counts = comb_users.isin(hot_keys).sum()
            actual_skew_stats = {
                "hot_keys": hot_keys,
                "hot_rows": int(hot_counts),
                "hot_ratio_actual": round(float(hot_counts / total_rows), 4) if total_rows > 0 else 0.0,
            }

        print("\n" + "=" * 85)
        print(f"BATCH DATA GENERATOR REPORT ({self.mode.upper()} MODE)")
        print("=" * 85)
        print("1. VOLUME & TIMING:")
        print(f"   - Execution time:                {total_time:.2f} seconds")
        print(f"   - Total records generated:       {total_rows:,} rows ({p1_size_mb + p2_size_mb:.2f} MB)")
        print(f"   - Part 1 (Old Schema, 9 cols):   {len(df_p1):,} rows -> s3://{bucket}/{p1_obj}")
        print(f"   - Part 2 (New Schema, 10 cols):  {len(df_p2):,} rows -> s3://{bucket}/{p2_obj}")
        print("-" * 85)
        print("2. SCHEMA EVOLUTION:")
        print(f"   - Part 1 columns: {list(df_p1.columns)}")
        print(f"   - Part 2 columns: {list(df_p2.columns)}")
        print("   - New column: 'discount_percent' (present only in Part 2)")
        print("-" * 85)
        print("3. FAULT INJECTION (DUPLICATES & SKEW):")
        print(f"   - Injected duplicates:           {total_dups:,} rows")
        print(f"   - Actual duplicate rate:         {actual_dup_rate:.2f}% (Target: ~2%)")
        if actual_skew_stats:
            print(f"   - Injected Hot Keys:             {actual_skew_stats['hot_keys']}")
            print(f"   - Actual Hot Key Ratio:          {actual_skew_stats['hot_ratio_actual'] * 100:.2f}%")
        print("-" * 85)
        print("4. CARDINALITY (SAMPLE):")
        u_comb = pd.concat([df_p1["user_id"], df_p2["user_id"]], ignore_index=True)
        print(f"   - Unique user_id in sample:      {u_comb.nunique():,}")
        print("=" * 85 + "\n")

        manifest = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "mode": self.mode,
            "sample_size": sample_size,
            "total_rows": total_rows,
            "total_bytes": int((p1_size_mb + p2_size_mb) * 1024 * 1024),
            "total_size_mb": round(p1_size_mb + p2_size_mb, 2),
            "duplicate_rate_target": dup_rate,
            "duplicate_rows_injected": total_dups,
            "duplicate_rate_actual": round(actual_dup_rate, 4),
            "schema_evolution": {
                "effective_date": effective_date,
                "part1_columns": list(df_p1.columns),
                "part2_columns": list(df_p2.columns),
                "new_column": "discount_percent",
            },
            "skew_config": self.skew_cfg,
            "skew_actual": actual_skew_stats,
            "drift_config": self.drift_cfg,
            "stats_only": self.stats_only,
        }
        self._save_manifest(manifest)

    def run_benchmark_scale_mode(self):
        """
        Execute large scale benchmark mode (medium 5GB or full >=100GB).
        Uses chunked streaming, deterministic user ID offset mapping, product price jitter,
        and authoritative byte-based stopping conditions.
        """
        target_gb = self.target_size_gb or 100.0
        target_bytes_total = int(target_gb * 1024 * 1024 * 1024)
        target_bytes_per_stage = target_bytes_total // 2

        # Check disk space if not stats_only
        if not self.stats_only:
            _, _, free_disk = shutil.disk_usage(".")
            free_disk_gb = free_disk / (1024**3)
            required_disk_gb = target_gb * 1.3
            if free_disk_gb < required_disk_gb:
                raise RuntimeError(
                    f"Available disk space ({free_disk_gb:.2f} GB) is less than safe requirement "
                    f"1.3x target ({required_disk_gb:.2f} GB). Execution aborted to prevent disk exhaustion. "
                    f"Use --stats-only to profile without disk overhead."
                )

        bucket = self.minio_cfg.get("bucket_name", "ecommerce-raw")
        dup_rate = self.batch_cfg.get("fault_injection", {}).get("duplicate", {}).get("rate", 0.02)
        effective_date = "2019-10-16"
        skip_start = 20500000

        start_time = time.time()
        logger.info(f"Starting {self.mode.upper()} mode (Target: {target_gb:.1f} GB across 2 stages)...")

        # STAGE 1: RAW_EVENTS_OLD (9 COLS)
        logger.info(f"[STAGE 1/2]: Generating raw_events_old (Pre-{effective_date}, 9 cols)...")
        p1_bytes_written = 0
        p1_rows_written = 0
        p1_dups_injected = 0
        p1_part_idx = 0
        replica_idx = 0

        while p1_bytes_written < target_bytes_per_stage:
            logger.info(
                f" -> Replica {replica_idx} for Stage 1 (Time shift: +{replica_idx * self.time_shift_days} days)..."
            )
            part_buffer = io.StringIO()
            part_buffer_bytes = 0
            is_first_chunk_in_part = True

            for chunk_idx, chunk in enumerate(pd.read_csv(self.input_csv, chunksize=self.chunk_size)):
                if p1_bytes_written >= target_bytes_per_stage:
                    break

                chunk_dates = chunk["event_time"].astype(str).str[:10]
                chunk_valid = chunk[chunk_dates < effective_date]
                if chunk_valid.empty:
                    continue

                processed_chunk, n_dup = self._transform_chunk(
                    chunk_valid,
                    is_part2=False,
                    replica_idx=replica_idx,
                    chunk_idx=chunk_idx,
                    part_idx=p1_part_idx,
                    duplicate_rate=dup_rate,
                )
                csv_str = processed_chunk.to_csv(index=False, header=is_first_chunk_in_part, encoding="utf-8")
                chunk_bytes_len = len(csv_str.encode("utf-8"))

                part_buffer.write(csv_str)
                part_buffer_bytes += chunk_bytes_len
                p1_bytes_written += chunk_bytes_len
                p1_rows_written += len(processed_chunk)
                p1_dups_injected += n_dup
                is_first_chunk_in_part = False

                del chunk, chunk_valid, processed_chunk
                gc.collect()

                if part_buffer_bytes >= self.part_size_mb * 1024 * 1024:
                    part_obj_name = f"batch/raw_events_old_part-{p1_part_idx:05d}.csv"
                    logger.info(f"   [Upload Stage 1] Part {p1_part_idx:05d} ({part_buffer_bytes / (1024**2):.2f} MB)")
                    if not self.stats_only:
                        self._upload_bytes_to_minio(part_buffer.getvalue().encode("utf-8"), part_obj_name, bucket)
                    p1_part_idx += 1
                    part_buffer.close()
                    part_buffer = io.StringIO()
                    part_buffer_bytes = 0
                    is_first_chunk_in_part = True

            if part_buffer_bytes > 0:
                part_obj_name = f"batch/raw_events_old_part-{p1_part_idx:05d}.csv"
                logger.info(f"   [Upload Stage 1 End] Part {p1_part_idx:05d} ({part_buffer_bytes / (1024**2):.2f} MB)")
                if not self.stats_only:
                    self._upload_bytes_to_minio(part_buffer.getvalue().encode("utf-8"), part_obj_name, bucket)
                p1_part_idx += 1
                part_buffer.close()

            replica_idx += 1

        # STAGE 2: RAW_EVENTS_NEW (10 COLS)
        logger.info(f"[STAGE 2/2]: Generating raw_events_new (Post-{effective_date}, 10 cols)...")
        p2_bytes_written = 0
        p2_rows_written = 0
        p2_dups_injected = 0
        p2_part_idx = 0
        replica_idx = 0

        while p2_bytes_written < target_bytes_per_stage:
            logger.info(
                f" -> Replica {replica_idx} for Stage 2 (Time shift: +{replica_idx * self.time_shift_days} days)..."
            )
            part_buffer = io.StringIO()
            part_buffer_bytes = 0
            is_first_chunk_in_part = True

            for chunk_idx, chunk in enumerate(
                pd.read_csv(self.input_csv, skiprows=range(1, skip_start), chunksize=self.chunk_size)
            ):
                if p2_bytes_written >= target_bytes_per_stage:
                    break

                chunk_dates = chunk["event_time"].astype(str).str[:10]
                chunk_valid = chunk[(chunk_dates >= effective_date) & (chunk_dates <= "2019-10-25")]
                if chunk_valid.empty:
                    continue

                processed_chunk, n_dup = self._transform_chunk(
                    chunk_valid,
                    is_part2=True,
                    replica_idx=replica_idx,
                    chunk_idx=chunk_idx,
                    part_idx=p2_part_idx,
                    duplicate_rate=dup_rate,
                )
                csv_str = processed_chunk.to_csv(index=False, header=is_first_chunk_in_part, encoding="utf-8")
                chunk_bytes_len = len(csv_str.encode("utf-8"))

                part_buffer.write(csv_str)
                part_buffer_bytes += chunk_bytes_len
                p2_bytes_written += chunk_bytes_len
                p2_rows_written += len(processed_chunk)
                p2_dups_injected += n_dup
                is_first_chunk_in_part = False

                del chunk, chunk_valid, processed_chunk
                gc.collect()

                if part_buffer_bytes >= self.part_size_mb * 1024 * 1024:
                    part_obj_name = f"batch/raw_events_new_part-{p2_part_idx:05d}.csv"
                    logger.info(f"   [Upload Stage 2] Part {p2_part_idx:05d} ({part_buffer_bytes / (1024**2):.2f} MB)")
                    if not self.stats_only:
                        self._upload_bytes_to_minio(part_buffer.getvalue().encode("utf-8"), part_obj_name, bucket)
                    p2_part_idx += 1
                    part_buffer.close()
                    part_buffer = io.StringIO()
                    part_buffer_bytes = 0
                    is_first_chunk_in_part = True

            if part_buffer_bytes > 0:
                part_obj_name = f"batch/raw_events_new_part-{p2_part_idx:05d}.csv"
                logger.info(f"   [Upload Stage 2 End] Part {p2_part_idx:05d} ({part_buffer_bytes / (1024**2):.2f} MB)")
                if not self.stats_only:
                    self._upload_bytes_to_minio(part_buffer.getvalue().encode("utf-8"), part_obj_name, bucket)
                p2_part_idx += 1
                part_buffer.close()

            replica_idx += 1

        total_time = time.time() - start_time
        total_bytes = p1_bytes_written + p2_bytes_written
        total_gb = total_bytes / (1024**3)
        total_rows = p1_rows_written + p2_rows_written
        total_dups = p1_dups_injected + p2_dups_injected
        actual_dup_rate = (total_dups / total_rows) * 100 if total_rows > 0 else 0.0

        print("\n" + "=" * 90)
        print(f"BENCHMARK DATA GENERATOR REPORT ({self.mode.upper()} MODE)")
        print("=" * 90)
        print("1. SCALE & THROUGHPUT:")
        print(f"   - Total volume generated:        {total_gb:.2f} GB ({total_bytes:,} bytes)")
        print(f"   - Total records generated:       {total_rows:,} rows")
        print(f"   - Elapsed time:                  {total_time:.2f}s ({total_time / 60:.2f} min)")
        print(f"   - Throughput:                    {total_gb / (total_time / 3600):.2f} GB/hour")
        print("-" * 90)
        print("2. PARTITION STRUCTURE ON MINIO / LAKEHOUSE:")
        print(
            f"   - Stage 1 (raw_events_old, 9 cols):  {p1_part_idx} parts ({p1_bytes_written / (1024**3):.2f} GB, {p1_rows_written:,} rows)"
        )
        print(
            f"   - Stage 2 (raw_events_new, 10 cols): {p2_part_idx} parts ({p2_bytes_written / (1024**3):.2f} GB, {p2_rows_written:,} rows)"
        )
        print("-" * 90)
        print("3. QUALITY & SCHEMA VERIFICATION:")
        print("   - Schema Evolution: 9 cols pre-16/10 vs 10 cols post-16/10")
        print(f"   - Actual Duplicate Rate: {actual_dup_rate:.2f}%")
        print(f"   - Stats Only Mode: {self.stats_only}")
        print("=" * 90 + "\n")

        manifest = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "mode": self.mode,
            "target_size_gb": target_gb,
            "total_rows": total_rows,
            "total_bytes": total_bytes,
            "total_gb": round(total_gb, 4),
            "part_files_count": p1_part_idx + p2_part_idx,
            "duplicate_rate_target": dup_rate,
            "duplicate_rows_injected": total_dups,
            "duplicate_rate_actual": round(actual_dup_rate, 4),
            "schema_evolution": {
                "effective_date": effective_date,
                "part1_columns": CANONICAL_9_COLUMNS,
                "part2_columns": CANONICAL_10_COLUMNS,
                "new_column": "discount_percent",
            },
            "skew_config": self.skew_cfg,
            "drift_config": self.drift_cfg,
            "stats_only": self.stats_only,
        }
        self._save_manifest(manifest)

    def run(self):
        """Dispatch execution based on mode."""
        if self.dry_run:
            self.run_dry_run_estimation()
        elif self.mode in ("full", "medium") and self.target_size_gb:
            self.run_benchmark_scale_mode()
        else:
            self.run_sample_mode()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E-Commerce Batch Data Generator & Benchmark Scaler")
    parser.add_argument(
        "--mode",
        choices=["small", "medium", "full"],
        default=None,
        help="Generation mode: small (1M), medium (5GB), full (>=100GB)",
    )
    parser.add_argument("--sample-size", type=int, default=None, help="Custom sample size (for small/medium sample)")
    parser.add_argument("--target-size-gb", type=float, default=None, help="Target benchmark size in GB")
    parser.add_argument("--config", default="config/generator_config.yaml", help="Path to config YAML")
    parser.add_argument("--skewed", action="store_true", help="Enable opt-in synthetic skew injection")
    parser.add_argument(
        "--dry-run", action="store_true", help="Estimate metrics and disk requirements without writing data"
    )
    parser.add_argument(
        "--stats-only", action="store_true", help="Run transformations and write manifest without uploading to MinIO"
    )
    args = parser.parse_args()

    gen = BatchDataGenerator(
        config_path=args.config,
        mode=args.mode,
        sample_size=args.sample_size,
        target_size_gb=args.target_size_gb,
        skew_override=args.skewed,
        dry_run=args.dry_run,
        stats_only=args.stats_only,
    )
    gen.run()
