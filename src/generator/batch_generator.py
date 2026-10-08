"""
================================================================================
MODULE: BATCH DATA GENERATOR (OFFLINE DATA FEEDER & BENCHMARK SCALER)
Project: E-Commerce Real-Time Purchase Propensity Prediction System
Author: Hoang Minh Nhan

Design objectives:
1. Simulate Skew & High Cardinality (inheriting natural REES46 distributions + synthetic hot keys)
2. Simulate Schema Evolution:
   - Part 1 (01/10 -> 15/10): Exactly 9 canonical columns (no discount_percent)
   - Part 2 (16/10 -> 31/10): Exactly 10 canonical columns (with discount_percent)
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
import csv
import hashlib
import platform
import tempfile
from importlib.metadata import version
from fractions import Fraction
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dotenv import load_dotenv
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
        local_output_dir: str = None,
    ):
        self.config_path = config_path
        self.config = self._load_config()
        self.batch_cfg = self.config.get("batch_generator", {})
        self.minio_cfg = self.batch_cfg.get("minio", {})
        self.dry_run = dry_run
        self.stats_only = stats_only
        self.local_output_dir = local_output_dir
        if local_output_dir and mode not in (None, "small"):
            raise ValueError("local_output_dir is only supported for small mode")
        self._load_batch_boundaries()

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
            self.sample_size = sample_size if sample_size is not None else (
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

        if self.mode == "small" and (not isinstance(self.sample_size, int) or self.sample_size <= 0):
            raise ValueError("small sample_size must be a positive integer")
        if local_output_dir and self.mode != "small":
            raise ValueError("local_output_dir is only supported for small mode")

        self.input_csv = (
            self._resolve_input_csv() if not self.dry_run else self.batch_cfg.get("input_csv", "2019-Oct.csv")
        )
        self.s3_client = self._init_s3_client() if (not self.dry_run and not self.stats_only and not self.local_output_dir) else None

        logger.info(
            f"Init BatchGenerator: mode='{self.mode}', dry_run={self.dry_run}, stats_only={self.stats_only}, "
            f"skew_enabled={self.skew_cfg.get('enabled', False)}, base_seed={self.base_seed}, "
            f"sample_size={f'{self.sample_size:,}' if self.sample_size else 'N/A'}, "
            f"target_size_gb={self.target_size_gb or 'N/A'}, chunk_size={self.chunk_size:,}"
        )

    def _load_batch_boundaries(self):
        """Read the sole UTC boundary contract; reject ambiguous configuration."""
        date_range = self.batch_cfg.get("date_range", {})
        evolution = self.batch_cfg.get("fault_injection", {}).get("schema_evolution", {})
        values = [date_range.get("start_timestamp"), evolution.get("effective_timestamp"),
                  date_range.get("end_timestamp")]
        if any(not isinstance(value, str) or not value.endswith("Z") for value in values):
            raise ValueError("batch boundaries must be explicit UTC timestamps ending in Z")
        self.batch_start, self.evolution_timestamp, self.batch_end = [pd.Timestamp(value) for value in values]
        if not self.batch_start < self.evolution_timestamp < self.batch_end:
            raise ValueError("batch boundaries must satisfy start < effective < end")
        if not evolution.get("enabled") or evolution.get("new_column") != "discount_percent":
            raise ValueError("Batch Generator requires enabled schema evolution with discount_percent")

    def _classify_chunk(self, chunk: pd.DataFrame) -> pd.Series:
        """Return one classification per row without modifying its source values."""
        text = chunk["event_time"].astype("string")
        valid_format = text.str.fullmatch(
            r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{1,9})? UTC", na=False
        )
        timestamps = pd.to_datetime(
            text.where(valid_format).str.replace(" UTC", "", regex=False),
            format="ISO8601", errors="coerce", utc=True,
        )
        result = pd.Series("EXCLUDED", index=chunk.index, dtype="string")
        result.loc[timestamps.isna()] = "INVALID"
        inside = timestamps.ge(self.batch_start) & timestamps.lt(self.batch_end)
        result.loc[inside & timestamps.lt(self.evolution_timestamp)] = "OLD"
        result.loc[inside & timestamps.ge(self.evolution_timestamp)] = "NEW"
        return result

    def _iter_classified_chunks(self):
        """Scan unsorted source; expose counts before any sampling/transformation."""
        self.selection_counts = dict.fromkeys(("OLD", "NEW", "EXCLUDED", "INVALID"), 0)
        for chunk in pd.read_csv(self.input_csv, chunksize=self.chunk_size):
            classifications = self._classify_chunk(chunk)
            for group, count in classifications.value_counts().items():
                self.selection_counts[group] += int(count)
            yield chunk, classifications
        logger.info("Source classification counts: %s", self.selection_counts)

    def _sample_classified_rows(self):
        """Uniform random-priority reservoirs per schema, bounded by quota + one chunk.

        Independent seeded random priorities retain the highest k source rows.
        Insufficient populations are returned as-is; no duplication or quota transfer.
        """
        quotas = {"OLD": self.sample_size // 2, "NEW": self.sample_size - self.sample_size // 2}
        rngs = {group: np.random.default_rng(np.random.SeedSequence([self.base_seed, i]))
                for i, group in enumerate(quotas)}
        reservoirs = {}
        priorities = {group: np.empty(0) for group in quotas}
        empty = None
        for chunk, classifications in self._iter_classified_chunks():
            empty = chunk.iloc[:0].copy()
            for group, quota in quotas.items():
                candidates = chunk.loc[classifications.eq(group)]
                keys = rngs[group].random(len(candidates))
                combined = pd.concat([reservoirs.get(group, empty), candidates], ignore_index=True)
                keys = np.concatenate([priorities[group], keys])
                if len(keys) > quota:
                    keep = np.argpartition(keys, len(keys) - quota)[-quota:] if quota else np.empty(0, dtype=int)
                    keep.sort()
                    combined = combined.iloc[keep].reset_index(drop=True)
                    keys = keys[keep]
                reservoirs[group], priorities[group] = combined, keys
        if empty is None:
            empty = pd.DataFrame(columns=CANONICAL_9_COLUMNS)
        self.selected_source_counts = {group: len(reservoirs.get(group, empty)) for group in quotas}
        for group, quota in quotas.items():
            if self.selected_source_counts[group] < quota:
                logger.warning("%s population below quota: requested=%s selected=%s; no backfill",
                               group, quota, self.selected_source_counts[group])
        return reservoirs.get("OLD", empty), reservoirs.get("NEW", empty)

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
        output_dir = self.local_output_dir or "data"
        os.makedirs(output_dir, exist_ok=True)
        local_path = os.path.join(output_dir, "generation_manifest.json")
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
        effective_date = self.evolution_timestamp.isoformat()
        bucket = self.minio_cfg.get("bucket_name", "ecommerce-raw")
        p1_obj = self.minio_cfg.get("part1_object_name", "batch/raw_events_old.csv")
        p2_obj = self.minio_cfg.get("part2_object_name", "batch/raw_events_new.csv")
        dup_rate = self.batch_cfg.get("fault_injection", {}).get("duplicate", {}).get("rate", 0.02)
        start_time = time.time()
        old_rows, new_rows = self._sample_classified_rows()
        logger.info("Selected source rows: %s", self.selected_source_counts)
        processed_old, p1_dups = self._transform_chunk(old_rows, is_part2=False, duplicate_rate=dup_rate)
        p1_chunks = [processed_old]
        df_p1 = pd.concat(p1_chunks, ignore_index=True)
        del p1_chunks
        gc.collect()

        p1_destination = (os.path.join(self.local_output_dir, "raw_events_old.csv")
                          if self.local_output_dir else f"s3://{bucket}/{p1_obj}")
        p2_destination = (os.path.join(self.local_output_dir, "raw_events_new.csv")
                          if self.local_output_dir else f"s3://{bucket}/{p2_obj}")
        p1_csv_bytes = df_p1.to_csv(index=False, encoding="utf-8").encode("utf-8")
        p1_size_mb = len(p1_csv_bytes) / (1024 * 1024)
        if not self.stats_only or self.local_output_dir:
            logger.info(f"[Part 1]: Writing {len(df_p1):,} rows ({df_p1.shape[1]} cols) to {p1_destination}...")
            self._upload_bytes_to_minio(p1_csv_bytes, p1_obj, bucket)
            # Also save local copies for quick offline access
            output_dir = self.local_output_dir or "data"
            os.makedirs(output_dir, exist_ok=True)
            with open(os.path.join(output_dir, "raw_events_old.csv"), "wb") as f_local:
                f_local.write(p1_csv_bytes)
        del p1_csv_bytes

        processed_new, p2_dups = self._transform_chunk(new_rows, is_part2=True, duplicate_rate=dup_rate)
        p2_chunks = [processed_new]
        df_p2 = pd.concat(p2_chunks, ignore_index=True)
        del p2_chunks
        gc.collect()

        p2_csv_bytes = df_p2.to_csv(index=False, encoding="utf-8").encode("utf-8")
        p2_size_mb = len(p2_csv_bytes) / (1024 * 1024)
        if not self.stats_only or self.local_output_dir:
            logger.info(f"[Part 2]: Writing {len(df_p2):,} rows ({df_p2.shape[1]} cols) to {p2_destination}...")
            self._upload_bytes_to_minio(p2_csv_bytes, p2_obj, bucket)
            with open(os.path.join(self.local_output_dir or "data", "raw_events_new.csv"), "wb") as f_local:
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
        print(f"   - Part 1 (Old Schema, 9 cols):   {len(df_p1):,} rows -> {p1_destination}")
        print(f"   - Part 2 (New Schema, 10 cols):  {len(df_p2):,} rows -> {p2_destination}")
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
            "source_classification_counts": self.selection_counts,
            "selected_source_counts": self.selected_source_counts,
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
        effective_date = self.evolution_timestamp.isoformat()

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

            for chunk_idx, (chunk, classifications) in enumerate(self._iter_classified_chunks()):
                if p1_bytes_written >= target_bytes_per_stage:
                    break

                chunk_valid = chunk.loc[classifications.eq("OLD")]
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

            for chunk_idx, (chunk, classifications) in enumerate(self._iter_classified_chunks()):
                if p2_bytes_written >= target_bytes_per_stage:
                    break

                chunk_valid = chunk.loc[classifications.eq("NEW")]
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


def _file_digest(path):
    checksum = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            checksum.update(block)
    return {"bytes": Path(path).stat().st_size, "sha256": checksum.hexdigest()}


class _MultipartCSV:
    """Bounded buffer for one final CSV object; multipart pieces are not dataset files."""

    def __init__(self, client, bucket, key, part_bytes):
        self.client, self.bucket, self.key = client, bucket, key
        self.part_bytes = part_bytes
        self.buffer = bytearray()
        self.checksum, self.size, self.parts = hashlib.sha256(), 0, []
        self.upload_id = client.create_multipart_upload(
            Bucket=bucket, Key=key, ContentType="text/csv"
        )["UploadId"]

    def write(self, text):
        block = text.encode("utf-8")
        self.checksum.update(block)
        self.size += len(block)
        self.buffer.extend(block)
        if len(self.buffer) >= self.part_bytes:
            self.flush()
        return len(text)

    def flush(self):
        if not self.buffer:
            return
        number = len(self.parts) + 1
        if number > 10000:
            raise ValueError("Multipart part limit exceeded")
        result = self.client.upload_part(
            Bucket=self.bucket, Key=self.key, UploadId=self.upload_id,
            PartNumber=number, Body=bytes(self.buffer),
        )
        self.parts.append({"PartNumber": number, "ETag": result["ETag"]})
        self.buffer.clear()

    def finish(self):
        self.flush()
        self.client.complete_multipart_upload(
            Bucket=self.bucket, Key=self.key, UploadId=self.upload_id,
            MultipartUpload={"Parts": self.parts},
        )
        self.upload_id = None
        return {"bytes": self.size, "sha256": self.checksum.hexdigest()}

    def abort(self):
        if self.upload_id:
            self.client.abort_multipart_upload(
                Bucket=self.bucket, Key=self.key, UploadId=self.upload_id
            )
            self.upload_id = None


def ingest_source(source, directory, config, client, prefix):
    """Stream every source row into exactly two schema-versioned MinIO objects."""
    started_at, started_clock = datetime.now(timezone.utc).isoformat(), time.monotonic()
    source, directory = Path(source).resolve(), Path(directory)
    batch = config["batch_generator"]
    evolution = batch["fault_injection"]["schema_evolution"]
    values = [batch["date_range"]["start_timestamp"], evolution["effective_timestamp"],
              batch["date_range"]["end_timestamp"]]
    if any(not isinstance(value, str) or not value.endswith("Z") for value in values):
        raise ValueError("Source boundaries must be explicit UTC ending in Z")
    start, boundary, end = [datetime.fromisoformat(value.replace("Z", "+00:00")) for value in values]
    if not start < boundary < end or not evolution["enabled"] or evolution["new_column"] != "discount_percent":
        raise ValueError("Invalid date/schema contract")
    discounts = evolution["discount_values"]
    if not discounts or any(type(value) is not int for value in discounts):
        raise ValueError("discount_values must be nonempty integers")
    duplicate = batch["fault_injection"]["duplicate"]
    rate = Fraction(str(duplicate["rate"])) if duplicate["enabled"] else Fraction(0)
    if not 0 <= rate <= 1:
        raise ValueError("Duplicate rate must be between 0 and 1")
    if batch["fault_injection"].get("skew", {}).get("enabled") or batch["fault_injection"].get("drift", {}).get("enabled"):
        raise ValueError("Source mode preserves natural skew; synthetic skew/drift must be disabled")
    part_bytes = batch["source"]["multipart_size_mb"] * 1024**2
    if not 5 * 1024**2 <= part_bytes <= 512 * 1024**2:
        raise ValueError("multipart_size_mb must be between 5 and 512")
    prefix = _source_prefix(config, prefix)
    bucket = batch["minio"]["bucket_name"]
    client.head_bucket(Bucket=bucket)
    if client.list_objects_v2(Bucket=bucket, Prefix=prefix + "/", MaxKeys=1).get("KeyCount", 0):
        raise ValueError("Destination prefix is nonempty; refusing overwrite")
    if client.list_multipart_uploads(Bucket=bucket, Prefix=prefix + "/", MaxUploads=1).get("Uploads"):
        raise ValueError("Destination has unfinished multipart uploads; choose a new prefix")
    directory.mkdir(parents=True, exist_ok=False)
    before = _file_digest(source)
    counts, sinks = {"old": 0, "new": 0}, {}
    duplicates = {"old": 0, "new": 0}
    try:
        with source.open(newline="", encoding="utf-8") as stream:
            reader = csv.reader(stream, strict=True)
            if next(reader, None) != CANONICAL_9_COLUMNS:
                raise ValueError("Unexpected source header")
            for group in counts:
                sinks[group] = _MultipartCSV(client, bucket, prefix + "/raw_events_" + group + ".csv", part_bytes)
            writers = {group: csv.writer(sink) for group, sink in sinks.items()}
            writers["old"].writerow(CANONICAL_9_COLUMNS)
            writers["new"].writerow(CANONICAL_10_COLUMNS)
            for record, row in enumerate(reader, 1):
                if len(row) != 9:
                    raise ValueError(f"Record {record}: expected 9 fields")
                try:
                    timestamp = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S UTC").replace(tzinfo=timezone.utc)
                except ValueError as error:
                    raise ValueError(f"Record {record}: invalid UTC timestamp") from error
                if not start <= timestamp < end:
                    raise ValueError(f"Record {record}: outside configured October interval")
                group = "old" if timestamp < boundary else "new"
                if group == "new":
                    identity = f"{batch['base_seed']}:{record}".encode()
                    index = int.from_bytes(hashlib.sha256(identity).digest()[:8], "big") % len(discounts)
                    row = row + [discounts[index]]
                writers[group].writerow(row)
                counts[group] += 1
                # Exact incremental quota per schema; copy the already-adapted row verbatim.
                quota = counts[group] * rate.numerator // rate.denominator
                if quota > duplicates[group]:
                    writers[group].writerow(row)
                    duplicates[group] += 1
                if record % 1000000 == 0:
                    print(f"Processed {record:,} source records", flush=True)
        if _file_digest(source) != before:
            raise ValueError("Source changed during ingestion")
        objects = {"raw_events_" + group + ".csv": sink.finish() for group, sink in sinks.items()}
        manifest = {
            "status": "UPLOADED_NOT_VERIFIED",
            "created_timestamp": datetime.now(timezone.utc).isoformat(),
            "timing": {"started_timestamp": started_at,
                       "finished_timestamp": datetime.now(timezone.utc).isoformat(),
                       "elapsed_seconds": time.monotonic() - started_clock,
                       "scope": "ingest preflight, source hashes, CSV processing and CSV uploads; excludes manifest publication and subsequent verification"},
            "bucket": bucket, "prefix": prefix,
            "source": {"path": str(source), **before}, "counts": counts,
            "total_records": sum(counts.values()), "objects": objects,
            "boundaries": dict(zip(("start", "evolution", "end"), values)),
            "seed": batch["base_seed"], "discount_values": discounts,
            "discount_rule": "sha256(seed:1-based-source-record), first 8 bytes modulo configured values",
            "injected_duplicates": sum(duplicates.values()),
            "duplicate_counts": duplicates, "duplicate_rate": float(rate),
            "duplicate_rule": "per-schema floor(source_count * rate); adjacent identical adapted-row copy at each quota increment",
            "output_counts": {group: counts[group] + duplicates[group] for group in counts},
            "sampling": False, "replication": False,
            "limitations": "Natural source duplicates retained; no semantic source-quality or feature/label coverage guarantee.",
        }
        payload = (json.dumps(manifest, indent=2) + "\n").encode()
        (directory / "manifest.json").write_bytes(payload)
        client.put_object(Bucket=bucket, Key=prefix + "/manifest.json", Body=payload, ContentType="application/json")
        print(json.dumps({"status": manifest["status"], "source_counts": counts, "output_counts": manifest["output_counts"], "injected_duplicates": manifest["injected_duplicates"], "total_records": manifest["total_records"]}))
    except BaseException:
        for sink in sinks.values():
            try:
                sink.abort()
            except Exception:
                logger.warning("Could not abort multipart upload; inspect destination before cleanup")
        raise


def _source_prefix(config, prefix):
    prefix = prefix.strip("/")
    root = config["batch_generator"]["source"]["prefix_root"].rstrip("/")
    if not prefix.startswith(root + "/") or any(part in ("", ".", "..") for part in prefix.split("/")):
        raise ValueError("Use a fresh run-id under the configured source prefix_root")
    return prefix


def _source_client(config):
    load_dotenv(Path(__file__).resolve().parents[2] / ".env", override=False)
    storage = config["batch_generator"]["minio"]
    access = os.environ.get("MINIO_ACCESS_KEY") or os.environ.get("AWS_ACCESS_KEY_ID")
    secret = os.environ.get("MINIO_SECRET_KEY") or os.environ.get("AWS_SECRET_ACCESS_KEY")
    if not access or not secret:
        raise ValueError("MinIO credentials required; no fallback credentials")
    return boto3.client("s3", endpoint_url=os.environ.get("MINIO_ENDPOINT") or storage["endpoint_url"],
                        aws_access_key_id=access, aws_secret_access_key=secret, region_name="us-east-1")


def _write_json_atomic(path, value):
    """Replace a report only after a complete JSON file has been written."""
    path = Path(path)
    if not path.parent.is_dir():
        raise ValueError(f"Report directory must already exist: {path.parent}")
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                         prefix="." + path.name + ".", delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(value, stream, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def verify_source(directory, config, client, prefix, evidence_output=None):
    started_at, started_clock = datetime.now(timezone.utc).isoformat(), time.monotonic()
    directory = Path(directory)
    if evidence_output:
        evidence_path = Path(evidence_output).resolve()
        if evidence_path in {(directory / name).resolve() for name in ("manifest.json", "readback.json")}:
            raise ValueError("Evidence output must differ from runtime manifest/readback")
        if not evidence_path.parent.is_dir() or evidence_path.suffix != ".json":
            raise ValueError("Evidence output requires an existing directory and .json suffix")
    # Remove only a previous verification report, so a failed rerun cannot leave stale PASS.
    (directory / "readback.json").unlink(missing_ok=True)
    manifest_path = directory / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    prefix = _source_prefix(config, prefix)
    bucket = config["batch_generator"]["minio"]["bucket_name"]
    if (bucket, prefix) != (manifest["bucket"], manifest["prefix"]):
        raise ValueError("Destination differs from manifest")
    expected_objects = {**manifest["objects"], "manifest.json": _file_digest(manifest_path)}
    results, audits = {}, {}
    for name, expected in expected_objects.items():
        response = client.get_object(Bucket=bucket, Key=prefix + "/" + name)
        checksum, size = hashlib.sha256(), 0

        class HashingReader(io.RawIOBase):
            def readable(self):
                return True

            def readinto(self, buffer):
                nonlocal size
                block = response["Body"].read(len(buffer))
                checksum.update(block)
                size += len(block)
                buffer[:len(block)] = block
                return len(block)

        try:
            if name.endswith(".csv"):
                group = "old" if name == "raw_events_old.csv" else "new"
                fields = CANONICAL_9_COLUMNS if group == "old" else CANONICAL_10_COLUMNS
                start, boundary, end = [datetime.fromisoformat(manifest["boundaries"][key].replace("Z", "+00:00"))
                                        for key in ("start", "evolution", "end")]
                rate = Fraction(str(manifest.get("duplicate_rate", 0)))
                originals = copies = rows = 0
                with io.TextIOWrapper(io.BufferedReader(HashingReader()), encoding="utf-8", newline="") as stream:
                    reader = csv.reader(stream, strict=True)
                    if next(reader, None) != fields:
                        raise ValueError(f"Schema header mismatch: {name}")

                    def check_row(row):
                        if len(row) != len(fields):
                            raise ValueError(f"Field count mismatch: {name}")
                        timestamp = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S UTC").replace(tzinfo=timezone.utc)
                        low, high = (start, boundary) if group == "old" else (boundary, end)
                        if not low <= timestamp < high:
                            raise ValueError(f"Schema date membership mismatch: {name}")
                        if group == "new" and int(row[-1]) not in manifest["discount_values"]:
                            raise ValueError(f"Invalid discount: {name}")

                    for row in reader:
                        check_row(row)
                        originals += 1
                        rows += 1
                        quota = originals * rate.numerator // rate.denominator
                        if quota > copies:
                            copy = next(reader, None)
                            if copy != row:
                                raise ValueError(f"Injected duplicate pair mismatch: {name}")
                            copies += 1
                            rows += 1
                if originals != manifest["counts"][group] or copies != manifest.get("duplicate_counts", {}).get(group, 0):
                    raise ValueError(f"Source/duplicate count mismatch: {name}")
                if rows != manifest.get("output_counts", manifest["counts"])[group]:
                    raise ValueError(f"Output count mismatch: {name}")
                audits[group] = {"schema_date_count": "PASS", "source_records": originals,
                                 "injected_copy_pairs": copies, "output_records": rows,
                                 "injected_duplicate_rate_per_source": copies / originals if originals else None,
                                 "injected_duplicate_fraction_of_output": copies / rows if rows else None,
                                 "natural_duplicates": "NOT_AUDITED"}
            else:
                for block in response["Body"].iter_chunks(chunk_size=8 * 1024 * 1024):
                    checksum.update(block)
                    size += len(block)
        finally:
            response["Body"].close()
        observed = {"bytes": size, "sha256": checksum.hexdigest()}
        if observed != expected:
            raise ValueError(f"Readback mismatch: {name}")
        results[name] = observed
        print(f"READBACK_PASS {name}", flush=True)
    report = {"status": "READBACK_PASS", "verified_timestamp": datetime.now(timezone.utc).isoformat(),
              "bucket": bucket, "prefix": prefix, "counts": manifest["counts"], "objects": results, "audits": audits,
              "timing": {"started_timestamp": started_at,
                         "finished_timestamp": datetime.now(timezone.utc).isoformat(),
                         "elapsed_seconds": time.monotonic() - started_clock,
                         "scope": "verification preflight and complete remote hash/CSV audit; excludes report file write"}}
    _write_json_atomic(directory / "readback.json", report)
    if evidence_output:
        evidence = {
            "status": "VERIFIED_DECLARED_CHECKS",
            "generated_timestamp": datetime.now(timezone.utc).isoformat(),
            "provenance": "Automatically generated by Batch Generator verify from full remote object readback; not manual measurements.",
            "runtime": {"python": platform.python_version(), "boto3": version("boto3"),
                        "botocore": version("botocore"),
                        "code": _file_digest(Path(__file__))},
            "run_dir": str(directory),
            "artifact_hashes": {"manifest.json": _file_digest(manifest_path),
                                "readback.json": _file_digest(directory / "readback.json")},
            "manifest": manifest, "readback": report,
            "limitations": ["Verifier uses producer manifest and declared injection schedule; not an independent reconstruction of all source events.",
                            "Natural duplicates, statistical skew and source business identity are not audited.",
                            "No Spark dedup/feature/label runtime, no >=100 GB benchmark or full-rubric completion established.",
                            "Timing scopes exclude final report publication; not a throughput benchmark."],
        }
        _write_json_atomic(evidence_path, evidence)
        print(f"EVIDENCE_WRITTEN {evidence_path}", flush=True)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E-Commerce Batch Data Generator & Benchmark Scaler")
    parser.add_argument(
        "--mode",
        choices=["small", "medium", "full", "source"],
        default=None,
        help="small: sample; medium/full: benchmark; source: complete October with date-based schema evolution and configured duplicates",
    )
    parser.add_argument("--sample-size", type=int, default=None, help="Custom sample size (for small/medium sample)")
    parser.add_argument("--target-size-gb", type=float, default=None, help="Target benchmark size in GB")
    parser.add_argument("--local-output-dir", help="Small-mode isolated local CSV/manifest output; disables MinIO")
    parser.add_argument("--config", default="config/generator_config.yaml", help="Path to config YAML")
    parser.add_argument("--skewed", action="store_true", help="Enable opt-in synthetic skew injection")
    parser.add_argument(
        "--dry-run", action="store_true", help="Estimate metrics and disk requirements without writing data"
    )
    parser.add_argument(
        "--stats-only", action="store_true", help="Run transformations and write manifest without uploading to MinIO"
    )
    parser.add_argument("--action", choices=["ingest", "verify"], default=None,
                        help="Required with --mode source")
    parser.add_argument("--run-dir", help="Source-mode local artifacts directory")
    parser.add_argument("--prefix", help="Source-mode fresh MinIO destination prefix")
    parser.add_argument("--evidence-output", help="Verify only: atomically export verified evidence JSON")
    args = parser.parse_args()
    if args.mode == "source":
        if not args.action or not args.run_dir:
            parser.error("source mode requires --action and --run-dir")
        if any((args.sample_size is not None, args.target_size_gb is not None,
                args.local_output_dir, args.skewed, args.dry_run, args.stats_only)):
            parser.error("source mode cannot use sampling, benchmark or fault options")
        if args.evidence_output and args.action != "verify":
            parser.error("--evidence-output requires --action verify")
        config = yaml.safe_load(Path(args.config).read_text())
        if not args.prefix:
            parser.error("source mode requires --prefix")
        client = _source_client(config)
        if args.action == "ingest":
            ingest_source(config["batch_generator"]["input_csv"], args.run_dir, config, client, args.prefix)
        else:
            verify_source(args.run_dir, config, client, args.prefix, args.evidence_output)
        sys.exit(0)
    if args.action or args.run_dir or args.prefix or args.evidence_output:
        parser.error("--action, --run-dir and --prefix require --mode source")

    load_dotenv(
        dotenv_path=Path(__file__).resolve().parents[2] / ".env",
        override=False,
    )

    gen = BatchDataGenerator(
        config_path=args.config,
        mode=args.mode,
        sample_size=args.sample_size,
        target_size_gb=args.target_size_gb,
        skew_override=args.skewed,
        dry_run=args.dry_run,
        stats_only=args.stats_only,
        local_output_dir=args.local_output_dir,
    )
    gen.run()
