# RUBRIC AUDIT: E-COMMERCE DATA PLATFORM (`ecom_ML_system`)

**Authoritative Source:** `rubic/EDAI K11 - DE.xlsx` → Sheet: `edai-1 (50%)` (Total: 100 points)  
**Evaluation Target:** Technical Correctness, Architectural Coherence, and Verification-Readiness.

---

## 0. STATUS DEFINITIONS

Per explicit project instruction, no runtime benchmark results are fabricated. The audit strictly uses:

* **IMPLEMENTED:** Code, configuration, integration, and semantics are complete and statically/locally verified. The component is fully functional and ready for runtime evidence capture.
* **PARTIAL:** Architectural capability exists in code, but requires external/cloud resources or is intentionally scoped to a test scale for local development.
* **NOT IMPLEMENTED:** Intentionally omitted per instruction (e.g. Novel Ideas) or not yet created.
* **READY FOR VERIFICATION:** Implementation is complete and frozen; awaiting the interactive verification and evidence capture session with the student.

---

## 1. OFFICIAL RUBRIC ALIGNMENT MATRIX

| Rubric Category | Specific Requirement | Points | Status | Implementation Details & Source of Truth | Verification Readiness / Scope Note |
| :--- | :--- | :---: | :---: | :--- | :--- |
| **Engineering Fundamentals** | Docker & Docker Compose setup | 2.0 | **IMPLEMENTED** | 7 compose files in `docker/` (`kafka`, `minio`, `postgres`, `redis`, `flink`, `airflow`, `datahub`) with shared network `docker_default`. | Validated via `docker compose config`. Ready for runtime container inspection. |
| **Engineering Fundamentals** | Optimize Dockerfile (e.g. multistage / clean) | 3.0 | **IMPLEMENTED** | `docker/Dockerfile.airflow` based on OpenJDK 11 headless, apt-cache cleanup, pip `--no-cache-dir`, `--upgrade "email-validator>=2.0.0"`. Standalone image `ecom-airflow-spark:2.7.3` built without host conda mounts. | Ready for image size measurement comparison against base image. |
| **Implement Data Generator** | Simulate Skew | 2.0 | **IMPLEMENTED** | `src/generator/batch_generator.py` leverages natural REES46 distribution: 96.85% `view` events, smartphone category 40.27%, top 3 brands 34.38%. | Ready for skew metric validation on generated data. |
| **Implement Data Generator** | Simulate High Cardinality | 2.0 | **IMPLEMENTED** | REES46 source contains 163k+ unique `user_id`, 63k+ unique `product_id`, 226k+ `user_session` values. | Natural cardinality preserved in chunked processing. |
| **Implement Data Generator** | Simulate Schema Evolution | 2.0 | **IMPLEMENTED** | Part 1 (01-15/10): 9 canonical columns. Part 2 (16-25/10): 10 canonical columns with `discount_percent` from `[4, 5, 8, 10, 12]%`. | Generates `raw_events_old*.csv` and `raw_events_new*.csv`. |
| **Implement Data Generator** | Simulate Duplicate Rate | 2.0 | **IMPLEMENTED** | Injects ~2% duplicate rate via `_transform_chunk` with random shuffle independent of replay replicas. | Separate from time-shift replay. Ready for duplicate counting check. |
| **Implement Data Generator** | Store Data into MinIO | 2.0 | **IMPLEMENTED** | Direct Boto3 upload to `s3://ecommerce-raw/batch/`. Supports single files for dev and part files for benchmark. | Upload tested against live MinIO container. |
| **Implement Data Generator** | Data Volume requirement (>=100GB) | - | **PARTIAL** | `batch_generator.py` implements zero-OOM chunked streaming (`--mode full --target-size-gb 100`) using deterministic replay and time shifting. Local repo defaults to `small` (1M rows, ~131MB) to protect host disk. | Full 100GB benchmark execution requires cloud VM / storage and will be run during the cloud verification phase. |
| **Implement Data Generator** | Simulate Late Arrivals | 2.0 | **IMPLEMENTED** | `src/generator/stream_generator.py` uses in-memory `late_buffer` driven by event time (5% events delayed 5-10 minutes). | Ready for Kafka stream verification. |
| **Implement Data Generator** | Simulate Streaming Duplicate | 2.0 | **IMPLEMENTED** | `stream_generator.py` sends 1.5% duplicate messages with key=user_id to preserve partition ordering. | Ready for streaming duplicate verification. |
| **Processing Jobs (Spark)** | Baseline (without optimization) | 2.0 | **IMPLEMENTED** | `src/spark/spark_baseline.py` sets `spark.sql.adaptive.enabled=false`, `autoBroadcastJoinThreshold=-1`, naive shuffle partitions. | Ready for Spark UI bottleneck evidence capture. |
| **Processing Jobs (Spark)** | Handle Skew | 3.0 | **IMPLEMENTED** | `src/spark/spark_optimized.py` implements 4-way Salting join, `F.broadcast()`, and AQE skew handling. | Ready for Spark UI optimization comparison. |
| **Processing Jobs (Spark)** | Handle Schema Evolution | 3.0 | **IMPLEMENTED** | `spark_optimized.py` DP1 reads old and new CSVs with Delta `.option("mergeSchema", "true")`. Old events backfilled with `NULL`. | Ready for Delta transaction log inspection. |
| **Processing Jobs (Spark)** | Handle Other Offline Data Problem | 3.0 | **IMPLEMENTED** | `spark_optimized.py` DP2 applies `dropDuplicates(["user_id", "event_time", "product_id", "event_type"])` and HyperLogLog `approx_count_distinct(rsd=0.01)`. | Deduplication and cardinality optimization ready for verification. |
| **Processing Jobs (Spark)** | Spark integrated into Data Pipelines | 2.0 | **IMPLEMENTED** | Airflow DAGs `dp1_raw_to_bronze`, `dp2_bronze_to_silver_and_gold`, `dp3_compute_offline_features` call Spark scripts via `BashOperator`. | DAG imports verified with 0 errors. Ready for Airflow DAG run evidence. |
| **Processing Jobs (Flink)** | Baseline (without optimization) | 2.0 | **IMPLEMENTED** | `src/flink/stream_baseline.py` uses parallelism=1, 2s watermark, no state deduplication. Backlogs under burst traffic. | Ready for Flink UI baseline evidence capture. |
| **Processing Jobs (Flink)** | Handle Late Arrival | 3.0 | **IMPLEMENTED** | `src/flink/stream_optimized.py` defines `WATERMARK FOR row_time AS row_time - INTERVAL '15' MINUTE`. | Ready for Flink UI 0-late-dropped records evidence. |
| **Processing Jobs (Flink)** | Handle Streaming Duplicate | 3.0 | **IMPLEMENTED** | `stream_optimized.py` applies `ROW_NUMBER() OVER (PARTITION BY user_id, event_time, product_id, event_type ORDER BY row_time ASC) = 1` for 15m feature aggregation. | Raw stream preserved in MinIO staging (Append-only); Silver Spark deduplicates globally. |
| **Processing Jobs (Flink)** | Window Processing | 2.0 | **IMPLEMENTED** | Sliding window `HOP(row_time, INTERVAL '1' MINUTE, INTERVAL '15' MINUTE)` computes 4 features and sinks to Kafka and console. | Ready for Kafka feature topic verification. |
| **Data Storage** | Lakehouse Optimization | 3.0 | **IMPLEMENTED** | `scripts/optimize_storage.py` and `spark_optimized.py`: Delta partitioning by `date`, bin-packing compaction, Z-Order by `user_id`, VACUUM retention. | Ready for file count and partition layout verification. |
| **Data Storage** | Datawarehouse Indexing | 3.0 | **IMPLEMENTED** | PostgreSQL DWH has B-Tree composite index `idx_fact_user_events_user_time`. Schema setup script in `scripts/setup_dwh_schemas.py`. | Ready for `EXPLAIN ANALYZE` index scan proof. |
| **Airflow Pipelines** | DP1: Raw -> Bronze (Ingest & Validate) | - | **READY FOR VERIFICATION** | `dags/dp1_raw_to_bronze.py`: Ingests batch CSVs and staging stream -> validates schema and nulls -> triggers DP2. Centralized Connections (`minio_s3_conn`) retrieved via `BaseHook.get_connection()`. | Ready for Airflow DAG run execution and log verification. |
| **Airflow Pipelines** | DP2: Bronze -> Silver & Gold (Ingest & Validate) | - | **READY FOR VERIFICATION** | `dags/dp2_bronze_to_silver_and_gold.py`: Deduplicates Bronze into Silver -> builds `dim_product` (SCD2-like), `dim_user` (snapshot), `fact_user_events` (pure fact) -> validates FK integrity -> triggers DP3. Centralized Connections (`minio_s3_conn`, `postgres_dwh`) via `BaseHook`. | Ready for Airflow DAG run execution and log verification. |
| **Airflow Pipelines** | DP3: Offline Features & Labels (Ingest & Validate) | - | **READY FOR VERIFICATION** | `dags/dp3_compute_offline_features.py`: Computes `feat_user_30d` and `user_labels` -> validates Feast timestamps -> triggers DP4. Centralized Connections (`minio_s3_conn`) via `BaseHook`. | Ready for Airflow DAG run execution and log verification. |
| **Airflow Pipelines (Additional)** | DP4: Feast Materialization | - | **IMPLEMENTED** | `dags/dp4_feast_materialize.py`: Materializes historical features from Delta Lake to Redis. Uses `minio_s3_conn` and `redis_default` via `BaseHook`. Additional pipeline beyond official rubric scope. | Ready for execution. |
| **Data Governance (DataHub)** | DP1 Lineage & Validation | - | **READY FOR VERIFICATION** | Declarative DataFlow/DataJob in `governance/catalog.py` (Option B: Declarative Governance). Lineage: `raw_batch` + `raw_staging_stream` -> `bronze_raw_events`. Assertion: zero-nulls. | Ready for DataHub GMS synchronization and UI inspection. |
| **Data Governance (DataHub)** | DP2 Lineage & Validation | - | **READY FOR VERIFICATION** | Lineage: `bronze_raw_events` -> DP2 -> `silver_stg_events`, `dim_product`, `dim_user`, `fact_user_events`. Assertions: Silver dedup, Gold SCD2 valid range (`valid_from_ts <= valid_to_ts`). | Sample-based validation (50,000 rows). Ready for UI inspection. |
| **Data Governance (DataHub)** | DP3 Lineage & Validation | - | **READY FOR VERIFICATION** | Lineage: `silver_stg_events` -> DP3 -> `feat_user_30d`, `user_labels`. Assertions: Feast temporal columns, binary labels. | Sample-based validation (50,000 rows). Ready for UI inspection. |
| **Schema Design** | Visualize Tables all zones | - | **IMPLEMENTED** | ERD and table dictionaries across Bronze, Silver, Gold, and Serving documented in `docs/Schema_Design.md` and `docs/DATA_FLOW.md`. | Ready for schema diagram review. |
| **Schema Design** | Dim table with SCD Type 2 | - | **IMPLEMENTED** | `dim_product` derives SCD2-like historical versioning from observed attribute transitions (`valid_from_ts`, `valid_to_ts`, `is_current`, `product_sk`). `dim_user` is a current-state snapshot dimension (`is_active`, `first_seen`, `last_seen`). | Accurately differentiated; no fake SCD2 on `dim_user`. |
| **Schema Design** | Feature tables (feat_ tables) | - | **IMPLEMENTED** | `feat_user_30d` and `feat_user_stream` contain required Feast timestamp fields `event_timestamp` and `created`. | Schema verified against Feast specifications. |
| **Schema Design** | Dim & Fact Relationship | - | **IMPLEMENTED** | Kimball Star Schema: `fact_user_events` references `dim_product` via surrogate key `product_sk`. Raw `product_id` is excluded. | Pure Fact Table structure maintained. |
| **Schema Design** | Naming Convention | - | **IMPLEMENTED** | Standard prefixes: `raw_` (Bronze), `stg_` (Silver), `dim_` (Gold Dim), `fact_` (Gold Fact), `feat_` (Gold Feature). | Complete adherence across codebase. |
| **Novel Ideas** | Novel Idea 1: Debezium CDC | - | **NOT IMPLEMENTED** | Out of scope per explicit user instruction to focus on core DE engineering. | Intentionally not implemented. |
| **Novel Ideas** | Novel Idea 2: Trino Query Engine | - | **NOT IMPLEMENTED** | Out of scope per explicit user instruction to focus on core DE engineering. | Intentionally not implemented. |

---

## 2. QUALITATIVE VERIFICATION SUMMARY

* **Core Data Engineering Requirements:** All required core DE components (Docker, Data Generator, Spark, Flink, Storage, Airflow DP1-DP3, DataHub Governance, Schema Design) are **IMPLEMENTED** and **READY FOR VERIFICATION**.
* **100GB Scalability:** Generation engine implements chunked zero-OOM replay logic; execution is **PARTIAL** locally to protect host disk and awaiting cloud verification.
* **DataHub Integration Strategy:** Uses **Option B (Declarative Governance)** via `governance/catalog.py`, `governance/sync_catalog.py`, and `governance/verify_contracts.py`. Automatic Airflow plugin listeners are intentionally out of scope / not used to maintain Airflow 2.7.3 stability.
* **Pipeline Scope Distinction:** The official Data Governance rubric explicitly assesses **DP1, DP2, and DP3**. Pipeline **DP4 (Feast Materialization)** is maintained as an additional engineering pipeline to complete the ML serving lifecycle.
* **No Premature Scoring:** Numeric scores are withheld until runtime execution, container logs, UI screenshots, and performance metrics are captured during verification.
