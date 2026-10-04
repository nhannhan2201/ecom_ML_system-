# WorkFlow học và thực hiện dự án theo rubric

Cập nhật 04/10/2026. Đây là lộ trình học, triển khai và kiểm chứng theo từng component; không phải bằng chứng dự án đã đạt rubric. Novel Ideas được bỏ qua theo yêu cầu. Có code hoặc test không đồng nghĩa pipeline đã chạy đúng.

Điểm vào: [INDEX](INDEX.md), [DATA_FLOW](DATA_FLOW.md), [LEARNING_ROADMAP](LEARNING_ROADMAP.md), [RUBRIC](../RUBRIC.md). Tài liệu này giữ thiết kế bài toán, luồng đích và ánh xạ yêu cầu; roadmap chỉ giữ milestone/tiến độ, DATA_FLOW chỉ mô tả source hiện có.

## 1. Cách dùng và trạng thái

Mỗi lượt một milestone. Trước khi sửa, giải thích component làm gì, input/output, file, kết nối, ví dụ lỗi và invariant. Đọc Quickstart/tài liệu chính thức đúng version sau khi xác định version ở requirements/image/config. Người học được hỏi tối đa một câu ngắn để dự đoán. Sau thay đổi nhỏ, đọc diff và hướng dẫn tự trace/debug.

Phân biệt: **Yêu cầu rubric** (ghi trực tiếp trong workbook); **Thiết kế đề xuất** (lựa chọn dự án); **Có code, chưa kiểm chứng**; **Khác tài liệu** (có bằng chứng hai phía); **Đã kiểm chứng** (lệnh, input/config, thời điểm, kết quả và giới hạn ghi lại). Trạng thái bảng: Chưa kiểm tra / Đang làm / Đã kiểm chứng. Không đánh dấu đạt từ tên file, YAML, DAG trigger, unit test riêng lẻ hoặc số liệu cũ.

## 2. Bài toán baseline đã chốt: chỉ dùng feature 15 phút

- Dự đoán xác suất user có ít nhất một purchase bất kỳ trong một giờ sau thời điểm dự đoán.
- Mỗi đầu phút t, tạo tối đa một lượt/user nếu view/cart hợp lệ được xử lý trong phút trước [t−1m,t).
- Bốn feature baseline: f_views_15m, f_carts_15m, f_purchases_15m, total_spend_15m. Dùng event-time [t−15m,t) đã được xử lý trước điểm chốt.
- Label là purchase của user trong [t,t+1h). Lưu prediction cùng feature thực dùng; bổ sung label về sau. Chỉ gán 0 khi xác nhận đủ dữ liệu tương lai theo chính sách completeness/lateness; nếu thiếu thì để pending.
- Không chờ watermark trước khi dự đoán; vẫn học window, watermark, late events vì rubric yêu cầu.
- Mốc UTC, cửa sổ trái đóng/phải mở. Event trễ không sửa ngược feature của lượt đã phát.

Ví dụ: view lúc 10:27:05, xử lý xong 10:27:06 → dự đoán 10:28, feature [10:13,10:28), label [10:28,11:28). Đây là thiết kế, chưa xác nhận runtime.

Baseline này được chọn vì mục tiêu là propensity ngắn hạn và phép tính gần hành vi hiện tại dễ giải thích, đối chiếu giữa Spark/Flink. Tradeoff: không có tín hiệu lịch sử mua sắm dài hạn. Đo baseline trước; chỉ thêm long-term features nếu temporal holdout chứng minh có ích. Rubric không bắt buộc 30d, nhưng yêu cầu offline feature/materialize và stream push tới cả offline lẫn online vẫn giữ nguyên.

## 3. Bản đồ rubric

Đã đọc toàn bộ sheet hai workbook. DE edai-1 (50%) trùng MLE de; sheet final coursework trùng sheet cùng tên; DE edai-2 (50%) trùng MLE mle. Yêu cầu trùng tính một lần. Final/MLE khác nhau được giữ riêng. Hàng là số hàng trong workbook. Mọi trạng thái hiện tại: **Chưa kiểm tra**.

| Workbook/sheet/hàng | Yêu cầu rubric | Component/file cần đối chiếu | Đầu ra và kiểm chứng |
|---|---|---|---|
| DE edai-1, MLE de r3–4 | Docker/Compose; tối ưu Dockerfile | docker/, Compose, Makefile | Stack; build; image sizes đo lại |
| DE edai-1, MLE de r5–8 | Generator offline >=100GB, skew, schema evolution, lỗi offline khác như duplicate, lưu để ingest Bronze | config/generator_config.yaml, src/generator/batch_generator.py, tests | Raw outputs/manifest/profile; fixture; schema/tỷ lệ/mẫu số; bytes thực >=100GB |
| DE edai-1, MLE de r9–10 | Late arrival và lỗi streaming khác như duplicate | stream_generator.py, late_event_buffer.py | Kafka output, delay/duplicate profile, ACK và replay/restart check |
| DE edai-1, MLE de r11–15 | Spark baseline, skew/schema/other handling, tích hợp pipeline | spark_baseline.py, spark_optimized.py, skew_experiment.py, DAGs | Cùng input/config; Spark UI; correctness; Airflow output |
| DE edai-1, MLE de r16–19 | Flink baseline, late arrival, lỗi streaming khác, window | stream_baseline.py, stream_optimized.py, submit script | Fixture event-time, UI/log/checkpoint, expected window output |
| DE edai-1, MLE de r20–21 | Lakehouse và DWH optimization | optimize_storage.py, setup_dwh_schemas.py, Spark | Snapshot/count parity; query plans và số đo trước/sau |
| DE edai-1, MLE de r22–27 | Airflow DP1/2/3 ingest và validate; connections/variables dùng lại | dags/dp1*, dp2*, dp3*, Spark | DAG hoàn tất, task state, data validation; trigger chưa đủ |
| DE edai-1, MLE de r28–33 | DataHub lineage và validation cho DP1/2/3 | governance/catalog.py, sync_catalog.py, verify_contracts.py | UI/API tables, lineage edges, assertions gắn đúng job/table |
| DE edai-1, MLE de r34–38 | Zone/table visualization, SCD2, feature event_timestamp/created, dim/fact, naming | Spark schemas, DWH setup, feature_store/features.py | Schema inspection, A→B→A fixture, ERD/relationship export |
| DE final r3–8 | LLM inference platform, custom model, gateway/security, benchmark, global config, registry | Cần inventory; chưa xác định file | Deployment/config, gọi model, registry UI, benchmark run |
| DE final r9–10 | RAG pipeline text→chunk→embedding vào Feast, governance | Cần inventory; dags/feature_store/governance là ứng viên | Airflow success, retrieval, DataHub lineage/validation |
| DE final r11–12; MLE mle r11–12 | Incremental offline→online materialize; stream feature push offline và online | dp4_feast_materialize.py, feature_store/materialize.py, features.py, stream_push_job.py | Airflow incremental run; đọc lại offline và Redis; chứng minh hai đích |
| DE final r13–14; MLE mle r13–14 | KFP tối thiểu 4 bước; MLflow model/data/metadata/tag và versioning | Pipeline/registry files cần inventory | KFP run, MLflow version/tags; data incremental version qua hai run |
| DE final r15; MLE mle r15 | KServe model-inside-image | src/api/README.md placeholder; manifests cần tìm | Pod/config và request inference thật |
| DE final r16–27; MLE mle r16–27 | FastAPI/Pydantic/async/health; MCP tool/agents/sandbox/registry; agent inference+RAG; MCP/A2A gateway; coordinator/warm-up/autoscale | API placeholder; agent paths cần inventory | Contract tests; deployed UI/policies; scale và agent/tool traces |
| DE final r28–32; MLE mle r28–32 | Coverage >90%, boundary/equivalence, mutation >80%, property/idempotency, load test HTML | tests/, pyproject.toml; API TBD | Test outputs, scoped mutation report, property cases, Locust HTML/SLA |
| DE final r33–36; MLE mle r33–36 | CI/CD data pipeline (MLE yêu cầu RAG demo), MCP/agents và stream feature jobs | CI config cần inventory; DAGs/pusher | Successful test/build/deploy runs và artifact refs |
| DE final r37–45; MLE mle r37–45 | External gateway, auth/rate limit/domain/HTTPS; Terraform; Ansible | IaC files cần inventory | Config/plan validation và deployment evidence |
| DE final r46–51; MLE mle r46–51 | API/compute/LLM/agent telemetry, logs/traces | Observability source cần inventory | Dashboard; correlated metrics/logs/traces và LLM/tool counts |
| DE final r52–55; MLE mle r52–55 | A/B, secret management, repo design/docs/README | README, CI, infra/security paths | Traffic split, secret integration, docstrings/diagram/links |

Novel Ideas không làm: DE edai-1 r39–40, DE edai-2/MLE mle r56–57. Workbook DE r5 ghi offline tối thiểu 100GB; fixture local không thỏa scale. DE r11–19 cần baseline và giải thích tối ưu bằng Spark/Flink UI. Final r18 yêu cầu label table có id/label; prediction_id là lựa chọn thiết kế cần map. Final r19–22 yêu cầu materialize, stream push vào cả stores và TTL. MLE nâng cao không phải Novel Ideas nên vẫn trong lộ trình toàn rubric.

## 4. System map: hiện tại và mục tiêu

Sơ đồ đầu: cạnh liền là kết nối static đã được ghi trong DATA_FLOW/source, runtime vẫn chưa xác nhận. Sơ đồ sau: nét đứt là thiết kế mục tiêu chưa xác nhận. Đây không phải deployment diagram hoàn chỉnh.

~~~mermaid
flowchart LR
  subgraph source["Source links in repo; runtime unverified"]
    OC["Oct CSV"] --> BG["Batch generator"]
    BG --> MR["MinIO raw batch"]
    CSV["Stream generator input"] --> K["Kafka events"]
    K --> F["Flink optimized"]
    F --> ST["MinIO stream staging"]
    F --> KF["Kafka feature topic"]
    KF --> PUSH["Feast stream pusher"]
    PUSH --> REDIS["Redis"]
    MR --> D1["Spark DP1"]
    ST --> D1
    D1 --> BR["Bronze Delta"]
    BR --> D2["Spark DP2"]
    D2 --> SILVER["Silver/Gold Delta"]
    SILVER --> D3["Spark DP3"]
    D3 --> OLD["Current source: 30d features/labels"]
    OLD --> D4["Feast materialize DAG"]
    D4 --> REDIS
  end
~~~

~~~mermaid
flowchart TD
  OCT["Oct CSV"] -.-> BG["Batch generator"]
  BG -. Raw .-> SP["Spark DP1/2: Bronze to Silver"]
  NOV["Nov CSV"] -. replay .-> GEN["Stream generator"]
  GEN -. events .-> K["Kafka"]
  K -.-> F["Flink: windows, late/dedup, features"]
  F -. raw events .-> SP
  F -. updates .-> WO["Offline stream writer"]
  F -. updates .-> WR["Online Feast writer"]
  WO -.-> OFF["Feast offline source"]
  WR -.-> RED["Redis online store"]
  SP -. Silver .-> SF["Spark DP3: offline features"]
  SF -.-> OFF
  OFF -. incremental .-> MAT["Airflow materialize"]
  MAT -.-> RED
  OFF -. retrieval .-> TRAIN["Notebook then KFP training"]
  TRAIN -. model .-> API["KServe/API inference"]
  RED -. fresh features .-> API
  API -. prediction plus actual features .-> PLOG["Prediction log"]
  PLOG -.-> LABEL["Delayed label job"]
  SP -. future clean purchases .-> LABEL
  LABEL -.-> TRAIN
  AIR["Airflow"] -. orchestrates batch .-> SP
  DH["DataHub"] -. metadata/lineage/assertions .-> SP
~~~

Kafka transports/retains events; Flink handles continuous event-time/window processing; Spark handles historical batches; MinIO stores objects and Delta manages table snapshots; Airflow orchestrates jobs; Feast defines/retrieves/materializes/pushes features; Redis serves online values; DataHub catalogs metadata/lineage/assertions; PostgreSQL DWH supports analytics. Serving/model/prediction log and label job in proposed graph need implementation. Airflow does not carry data rows between tasks. DataHub assertions do not replace validation.

## 5. Dataset and table contracts to learn

Proposal: October CSV is bootstrap batch [2019-10-01,2019-11-01); split at 16 October for old/new schema simulation. November CSV replays [2019-11-01,2019-12-01) through Kafka. Do not preload November into history and ingest it again from CSV if it already arrived through stream. Local selection should retain all events for a fixed cohort of users. A separate 100GB config and destination come later.

Two CSVs were previously checked for headers, size, endpoint rows and first 100k sample: same 9 source columns; sample had all three event types and no selected format errors; category/brand can be null. Full timestamp ordering, gaps, duplicate rate and cross-month ID semantics remain unverified.

| Object/table | Grain | Producer/consumer |
|---|---|---|
| Raw file/object | Source/generated event, may contain injected fault | Generator → DP1 |
| raw_events Bronze | Ingested event | DP1 → DP2 |
| stg_events Silver | Normalized valid event after agreed dedup | DP2 → feature, Gold, label |
| dim_user/dim_product/fact_user_events | Entities/history and analytics facts | Spark → DWH |
| feat_user_15m (proposal) | One user's feature values at event timestamp t | Spark/Flink writers → Feast offline |
| Stream feature update (proposal) | User, four values, event_timestamp, created, availability/version metadata | Flink → separate offline/online jobs |
| Redis record | Latest online values plus timestamp/version | Feast writer → serving |
| prediction_log (proposal) | One prediction plus exact feature values used | Serving → label/training |
| prediction_labels (proposal) | Outcome keyed by prediction_id | Label job → training |
| Training dataset (proposal) | Historical feature row joined to valid label | Feast retrieval + join |

Event identity is open; source has no event_id column, so don't deduplicate solely by coincident values without analyzing risk of merging real events. A user can have multiple predictions; label grain is prediction_id or (user_id,prediction_timestamp), never user_id alone. Rubric requires feature temporal columns event_timestamp and created; their meaning and availability/version need point-in-time tests.

## 6. Milestones with study and verification

Commands below describe verification shapes, not exact verified CLI. At milestone start inspect Makefile/help/argparse and official docs; record exact command and destination before running. Use fixtures first, never default live data destination for tests. Large generation, overwrite, reset, volume/topic/checkpoint deletion or cloud spend require explaining impact and user agreement.

| M | Learn/inspect | Official Quickstart/docs | Small check and expected result | Debug question / exit criteria | Rubric |
|---|---|---|---|---|---|
| 0 Repo/data | Status/diff, config, CSV schema/time/null/order | Python CSV/pandas docs; version from requirements | git status --short; git diff; bounded CSV parser sample | Sample vs whole? UTC and coverage understood | Preparation |
| 1 Batch selection | generator config, batch_generator.py, relevant tests, Makefile | pandas read_csv/chunking Quickstart matching pin | Fixture before/at/after 16 Oct and Nov boundary | Where does 16 Oct 00:00 go? No boundary loss/overlap | DE r5,r8 |
| 2 Event identity/duplicate | Generator transforms/config/tests and Silver dedup | Pytest/Hypothesis docs by pin | Retry fixture, stable identity, measured numerator/denominator | Could matching events be real? Rule plus limits tested | DE r7,r10 |
| 3 Schema evolution | Effective date, parts, DP1 schema reader | pandas + Delta 3.0/Spark 3.5 schema docs | Two fixture files; expected columns/types/null | Is flag used? Can consumer read both? | DE r6,r13 |
| 4 Skew/scale config | Natural vs synthetic skew, seed/replica | Spark 3.5 tuning docs | Tiny hot-key/cardinality profile; no 100GB run | Define denominator; synthetic data doesn't represent real behavior | DE r5,r12 |
| 5 Raw output | Upload/manifest, MinIO Compose/bucket | MinIO client/S3 Quickstart matching library | Dry-run/temp then isolated prefix and read-back | Partial upload/retry; read-back agrees with manifest | DE r8 |
| 6 Kafka generator | Producer/key/callback/checkpoint/Compose | Kafka client and broker docs for configured version | Mock producer then bounded isolated topic | enqueue vs ACK? Explain replay/resume | DE r9,r10 |
| 7 Flink baseline/window | Source/schema/event-time/watermark/window/sink | Flink/PyFlink 1.17.1 Quickstart; check runtime Python | Known small ordered/out-of-order events | Event vs processing time; hand-calculated result matches | DE r16,r19 |
| 8 Flink late/dedup | Optimized job/state/allowed lateness/feature contract | Flink 1.17 state/window docs | Controlled late/duplicate fixture and sink inspection | Can late update prior output? Semantics/guarantees tested | DE r17,r18 |
| 9 DP1 Bronze | DP1 DAG, ingest, provenance/schema | Spark 3.5 + Delta 3.0 Quickstart | Fixture/local temp, isolated MinIO prefix, Delta snapshot check | Retry/bootstrap partial; no loss/double-ingest | DE r22,r23 |
| 10 DP2 Silver | Cast/null/quarantine/dedup | Spark SQL/DataFrame docs | Valid/invalid/duplicate fixture; reconcile counts | Deterministic dedup? Counts categorized | DE r24,r25 |
| 11 Gold/DWH | SCD2, dim/fact grain, join, PG setup | Spark/Delta and PostgreSQL 15 docs | A→B→A product fixture; key/orphan assertions | What is fact grain? Export ERD/query plan | DE r20,r21,r35,r37 |
| 12 DP3 audit | Current feature/label transform and tests | Spark SQL docs | Hand-computable fixture to temp | Label population/horizon/leakage; write code-vs-target gap | DE r26,r27,r32,r33,r36; final r18 |
| 13 Airflow | DAG edges/retries/connections/variables | Airflow 2.7.3 Quickstart/operators | Structure test then isolated DAG run | Trigger vs completed? Validate actual output | DE r22–27 |
| 14 DataHub | Catalog/sync/verify/Compose profile | DataHub 0.13 docs matching image | Contract test then local UI/API | Was lineage published or merely declared? | DE r28–33; final RAG governance |
| 15 Feast retrieval | features.py, yaml, historical retrieval | Feast 0.38 Quickstart/providers/retrieval | Local fixture and point-in-time query | Does result leak future? Provider capability confirmed | DE r36; final r19,r23 |
| 16 Materialize/TTL | materialize.py, DP4 DAG, provider, Redis | Feast 0.38 materialize/TTL and Redis 7 docs | Fixed interval to isolated store; compare before/after | Cursor, stale overwrite; TTL differs from 15m window | final r19,r22; MLE r11 |
| 17 Stream to offline/online | Feature contract, pusher, Flink sink | Feast 0.38 push/provider/offline-store docs | Separate test writers; inspect historical retrieval and Redis | Does target=both really write both? Test ordering/retry | final r20,r21; MLE r12 |
| 18 Prediction/label/notebook | Prediction rows, label job, Feast retrieval, notebook | Spark time join and model libs by pin | Positive/negative/pending/boundary fixture | Label prediction rows only; horizon complete; temporal split | final r18,r23 |
| 19 KFP/MLflow/data version | Inventory training and registry source | Kubeflow Pipelines/MLflow docs for selected versions | Minimal run when environment ready; inspect artifacts | Four required steps, distributed train, model/data linkage | final r24–27 |
| 20 KServe/API | Real source/manifests beyond README placeholder | KServe/FastAPI/Pydantic Quickstarts by version | Unit client with mocks then isolated deploy | Invalid input, async/health, model-in-image/inference | MLE r15–18 |
| 21 RAG/agents/gateway | Locate code before design | Official tutorials linked in workbook; verify hyperlinks | Toy text/chunk retrieval and isolated service | Provenance, policy, sandbox, replicas, coordinator trace | MLE r3–10,r16–27 |
| 22 CI/CD/security/IaC/telemetry/A-B | Inventory configs/source before implementation | Official docs for selected CI, Vault, NGINX, Terraform, Ansible, metrics | Validate config/plan; isolated deployment | Secrets/routes/correlation/traffic split verified | MLE r33–55 |
| 23 Docker/100GB/benchmarks | Dockerfiles, resource limits, output namespace | Docker/MinIO/Spark docs matching environment | Inspect/dry-run; scale only after bounded resource plan | Record exact bytes/input/config/commit/time | DE r3–5,r20–21 |

Where the official URL/version has not been verified, resolve it during that milestone; do not invent URL or command. Each closeout records file/version read, exact command/input, actual output, debug method, evidence/docs, uncertainty and paper-ready notes.

## 7. Open design questions and risks

- Source files have no event_id. Dedup by matching values may merge real events; decide identity before claiming exact duplicate rate.
- CSV has event_time but no reliable arrival_time. Historical training from CSV is idealized replay unless arrival behavior is recorded.
- “At minute t” availability semantics need an explicit stream fixture: a record with event_time before t may arrive after t.
- Prediction service, prediction log and delayed label job are design requirements, not identified implementations yet.
- Event-time feature calculations must align between Spark and Flink, and old stream features must not overwrite newer Redis values.
- October/November CSV full-file ordering, gaps, duplicate semantics and cross-month user/product behavior remain unverified.

## 8. Scope and cleanup status

The old benchmark/report/evidence collectors were removed in an earlier uncommitted cleanup. The remaining scripts were cross-checked against Makefile, Compose and docs. Operational scripts remain referenced or have a distinct use; no additional `.py` or `.sh` file is safe to delete based on this review.

There is one known operational issue: `make all-small` runs `scripts/rehearse_all_small.py`, which still references the deleted `docs/evidence/rehearsal_log.txt`; its DWH setup step invokes `scripts/setup_dwh_schemas.py`, which contains destructive `DROP TABLE ... CASCADE` statements. Do not run this rehearsal while learning until it is redesigned and reviewed. This is a follow-up code milestone, not a reason to delete scripts that Makefile calls.

The repository still contains only four Markdown files under `docs/`; they now have separate roles listed in [INDEX.md](INDEX.md). Keep [src/api/README.md](../src/api/README.md) as a scoped placeholder until the API milestone determines whether an implementation exists. No Markdown or script files were deleted in this review turn; prior working-tree deletions were preserved.

## 9. Starting point

First implementation milestone is M1 batch selection in [LEARNING_ROADMAP.md](LEARNING_ROADMAP.md). Use a tiny fixture and an isolated output; do not run the large generator or the `make all-small` rehearsal.
