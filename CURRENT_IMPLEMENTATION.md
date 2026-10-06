# Current implementation map

## Purpose and confidence

Bản đồ source sau cleanup và Batch Generator → MinIO integration (2026-10-06). Chỉ mô tả file đang tồn tại; source inspection và unit test không chứng minh pipeline runtime. Thiết kế ở [TARGET_ARCHITECTURE.md](TARGET_ARCHITECTURE.md); schema/ý nghĩa dữ liệu ở [DATA_CONTRACT.md](DATA_CONTRACT.md); tiến độ ở [IMPLEMENTATION_ROADMAP.md](IMPLEMENTATION_ROADMAP.md).

## Source còn lại

| File | Vai trò | Giới hạn kiểm chứng |
| --- | --- | --- |
| `src/generator/batch_generator.py` | Batch Generator: parse UTC, classify OLD/NEW, sample rồi gọi transformation/writer hiện có | Unit + small MinIO readback PASS; medium/full chưa kiểm chứng ([evidence](docs/batch_generator_minio.md)) |
| `compose.yaml` | MinIO local duy nhất, named volume và bash HTTP healthcheck | Container healthy và small readback PASS; persistence sau recreate có dữ liệu chưa kiểm chứng |
| `requirements.txt` | Khai báo python-dotenv 1.2.1 cho CLI | Fixture dotenv pass; upload logic không đổi |
| `src/generator/__init__.py` | Package generator; import tới late-event buffer cũ đã được gỡ | Không còn stream generator |
| `config/generator_config.yaml` | Config generator; vẫn có khai báo cho các phần chưa rebuild | Config không chứng minh implementation/service tồn tại |
| `tests/test_batch_generator.py` | Chín test Batch Generator | Không chứng minh pipeline/scale |
| `tests/conftest.py` | Đường dẫn import và fixture môi trường cho test | Mock Airflow/governance cũ đã được gỡ |
| `tests/fixtures/batch_generator_october_boundaries.csv.fixture` | Fixture boundary nhỏ, cố ý không sắp xếp | Giữ trong Git; không phải output sinh ra |

## Source-declared flow

```mermaid
flowchart LR
  CSV["2019-Oct.csv — local, ignored"] --> CLASS["Batch generator: UTC classification"]
  CFG["generator_config.yaml"] --> CLASS
  CLASS --> SAMPLE["small: OLD/NEW reservoirs"] --> TRANS["existing transformation"]
  TRANS --> LOCAL["local CSV + manifest"]
  TRANS --> RAW["MinIO raw batch — small readback PASS"]
  FIX["Batch Generator fixture"] --> TEST["Batch Generator unit tests"] --> CLASS
```

Sơ đồ mô tả source; nhánh small local/MinIO đã có [runtime readback](docs/batch_generator_minio.md) ngày 2026-10-06. CLI nạp root `.env` vào `os.environ`, giữ ưu tiên environment có sẵn; bucket vẫn lấy từ YAML. `--local-output-dir` dành cho small mode, ghi CSV/manifest vào đích cục bộ và tắt MinIO. Medium/full vẫn có byte target và replica transformations; chưa chạy benchmark.

## Component đã gỡ khỏi baseline

Source/config/DAG của stream generator, late-event buffer, Spark, Flink, Feast, governance, DWH/setup scripts, Airflow và API placeholder đã được gỡ; Docker/Compose cũ đã gỡ, nay có `compose.yaml` chỉ cho MinIO. Makefile, notebook và test cũ ngoài Batch Generator cũng đã được gỡ. Không có downstream consumer implementation trong baseline hiện tại.

Bản đồ pipeline và các phát hiện cũ thuộc snapshot `old-vibe-backup` tại `2cf00cf`, không phải source đang tồn tại. Các phần CURRENT của data contract và trạng thái code trong rubric được giữ nguyên theo yêu cầu; khi đọc phải đối chiếu bản đồ này, không dùng chúng để kết luận component cũ còn trong baseline. Thiết kế/contract/rubric requirements không thay đổi.

## Batch Generator và evidence

Shared `_classify_chunk` dùng UTC start/evolution/end từ config. Small mode sample riêng từng population bằng seeded random-priority reservoirs; shortage không backfill. October là batch target, November là stream target chưa có implementation trong baseline.

Giữ [ghi chú Batch Generator](docs/batch_generator.md) và fixture/tests. JSON/manifest/log evidence runtime cũ và script independent readback đã được dọn. Số liệu small runtime trong ghi chú là lịch sử; không xác nhận runtime hiện tại. Final cleanup chỉ chạy unit Batch Generator, không chạy generator trên toàn bộ CSV, MinIO hay downstream.

Lần chạy mới: [Batch Generator → MinIO small runtime evidence](docs/batch_generator_minio.md), 1.020.000 dòng; schema/count/bytes và CSV SHA-256 readback PASS. Không xác nhận ≥100 GB hoặc downstream.

Ghi chú component mới và evidence mới được index tại [docs/INDEX.md](docs/INDEX.md). Không bắt đầu Batch Generator — schema and deterministic fields trong cleanup này.
