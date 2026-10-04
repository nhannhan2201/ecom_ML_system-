# Learning Roadmap — cách học và tiến độ

File này là giáo án và sổ tiến độ: trả lời **ta sẽ học từng phần như thế nào**, **theo thứ tự nào**, và **đang ở milestone nào**. Nó không phải report bằng chứng kỹ thuật. Mỗi lượt tập trung một component hoặc một lát data flow.

Vai trò liên quan: [AGENTS.md](AGENTS.md) giữ quy tắc pair-programming; [WorkFlow.md](WorkFlow.md) giữ bài toán/thiết kế mục tiêu; [DATA_FLOW.md](DATA_FLOW.md) giữ đường nối hiện tại theo source; [RUBRIC.md](RUBRIC.md) theo dõi tiêu chí; [docs/INDEX.md](docs/INDEX.md) dành cho docs kỹ thuật và evidence của từng công nghệ.

## Cách dạy và học một milestone

1. Bắt đầu bằng mục đích component và problema nó giải quyết; nêu input/output và file thuộc milestone. Giải thích kết nối sang component khác sau khi vai trò riêng đã rõ.
2. Vẽ Mermaid từ code đã kiểm tra, ghi component/role/file; phân biệt cạnh thấy trong source với cạnh chỉ được tài liệu mô tả. Source connection không chứng minh runtime.
3. Xác định phiên bản thư viện từ requirements/config/image. Dùng Quickstart/tài liệu chính thức đúng phiên bản; không suy ra công nghệ đang dùng đúng chỉ từ tên component.
4. Trace dữ liệu theo thứ tự input → schema → xử lý → output → consumer. Chỉ đọc file cần cho milestone hiện tại.
5. Đối chiếu code với rubric, tài liệu chính thức, docs nội bộ, tests/logs/evidence. Phân loại: đúng và đã kiểm chứng; có code nhưng chưa kiểm chứng; khác tài liệu; cần điều tra thêm.
6. Cho người học cơ hội dự đoán input/output hoặc diễn giải một đoạn code trước khi đưa đáp án; mỗi lần tối đa một câu ngắn. Trước khi sửa, giải thích ví dụ nhỏ, vấn đề, invariant và phạm vi thay đổi.
7. Chỉ vibe-code một thay đổi nhỏ thuộc milestone. Sau đó đọc diff từng phần: vai trò, vì sao cần, caller/callee, input/output và cách tự trace/debug nếu hỏng.
8. Chạy kiểm chứng phù hợp, ghi đúng command và kết quả thật. Không xem unit test là bằng chứng pipeline runtime hoặc benchmark.
9. Kết thúc bằng paper notes: mục đích; input/output; sơ đồ; file quan trọng; lệnh chạy/kiểm tra; lỗi thường gặp; điều vừa học; thay đổi/lý do; kết quả kiểm chứng; câu hỏi chưa rõ.
10. Cập nhật tiến độ dưới đây. Tài liệu kỹ thuật/evidence trong `docs/` chỉ tạo/cập nhật khi có phần giải thích hoặc kết quả có giá trị tái dùng; không tạo report cho mỗi sửa nhỏ.

Với code change, chạy `make test` và `make lint` theo repository workflow, nhưng diễn giải đúng phạm vi của từng check.

## Tiến độ được ghi như thế nào

Roadmap là nơi duy nhất ghi nhật ký học ngắn: milestone hiện tại, trạng thái, điều đã hiểu, thay đổi, command/kết quả, tài liệu được cập nhật và phần chưa chắc chắn. `RUBRIC.md` giữ trạng thái tiêu chí, không chép lại câu chuyện buổi học. Evidence dài hoặc ảnh/log thuộc `docs/`; roadmap chỉ liên kết và tóm tắt kết quả.

## Thứ tự học

| # | Milestone | Mục tiêu hiểu/tiêu chí thoát |
|---|---|---|
| 1 | Batch generator: chọn dữ liệu | Hiểu sampling, chunk và biên ngày; fixture chứng minh part không mất/chồng lấn |
| 2 | Batch generator: schema/seed | Theo config có hiệu lực, evolution 9/10 cột và tính tái lập |
| 3 | Batch duplicate/skew | Nêu identity, mẫu số duplicate, skew và ảnh hưởng; fixture nhỏ xác nhận |
| 4 | Raw output → MinIO | Theo manifest/upload/retry; isolated prefix đọc lại khớp đầu vào |
| 5 | Stream replay → Kafka | Phân biệt event-time, enqueue, ACK, key, checkpoint và replay |
| 6 | Late-event buffer/restart | Xác định trạng thái được lưu và điều kiện mất/lặp sau restart |
| 7 | Spark DP1: Raw → Bronze | Bootstrap/staging/archive, schema và retry; đối chiếu Delta snapshot |
| 8 | Spark DP2: Bronze → Silver | Cast/null/quarantine/dedup; reconcile số dòng theo nhóm |
| 9 | Flink event-time/window/feature | Watermark, dedup và window; event fixture có aggregate tính tay |
| 10 | Spark Silver → Gold/DWH | Grain, SCD2, temporal join, khóa và orphan |
| 11 | Airflow orchestration | Đọc từng cạnh/task; tách trigger khỏi DAG hoàn tất và output đúng |
| 12 | DP3 feature/label | 15m-only offline feature, prediction grain, label delay/completeness, leakage |
| 13 | Feast historical retrieval | Offline schema, point-in-time retrieval và không nhìn tương lai |
| 14 | DP4 materialize | Incremental offline→online, cursor/timestamp, stale overwrite và Redis readback |
| 15 | Stream feature pusher | Kiểm tra riêng write offline và online, ordering, retry/flush |
| 16 | Governance | Một contract/lineage path mỗi lượt; kiểm tra nguồn metadata thực |
| 17 | Storage optimization | Một thao tác compaction/index/snapshot, correctness trước và sau |
| 18 | Docker/resources | Một service/image, dependency/version và giới hạn tài nguyên |
| 19 | Training/API/advanced rubric | Chỉ mở sau inventory; chia thành milestone nhỏ theo rubric, bỏ Novel Ideas |
| 20 | Scale/benchmark | Chỉ sau correctness local; input/config/resource/namespace và phép đo tái lập |

Thứ tự ưu tiên ban đầu là generator và batch/stream, sau đó Spark/Flink/Airflow/storage/Feast/governance; bảng nêu các lát nhỏ để không làm nhiều component cùng lúc. Milestone nâng cao được thu hẹp/mở rộng theo workbook và inventory thật.

## Tiến độ hiện tại

- **Thiết kế đã thống nhất:** baseline model dùng bốn feature 15 phút để dự đoán purchase trong giờ sau; xem [WorkFlow.md](WorkFlow.md).
- **Hiện trạng quan trọng:** source vẫn chứa implementation 30 ngày. Chưa sửa để khớp thiết kế 15 phút.
- **Milestone kế tiếp:** M1 — batch generator chọn dữ liệu, ranh giới ngày. Chưa có walkthrough chi tiết, thay đổi hoặc kiểm chứng M1 được ghi nhận.
- **Kiểm chứng milestone:** chưa có.
- **Kiểm tra nền gần nhất:** ở phiên đẩy commit `ea66941`, `make test` cho 32 passed, coverage tổng 18%; `make lint` All checks passed. Đây là unit/lint, không phải xác nhận luồng runtime hay hoàn tất milestone.
- **Rủi ro vận hành đã phát hiện:** chưa chạy `make all-small`; rehearsal còn ghi vào evidence path đã xóa và gọi DWH setup có `DROP TABLE ... CASCADE`. Không dùng target này đến khi được xử lý trong milestone vận hành.

### Paper notes cho milestone đã học

Chưa có milestone code nào hoàn tất. Sau milestone đầu tiên, bổ sung ghi chú ngắn ngay dưới đây theo mẫu: mục đích; input/output; sơ đồ và file; lệnh chạy/debug; invariant; lỗi thường gặp; điều vừa học; thay đổi; kiểm chứng thật; điều chưa chắc chắn.
