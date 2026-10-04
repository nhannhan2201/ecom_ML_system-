# Lộ trình học và thực hành

## Mục tiêu và cách dùng

Học như người mới để tự giải thích kiến trúc, đọc code và debug; cùng vibe-code một thay đổi nhỏ rồi đọc lại phần vừa viết. Đối chiếu hai workbook trong `rubic/`, tạm bỏ qua Novel Ideas. Code hiện có là đối tượng cần kiểm tra, không mặc định là đáp án đúng.

[AGENTS.md](../AGENTS.md) giữ quy tắc làm việc bền vững; file này giữ thứ tự và tiến độ; [RUBRIC.md](../RUBRIC.md) giữ tiêu chí; [WorkFlow.md](WorkFlow.md) giữ bài toán/luồng mục tiêu; [DATA_FLOW.md](DATA_FLOW.md) giữ luồng hiện tại theo source. Không lặp lại chi tiết thiết kế hoặc tạo report riêng cho mỗi sửa nhỏ.

## Bài toán đã chốt — baseline chỉ dùng feature 15 phút

Đây là quyết định hiện hành cho baseline đầu tiên. Nó thay thế phương án cũ có 5 feature 30 ngày + 4 feature 15 phút. Không thay đổi code trong cập nhật tài liệu này; implementation cũ vẫn cần audit theo từng milestone.

### Mục tiêu và thời điểm dự đoán

Dự đoán xác suất user có ít nhất một event purchase với bất kỳ sản phẩm nào trong một giờ tiếp theo. Tất cả thời gian UTC; khoảng [a,b) gồm a, loại b.

- Mỗi đầu phút t, xét user có view/cart hợp lệ mới được xử lý trong phút trước [t−1 phút,t). Nhiều event cùng user tạo tối đa một lượt trong phút đó. Không có activity mới thì không dự đoán lại.
- Feature nhìn lại 15 phút theo event time [t−15 phút,t), chỉ dùng event đã được nhận và xử lý trước điểm chốt. Không chờ watermark để dự đoán và không sửa prediction cũ khi event trễ tới.
- Label tìm purchase của user trong [t,t+1 giờ). Gắn label vào lượt dự đoán đã lưu, không vào mọi event CSV. Label 0 chỉ khi nguồn tương lai đủ đầy đủ theo chính sách lateness; nếu chưa đủ thì pending/unknown.
- Dự đoán và label có grain (user_id,prediction_timestamp), hoặc prediction_id duy nhất trỏ tới cặp đó.

Ví dụ: view lúc 10:27:05 xử lý xong lúc 10:27:06 → dự đoán lúc 10:28; feature window [10:13,10:28); label window [10:28,11:28). Purchase lúc 11:05 có thể bổ sung label sau, không làm thay đổi feature đầu vào lúc 10:28.

### Bốn feature baseline

| Cột | Định nghĩa |
|---|---|
| f_views_15m | Số view hợp lệ, sau dedup, trong [t−15 phút,t) |
| f_carts_15m | Số cart hợp lệ, sau dedup, trong cùng cửa sổ |
| f_purchases_15m | Số purchase hợp lệ, sau dedup, trong cùng cửa sổ |
| total_spend_15m | Tổng price của purchase trong cùng cửa sổ; chưa khẳng định là doanh thu đơn hàng |

User_id và timestamp là khóa/metadata, không phải model features. Null/invalid category không ảnh hưởng baseline bốn feature. Phải phân biệt không có event với missing do pipeline; không tự đổi ingest lỗi thành giá trị 0.

### Vì sao 15 phút-only hợp lý, và giới hạn

Mục tiêu là purchase gần hạn sau một hành vi có ý định mua. Bốn tín hiệu gần nhất tạo baseline dễ hiểu, giảm việc ghép snapshot dài hạn, và cho phép Spark tính offline/Flink tính online cùng một định nghĩa. Rubric yêu cầu feature store, offline materialize, stream push tới offline/online; không bắt buộc model phải có feature 30 ngày.

Đổi lại, model không thấy lịch sử dài hạn và có thể không phân biệt user thường xuyên mua với user mới. Ta đo baseline trước; nếu thêm lịch sử dài hạn sau này thì coi đó là thí nghiệm riêng, so trên temporal holdout bằng cùng split/metric. Không tuyên bố 15m tốt hơn khi chưa đo.

### Offline/online phải khớp về thời gian

Spark tạo feature training từ event history tại từng mốc đủ điều kiện; Flink tạo feature stream với cùng bốn phép tính và cửa sổ. CSV có event_time nhưng không có thời điểm arrival đáng tin cậy, nên lần train lịch sử đầu là giả định replay lý tưởng theo event time. Khi mô phỏng late arrival, training/evaluation cần dùng feature thực sự có sẵn tại thời điểm dự đoán, hoặc nêu rõ giới hạn lý tưởng; không backfill event tương lai vào feature cũ.

Flink không đợi watermark để dự đoán, nhưng vẫn cần window/event-time xử lý theo rubric. Cơ chế phát snapshot/feature đầu phút mà không chờ final window phải được chọn sau khi đọc code/API đúng version và kiểm bằng fixture.

### Feature store và materialize vẫn cần

- Spark offline feature rows đi vào Feast offline source để historical retrieval/training.
- Có pipeline Airflow incremental materialize offline→online theo rubric; dùng để khởi tạo/khôi phục hoặc kiểm chứng batch cập nhật. Chạy có kiểm soát để không cho batch cũ ghi đè stream feature mới hơn.
- Stream feature có hai job/đích cần chứng minh: ghi OFFLINE history và cập nhật ONLINE Redis qua Feast. Không mặc định tùy chọn target=both đã đáp ứng nếu chưa đọc lại cả hai store.
- Redis online thường giữ giá trị mới nhất theo entity; cần timestamp/freshness guard để serving không dùng nhầm feature cũ. Feature lịch sử đã dùng để dự đoán phải lưu riêng ở offline/log để train/debug.
- TTL là chính sách freshness/retention riêng, không tự đặt bằng 15 phút chỉ vì window dài 15 phút.

### Label và training

Label job nhận hai nguồn: prediction records cho biết user/mốc cần đánh giá; Silver events đã qua Kafka cho biết outcome tương lai. Tìm purchase theo event_time trong một giờ sau prediction. Có purchase → 1. Không có purchase chỉ → 0 sau khi xác nhận horizon và nguồn đủ; cuối file, outage hoặc late events chưa ổn định thì pending và bỏ khỏi evaluation.

Train/validation/test chia theo thời gian. Không để label horizon vượt qua ranh giới split; các cửa sổ label chồng nhau có thể làm phụ thuộc mẫu, cần ghi cách xử lý. CSV tháng 11 dùng làm stream replay và đối chiếu mất event; không đưa tương lai vào feature hoặc coi toàn bộ tháng 11 đã được Kafka nhận.

### Hai CSV và rubric generator

- 2019-Oct.csv: bootstrap batch [2019-10-01,2019-11-01), chia part tại 16/10 cho schema evolution.
- 2019-Nov.csv: replay [2019-11-01,2019-12-01) qua Kafka. Không pre-load toàn bộ tháng 11 vào Silver rồi lại ingest từ stream.
- Generator cần tiếp tục đáp ứng skew, schema evolution, offline duplicate, Raw persistence, late arrival và stream duplicate theo rubric.
- Local fixture/cohort nhỏ để kiểm logic; cấu hình ≥100GB độc lập và chỉ chạy sau khi resource/output target được xem xét.
- Rubric DE dùng ví dụ feature 90d nhưng không bắt buộc cửa sổ đó. Rubric final/MLE yêu cầu offline materialization, stream feature vào offline và online, feature timestamp/created, training và label join; baseline 15m-only vẫn giữ những yêu cầu này.

### Giới hạn đã biết và trạng thái

Hai CSV có cùng header 9 cột theo lần đọc trước; chỉ sample 100.000 dòng đầu được kiểm tra. Chưa xác nhận toàn file về sort/gap/duplicate hoặc ID consistency. CSV tháng 10/11 cung cấp nguồn theo tháng, nhưng correctness của ingestion và arrival simulation chưa được chứng minh.

Trạng thái: **đã chốt baseline 15 phút-only; chưa triển khai/kiểm chứng code theo baseline này**. Lệnh, fixture và kết quả kiểm chứng được bổ sung ở từng milestone; đây không phải runtime evidence.

## Cách làm và ghi nhận

Quy trình giải thích trước, sơ đồ thực tế, Quickstart đúng version, trace input→output, đối chiếu docs/tests/evidence, hỏi tối đa một câu dự đoán, nêu invariant trước sửa, review diff/debug và paper-notes được định nghĩa ở [AGENTS.md](../AGENTS.md). Không chép quy trình vào đây lần nữa.

Trạng thái kiểm chứng cần phân biệt: đọc source, unit test, integration/runtime, scale/benchmark. Chỉ ghi **đúng và đã kiểm chứng** khi có lệnh, input/config, thời điểm, kết quả và giới hạn; nếu chưa đo ghi `TBD`. Bảng tình trạng rubric nằm ở [RUBRIC.md](../RUBRIC.md).

## Thứ tự milestone

Mỗi hàng là một lát học, không phải yêu cầu sửa ngay. Tách nhỏ thêm nếu cần; lệnh vận hành chính xác chỉ chốt sau khi đọc code và xác định đích thử.

| ID | Lát học | Điểm cần hiểu và tiêu chí kiểm chứng | Tiến độ |
| --- | --- | --- | --- |
| M1 | Batch chọn dữ liệu | Sampling, biên ngày; part và độ phủ mô tả đúng | Đã bắt đầu đọc; chưa sửa/chạy kiểm chứng |
| M2 | Batch schema/seed | Schema nguồn/đích, cấu hình và tính tái lập | Chưa học |
| M3 | Batch duplicate | Replica/bản sao, counter và mẫu số | Chưa học |
| M4 | Batch skew/cardinality | Phân bố khóa khác tăng dung lượng; flag có hiệu lực | Chưa học |
| M5 | Stream replay | CSV → JSON → Kafka; event time, ACK và counter | Chưa học |
| M6 | Stream late/restart | Buffer, checkpoint; khả năng mất/lặp khi restart | Chưa học |
| M7 | Raw → Bronze | CSV/JSON → Delta; bootstrap, staging/archive và retry | Chưa học |
| M8 | Bronze → Silver | Cast/null/dedup; giải thích count và dòng bị loại | Chưa học |
| M9 | Kafka → Flink feature | Watermark/dedup/window; aggregate fixture biết trước | Chưa học |
| M10 | Silver → Gold | SCD2/temporal join; fact không nhân dòng hoặc orphan | Chưa học |
| M11 | Gold → PostgreSQL | Lakehouse khác DWH; đối chiếu khóa/count | Chưa học |
| M12 | Airflow | Một cạnh/task chain mỗi lượt; trigger khác hoàn tất | Chưa học |
| M13 | DP3 feature/label | Thời gian, schema và leakage; thu nhỏ từng phép tính | Chưa học |
| M14 | DP4 materialize | Offline → online, thời gian và retrieval | Chưa học |
| M15 | Stream feature pusher | Khóa, flush, offset và cập nhật online | Chưa học |
| M16 | Governance | Một contract/lineage path mỗi lượt | Chưa học |
| M17 | Storage | Một thao tác bảo trì, snapshot và tính đúng dữ liệu | Chưa học |
| M18 | Docker | Một image/service và giới hạn tài nguyên | Chưa học |
| M19 | Benchmark theo rubric | Một phép đo tái lập, baseline/config/input rõ ràng | Chưa học |

API và các phần còn lại chỉ thêm milestone sau khi đối chiếu yêu cầu rubric; tên thư mục không chứng minh đã implement.

## Milestone hiện tại — M1: batch chọn dữ liệu

- **Mục tiêu:** giải thích CSV được chọn thành hai part ra sao; kiểm tra ranh giới ngày theo rubric trước khi sửa.
- **File cần đọc:** [batch_generator.py](../src/generator/batch_generator.py), [generator_config.yaml](../config/generator_config.yaml), phần batch trong [test_generator.py](../tests/test_generator.py); đọc entrypoint Makefile liên quan khi trace lệnh.
- **Kiến thức:** sampling theo vị trí khác lọc theo thời gian; chunk, schema 9/10 cột, số dòng và min/max `event_time`.
- **Invariant cần đối chiếu:** part thuộc khoảng ngày đã thống nhất; số dòng/độ phủ đúng; không nhầm duplicate với dòng nguồn. Đây là tiêu chí mục tiêu, chưa khẳng định code hiện tại đạt.
- **Kiểm chứng dự kiến:** fixture nhỏ với event trước/đúng/sau biên ngày, sample lẻ và schema nguồn. Lệnh test cụ thể sẽ ghi sau khi chọn/viết fixture; `make test`, `make lint` sau code changes. Chưa chạy generator hoặc integration.
- **Rủi ro:** sampling theo vị trí phụ thuộc thứ tự CSV, thiếu dòng hợp lệ, đọc file lớn; không dùng generator lớn để thử logic.
- **Bước tiếp theo:** bắt đầu M1 bằng fixture biên ngày và trace chọn dữ liệu batch; clock/availability được kiểm chứng khi tới stream và feature.

Sơ đồ chi tiết có hàm, nhánh và dòng code sẽ bổ sung sau lượt đọc M1; chưa dựng sơ đồ như thể đã xác nhận lại.

## An toàn và bằng chứng

Giữ thay đổi chưa commit. Không đọc/in `.env` hoặc secrets; không commit thay người học. Integration phải có namespace/đích thử xác định trước. Reset, xóa volume/topic/checkpoint/dữ liệu, ghi đè dữ liệu hay generator lớn cần giải thích tác động và người học đồng ý. Chỉ xét quy mô 100GB sau khi generator, ingest và tài nguyên đã kiểm chứng.

Evidence mới cần: lệnh/exit code, commit và diff chưa commit, nguồn input/checksum hoặc snapshot, config không có secret, thời điểm, kết quả thật và giới hạn. Không chạy target có tác dụng ghi chỉ để chứng minh tên target tồn tại.

## Tiến độ xác nhận

- **Bài toán:** baseline bốn feature 15m-only được chọn; source vẫn có implementation 30d. Chi tiết giả định/label ở [WorkFlow.md](WorkFlow.md).
- **Hiện tại:** M1 batch selection; đang chuẩn bị fixture và trace, chưa sửa hoặc chạy pipeline.
- **Kiểm chứng cũ được kế thừa:** lần trước `make test` ghi 32 passed, `make lint` pass, checksum manifest không đổi. Chưa chạy lại trong lượt tài liệu này; không phải bằng chứng pipeline runtime.
- **Cleanup docs/collectors:** trạng thái xóa hiện có trong working diff, chưa commit. Chưa rà xong rehearsal hazard; xem DATA_FLOW và Makefile trước khi dùng `make all-small`.
- **Lượt này:** chỉ cập nhật tài liệu điều hướng/phân vai. Kết quả kiểm tra link và diff sẽ ghi sau khi rà soát cuối.

## Mẫu ghi chú cuối milestone

- **Điều đã hiểu:** mục đích component; input/output; Mermaid và trạng thái từng cạnh.
- **Code cần nhớ:** file/hàm, thứ tự gọi, nhánh/trạng thái quan trọng.
- **Thay đổi và lý do:** ví dụ gây lỗi; invariant; diff đã đọc lại.
- **Tự chạy/debug:** lệnh, đích thử, event/khóa để trace, điểm quan sát, lỗi thường gặp.
- **Kiểm chứng thật:** lệnh → kết quả/exit code → phạm vi và giới hạn.
- **Tài liệu cập nhật:** mục đã cập nhật ngay trong roadmap/DATA_FLOW/RUBRIC khi liên quan.
- **Chưa chắc chắn:** câu hỏi còn mở và phép thử tiếp theo.
