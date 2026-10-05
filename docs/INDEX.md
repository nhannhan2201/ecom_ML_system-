# Tài liệu kỹ thuật và evidence

`docs/` dành cho ghi chú kỹ thuật theo technology/component và bằng chứng được tạo sau khi kiểm chứng. Tài liệu ở đây cần ghi version/phạm vi, code liên quan, lệnh/input, thời điểm, kết quả thật và giới hạn. Không chép roadmap hoặc thiết kế tổng thể vào đây.

Giữ [ghi chú M1 batch selection và lịch sử small local readback](m1_batch_selection.md); evidence runtime cũ đã được dọn; các component khác chưa có evidence mới. Khi hoàn tất milestone, thêm hoặc cập nhật ghi chú component phù hợp rồi liên kết tại đây. Chỉ tạo evidence cho lần chạy mới; unit test không phải bằng chứng pipeline runtime.

| Area | Technical notes / evidence | Status |
|---|---|---|
| Generator | [M1 batch selection](m1_batch_selection.md) | M1 unit suite retained; historical runtime artifacts removed; full output/MinIO not verified |
| Kafka / Flink | TBD — add after stream milestone | Not verified |
| Spark / Lakehouse / DWH | TBD — add after batch processing milestone | Not verified |
| Airflow | TBD — add after DAG runtime milestone | Not verified |
| Feast / Redis | TBD — add after feature-store milestone | Not verified |
| Governance | TBD — add after DataHub verification | Not verified |

Project-level design, canonical contract, current source map, rubric and learning progress live at repo root; use the ordered [project index](../INDEX.md).
