# Tài liệu kỹ thuật và evidence

`docs/` dành cho ghi chú kỹ thuật theo technology/component và bằng chứng được tạo sau khi kiểm chứng. Tài liệu ở đây cần ghi version/phạm vi, code liên quan, lệnh/input, thời điểm, kết quả thật và giới hạn. Không chép roadmap hoặc thiết kế tổng thể vào đây.

Giữ [ghi chú Batch Generator batch selection và lịch sử small local readback](batch_generator.md); evidence runtime cũ đã được dọn. Lịch sử small MinIO nằm trong roadmap; evidence riêng đã dọn theo learner. Full October declared-check runtime PASS; xem evidence tự động. Khi hoàn tất milestone, thêm hoặc cập nhật ghi chú component phù hợp rồi liên kết tại đây. Chỉ tạo evidence cho lần chạy mới; unit test không phải bằng chứng pipeline runtime.

| Area | Technical notes / evidence | Status |
|---|---|---|
| Generator | [Batch Generator batch selection](batch_generator.md) | 18 unit cases PASS; [October runtime evidence](evidence/batch_generator_october.json): full 43,297,739 output rows, schema/date/injected-copy/hash PASS; natural duplicate/skew/≥100 GB unverified |
| Kafka Stream Replay | [Bounded replay/recovery evidence](kafka_stream_replay.md) | Native 2.031/3.041 message readback PASS; bounded producer resume verified; scale/exactly-once unverified |
| Flink | TBD — implementation proposal requires learner approval | Not implemented / not verified |
| Spark / Lakehouse / DWH | TBD — add after batch processing milestone | Not verified |
| Airflow | TBD — add after DAG runtime milestone | Not verified |
| Feast / Redis | TBD — add after feature-store milestone | Not verified |
| Governance | TBD — add after DataHub verification | Not verified |

Project-level design, canonical contract, current source map, rubric and learning progress live at repo root; use the ordered [project index](../INDEX.md).
