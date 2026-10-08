# Tài liệu kỹ thuật và evidence

`docs/` dành cho ghi chú kỹ thuật theo technology/component và bằng chứng được tạo sau khi kiểm chứng. Tài liệu ở đây cần ghi version/phạm vi, code liên quan, lệnh/input, thời điểm, kết quả thật và giới hạn. Không chép roadmap hoặc thiết kế tổng thể vào đây.

Giữ [ghi chú Batch Generator batch selection và lịch sử small local readback](batch_generator.md); evidence runtime cũ đã được dọn. Có [small MinIO runtime evidence mới](batch_generator_minio.md); các component downstream chưa có evidence mới. Khi hoàn tất milestone, thêm hoặc cập nhật ghi chú component phù hợp rồi liên kết tại đây. Chỉ tạo evidence cho lần chạy mới; unit test không phải bằng chứng pipeline runtime.

| Area | Technical notes / evidence | Status |
|---|---|---|
| Generator | [Batch Generator batch selection](batch_generator.md) | Batch Generator unit suite retained; historical runtime artifacts removed; fresh small MinIO readback PASS, ≥100 GB unverified |
| MinIO Raw Storage | [Batch Generator → MinIO evidence](batch_generator_minio.md) | Healthy; bucket/objects/schema/counts/bytes/CSV SHA-256 PASS (2026-10-06); persistence after recreate unverified |
| Kafka Stream Replay | [Bounded replay/recovery evidence](kafka_stream_replay.md) | Native 2.031/3.041 message readback PASS; bounded producer resume verified; scale/exactly-once unverified |
| Flink | TBD — implementation proposal requires learner approval | Not implemented / not verified |
| Spark / Lakehouse / DWH | TBD — add after batch processing milestone | Not verified |
| Airflow | TBD — add after DAG runtime milestone | Not verified |
| Feast / Redis | TBD — add after feature-store milestone | Not verified |
| Governance | TBD — add after DataHub verification | Not verified |

Project-level design, canonical contract, current source map, rubric and learning progress live at repo root; use the ordered [project index](../INDEX.md).
