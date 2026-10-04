# ecom_ML_system

Dự án dữ liệu clickstream thương mại điện tử REES46: generator → batch/stream processing → Lakehouse/DWH → feature store. Code hiện có sẽ được đọc, sửa hoặc thay từng phần theo hai workbook trong `rubic/`. Chưa xác nhận lại hệ thống end-to-end; bỏ qua Novel Ideas.

Baseline dự đoán đang học dùng bốn feature hành vi 15 phút để dự đoán purchase trong giờ kế tiếp. Xem [WorkFlow](docs/WorkFlow.md) để biết thiết kế mục tiêu và phần nào chưa kiểm chứng; source hiện tại còn feature dài hạn cần được điều chỉnh theo milestone.

## Bắt đầu

- [Mục lục](docs/INDEX.md)
- [Workflow theo rubric và milestone](docs/WorkFlow.md)
- [Luồng hiện có theo code](docs/DATA_FLOW.md)
- [Cách học và tiến độ từng phần](docs/LEARNING_ROADMAP.md)
- [Theo dõi rubric](RUBRIC.md)

```bash
make test
make lint
```

Unit tests không chứng minh pipeline đã chạy. Các target generator, Spark, Flink, Feast và governance có tác dụng ghi; chỉ chạy khi đã xác định dữ liệu và đích thử của milestone.

## Cấu trúc giữ lại

| Thư mục | Vai trò |
| --- | --- |
| `rubic/` | Hai workbook yêu cầu |
| `config/` | Cấu hình generator |
| `src/` | Generator, Spark, Flink; API hiện là placeholder |
| `dags/` | Airflow DP1–DP4 |
| `docker/` | Compose, Dockerfiles và dependencies |
| `feature_store/` | Feast definitions và consumers |
| `governance/` | Catalog, lineage và contracts |
| `scripts/` | Provision, vận hành, kiểm tra dịch vụ và smoke checks; một số thao tác có tác dụng ghi |
| `tests/` | Unit/property/structure tests |
| `docs/` | WorkFlow (thiết kế), DATA_FLOW (hiện trạng), LEARNING_ROADMAP (tiến độ), RUBRIC (tiêu chí) |

Tài liệu/report và collector kết quả cũ đã được loại khỏi working tree ở đợt dọn trước; việc đó chưa commit. Một số scripts smoke/ops còn cần rà soát riêng. Dữ liệu nguồn/local, checkpoints và dữ liệu dịch vụ được giữ nguyên. Không commit tự động.
