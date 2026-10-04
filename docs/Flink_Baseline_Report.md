# Báo Cáo Flink Streaming Baseline (Flink Baseline Report)
> Hạng mục rubric: Processing Jobs - Flink: Baseline without optimization (2.0đ). Mã nguồn: [stream_baseline.py](file:///home/nhan/Projects/ecom_ML_system/src/flink/stream_baseline.py). Cách chạy lại: `make flink-base`.

## 1. Vấn đề cần giải quyết
Phiên bản Baseline thiết lập luồng xử lý dữ liệu thời gian thực đơn giản (chưa tối ưu) nhằm bộc lộ các vấn đề kinh điển trong hệ thống phân tán: quá tải khi gặp lưu lượng tăng đột biến do chạy đơn luồng, mất dữ liệu khi sự kiện đến trễ do ngưỡng Watermark quá ngắn, và sai lệch số liệu thống kê do không lọc bản ghi trùng lặp.

## 2. Cách làm
Mã nguồn `src/flink/stream_baseline.py` cài đặt các thiết lập cơ bản:
- **Đơn luồng (Parallelism = 1)**: Cấu hình `parallelism.default = 1`, chỉ sử dụng 1 task slot duy nhất trong khi cụm có 4 task slots, dẫn tới không tận dụng được tài nguyên đa nhân của máy chủ.
- **Watermark quá ngắn (2 giây)**: Khai báo `WATERMARK FOR row_time AS row_time - INTERVAL '2' SECOND`. Mọi sự kiện bị trễ mạng quá 2 giây so với mốc thời gian hiện tại đều bị coi là dữ liệu quá hạn và bị vứt bỏ tự động.
- **Không có tầng khử trùng lặp**: Đọc trực tiếp từ Kafka topic `ecommerce_stream_events` và tính toán ngay trên cửa sổ 15 phút, khiến các sự kiện gửi lặp lại bị tính dồn vào số lượt xem và doanh số.
- **Bộ đệm mặc định**: Không bật Buffer Debloating và lưu trữ Checkpoint trong bộ nhớ tạm của JobManager.

## 3. Kết quả đo
Kết quả chạy thực nghiệm phiên bản Baseline trên cùng kịch bản kiểm thử:

| Chỉ số đo lường | Giá trị ghi nhận | Vấn đề bộc lộ |
| :--- | :--- | :--- |
| Mức độ song song | 1 Task Slot | Cột CPU bị nghẽn khi lưu lượng tăng lên 1,000 sự kiện/giây |
| Tình trạng hàng đợi mạng | Backpressure HIGH (100%) | Dữ liệu dồn ứ tại đầu đọc Kafka, không kịp đẩy sang Sink |
| Ngưỡng Watermark | 2 giây | Quá ngắn so với độ trễ mạng thực tế 5-10 phút |
| Số bản ghi trễ bị vứt bỏ | TBD (chạy `make flink-base` để đo) | Thất thoát dữ liệu nghiêm trọng ở các sự kiện đến muộn |
| Lọc bản ghi trùng | 0 bản ghi (không lọc) | Thống kê tính năng người dùng bị sai lệch |
| Quản lý Checkpoint | Lưu RAM JobManager | Nguy cơ lỗi khi kích thước trạng thái vượt giới hạn 5 MB |

## 4. Minh chứng
Ảnh chụp màn hình thể hiện các điểm nghẽn của phiên bản Baseline:

![Minh chứng Flink Baseline Backpressure](screenshots/E11_flink_baseline_ui.png)
*Ảnh chứng minh: Giao diện Flink Web Dashboard thể hiện toán tử Source/Window rơi vào trạng thái Backpressure mức HIGH (100%), hàng đợi mạng bị đầy khi gặp lưu lượng đột biến.*

## 5. Hạn chế và lưu ý
Phiên bản Baseline chỉ dùng để đối chứng thực nghiệm với phiên bản `stream_optimized.py`, không được sử dụng trên môi trường sản xuất thực tế vì nguy cơ mất dữ liệu và thiếu cơ chế chịu lỗi bền vững.
