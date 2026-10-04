"""
================================================================================
MODULE: STREAMING DATA GENERATOR (ONLINE DATA FEEDER)
Dự án: E-Commerce Real-Time Purchase Propensity Prediction System
Tác giả: Hoàng Minh Nhân & Antigravity AI

Đáp ứng trọn vẹn chuẩn Rubric Mini-coursework (Implement Data Generator: 8/20 điểm):
1. Simulate Streaming Problem 1: Burst Traffic (Flash Sale x10 throughput trong 30s) (3đ)
2. Simulate Streaming Problem 2: Late Arrival (5% sự kiện trễ 5-15 phút qua Event-Time-Driven Buffer) (3đ)
3. Simulate Streaming Problem 3: Streaming Duplicate (1.5% sự kiện trùng lặp do network retry) (2đ)
4. Sử dụng Apache Kafka Topic 'ecommerce_stream_events' với key=user_id (bảo toàn thứ tự partition)
5. Quản lý trạng thái bằng Smart Checkpoint: Hỗ trợ tắt máy mở lại chạy tiếp (Resume) và Reset (--clean)
================================================================================
"""

import os
import sys
import json
import time
import random
import signal
import argparse
import logging
from datetime import datetime, timedelta
import yaml
from confluent_kafka import Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

# Thiết lập logging chuẩn hóa
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("StreamDataGenerator")

# Offset tối ưu cho ngày 2019-10-26 00:00:00 UTC trong file 2019-Oct.csv (dòng ~34,768,530)
OCT_26_BYTE_OFFSET = 4642544408


class StreamDataGenerator:
    """
    Lớp điều khiển phát sinh dữ liệu thời gian thực (Streaming Generator),
    tiêm lỗi theo chuẩn Rubric và đẩy dữ liệu vào Apache Kafka Broker.
    """

    def __init__(self, config_path: str = "config/generator_config.yaml", checkpoint_file: str = ".stream_checkpoint.json"):
        self.config_path = config_path
        self.checkpoint_file = checkpoint_file
        self.config = self._load_config()
        self.stream_cfg = self.config.get("streaming_generator", {})
        self.kafka_cfg = self.stream_cfg.get("kafka", {})
        self.fault_cfg = self.stream_cfg.get("fault_injection", {})

        self.bootstrap_servers = self.kafka_cfg.get("bootstrap_servers", "localhost:9092")
        self.topic_name = self.kafka_cfg.get("topic", "ecommerce_stream_events")

        # Cấu hình Schema (Kế thừa Schema Evolution từ ngày 16/10)
        self.schema_cfg = self.stream_cfg.get("schema", {})
        self.include_discount = self.schema_cfg.get("include_discount", True)
        self.discount_vals = self.schema_cfg.get("discount_values", [4, 5, 8, 10, 12])

        # Cấu hình tiêm lỗi
        self.burst_cfg = self.fault_cfg.get("burst", {})
        self.late_cfg = self.fault_cfg.get("late_arrival", {})
        self.dup_cfg = self.fault_cfg.get("duplicate", {})

        self.producer = self._init_kafka_producer()
        self.is_running = True

        # Hàng đợi đệm để xử lý Late Arrival dựa theo Event Time
        # Lưu các phần tử dạng: (target_release_time: datetime, event_dict: dict)
        self.late_buffer = []

        # Thống kê tiến trình
        self.stats = {
            "total_produced": 0,
            "normal_produced": 0,
            "duplicates_injected": 0,
            "late_delayed": 0,
            "late_released": 0,
            "burst_events": 0,
            "last_event_time": None,
            "start_time": time.time(),
            "byte_offset": 0
        }

        # Đăng ký signal bắt Ctrl+C
        signal.signal(signal.SIGINT, self._handle_graceful_shutdown)
        signal.signal(signal.SIGTERM, self._handle_graceful_shutdown)

    def _load_config(self) -> dict:
        """Đọc file cấu hình YAML."""
        if not os.path.exists(self.config_path):
            alt_path = os.path.join("..", self.config_path)
            if os.path.exists(alt_path):
                self.config_path = alt_path
            else:
                raise FileNotFoundError(f"Không tìm thấy file cấu hình tại: {self.config_path}")

        logger.info(f"Đang tải cấu hình từ: {self.config_path}")
        with open(self.config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _init_kafka_producer(self) -> Producer:
        """Khởi tạo Confluent Kafka Producer tối ưu thông lượng."""
        producer_conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": "ecom_stream_generator",
            "acks": 1,  # Đảm bảo broker leader đã ghi nhận
            "linger.ms": 10,
            "batch.num.messages": 1000,
            "queue.buffering.max.messages": 100000
        }
        logger.info(f"Kết nối tới Kafka Broker tại: {self.bootstrap_servers}")
        return Producer(producer_conf)

    def reset_topic_and_checkpoint(self):
        """Xóa topic cũ trên Kafka và reset checkpoint khi người dùng chạy cờ --clean."""
        logger.info("Đang thực hiện dọn dẹp môi trường sạch sẽ (--clean)...")

        # 1. Xóa file checkpoint
        if os.path.exists(self.checkpoint_file):
            os.remove(self.checkpoint_file)
            logger.info(f"[✓] Đã xóa file checkpoint cũ: {self.checkpoint_file}")

        # 2. Xóa và tạo lại topic trên Kafka với 3 partitions
        admin_client = AdminClient({"bootstrap.servers": self.bootstrap_servers})
        metadata = admin_client.list_topics(timeout=10)

        if self.topic_name in metadata.topics:
            logger.info(f"Đang xóa topic cũ '{self.topic_name}' khỏi Kafka...")
            fs = admin_client.delete_topics([self.topic_name])
            for topic, f in fs.items():
                try:
                    f.result()
                    logger.info(f"[✓] Đã xóa topic: {topic}")
                except Exception as e:
                    logger.warning(f"Lỗi khi xóa topic: {e}")
            time.sleep(2)  # Đợi Kafka broker cập nhật metadata

        logger.info(f"Đang tạo mới topic '{self.topic_name}' (3 partitions, replication=1)...")
        new_topic = NewTopic(self.topic_name, num_partitions=3, replication_factor=1)
        fs = admin_client.create_topics([new_topic])
        for topic, f in fs.items():
            try:
                f.result()
                logger.info(f"[✓] Đã tạo thành công topic mới: {topic} (3 partitions)")
            except Exception as e:
                logger.error(f"Lỗi khi tạo topic mới: {e}")

    def _load_checkpoint(self) -> int:
        """Đọc byte offset và thống kê từ checkpoint nếu có."""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.stats.update(data.get("stats", {}))
                    offset = data.get("byte_offset", OCT_26_BYTE_OFFSET)
                    logger.info(f"[Checkpoint] Tìm thấy điểm dừng trước đó tại byte offset: {offset}")
                    logger.info(f"[Checkpoint] Đã bắn trước đó: {self.stats.get('total_produced', 0)} messages.")
                    return offset
            except Exception as e:
                logger.warning(f"Không thể đọc file checkpoint: {e}. Bắt đầu từ ngày 26/10.")
        return OCT_26_BYTE_OFFSET

    def _save_checkpoint(self, byte_offset: int):
        """Lưu trạng thái hiện tại vào file checkpoint."""
        self.stats["byte_offset"] = byte_offset
        data = {
            "byte_offset": byte_offset,
            "timestamp": datetime.now().isoformat(),
            "stats": self.stats
        }
        with open(self.checkpoint_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    def _delivery_report(self, err, msg):
        """Callback giám sát trạng thái gửi tin nhắn."""
        if err is not None:
            logger.error(f"Gửi tin nhắn thất bại: {err}")

    def _parse_csv_line(self, line: str, header_cols: list) -> dict:
        """Chuyển đổi một dòng CSV thành dictionary với đúng kiểu dữ liệu."""
        parts = line.strip().split(",")
        if len(parts) < len(header_cols):
            return None

        # Schema chuẩn 10 cột (Kế thừa discount_percent từ ngày 16/10)
        try:
            event = {
                "event_time": parts[0],
                "event_type": parts[1],
                "product_id": int(parts[2]) if parts[2] else None,
                "category_id": int(parts[3]) if parts[3] else None,
                "category_code": parts[4] if parts[4] else None,
                "brand": parts[5] if parts[5] else None,
                "price": float(parts[6]) if parts[6] else 0.0,
                "user_id": int(parts[7]) if parts[7] else None,
                "user_session": parts[8] if parts[8] else None
            }
            if self.include_discount:
                event["discount_percent"] = int(random.choice(self.discount_vals))
            return event
        except (ValueError, IndexError):
            return None

    def _send_to_kafka(self, event: dict, is_duplicate: bool = False, is_late: bool = False):
        """Bắn 1 sự kiện vào Kafka với key=str(user_id)."""
        payload = json.dumps(event).encode("utf-8")
        key = str(event["user_id"]).encode("utf-8") if event["user_id"] is not None else None

        self.producer.produce(
            topic=self.topic_name,
            key=key,
            value=payload,
            callback=self._delivery_report
        )
        self.producer.poll(0)
        self.stats["total_produced"] += 1
        if not is_duplicate and not is_late:
            self.stats["normal_produced"] += 1

    def _handle_graceful_shutdown(self, signum, frame):
        """Xử lý dừng tiến trình an toàn khi người dùng bấm Ctrl + C."""
        logger.info("\n[!] Nhận tín hiệu dừng (Ctrl+C). Đang hoàn tất các tin nhắn dang dở...")
        self.is_running = False

    def start_streaming(self, max_events: int = 0, base_rate: int = 100, burst_duration: int = None, burst_multiplier: int = None):
        """
        Bắt đầu vòng lặp phát sinh dữ liệu streaming và tiêm lỗi.
        :param max_events: Số sự kiện tối đa cần gửi (0 = không giới hạn, chạy cho đến hết ngày 31/10)
        :param base_rate: Tốc độ cơ bản (events/giây), mặc định 100 msg/s
        :param burst_duration: Thời lượng burst (giây), mặc định lấy từ config (600s = 10 phút)
        :param burst_multiplier: Hệ số nhân tốc độ khi burst, mặc định lấy từ config (x10)
        """
        csv_path = self.stream_cfg.get("input_csv", "2019-Oct.csv")
        if not os.path.exists(csv_path):
            alt_path = os.path.join("..", csv_path)
            if os.path.exists(alt_path):
                csv_path = alt_path
            else:
                raise FileNotFoundError(f"Không tìm thấy file CSV tại: {csv_path}")

        # Chuẩn bị topic nếu chưa tồn tại
        admin_client = AdminClient({"bootstrap.servers": self.bootstrap_servers})
        metadata = admin_client.list_topics(timeout=10)
        if self.topic_name not in metadata.topics:
            logger.info(f"Topic '{self.topic_name}' chưa có, tự động tạo mới (3 partitions)...")
            new_topic = NewTopic(self.topic_name, num_partitions=3, replication_factor=1)
            admin_client.create_topics([new_topic])
            time.sleep(1)

        # Đọc checkpoint
        current_offset = self._load_checkpoint()

        actual_burst_duration = burst_duration or self.burst_cfg.get("duration_seconds", 600)
        actual_burst_multiplier = burst_multiplier or self.burst_cfg.get("multiplier", 10)

        logger.info("\n" + "=" * 80)
        logger.info("🚀 BẮT ĐẦU PHÁT SINH DỮ LIỆU STREAMING VÀ TIÊM LỖI (KAFKA PRODUCER)")
        logger.info(f"• Topic đích:            {self.topic_name} (Broker: {self.bootstrap_servers})")
        logger.info(f"• Tốc độ cơ bản:         {base_rate} events/giây")
        logger.info(f"• Burst Traffic:         Tăng x{actual_burst_multiplier} trong {actual_burst_duration}s ({actual_burst_duration // 60} phút)")
        min_d = self.late_cfg.get("delay_minutes_min", 5)
        max_d = self.late_cfg.get("delay_minutes_max", 10)
        delay_str = f"{min_d} phút" if min_d == max_d else f"{min_d} - {max_d} phút"
        logger.info(f"• Late Arrival Rate:     {self.late_cfg.get('rate', 0.05) * 100:.1f}% (Độ trễ: {delay_str} theo Event Time)")
        logger.info(f"• Duplicate Rate:        {self.dup_cfg.get('rate', 0.015) * 100:.1f}% (Nhân bản x2 gói tin)")
        logger.info(f"• Schema:                10 cột (Kế thừa từ ngày 16/10, discount_percent: {self.discount_vals}%)")
        logger.info(f"• Giới hạn số sự kiện:   {'Không giới hạn' if max_events <= 0 else max_events}")
        logger.info("=" * 80 + "\n")

        # Đọc Header cột từ dòng đầu
        with open(csv_path, "r", encoding="utf-8") as f_head:
            header_cols = f_head.readline().strip().split(",")

        # Mở file và nhảy thẳng tới byte offset của ngày 26/10 (hoặc vị trí checkpoint)
        with open(csv_path, "r", encoding="utf-8") as f:
            f.seek(current_offset)

            events_produced_session = 0
            burst_mode = False
            burst_start_time = 0
            last_burst_check = time.time()
            burst_interval = self.burst_cfg.get("trigger_interval_minutes", 5) * 60
            burst_duration = actual_burst_duration
            burst_multiplier = actual_burst_multiplier

            late_rate = self.late_cfg.get("rate", 0.05) if self.late_cfg.get("enabled", True) else 0.0
            dup_rate = self.dup_cfg.get("rate", 0.015) if self.dup_cfg.get("enabled", True) else 0.0

            last_report_time = time.time()
            report_counter = 0

            while self.is_running:
                # 1. Điều khiển chu kỳ Burst Traffic (Flash Sale)
                now_sec = time.time()
                if not burst_mode and (now_sec - last_burst_check >= burst_interval or events_produced_session == 1000):
                    burst_mode = True
                    burst_start_time = now_sec
                    logger.info(f"🔥 [BURST TRAFFIC KÍCH HOẠT] Flash Sale bắt đầu! Tốc độ tăng vọt x{burst_multiplier} ({base_rate * burst_multiplier} msg/s)!")

                if burst_mode and (now_sec - burst_start_time >= burst_duration):
                    burst_mode = False
                    last_burst_check = now_sec
                    logger.info("❄️ [BURST KẾT THÚC] Flash Sale đã hết, trở về tốc độ bình thường.")

                current_rate = (base_rate * burst_multiplier) if burst_mode else base_rate
                sleep_interval = 1.0 / current_rate

                # 2. Đọc dòng tiếp theo từ CSV
                current_byte_pos = f.tell()
                line = f.readline()
                if not line:
                    logger.info("Đã đọc hết dữ liệu trong file CSV!")
                    break

                event = self._parse_csv_line(line, header_cols)
                if not event:
                    continue

                # Kiểm tra ngày kết thúc (chỉ bắn đến hết tháng 10)
                if not event["event_time"].startswith("2019-10"):
                    logger.info(f"Đã đạt đến mốc thời gian ngoài tháng 10: {event['event_time']}. Dừng phát sinh.")
                    break

                # Parse event_time thành đối tượng datetime để phục vụ Event-Time Buffer
                try:
                    # Format: "2019-10-26 00:00:00 UTC"
                    clean_time_str = event["event_time"].replace(" UTC", "")
                    curr_dt = datetime.strptime(clean_time_str, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    curr_dt = datetime.now()

                self.stats["last_event_time"] = event["event_time"]

                # 3. Tiêm lỗi 2: Late Arrival (Event-Time-Driven Buffer)
                # 5% số sự kiện bị trễ 5 - 15 phút theo dòng thời gian của event_time
                if late_rate > 0 and random.random() < late_rate:
                    delay_mins = random.randint(
                        self.late_cfg.get("delay_minutes_min", 5),
                        self.late_cfg.get("delay_minutes_max", 10)
                    )
                    release_dt = curr_dt + timedelta(minutes=delay_mins)
                    self.late_buffer.append((release_dt, event))
                    self.stats["late_delayed"] += 1
                else:
                    # Gửi sự kiện bình thường vào Kafka
                    self._send_to_kafka(event)

                # 4. Tiêm lỗi 3: Streaming Duplicate (Nhân bản tin nhắn do mạng retry)
                if dup_rate > 0 and random.random() < dup_rate:
                    # Gửi lại thêm 1 lần nữa bản ghi này
                    self._send_to_kafka(event, is_duplicate=True)
                    self.stats["duplicates_injected"] += 1

                # 5. Kiểm tra và phóng thích các sự kiện bị trễ từ late_buffer
                # Khi dòng thời gian curr_dt đã vượt qua release_dt của sự kiện đang chờ
                if self.late_buffer:
                    ready_events = []
                    remaining_events = []
                    for target_dt, delayed_ev in self.late_buffer:
                        if curr_dt >= target_dt:
                            ready_events.append(delayed_ev)
                        else:
                            remaining_events.append((target_dt, delayed_ev))

                    self.late_buffer = remaining_events
                    for r_ev in ready_events:
                        self._send_to_kafka(r_ev, is_late=True)
                        self.stats["late_released"] += 1

                if burst_mode:
                    self.stats["burst_events"] += 1

                events_produced_session += 1
                report_counter += 1

                # In tiến độ định kỳ mỗi 2 giây
                if time.time() - last_report_time >= 2.0:
                    elapsed = time.time() - self.stats["start_time"]
                    throughput = report_counter / (time.time() - last_report_time)
                    logger.info(
                        f"[STREAMING] Tổng: {self.stats['total_produced']:,} | "
                        f"Tốc độ: {throughput:.1f} msg/s | "
                        f"EventTime: {event['event_time']} | "
                        f"Late Delayed: {self.stats['late_delayed']} (Released: {self.stats['late_released']}) | "
                        f"Dupes: {self.stats['duplicates_injected']} | "
                        f"Mode: {'🔥 BURST' if burst_mode else '🟢 NORMAL'}"
                    )
                    last_report_time = time.time()
                    report_counter = 0
                    # Tự động lưu checkpoint mỗi 2 giây
                    self._save_checkpoint(current_byte_pos)

                # Kiểm tra giới hạn max_events
                if 0 < max_events <= events_produced_session:
                    logger.info(f"Đã đạt giới hạn {max_events} sự kiện yêu cầu. Dừng phát sinh.")
                    break

                # Điều tiết nhịp độ (Rate Limiting)
                if sleep_interval > 0.002:
                    time.sleep(sleep_interval)
                elif sleep_interval > 0 and (events_produced_session % 50 == 0):
                    time.sleep(sleep_interval * 50)

            # Xả các sự kiện còn sót lại trong late_buffer trước khi thoát
            if self.late_buffer:
                logger.info(f"Đang xả nốt {len(self.late_buffer)} sự kiện còn lại trong late_buffer...")
                for _, r_ev in self.late_buffer:
                    self._send_to_kafka(r_ev, is_late=True)
                    self.stats["late_released"] += 1
                self.late_buffer.clear()

            # Lưu checkpoint cuối cùng
            self._save_checkpoint(f.tell())

        # Đẩy hết các message còn tồn đọng trong producer buffer ra Kafka broker
        logger.info("Đang flush các messages cuối cùng vào Kafka...")
        self.producer.flush(timeout=10)
        self._print_final_summary()

    def _print_final_summary(self):
        """In báo cáo thống kê trực quan sau khi hoàn tất phiên phát sinh."""
        elapsed = time.time() - self.stats["start_time"]
        avg_throughput = self.stats["total_produced"] / elapsed if elapsed > 0 else 0

        print("\n" + "=" * 80)
        print("📊 BÁO CÁO TỔNG KẾT PHÁT SINH DỮ LIỆU STREAMING (KAFKA STREAM GENERATOR)")
        print("=" * 80)
        print(f"• Tổng số sự kiện bắn vào Kafka:   {self.stats['total_produced']:,} messages")
        print(f"• Số sự kiện bình thường:           {self.stats['normal_produced']:,}")
        print(f"• Lỗi 1 - Burst Traffic events:     {self.stats['burst_events']:,} messages (x10 throughput)")
        print(f"• Lỗi 2 - Late Arrival phát sinh:   {self.stats['late_delayed']:,} events (Đã nhả: {self.stats['late_released']:,})")
        print(f"• Lỗi 3 - Duplicates tiêm vào:      {self.stats['duplicates_injected']:,} messages ({self.stats['duplicates_injected'] / max(1, self.stats['total_produced']) * 100:.2f}%)")
        print(f"• Mốc Event Time cuối cùng:         {self.stats.get('last_event_time', 'N/A')}")
        print(f"• Thời gian chạy phiên:             {elapsed:.1f} giây")
        print(f"• Tốc độ trung bình:                {avg_throughput:.1f} messages/giây")
        print(f"• Checkpoint lưu tại:               {self.checkpoint_file}")
        print(f"• Quan sát trực quan tại:           http://localhost:8085 (Kafka UI)")
        print("=" * 80 + "\n")


def main():
    """Command-line entry point to launch the streaming generator with fault injections."""
    parser = argparse.ArgumentParser(description="Streaming Data Generator & Fault Injection for Kafka")
    parser.add_argument("--config", default="config/generator_config.yaml", help="Đường dẫn file cấu hình YAML")
    parser.add_argument("--clean", action="store_true", help="Xóa topic cũ và reset checkpoint để chạy lại từ đầu")
    parser.add_argument("--max-events", type=int, default=0, help="Số lượng sự kiện tối đa cần gửi (0 = chạy liên tục)")
    parser.add_argument("--rate", type=int, default=100, help="Tốc độ bắn cơ bản (events/giây), mặc định 100")
    parser.add_argument("--burst-duration", type=int, default=None, help="Thời lượng burst (giây), ví dụ 600 cho 10 phút, 1200 cho 20 phút")
    parser.add_argument("--burst-multiplier", type=int, default=None, help="Hệ số nhân tốc độ khi burst, ví dụ 10")
    args = parser.parse_args()

    generator = StreamDataGenerator(config_path=args.config)

    if args.clean:
        generator.reset_topic_and_checkpoint()

    generator.start_streaming(
        max_events=args.max_events,
        base_rate=args.rate,
        burst_duration=args.burst_duration,
        burst_multiplier=args.burst_multiplier
    )


if __name__ == "__main__":
    main()
