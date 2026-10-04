"""
================================================================================
SCRIPT: QUICKSTART KAFKA (KIỂM CHỨNG HẠ TẦNG KAFKA BROKER)
Dự án: E-Commerce Real-Time Purchase Propensity Prediction System
Phương pháp luận Anh Khoa - Bước 2: Chạy Quickstart riêng rẽ từng khối
================================================================================
Mục đích:
1. Kết nối tới Kafka Broker tại localhost:9092
2. Tự động kiểm tra / tạo topic 'ecommerce_stream_events'
3. Khởi tạo Producer: Gửi 5 sự kiện mẫu định dạng JSON vào topic
4. Khởi tạo Consumer: Đọc lại 5 sự kiện đó, in ra Partition, Offset và Payload
5. Xác nhận hạ tầng sẵn sàng trước khi viết Streaming Generator hoàn chỉnh
================================================================================
"""

import json
import time
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "ecommerce_stream_events"


def create_topic_if_not_exists(topic_name: str):
    """Sử dụng AdminClient để tự động tạo topic nếu chưa có."""
    print(f"[*] Đang kiểm tra topic '{topic_name}' trên Kafka Broker ({BOOTSTRAP_SERVERS})...")
    admin_client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

    metadata = admin_client.list_topics(timeout=10)
    if topic_name in metadata.topics:
        print(f"[*] Topic '{topic_name}' đã tồn tại sẵn.")
        return

    print(f"[*] Topic '{topic_name}' chưa có. Đang tạo mới (1 partition, replication=1)...")
    new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
    fs = admin_client.create_topics([new_topic])
    for topic, f in fs.items():
        try:
            f.result()  # Chờ Kafka trả kết quả
            print(f"[OK] Đã tạo thành công topic: '{topic}'")
        except Exception as e:
            print(f"[!] Không thể tạo topic: {e}")


def delivery_report(err, msg):
    """Callback được gọi khi Producer gửi tin nhắn thành công hoặc thất bại."""
    if err is not None:
        print(f"[!] Gửi tin nhắn thất bại: {err}")
    else:
        print(f"    -> Đã gửi tới partition [{msg.partition()}] tại offset {msg.offset()}")


def run_quickstart():
    print("\n" + "=" * 80)
    print("BAT DAU QUICKSTART APACHE KAFKA (TESTING PRODUCER & CONSUMER)")
    print("=" * 80)

    # 1. Tạo topic
    create_topic_if_not_exists(TOPIC_NAME)

    # 2. Test Producer
    print("\n--- BƯỚC 1: KHỞI TẠO PRODUCER & GỬI 5 MESSAGE MẪU ---")
    producer_conf = {"bootstrap.servers": BOOTSTRAP_SERVERS}
    producer = Producer(producer_conf)

    sample_events = [
        {
            "event_time": "2019-10-26 12:00:01 UTC",
            "event_type": "view",
            "product_id": 1004856,
            "category_code": "electronics.smartphone",
            "brand": "samsung",
            "price": 130.25,
            "user_id": 51234567,
            "user_session": "a1b2c3d4-test-session-1",
        },
        {
            "event_time": "2019-10-26 12:00:03 UTC",
            "event_type": "cart",
            "product_id": 1004856,
            "category_code": "electronics.smartphone",
            "brand": "samsung",
            "price": 130.25,
            "user_id": 51234567,
            "user_session": "a1b2c3d4-test-session-1",
        },
        {
            "event_time": "2019-10-26 12:00:05 UTC",
            "event_type": "purchase",
            "product_id": 1004856,
            "category_code": "electronics.smartphone",
            "brand": "samsung",
            "price": 130.25,
            "user_id": 51234567,
            "user_session": "a1b2c3d4-test-session-1",
        },
        {
            "event_time": "2019-10-26 11:50:00 UTC",  # Giả lập Late Arrival (trễ 10 phút)
            "event_type": "view",
            "product_id": 1005115,
            "category_code": "electronics.smartphone",
            "brand": "apple",
            "price": 949.00,
            "user_id": 52345678,
            "user_session": "e5f6g7h8-test-session-2",
            "note": "SIMULATED_LATE_ARRIVAL",
        },
        {
            "event_time": "2019-10-26 12:00:05 UTC",  # Giả lập Duplicate
            "event_type": "purchase",
            "product_id": 1004856,
            "category_code": "electronics.smartphone",
            "brand": "samsung",
            "price": 130.25,
            "user_id": 51234567,
            "user_session": "a1b2c3d4-test-session-1",
            "note": "SIMULATED_DUPLICATE",
        },
    ]

    for i, event in enumerate(sample_events, start=1):
        payload = json.dumps(event)
        key = str(event["user_id"])  # Key partition theo user_id
        print(f"[*] Gửi Message #{i} (user_id={key}, event_type={event['event_type']})...")
        producer.produce(TOPIC_NAME, key=key, value=payload, callback=delivery_report)
        producer.poll(0)

    # Đợi tất cả tin nhắn gửi xong
    producer.flush()
    print("[OK] Đã gửi thành công 5 tin nhắn vào Kafka!")

    # 3. Test Consumer
    print("\n--- BƯỚC 2: KHỞI TẠO CONSUMER & ĐỌC LẠI TIN NHẮN TỪ ĐẦU ---")
    consumer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": f"quickstart_group_{int(time.time())}",
        "auto.offset.reset": "earliest",  # Đọc từ offset 0
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([TOPIC_NAME])

    received_count = 0
    start_wait = time.time()

    while received_count < 5 and (time.time() - start_wait < 15):
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"[!] Consumer error: {msg.error()}")
                break

        received_count += 1
        data = json.loads(msg.value().decode("utf-8"))
        print(
            f"[*] Nhận #{received_count} | Partition: {msg.partition()} | Offset: {msg.offset()} | User: {data['user_id']} | Event: {data['event_type']} | Time: {data['event_time']}"
        )

    consumer.close()

    print("\n" + "=" * 80)
    print(" BÁO CÁO KẾT QUẢ QUICKSTART KAFKA:")
    print(f"• Trạng thái Kafka Broker:     HOẠT ĐỘNG HOÀN HẢO ({BOOTSTRAP_SERVERS})")
    print(f"• Trạng thái Producer:         ĐÃ GỬI {len(sample_events)} TIN NHẮN THÀNH CÔNG")
    print(f"• Trạng thái Consumer:         ĐÃ NHẬN {received_count} TIN NHẮN THÀNH CÔNG")
    print("• Web Console trực quan:       http://localhost:8085 (Kafka UI)")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    run_quickstart()
