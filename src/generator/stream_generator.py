"""
================================================================================
MODULE: STREAMING DATA GENERATOR (ONLINE DATA FEEDER)
Project: E-Commerce Real-Time Purchase Propensity Prediction System
Author: Hoang Minh Nhan

Design objectives:
1. Simulate Streaming Problem 1: Burst Traffic (Flash Sale x10 throughput in 30s)
2. Simulate Streaming Problem 2: Late Arrival (5% events delayed 5-15 min via LateEventBuffer)
3. Simulate Streaming Problem 3: Streaming Duplicate (1.5% duplicate events due to network retry)
4. Target Apache Kafka Topic 'ecommerce_stream_events' with key=user_id (preserving partition ordering)
5. Manage state with Checkpoint (.stream_checkpoint.json): resume and reset (--clean)
6. Write data/stream_manifest.json with latency distribution and streaming statistics
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
from datetime import datetime, timedelta, timezone
import yaml
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

try:
    from src.generator.late_event_buffer import LateEventBuffer
except ImportError:
    from late_event_buffer import LateEventBuffer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [StreamDataGenerator] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("StreamDataGenerator")

# Byte offset for 2019-10-26 00:00:00 UTC in 2019-Oct.csv (~line 34,768,530)
OCT_26_BYTE_OFFSET = 4642544408


class StreamDataGenerator:
    """
    Streaming data generator feeding Kafka broker with fault injections
    (Burst Traffic, Late Arrival, Duplicate Records) according to rubric specifications.
    """

    def __init__(
        self, config_path: str = "config/generator_config.yaml", checkpoint_file: str = ".stream_checkpoint.json"
    ):
        self.config_path = config_path
        self.checkpoint_file = checkpoint_file
        self.config = self._load_config()
        self.stream_cfg = self.config.get("streaming_generator", {})
        self.kafka_cfg = self.stream_cfg.get("kafka", {})
        self.fault_cfg = self.stream_cfg.get("fault_injection", {})

        self.bootstrap_servers = self.kafka_cfg.get("bootstrap_servers", "localhost:9092")
        self.topic_name = self.kafka_cfg.get("topic", "ecommerce_stream_events")

        # Schema config (10 cols post-16/10)
        self.schema_cfg = self.stream_cfg.get("schema", {})
        self.include_discount = self.schema_cfg.get("include_discount", True)
        self.discount_vals = self.schema_cfg.get("discount_values", [4, 5, 8, 10, 12])

        # Fault injection configs
        self.burst_cfg = self.fault_cfg.get("burst", {})
        self.late_cfg = self.fault_cfg.get("late_arrival", {})
        self.dup_cfg = self.fault_cfg.get("duplicate", {})

        self.producer = self._init_kafka_producer()
        self.is_running = True

        # Late arrival buffer based on event time progression
        self.late_buffer = LateEventBuffer()

        # Operational metrics
        self.stats = {
            "total_produced": 0,
            "normal_produced": 0,
            "duplicates_injected": 0,
            "late_delayed": 0,
            "late_released": 0,
            "burst_events": 0,
            "last_event_time": None,
            "start_time": time.time(),
            "byte_offset": 0,
        }

        signal.signal(signal.SIGINT, self._handle_graceful_shutdown)
        signal.signal(signal.SIGTERM, self._handle_graceful_shutdown)

    def _load_config(self) -> dict:
        """Load YAML configuration."""
        if not os.path.exists(self.config_path):
            alt_path = os.path.join("..", self.config_path)
            if os.path.exists(alt_path):
                self.config_path = alt_path
            else:
                raise FileNotFoundError(f"Config file not found at: {self.config_path}")

        logger.info(f"Loading configuration from: {self.config_path}")
        with open(self.config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _init_kafka_producer(self) -> Producer:
        """Initialize Confluent Kafka Producer."""
        producer_conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": "ecom_stream_generator",
            "acks": 1,
            "linger.ms": 10,
            "batch.num.messages": 1000,
            "queue.buffering.max.messages": 100000,
        }
        logger.info(f"Connecting to Kafka Broker at: {self.bootstrap_servers}")
        return Producer(producer_conf)

    def reset_topic_and_checkpoint(self):
        """Purge previous checkpoint and recreate topic upon --clean flag."""
        logger.info("Cleaning streaming environment (--clean)...")

        if os.path.exists(self.checkpoint_file):
            os.remove(self.checkpoint_file)
            logger.info(f"Deleted old checkpoint file: {self.checkpoint_file}")

        admin_client = AdminClient({"bootstrap.servers": self.bootstrap_servers})
        metadata = admin_client.list_topics(timeout=10)

        if self.topic_name in metadata.topics:
            logger.info(f"Deleting existing topic '{self.topic_name}' from Kafka...")
            fs = admin_client.delete_topics([self.topic_name])
            for topic, f in fs.items():
                try:
                    f.result()
                    logger.info(f"Deleted topic: {topic}")
                except Exception as e:
                    logger.warning(f"Error deleting topic: {e}")
            time.sleep(2)

        logger.info(f"Creating topic '{self.topic_name}' (3 partitions, replication=1)...")
        new_topic = NewTopic(self.topic_name, num_partitions=3, replication_factor=1)
        fs = admin_client.create_topics([new_topic])
        for topic, f in fs.items():
            try:
                f.result()
                logger.info(f"Successfully created topic: {topic} (3 partitions)")
            except Exception as e:
                logger.error(f"Error creating topic: {e}")

    def _load_checkpoint(self) -> int:
        """Read byte offset and stats from checkpoint file."""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.stats.update(data.get("stats", {}))
                    offset = data.get("byte_offset", OCT_26_BYTE_OFFSET)
                    logger.info(f"[Checkpoint] Resuming from byte offset: {offset}")
                    logger.info(f"[Checkpoint] Previously produced: {self.stats.get('total_produced', 0)} messages.")
                    return offset
            except Exception as e:
                logger.warning(f"Failed to read checkpoint: {e}. Starting from Oct 26 default.")
        return OCT_26_BYTE_OFFSET

    def _save_checkpoint(self, byte_offset: int):
        """Persist current state into checkpoint file."""
        self.stats["byte_offset"] = byte_offset
        data = {"byte_offset": byte_offset, "timestamp": datetime.now().isoformat(), "stats": self.stats}
        with open(self.checkpoint_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    def _delivery_report(self, err, msg):
        """Delivery callback for Kafka producer."""
        if err is not None:
            logger.error(f"Delivery failed: {err}")

    def _parse_csv_line(self, line: str, header_cols: list) -> dict:
        """Parse CSV row into structured dictionary with 64-bit integer IDs."""
        parts = line.strip().split(",")
        if len(parts) < len(header_cols):
            return None

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
                "user_session": parts[8] if parts[8] else None,
            }
            if self.include_discount:
                event["discount_percent"] = int(random.choice(self.discount_vals))
            return event
        except (ValueError, IndexError):
            return None

    def _send_to_kafka(self, event: dict, is_duplicate: bool = False, is_late: bool = False):
        """Produce a single event to Kafka with key=str(user_id)."""
        payload = json.dumps(event).encode("utf-8")
        key = str(event["user_id"]).encode("utf-8") if event["user_id"] is not None else None

        self.producer.produce(topic=self.topic_name, key=key, value=payload, callback=self._delivery_report)
        self.producer.poll(0)
        self.stats["total_produced"] += 1
        if not is_duplicate and not is_late:
            self.stats["normal_produced"] += 1

    def _handle_graceful_shutdown(self, signum, frame):
        """Handle interrupt signals cleanly."""
        logger.info("Shutdown signal received (Ctrl+C). Finishing pending messages...")
        self.is_running = False

    def save_stream_manifest(self, burst_duration: int, burst_multiplier: int):
        """Write streaming execution statistics to data/stream_manifest.json."""
        elapsed = time.time() - self.stats["start_time"]
        avg_throughput = self.stats["total_produced"] / elapsed if elapsed > 0 else 0
        total_prod = max(1, self.stats["total_produced"])

        os.makedirs("data", exist_ok=True)
        manifest = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "topic": self.topic_name,
            "total_produced": self.stats["total_produced"],
            "normal_produced": self.stats["normal_produced"],
            "duplicates_injected": self.stats["duplicates_injected"],
            "duplicate_rate_actual": round(self.stats["duplicates_injected"] / total_prod * 100, 4),
            "late_delayed": self.stats["late_delayed"],
            "late_released": self.stats["late_released"],
            "delay_distribution_minutes": self.late_buffer.get_delay_distribution(),
            "burst_events": self.stats["burst_events"],
            "burst_config": {"multiplier": burst_multiplier, "duration_seconds": burst_duration},
            "last_event_time": self.stats.get("last_event_time"),
            "elapsed_seconds": round(elapsed, 2),
            "average_throughput_msg_per_sec": round(avg_throughput, 2),
        }
        manifest_path = "data/stream_manifest.json"
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved stream manifest at: {manifest_path}")

    def start_streaming(
        self, max_events: int = 0, base_rate: int = 100, burst_duration: int = None, burst_multiplier: int = None
    ):
        """
        Main streaming loop with burst and late arrival simulation.
        """
        csv_path = self.stream_cfg.get("input_csv", "2019-Oct.csv")
        if not os.path.exists(csv_path):
            alt_path = os.path.join("..", csv_path)
            if os.path.exists(alt_path):
                csv_path = alt_path
            else:
                raise FileNotFoundError(f"Source CSV not found at: {csv_path}")

        admin_client = AdminClient({"bootstrap.servers": self.bootstrap_servers})
        metadata = admin_client.list_topics(timeout=10)
        if self.topic_name not in metadata.topics:
            logger.info(f"Topic '{self.topic_name}' does not exist. Creating (3 partitions)...")
            new_topic = NewTopic(self.topic_name, num_partitions=3, replication_factor=1)
            admin_client.create_topics([new_topic])
            time.sleep(1)

        current_offset = self._load_checkpoint()

        actual_burst_duration = burst_duration or self.burst_cfg.get("duration_seconds", 600)
        actual_burst_multiplier = burst_multiplier or self.burst_cfg.get("multiplier", 10)

        logger.info("=" * 80)
        logger.info("STREAMING DATA GENERATOR STARTING (KAFKA PRODUCER)")
        logger.info(f"- Destination Topic:       {self.topic_name} ({self.bootstrap_servers})")
        logger.info(f"- Base Throughput:         {base_rate} events/sec")
        logger.info(f"- Burst Traffic:           x{actual_burst_multiplier} for {actual_burst_duration}s")
        min_d = self.late_cfg.get("delay_minutes_min", 5)
        max_d = self.late_cfg.get("delay_minutes_max", 10)
        logger.info(
            f"- Late Arrival Rate:       {self.late_cfg.get('rate', 0.05) * 100:.1f}% ({min_d}-{max_d} min delay)"
        )
        logger.info(f"- Duplicate Rate:          {self.dup_cfg.get('rate', 0.015) * 100:.1f}%")
        logger.info(f"- Schema:                  10 columns (with discount_percent: {self.discount_vals}%)")
        logger.info(f"- Event Limit:             {'Unlimited' if max_events <= 0 else max_events}")
        logger.info("=" * 80)

        with open(csv_path, "r", encoding="utf-8") as f_head:
            header_cols = f_head.readline().strip().split(",")

        with open(csv_path, "r", encoding="utf-8") as f:
            f.seek(current_offset)

            events_produced_session = 0
            burst_mode = False
            burst_start_time = 0
            last_burst_check = time.time()
            burst_interval = self.burst_cfg.get("trigger_interval_minutes", 5) * 60

            late_rate = self.late_cfg.get("rate", 0.05) if self.late_cfg.get("enabled", True) else 0.0
            dup_rate = self.dup_cfg.get("rate", 0.015) if self.dup_cfg.get("enabled", True) else 0.0

            last_report_time = time.time()
            report_counter = 0

            while self.is_running:
                # 1. Burst Traffic Control
                now_sec = time.time()
                if not burst_mode and (now_sec - last_burst_check >= burst_interval or events_produced_session == 1000):
                    burst_mode = True
                    burst_start_time = now_sec
                    logger.info(
                        f"[BURST START] Flash Sale active: x{actual_burst_multiplier} throughput ({base_rate * actual_burst_multiplier} msg/s)"
                    )

                if burst_mode and (now_sec - burst_start_time >= actual_burst_duration):
                    burst_mode = False
                    last_burst_check = now_sec
                    logger.info("[BURST END] Flash Sale ended, returned to base rate.")

                current_rate = (base_rate * actual_burst_multiplier) if burst_mode else base_rate
                sleep_interval = 1.0 / current_rate

                # 2. Read next CSV line
                current_byte_pos = f.tell()
                line = f.readline()
                if not line:
                    logger.info("Reached end of CSV file.")
                    break

                event = self._parse_csv_line(line, header_cols)
                if not event:
                    continue

                if not event["event_time"].startswith("2019-10"):
                    logger.info(f"Reached time boundary beyond October: {event['event_time']}. Stopping.")
                    break

                try:
                    clean_time_str = event["event_time"].replace(" UTC", "")
                    curr_dt = datetime.strptime(clean_time_str, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    curr_dt = datetime.now()

                self.stats["last_event_time"] = event["event_time"]

                # 3. Fault Injection: Late Arrival
                if late_rate > 0 and random.random() < late_rate:
                    delay_mins = random.randint(min_d, max_d)
                    release_dt = curr_dt + timedelta(minutes=delay_mins)
                    self.late_buffer.add(release_dt, event, delay_minutes=delay_mins)
                    self.stats["late_delayed"] += 1
                else:
                    self._send_to_kafka(event)

                # 4. Fault Injection: Duplicate Records
                if dup_rate > 0 and random.random() < dup_rate:
                    self._send_to_kafka(event, is_duplicate=True)
                    self.stats["duplicates_injected"] += 1

                # 5. Release ready events from late_buffer
                ready_events = self.late_buffer.release_ready(curr_dt)
                for r_ev in ready_events:
                    self._send_to_kafka(r_ev, is_late=True)
                    self.stats["late_released"] += 1

                if burst_mode:
                    self.stats["burst_events"] += 1

                events_produced_session += 1
                report_counter += 1

                if time.time() - last_report_time >= 2.0:
                    throughput = report_counter / (time.time() - last_report_time)
                    logger.info(
                        f"[STREAMING] Total: {self.stats['total_produced']:,} | "
                        f"Rate: {throughput:.1f} msg/s | "
                        f"EventTime: {event['event_time']} | "
                        f"Late Delayed: {self.stats['late_delayed']} (Released: {self.stats['late_released']}) | "
                        f"Dupes: {self.stats['duplicates_injected']} | "
                        f"Mode: {'BURST' if burst_mode else 'NORMAL'}"
                    )
                    last_report_time = time.time()
                    report_counter = 0
                    self._save_checkpoint(current_byte_pos)

                if 0 < max_events <= events_produced_session:
                    logger.info(f"Target limit of {max_events} events reached. Stopping.")
                    break

                if sleep_interval > 0.002:
                    time.sleep(sleep_interval)
                elif sleep_interval > 0 and (events_produced_session % 50 == 0):
                    time.sleep(sleep_interval * 50)

            # Flush remaining events in buffer
            flushed = self.late_buffer.release_all()
            if flushed:
                logger.info(f"Flushing {len(flushed)} remaining delayed events from late_buffer...")
                for r_ev in flushed:
                    self._send_to_kafka(r_ev, is_late=True)
                    self.stats["late_released"] += 1

            self._save_checkpoint(f.tell())

        logger.info("Flushing pending messages to Kafka...")
        self.producer.flush(timeout=10)
        self.save_stream_manifest(actual_burst_duration, actual_burst_multiplier)
        self._print_final_summary()

    def _print_final_summary(self):
        """Print summary report after streaming session."""
        elapsed = time.time() - self.stats["start_time"]
        avg_throughput = self.stats["total_produced"] / elapsed if elapsed > 0 else 0
        delay_stats = self.late_buffer.get_delay_distribution()

        print("\n" + "=" * 80)
        print("STREAMING DATA GENERATOR SUMMARY REPORT")
        print("=" * 80)
        print(f"- Total events sent to Kafka:      {self.stats['total_produced']:,} messages")
        print(f"- Normal events:                   {self.stats['normal_produced']:,}")
        print(f"- Burst Traffic events:            {self.stats['burst_events']:,} messages")
        print(
            f"- Late Arrival events generated:   {self.stats['late_delayed']:,} (Released: {self.stats['late_released']:,})"
        )
        print(
            f"- Late Delay Distribution:         Min={delay_stats['min']}m, Median={delay_stats['median']}m, Max={delay_stats['max']}m"
        )
        print(
            f"- Injected duplicates:             {self.stats['duplicates_injected']:,} ({self.stats['duplicates_injected'] / max(1, self.stats['total_produced']) * 100:.2f}%)"
        )
        print(f"- Final Event Time:                {self.stats.get('last_event_time', 'N/A')}")
        print(f"- Session elapsed time:            {elapsed:.1f} seconds")
        print(f"- Average throughput:              {avg_throughput:.1f} messages/sec")
        print(f"- Checkpoint path:                 {self.checkpoint_file}")
        print("- Stream manifest:                 data/stream_manifest.json")
        print("=" * 80 + "\n")


def main():
    """Command-line entry point."""
    parser = argparse.ArgumentParser(description="Streaming Data Generator & Fault Injection for Kafka")
    parser.add_argument("--config", default="config/generator_config.yaml", help="Path to YAML config")
    parser.add_argument("--clean", action="store_true", help="Delete old topic and reset checkpoint to restart")
    parser.add_argument("--max-events", type=int, default=0, help="Maximum events to produce (0 = continuous)")
    parser.add_argument("--rate", type=int, default=100, help="Base rate (events/sec)")
    parser.add_argument("--burst-duration", type=int, default=None, help="Burst duration in seconds")
    parser.add_argument("--burst-multiplier", type=int, default=None, help="Burst rate multiplier")
    args = parser.parse_args()

    generator = StreamDataGenerator(config_path=args.config)

    if args.clean:
        generator.reset_topic_and_checkpoint()

    generator.start_streaming(
        max_events=args.max_events,
        base_rate=args.rate,
        burst_duration=args.burst_duration,
        burst_multiplier=args.burst_multiplier,
    )


if __name__ == "__main__":
    main()
