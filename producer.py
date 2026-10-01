"""
producer.py — Đẩy events sinh ra bởi generator.py vào topic `events-raw` trên Redpanda
python producer.py --rate 100 --duration 60

-----------------
Hỗ trợ 2 workload profile theo đề cương (mục 5):
  - normal:     throughput cố định (mục 5.1)
  - flash-sale: tăng dần 1,000 -> 2,000 -> 5,000 -> 10,000 events/s
 
Usage:
    python producer.py --profile normal --rate 1000 --duration 60 --workers 4
    python producer.py --profile flash-sale --stage-duration 60 --workers 4

    
    ===================
Đẩy data vào Redpanda(normal+flash-sale)
"""

import argparse
import multiprocessing as mp
from confluent_kafka import Producer

from generator import generate_stream
from schemas import EcommerceEvent

BOOTSTRAP_SERVERS = "localhost:19092"  # external listener, khớp docker-compose.yml
TOPIC = "events-raw"
FLASH_SALE_STAGES = [1000, 2000, 5000, 10000]

def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"[ERROR] gửi thất bại: {err}")
    # Bỏ comment dòng dưới nếu cần debug từng message (sẽ rất nhiều log ở throughput cao):
    # else:
    #     print(f"delivered -> {msg.topic()} [partition {msg.partition()}]")


def make_kafka_sink(producer: Producer):
    def sink(event: EcommerceEvent) -> None:
        producer.produce(
            TOPIC,
            key=event.user_id,
            value=event.model_dump_json(),
            callback=delivery_report,
        )
        producer.poll(0)  # xử lý delivery callback không chặn luồng chính

    return sink


def run_worker(rate: int, duration: int, bootstrap_servers: str, worker_id: int) -> None:
    """Một process độc lập — tự tạo Kafka producer riêng (không share qua process)."""
    producer = Producer({"bootstrap.servers": bootstrap_servers})
    sink = make_kafka_sink(producer)
    total = generate_stream(rate, duration, sink)
    producer.flush(10)
    print(f"[worker {worker_id}] đã đẩy {total} events trong {duration}s")
 
 
def run_stage(target_rate: int, duration: int, bootstrap_servers: str, workers: int) -> None:
    """Chia target_rate cho `workers` process chạy song song, mỗi process rate/workers events/s."""
    per_worker_rate = max(1, target_rate // workers)
    processes = [
        mp.Process(target=run_worker, args=(per_worker_rate, duration, bootstrap_servers, i))
        for i in range(workers)
    ]
    print(f"--- Stage: mục tiêu {target_rate} events/s ({workers} worker x ~{per_worker_rate}/s), {duration}s ---")
    for p in processes:
        p.start()
    for p in processes:
        p.join()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Đẩy VietCommerceOps events vào Redpanda")
    parser.add_argument("--profile", choices=["normal", "flash-sale"], default="normal")
    parser.add_argument("--rate", type=int, default=100, help="[normal] events/giây mục tiêu (tổng)")
    parser.add_argument("--duration", type=int, default=60, help="[normal] thời gian chạy (giây)")
    parser.add_argument("--stage-duration", type=int, default=60, help="[flash-sale] số giây mỗi stage")
    parser.add_argument("--workers", type=int, default=4, help="số process song song")
    parser.add_argument("--bootstrap-servers", default=BOOTSTRAP_SERVERS)
    args = parser.parse_args()

    if args.profile == "normal":
        run_stage(args.rate, args.duration, args.bootstrap_servers, args.workers)
    else:
        for stage_rate in FLASH_SALE_STAGES:
            run_stage(stage_rate, args.stage_duration, args.bootstrap_servers, args.workers)
        print("--- Flash-sale workload hoàn tất tất cả các stage ---")