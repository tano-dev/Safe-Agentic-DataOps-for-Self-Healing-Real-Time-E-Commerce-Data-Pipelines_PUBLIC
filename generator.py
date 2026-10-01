"""
generator.py-->Công cụ tạo data event behavior của system

Mô phỏng chuỗi hành vi: 
Search -> View Product -> Add to Cart -> Checkout -> Payment -> Order 
bash:
    python generator.py --rate 20 --duration 30 
    ===> tao 20 events/s trong 30s
"""

import argparse
import random
import time
from datetime import datetime, timezone
from uuid import uuid4

from faker import Faker

from schemas import EcommerceEvent, EventType, PaymentMethod

fake = Faker("vi_VN")

CATEGORIES = ["thoi_trang", 
              "dien_tu", 
              "gia_dung", 
              "sach", 
              "my_pham", 
              "thuc_pham"]
PROVINCES = [
    "Ha Noi",
    "Cao Bang",
    "Tuyen Quang",
    "Dien Bien",
    "Lai Chau",
    "Son La",
    "Lao Cai",
    "Thai Nguyen",
    "Lang Son",
    "Quang Ninh",
    "Bac Ninh",
    "Phu Tho",
    "Hai Phong",
    "Hung Yen",
    "Ninh Binh",
    "Thanh Hoa",
    "Nghe An",
    "Ha Tinh",
    "Quang Tri",
    "Hue",
    "Da Nang",
    "Quang Ngai",
    "Gia Lai",
    "Khanh Hoa",
    "Dak Lak",
    "Lam Dong",
    "Dong Nai",
    "Ho Chi Minh",
    "Tay Ninh",
    "Dong Thap",
    "Vinh Long",
    "An Giang",
    "Can Tho",
    "Ca Mau"
]

def random_price_vnd() -> float:
    return (random.uniform(20_000, 5_000_000))  # làm tròn đến hàng nghìn VND


def make_session() -> list[EcommerceEvent]:
    """Một hành trình khách hàng, với funnel drop-off thực tế ở mỗi bước."""
    user_id = f"user_{fake.uuid4()[:8]}"
    session_id = f"sess_{uuid4().hex[:12]}"
    # device = random.choice(list(DeviceType))
    province = random.choice(PROVINCES)

    events: list[EcommerceEvent] = []

    def emit(event_type: EventType, **kwargs) -> None:
        events.append(
            EcommerceEvent(
                event_type=event_type,
                timestamp=datetime.now(timezone.utc),
                user_id=user_id,
                session_id=session_id,
                # device=device,
                province=province,
                **kwargs,
            )
        )

    category = random.choice(CATEGORIES)
    product_id = f"prod_{fake.uuid4()[:8]}"
    price = random_price_vnd()

    emit(EventType.SEARCH, category=category)
    emit(EventType.VIEW_PRODUCT, product_id=product_id, category=category, price_vnd=price)

    if random.random() < 0.55:  # 55% xem xong thì thêm vào giỏ
        emit(
            EventType.ADD_TO_CART,
            product_id=product_id,
            category=category,
            price_vnd=price,
            quantity=random.randint(1, 3),
        )

        if random.random() < 0.15:  # 15% đổi ý, bỏ khỏi giỏ
            emit(EventType.REMOVE_FROM_CART, product_id=product_id, category=category)
            return events

        if random.random() < 0.65:  # 65% còn lại tiến hành checkout
            payment_method = random.choice(list(PaymentMethod))
            emit(
                EventType.CHECKOUT,
                product_id=product_id,
                category=category,
                price_vnd=price,
                payment_method=payment_method,
            )
            emit(
                EventType.PAYMENT,
                product_id=product_id,
                category=category,
                price_vnd=price,
                payment_method=payment_method,
            )

            if random.random() < 0.9:  # 90% thanh toán thành công
                emit(EventType.ORDER_CREATED, product_id=product_id, category=category, price_vnd=price)
            else:
                emit(EventType.ORDER_CANCELLED, product_id=product_id, category=category, price_vnd=price)

    return events


def generate_stream(target_eps: int, duration_seconds: int, sink) -> int:
    """Sinh session liên tục, gọi sink(event) cho từng event, xấp xỉ target_eps events/giây."""
    start = time.time()
    emitted = 0

    while time.time() - start < duration_seconds:
        batch_start = time.time()
        for event in make_session():
            sink(event)
            emitted += 1

        elapsed = time.time() - batch_start
        sleep_for = max(0.0, (1.0 / max(target_eps, 1)) - elapsed)
        time.sleep(sleep_for)

    return emitted


def print_sink(event: EcommerceEvent) -> None:
    print(event.model_dump_json())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="VietCommerceOps synthetic event generator")
    parser.add_argument("--rate", type=int, default=10, help="events/giây")
    parser.add_argument("--duration", type=int, default=30, help="thời gian chạy (giây)")
    args = parser.parse_args()

    total = generate_stream(args.rate, args.duration, print_sink)
    print(f"Da tao {total} events trong {args.duration}s ---")
