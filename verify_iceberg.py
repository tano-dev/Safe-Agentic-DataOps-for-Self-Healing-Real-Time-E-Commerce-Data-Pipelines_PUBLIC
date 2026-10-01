"""
verify_iceberg.py — Kiểm tra dữ liệu đã chảy từ Redpanda -> Flink -> Iceberg
thành công (deliverable Tuần 3-4: "dữ liệu chảy end-to-end, query được").

Dùng pyiceberg (client Python chính thức của Apache Iceberg) đọc trực tiếp
qua REST catalog + MinIO/RustFS (không cần dựng thêm Spark or Trino chỉ để kiểm tra)

Usage:
    python verify_iceberg.py
    python verify_iceberg.py --preview 20
"""

import argparse

from pyiceberg.catalog import load_catalog

CATALOG_CONFIG = {
    "type": "rest",
    "uri": "http://localhost:8181",
    "s3.endpoint": "http://localhost:9000",
    "s3.access-key-id": "minioadmin",
    "s3.secret-access-key": "minioadmin123",
}

TABLE_IDENTIFIER = "raw.events"


def main(preview_rows: int) -> None:
    catalog = load_catalog("vietcommerceops", **CATALOG_CONFIG)
    table = catalog.load_table(TABLE_IDENTIFIER)

    df = table.scan().to_pandas()
    print(f"--- Tổng số record hiện có trong '{TABLE_IDENTIFIER}': {len(df)} ---")
    if len(df) == 0:
        print("Chưa có dữ liệu — kiểm tra lại: job Flink đã RUNNING chưa (localhost:8081)?")
        print("producer.py đã chạy thành công chưa?")
        return

    print(f"\n--- {min(preview_rows, len(df))} dòng đầu tiên ---")
    print(df.head(preview_rows))

    print("\n--- Phân bố theo event_type ---")
    print(df["event_type"].value_counts())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kiểm tra dữ liệu trong Iceberg table")
    parser.add_argument("--preview", type=int, default=10, help="Số dòng preview")
    args = parser.parse_args()
    main(args.preview)
