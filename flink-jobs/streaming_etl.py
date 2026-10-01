"""
flink_jobs/streaming_etl.py — Data Plane ETL job (Tuần 3-4)

flow: Redpanda (topic events-raw) -> validate cơ bản -> Iceberg table
trên MinIO/Rustfs, qua REST catalog. 


Chạy sau khi `docker compose up -d` đã healthy):
    docker exec -it flink-jobmanager /opt/flink/bin/flink run -d -py /opt/flink/usrlib/streaming_etl.py
Xem job đang chạy tại: http://localhost:8081

# job: Redpanda -> validate -> Iceberg
"""

from pyflink.table import EnvironmentSettings, TableEnvironment


def main() -> None:
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    t_env.get_config().set("execution.checkpointing.interval", "30s")

    #1. Kafka source table (đọc từ Redpanda topic events-raw)
    t_env.execute_sql("""
        CREATE TABLE events_raw (
            event_id STRING,
            event_type STRING,
            `timestamp` TIMESTAMP(3),
            user_id STRING,
            session_id STRING,
            product_id STRING,
            category STRING,
            price_vnd DOUBLE,
            quantity INT,
            province STRING,
            payment_method STRING,
            WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'events-raw',
            'properties.bootstrap.servers' = 'redpanda:9092',
            'properties.group.id' = 'flink-streaming-etl',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true',
            'json.fail-on-missing-field' = 'false',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)

    #2. Iceberg REST catalog trên MinIO/Rustfs 
    t_env.execute_sql("""
        CREATE CATALOG iceberg_catalog WITH (
            'type' = 'iceberg',
            'catalog-type' = 'rest',
            'uri' = 'http://iceberg-rest:8181',
            'warehouse' = 's3://vietcommerceops-lakehouse/warehouse',
            'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
            's3.endpoint' = 'http://minio:9000',
            's3.path-style-access' = 'true'
        )
    """)
    t_env.execute_sql("CREATE DATABASE IF NOT EXISTS iceberg_catalog.`raw`")
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS iceberg_catalog.`raw`.events (
            event_id STRING,
            event_type STRING,
            event_time TIMESTAMP(3),
            user_id STRING,
            session_id STRING,
            product_id STRING,
            category STRING,
            price_vnd DOUBLE,
            quantity INT,
            province STRING,
            payment_method STRING
        )
    """)

    #3. Validate cơ bản + ghi vào Iceberg (chạy liên tục)
    t_env.execute_sql("""
        INSERT INTO iceberg_catalog.`raw`.events
        SELECT
            event_id, event_type, `timestamp`, user_id, session_id,
            product_id, category, price_vnd, quantity,
            province, payment_method
        FROM default_catalog.default_database.events_raw
        WHERE event_id IS NOT NULL
          AND user_id IS NOT NULL
          AND (price_vnd IS NULL OR price_vnd >= 0)
    """)


if __name__ == "__main__":
    main()
