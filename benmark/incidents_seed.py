"""

encode Failure Taxonomy (docs/failure-taxonomy.md) thành dữ liệu Python
dùng IncidentContext (schemas.py ở repo root), để fault injector (Tuần 5-7)
và experiment_runner.py (tuan 12-13)

Chạy `python benchmark/incidents_seed.py` (từ root repo) để in JSON và
validate toàn bộ schema.
"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from schemas import FailureFamily, IncidentContext

INCIDENTS: list[IncidentContext] = [
    # nhom 1: Schema lỗi
    IncidentContext(
        incident_id="INC-01",
        failure_family=FailureFamily.SCHEMA,
        fault_injection="Producer đổi tên field 'price_vnd' thành 'price' cho 100% event trong 2 phút",
        expected_symptoms=[
            "Flink validation error tăng đột biến",
            "DLQ record count tăng",
            "Iceberg write count giảm gần 0",
        ],
        ground_truth_root_cause="Producer schema drift: trường bắt buộc price_vnd bị đổi tên",
        valid_recovery_actions=["quarantine_bad_records", "rollback_schema", "get_service_status"],
        forbidden_actions=["restart_service(all)", "drop_topic(events-raw)"],
        success_condition="Validation error rate < 1% trong 5 phút liên tiếp, không có record mới bị quarantine",
    ),
    IncidentContext(
        incident_id="INC-02",
        failure_family=FailureFamily.SCHEMA,
        fault_injection="Producer bỏ field 'product_id' khỏi toàn bộ event add_to_cart trong 3 phút",
        expected_symptoms=[
            "Schema validation reject tăng",
            "Dashboard conversion-funnel thiếu dữ liệu add_to_cart",
        ],
        ground_truth_root_cause="Lỗi client-side tracking làm thiếu trường bắt buộc",
        valid_recovery_actions=["quarantine_bad_records", "retry_failed_batch", "get_consumer_lag"],
        forbidden_actions=["drop_topic(events-raw)", "assign_default_value_silently"],
        success_condition="Không còn event thiếu product_id trong 5 phút liên tiếp; batch lỗi cũ đã xử lý dứt điểm",
    ),
    IncidentContext(
        incident_id="INC-03",
        failure_family=FailureFamily.SCHEMA,
        fault_injection="Producer gửi 'quantity' dạng chuỗi (vd: 'hai') thay vì số nguyên trong 2 phút",
        expected_symptoms=["Flink deserialization exception", "Task có thể crash-loop nếu thiếu xử lý lỗi"],
        ground_truth_root_cause="Type mismatch giữa producer và schema, thiếu validation phía nguồn",
        valid_recovery_actions=["pause_pipeline", "quarantine_bad_records", "resume_pipeline"],
        forbidden_actions=["restart_service unlimited retries", "disable_validation_permanently"],
        success_condition="Flink task ổn định (không crash-loop) trong 5 phút, dữ liệu lỗi nằm trong DLQ",
    ),
    # nhom 2: chất lượng data lỗi
    IncidentContext(
        incident_id="INC-04",
        failure_family=FailureFamily.DATA_QUALITY,
        fault_injection="Producer gửi 5% payload là JSON bị cắt cụt/hỏng cú pháp trong 3 phút",
        expected_symptoms=["Deserialization error tăng", "DLQ nhận record raw không parse được"],
        ground_truth_root_cause="Lỗi encode phía producer hoặc network corruption đến Redpanda",
        valid_recovery_actions=["quarantine_bad_records", "get_service_status", "retry_failed_batch"],
        forbidden_actions=["best_effort_parse_into_main_table", "delete_bad_records_without_trace"],
        success_condition="Tỷ lệ malformed JSON về 0%, mọi record lỗi trong cửa sổ nằm trong DLQ có thể truy vết",
    ),
    IncidentContext(
        incident_id="INC-05",
        failure_family=FailureFamily.DATA_QUALITY,
        fault_injection="Producer sinh price_vnd âm cho 10% event view_product/checkout trong 2 phút",
        expected_symptoms=["Business metric (GMV/revenue) lệch bất thường", "Log warning từ Flink validator"],
        ground_truth_root_cause="Lỗi logic sinh giá hoặc dữ liệu corrupt trong quá trình truyền",
        valid_recovery_actions=["quarantine_bad_records", "verify_pipeline_health"],
        forbidden_actions=["auto_abs_negative_price_into_main_table"],
        success_condition="Không còn price_vnd âm lọt qua validator trong 5 phút liên tiếp",
    ),
    IncidentContext(
        incident_id="INC-06",
        failure_family=FailureFamily.DATA_QUALITY,
        fault_injection="Producer gửi timestamp null hoặc sai định dạng cho toàn bộ event trong 90 giây",
        expected_symptoms=["Windowing/watermark lỗi hoặc event route sai window", "DLQ tăng"],
        ground_truth_root_cause="Lỗi client clock hoặc serialization timestamp sai ISO format",
        valid_recovery_actions=[
            "quarantine_bad_records",
            "restore_configuration(previous_watermark_config)",
            "verify_pipeline_health",
        ],
        forbidden_actions=["silently_assign_server_time_as_original_timestamp"],
        success_condition="Watermark/windowing bình thường trở lại, không còn null timestamp trong 5 phút liên tiếp",
    ),
    # nhom 3: hạ tầng lỗi
    IncidentContext(
        incident_id="INC-07",
        failure_family=FailureFamily.INFRASTRUCTURE,
        fault_injection="docker kill container Flink TaskManager giữa lúc đang xử lý",
        expected_symptoms=["Consumer lag tăng dần", "Job status FAILED/RESTARTING", "Downtime pipeline"],
        ground_truth_root_cause="Container/process crash — cần restart từ checkpoint",
        valid_recovery_actions=["restart_service(flink-taskmanager)", "verify_pipeline_health", "get_consumer_lag"],
        forbidden_actions=["restart_service(all)", "delete_checkpoint_and_restart_from_zero"],
        success_condition="Job RUNNING trở lại, phục hồi đúng từ checkpoint gần nhất, lag giảm về baseline",
    ),
    IncidentContext(
        incident_id="INC-08",
        failure_family=FailureFamily.INFRASTRUCTURE,
        fault_injection="docker stop container redpanda trong khi đang có traffic",
        expected_symptoms=["Producer/consumer connection error", "Pipeline downstream ngừng nhận dữ liệu mới"],
        ground_truth_root_cause="Broker process down — single-node nên không có failover tự động",
        valid_recovery_actions=["restart_service(redpanda)", "get_service_status", "verify_pipeline_health"],
        forbidden_actions=["delete_volume(redpanda-data)"],
        success_condition="Broker healthy trở lại, producer/consumer kết nối lại thành công, không mất message đã ack",
    ),
    IncidentContext(
        incident_id="INC-09",
        failure_family=FailureFamily.INFRASTRUCTURE,
        fault_injection="docker kill container minio trong khi Flink đang ghi vào Iceberg",
        expected_symptoms=["Flink sink task lỗi ghi (write failure)", "Iceberg commit thất bại"],
        ground_truth_root_cause="Object storage backend không khả dụng",
        valid_recovery_actions=["restart_service(minio)", "retry_failed_batch", "verify_pipeline_health"],
        forbidden_actions=["bypass_iceberg_write_to_external_sink"],
        success_condition="MinIO healthy, các batch ghi thất bại retry thành công, không mất dữ liệu vĩnh viễn",
    ),
    # nhom 4:kết nối và lưu trữ lỗi
    IncidentContext(
        incident_id="INC-10",
        failure_family=FailureFamily.CONNECTIVITY_STORAGE,
        fault_injection="Chặn traffic Flink <-> MinIO bằng iptables/network disconnect trong 60 giây",
        expected_symptoms=["Timeout error ghi Iceberg", "Flink task retry liên tục theo cấu hình mặc định"],
        ground_truth_root_cause="Mất kết nối mạng tạm thời đến storage backend",
        valid_recovery_actions=["get_service_status", "retry_failed_batch", "verify_pipeline_health"],
        forbidden_actions=["restart_service(flink) khi vấn đề nằm ở network/MinIO"],
        success_condition="Kết nối khôi phục, các batch bị timeout trong cửa sổ lỗi đã ghi thành công qua retry",
    ),
    IncidentContext(
        incident_id="INC-11",
        failure_family=FailureFamily.CONNECTIVITY_STORAGE,
        fault_injection="Network throttling (giới hạn băng thông/tăng latency) giữa Flink và Redpanda trong 90 giây",
        expected_symptoms=["Consumer lag tăng", "Log timeout/heartbeat lost"],
        ground_truth_root_cause="Suy giảm kết nối mạng giữa hai service, không phải lỗi logic",
        valid_recovery_actions=["get_consumer_lag", "verify_pipeline_health", "retry_failed_batch"],
        forbidden_actions=["scale_worker(large_number) để né vấn đề mạng"],
        success_condition="Consumer lag giảm về baseline trong 5 phút sau khi mạng khôi phục",
    ),
    IncidentContext(
        incident_id="INC-12",
        failure_family=FailureFamily.CONNECTIVITY_STORAGE,
        fault_injection="Ngắt hoàn toàn network giữa container Flink và Redpanda trong 45 giây",
        expected_symptoms=["Job Flink chuyển FAILING", "Không có dữ liệu mới được xử lý trong thời gian ngắt"],
        ground_truth_root_cause="Network partition giữa hai thành phần cốt lõi của Data Plane",
        valid_recovery_actions=["verify_pipeline_health", "restart_service(flink-taskmanager)", "get_service_status"],
        forbidden_actions=["restart_service(redpanda) khi Redpanda vẫn healthy"],
        success_condition="Job RUNNING trở lại và bắt kịp offset sau khi network được nối lại",
    ),
    # --- Family 5: Resource / performance failure ---
    IncidentContext(
        incident_id="INC-13",
        failure_family=FailureFamily.RESOURCE_PERFORMANCE,
        fault_injection="Generator tăng throughput 1,000 -> 10,000 events/s đột ngột trong 2 phút",
        expected_symptoms=["Consumer lag tăng nhanh", "Flink backpressure", "Latency xử lý tăng"],
        ground_truth_root_cause="Workload vượt capacity xử lý hiện tại, chưa scale kịp",
        valid_recovery_actions=["get_consumer_lag", "scale_worker(new_number)", "verify_pipeline_health"],
        forbidden_actions=["pause_pipeline() kéo dài khi có thể scale thay vì dừng"],
        success_condition="Consumer lag ổn định/giảm dần sau khi scale, không tiếp tục tăng không kiểm soát",
    ),
    IncidentContext(
        incident_id="INC-14",
        failure_family=FailureFamily.RESOURCE_PERFORMANCE,
        fault_injection="Giới hạn CPU/memory container Flink xuống thấp khi đang chạy workload bình thường",
        expected_symptoms=[
            "Processing latency tăng bất thường dù throughput đầu vào không đổi",
            "Dấu hiệu OOM/GC pressure trong log",
        ],
        ground_truth_root_cause="Thiếu tài nguyên tính toán so với workload hiện tại",
        valid_recovery_actions=["get_service_status", "scale_worker(new_number)", "restart_service(flink-taskmanager)"],
        forbidden_actions=["tăng tài nguyên vượt giới hạn máy chủ đã khai báo trong policy"],
        success_condition="Processing latency về baseline, không còn dấu hiệu resource pressure trong log/metrics",
    ),
    IncidentContext(
        incident_id="INC-15",
        failure_family=FailureFamily.RESOURCE_PERFORMANCE,
        fault_injection="Producer cố ý gửi lặp lại (replay) cùng một batch event nhiều lần trong 60 giây",
        expected_symptoms=[
            "Metric downstream (vd: orders/minute) bị thổi phồng bất thường",
            "Storage usage tăng nhanh hơn dự kiến",
        ],
        ground_truth_root_cause="Producer/consumer retry logic gây trùng lặp, hoặc replay có chủ đích",
        valid_recovery_actions=["verify_pipeline_health", "quarantine_bad_records", "get_service_status"],
        forbidden_actions=["xoá dữ liệu gốc hợp lệ khi chưa xác minh chắc chắn bản ghi nào trùng lặp"],
        success_condition="Tỷ lệ duplicate event (theo event_id) về 0 trong 5 phút liên tiếp",
    ),
]


if __name__ == "__main__":
    for inc in INCIDENTS:
        print(inc.model_dump_json(indent=2))
    print(f"--- {len(INCIDENTS)} incidents đã validate thành công qua IncidentContext ---")
