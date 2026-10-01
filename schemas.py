"""
schemas.py — Shared Interface giữa Student A n B

gom 2 nhóm schema:
  1. EcommerceEvent: cau truc event
  2. IncidentContext  incident Context Schema 

"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Optional
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator


# event schema

class EventType(str, Enum):
    VIEW_PRODUCT = "view_product"           #xem san pham
    SEARCH = "search"                       #tim kiem san pham
    ADD_TO_CART = "add_to_cart"             #them vao gio hang
    REMOVE_FROM_CART = "remove_from_cart"   #xoa khoi gio hang
    CHECKOUT = "checkout"                   #kiem tra
    PAYMENT = "payment"                     #thanh toan
    ORDER_CREATED = "order_created"         #tao order
    ORDER_CANCELLED = "order_cancelled"     #huy tao order


# class DeviceType(str, Enum):                #loai thiet bi
#     MOBILE = "mobile"
#     DESKTOP = "desktop"
#     TABLET = "tablet"



class PaymentMethod(str, Enum):             #phuong thuc thanh toan
    COD = "cod"                             #tien mat khi nhan hang
    MOMO = "momo"
    ZALOPAY = "zalopay"
    VNPAY = "vnpay"
    BANK_TRANSFER = "bank_transfer"
    CREDIT_CARD = "credit_card"


class EcommerceEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: EventType
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    user_id: str
    session_id: str

    product_id: Optional[str] = None
    category: Optional[str] = None
    price_vnd: Optional[float] = Field(default=None, ge=0)
    quantity: Optional[int] = Field(default=None, ge=1)

    province: Optional[str] = None
    # device: Optional[DeviceType] = None
    payment_method: Optional[PaymentMethod] = None

    @field_validator("price_vnd")
    @classmethod
    def price_must_be_non_negative(cls, v):
        if v is not None and v < 0:
            raise ValueError("price_vnd phải >= 0")
        return v

    model_config = {"use_enum_values": True}


# Incident Context Schema

class FailureFamily(str, Enum):
    SCHEMA = "schema_failure"
    DATA_QUALITY = "data_quality_failure"
    INFRASTRUCTURE = "infrastructure_failure"
    CONNECTIVITY_STORAGE = "connectivity_storage_failure"
    RESOURCE_PERFORMANCE = "resource_performance_failure"


class IncidentContext(BaseModel):


    incident_id: str = Field(default_factory=lambda: str(uuid4()))
    failure_family: FailureFamily
    fault_injection: str = Field(
        ..., description="Cách lỗi được bơm vào, vd: 'killed flink-taskmanager container'"
    )
    expected_symptoms: list[str] = Field(default_factory=list)

    logs: list[str] = Field(default_factory=list)
    metrics: dict = Field(default_factory=dict)

    ground_truth_root_cause: str
    valid_recovery_actions: list[str] = Field(default_factory=list)
    forbidden_actions: list[str] = Field(default_factory=list)
    success_condition: str

    detected_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None

    model_config = {"use_enum_values": True}


if __name__ == "__main__":
    demo_event = EcommerceEvent(
        event_type=EventType.CHECKOUT,
        user_id="user_abc123",
        session_id="sess_xyz789",
        product_id="prod_001",
        category="dien_tu",
        price_vnd=1_250_000,
        payment_method=PaymentMethod.MOMO,
    )
    print(demo_event.model_dump_json(indent=2))

    demo_incident = IncidentContext(
        failure_family=FailureFamily.SCHEMA,
        fault_injection=f"dropped 'price_vnd' field from 10% of events",
        expected_symptoms=["Flink job validation errors", "DLQ record count spike"],
        ground_truth_root_cause="Producer schema drift: missing required field price_vnd",
        valid_recovery_actions=["quarantine_bad_records", "rollback_schema"],
        forbidden_actions=["restart_service(all)"],
        success_condition="DLQ rate returns to baseline for 5 consecutive minutes",
    )
    print(demo_incident.model_dump_json(indent=2))
