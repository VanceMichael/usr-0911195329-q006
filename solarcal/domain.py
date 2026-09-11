"""领域模型：观测、预测、设备状态、校准参数与批次结果。"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field

# 设备状态取值
DEVICE_NORMAL = "normal"
DEVICE_FAULT = "fault"
DEVICE_OFFLINE = "offline"
DEVICE_CURTAILMENT = "curtailment"
BLOCKING_DEVICE_STATES = {DEVICE_FAULT, DEVICE_OFFLINE, DEVICE_CURTAILMENT}

# 批次类型与状态
KIND_ORIGINAL = "original"
KIND_REVISION = "revision"
STATUS_OPEN = "open"
STATUS_SETTLED = "settled"


@dataclass(frozen=True)
class Observation:
    """一条辐照度/功率实测。obs_id 由上报方生成，用于乱序重放去重。"""
    obs_id: str
    ts_utc: object  # datetime(UTC)，采样时刻
    irradiance_wm2: float | None
    power_kw: float | None
    source: str = "scada"
    ts_quality: str = "ok"  # 时间解释质量（见 timegrid）


@dataclass(frozen=True)
class Forecast:
    ts_utc: object
    power_kw: float
    model_version: str


@dataclass(frozen=True)
class DeviceStatus:
    ts_utc: object
    status: str


@dataclass(frozen=True)
class CalibrationParams:
    """校准参数。参与 params_hash，一经结算即随模型版本冻结。"""
    method: str = "additive_bias"      # 加性偏差修正
    interval_minutes: int = 15
    min_valid_slots: int = 8           # 有效槽位不足则不做偏差修正
    irradiance_max_wm2: float = 1600.0
    outlier_abs_kw: float = 50.0       # |实测-预测| 绝对阈值
    outlier_rel: float = 0.25          # 相对装机容量阈值，两者取大
    bias_cap_rel: float = 0.30         # 偏差修正幅度上限（相对容量）

    def to_dict(self) -> dict:
        return {
            "method": self.method,
            "interval_minutes": self.interval_minutes,
            "min_valid_slots": self.min_valid_slots,
            "irradiance_max_wm2": self.irradiance_max_wm2,
            "outlier_abs_kw": self.outlier_abs_kw,
            "outlier_rel": self.outlier_rel,
            "bias_cap_rel": self.bias_cap_rel,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "CalibrationParams":
        known = {f for f in cls.__dataclass_fields__}
        return cls(**{k: v for k, v in d.items() if k in known})

    def params_hash(self) -> str:
        canonical = json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode()).hexdigest()[:16]


@dataclass
class SlotResult:
    slot_epoch: int
    forecast_kw: float | None
    observed_kw: float | None
    irradiance_wm2: float | None
    device_status: str
    flags: list[str] = field(default_factory=list)
    calibrated_kw: float | None = None

    @property
    def valid(self) -> bool:
        return not self.flags


@dataclass
class BatchContent:
    """校准引擎输出：槽位明细 + 原因汇总 + 指标。"""
    slots: list[SlotResult]
    reasons: list[dict]  # [{"reason": ..., "count": n}] 按 count 降序
    metrics: dict
