"""校准引擎：对齐采样、缺测与异常检测、加性偏差校准。

确定性要求：同一输入集合（无论到达顺序）必须产出同一批次内容，
因此所有归并按固定键排序，同槽多条观测取均值而非"后到覆盖"。
"""
from __future__ import annotations

import math
from collections import defaultdict

from .domain import (
    BLOCKING_DEVICE_STATES,
    DEVICE_CURTAILMENT,
    DEVICE_NORMAL,
    BatchContent,
    CalibrationParams,
    DeviceStatus,
    Forecast,
    Observation,
    SlotResult,
)
from .timegrid import SlotGrid

# 使槽位失效的标记（不参与偏差统计）；其余为提示性标记
INVALIDATING = {
    "missing_observation",
    "missing_forecast",
    "nonexistent_local_time",
    "irradiance_out_of_range",
    "observed_power_out_of_range",
    "forecast_out_of_range",
    "device_unavailable",
    "curtailment",
    "statistical_outlier",
    "conflicting_duplicates",
}

# 同槽重复观测取值差异超过该比例（相对容量）视为冲突——传感器漂移的典型信号
CONFLICT_REL = 0.05


def _group_observations(obs: list[Observation], grid: SlotGrid) -> dict[int, list[Observation]]:
    by_slot: dict[int, list[Observation]] = defaultdict(list)
    for o in sorted(obs, key=lambda o: o.obs_id):
        by_slot[grid.slot_start_epoch(o.ts_utc)].append(o)
    return by_slot


def _status_at(statuses: list[DeviceStatus], slot_epoch: int) -> str:
    """槽位时刻的设备状态：取不晚于槽位的最后一条；无记录视为 normal。"""
    best = None
    for s in statuses:
        epoch = s.ts_utc.timestamp()
        if epoch <= slot_epoch and (best is None or epoch > best.ts_utc.timestamp()):
            best = s
    return best.status if best else DEVICE_NORMAL


def build_batch_content(
    grid: SlotGrid,
    slot_epochs: list[int],
    forecasts: list[Forecast],
    observations: list[Observation],
    device_statuses: list[DeviceStatus],
    params: CalibrationParams,
    capacity_kw: float,
    model_version: str,
) -> BatchContent:
    fc_by_slot: dict[int, list[float]] = defaultdict(list)
    for f in sorted(forecasts, key=lambda f: (f.ts_utc.timestamp(), f.power_kw)):
        if f.model_version == model_version:
            fc_by_slot[grid.slot_start_epoch(f.ts_utc)].append(f.power_kw)

    obs_by_slot = _group_observations(observations, grid)
    outlier_threshold = max(params.outlier_abs_kw, params.outlier_rel * capacity_kw)

    slots: list[SlotResult] = []
    for epoch in slot_epochs:
        fc_vals = fc_by_slot.get(epoch, [])
        obs_vals = obs_by_slot.get(epoch, [])
        fc = sum(fc_vals) / len(fc_vals) if fc_vals else None

        flags: list[str] = []
        obs_power = None
        irr = None
        if obs_vals:
            powers = [o.power_kw for o in obs_vals if o.power_kw is not None]
            irrs = [o.irradiance_wm2 for o in obs_vals if o.irradiance_wm2 is not None]
            obs_power = sum(powers) / len(powers) if powers else None
            irr = sum(irrs) / len(irrs) if irrs else None
            if len(obs_vals) > 1:
                flags.append("duplicate_in_slot")
                if powers and (max(powers) - min(powers)) > CONFLICT_REL * capacity_kw:
                    flags.append("conflicting_duplicates")
            if any(o.ts_quality == "nonexistent_local_time" for o in obs_vals):
                flags.append("nonexistent_local_time")
            if any(o.ts_quality == "ambiguous_local_time" for o in obs_vals):
                flags.append("ambiguous_local_time")

        status = _status_at(device_statuses, epoch)

        if not obs_vals:
            flags.append("missing_observation")
        if fc is None:
            flags.append("missing_forecast")
        if irr is not None and not (0.0 <= irr <= params.irradiance_max_wm2):
            flags.append("irradiance_out_of_range")
        if obs_power is not None and not (0.0 <= obs_power <= capacity_kw * 1.05):
            flags.append("observed_power_out_of_range")
        if fc is not None and not (0.0 <= fc <= capacity_kw * 1.05):
            flags.append("forecast_out_of_range")
        if status in BLOCKING_DEVICE_STATES:
            flags.append("curtailment" if status == DEVICE_CURTAILMENT else "device_unavailable")
        if (
            obs_power is not None
            and fc is not None
            and abs(obs_power - fc) > outlier_threshold
        ):
            flags.append("statistical_outlier")

        slots.append(
            SlotResult(
                slot_epoch=epoch,
                forecast_kw=fc,
                observed_kw=obs_power,
                irradiance_wm2=irr,
                device_status=status,
                flags=flags,
            )
        )

    valid = [s for s in slots if not (set(s.flags) & INVALIDATING)
             and s.observed_kw is not None and s.forecast_kw is not None]
    batch_reasons: list[str] = []
    applied_bias = 0.0
    if len(valid) >= params.min_valid_slots:
        raw_bias = sum(s.observed_kw - s.forecast_kw for s in valid) / len(valid)
        cap = params.bias_cap_rel * capacity_kw
        applied_bias = max(-cap, min(cap, raw_bias))
        if abs(raw_bias) > cap:
            batch_reasons.append("bias_capped")
    else:
        batch_reasons.append("insufficient_valid_slots")

    for s in slots:
        if s.forecast_kw is not None:
            s.calibrated_kw = max(0.0, min(capacity_kw, s.forecast_kw + applied_bias))

    errors = [abs(s.observed_kw - s.forecast_kw) for s in valid]
    metrics = {
        "n_slots": len(slots),
        "n_valid": len(valid),
        "n_missing_observation": sum(1 for s in slots if "missing_observation" in s.flags),
        "n_out_of_range": sum(
            1 for s in slots if any(f.endswith("out_of_range") for f in s.flags)
        ),
        "n_device_blocked": sum(
            1 for s in slots if {"device_unavailable", "curtailment"} & set(s.flags)
        ),
        "n_statistical_outlier": sum(1 for s in slots if "statistical_outlier" in s.flags),
        "applied_bias_kw": round(applied_bias, 3),
        "mae_kw": round(sum(errors) / len(errors), 3) if errors else None,
        "rmse_kw": round(
            math.sqrt(sum((s.observed_kw - s.forecast_kw) ** 2 for s in valid) / len(valid)), 3
        )
        if valid
        else None,
    }

    counts: dict[str, int] = defaultdict(int)
    for s in slots:
        for f in s.flags:
            counts[f] += 1
    for r in batch_reasons:
        counts[r] += 1
    reasons = [
        {"reason": r, "count": c}
        for r, c in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    ]
    return BatchContent(slots=slots, reasons=reasons, metrics=metrics)
