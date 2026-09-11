"""站点时区采样网格。

所有持久化与计算都在 UTC 物理时间轴上进行；站点时区只用于两件事：
1. 把不带偏移量的本地时间戳解释成 UTC（处理夏令时缺口与重叠）；
2. 锚定采样槽位，使槽位在站点本地总是落在整刻上。

槽位 = 固定锚点（站点本地某午夜对应的 UTC 时刻）+ k * interval，
因此跨夏令时切换时槽位边界在本地依然整刻，且物理间隔严格相等，
乱序重放同一批观测必然得到同一分组结果。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

UTC = timezone.utc

# 时间质量标记（进入批次 reasons 的输入）
TS_OK = "ok"
TS_AMBIGUOUS = "ambiguous_local_time"   # 秋季重叠：本地时间出现两次，取第一次
TS_NONEXISTENT = "nonexistent_local_time"  # 春季缺口：本地时间不存在


@dataclass(frozen=True)
class ParsedTimestamp:
    utc: datetime | None
    quality: str  # TS_OK / TS_AMBIGUOUS / TS_NONEXISTENT


def _parse_iso(raw: str) -> datetime:
    s = raw.strip()
    if s.endswith(("Z", "z")):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    return dt


def interpret_timestamp(raw: str, station_tz: ZoneInfo) -> ParsedTimestamp:
    """把 ISO8601 时间戳解释为 UTC。

    - 带偏移量：直接换算（调用方对偏移负责）。
    - 不带偏移量：按站点本地时间解释。夏令时重叠时取第一次出现并标记
      ambiguous；落入春季缺口时不存在，返回 utc=None 并标记 nonexistent，
      由上层决定拒绝并记录原因（不静默挪动数据）。
    """
    dt = _parse_iso(raw)
    if dt.tzinfo is not None:
        return ParsedTimestamp(dt.astimezone(UTC), TS_OK)

    naive = dt
    utc_first = naive.replace(tzinfo=station_tz, fold=0).astimezone(UTC)
    utc_second = naive.replace(tzinfo=station_tz, fold=1).astimezone(UTC)
    if utc_first == utc_second:
        return ParsedTimestamp(utc_first, TS_OK)
    # fold 结果不同：可能是秋季重叠（歧义）或春季缺口（不存在）。
    # 用回译检验区分——歧义时间能原样译回，不存在的时间译回会漂移。
    if utc_first.astimezone(station_tz).replace(tzinfo=None) == naive:
        return ParsedTimestamp(utc_first, TS_AMBIGUOUS)
    return ParsedTimestamp(None, TS_NONEXISTENT)


class SlotGrid:
    """站点采样网格：把 UTC 时间轴切成等间隔槽位，槽界对齐站点本地整刻。"""

    def __init__(self, station_tz: ZoneInfo, interval_minutes: int = 15):
        if interval_minutes <= 0 or 24 * 60 % interval_minutes != 0:
            raise ValueError("interval_minutes 必须能整除 1440")
        self.tz = station_tz
        self.interval = timedelta(minutes=interval_minutes)
        # 锚点：站点本地 2000-01-01 00:00（现代时区 DST 切换均为整小时，
        # 因此该锚点保证任意日期下槽界都是本地整刻）
        anchor_local = datetime(2000, 1, 1, tzinfo=station_tz)
        self.anchor_epoch = anchor_local.astimezone(UTC).timestamp()
        self.interval_secs = int(self.interval.total_seconds())

    def slot_start_epoch(self, ts_utc: datetime) -> int:
        """某 UTC 时刻所属槽位的起始 epoch 秒。"""
        epoch = ts_utc.timestamp()
        k = int((epoch - self.anchor_epoch) // self.interval_secs)
        return int(self.anchor_epoch + k * self.interval_secs)

    def slots_between(self, start_utc: datetime, end_utc: datetime) -> list[int]:
        """[start, end) 覆盖的全部槽位起始 epoch 秒（升序、确定）。"""
        if end_utc <= start_utc:
            raise ValueError("window end 必须大于 start")
        first = self.slot_start_epoch(start_utc)
        slots = []
        s = first
        end_epoch = end_utc.timestamp()
        while s < end_epoch:
            slots.append(s)
            s += self.interval_secs
        return slots

    def slot_to_local(self, slot_epoch: int) -> datetime:
        return datetime.fromtimestamp(slot_epoch, UTC).astimezone(self.tz)


def epoch_to_utc(epoch: float) -> datetime:
    return datetime.fromtimestamp(epoch, UTC)


def utc_to_iso(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")
