"""Redis 短期窗口：最近观测的滚动缓冲。

窗口只是索引+缓存：观测本体同时落 PostgreSQL（可追溯），
Redis 丢失时从 PG 回源并回填。ZSET 以 obs_id 为 member，
乱序重放/重复上报天然幂等。
"""
from __future__ import annotations

import json

WINDOW_TTL_SECS = 48 * 3600  # 短期窗口：48 小时


class RedisWindow:
    def __init__(self, client, ttl_secs: int = WINDOW_TTL_SECS):
        self.r = client
        self.ttl = ttl_secs

    def _key(self, station_id: str) -> str:
        return f"win:obs:{station_id}"

    def add_observation(self, station_id: str, obs: dict) -> None:
        """obs 需含 obs_id 与 ts_epoch。重复 obs_id 覆盖，不膨胀。"""
        key = self._key(station_id)
        member = json.dumps(obs, sort_keys=True)
        pipe = self.r.pipeline()
        pipe.zadd(key, {member: obs["ts_epoch"]})
        pipe.expire(key, self.ttl)
        pipe.execute()

    def observations_between(self, station_id: str, start_epoch: float, end_epoch: float) -> list[dict]:
        """窗口内 [start, end) 的观测，按分数（采样时刻）升序。"""
        key = self._key(station_id)
        members = self.r.zrangebyscore(key, start_epoch, f"({end_epoch}")
        out = [json.loads(m) for m in members]
        out.sort(key=lambda o: (o["ts_epoch"], o["obs_id"]))
        return out

    def remove_observations(self, station_id: str, obs_ids: list[str]) -> None:
        """按 obs_id 从窗口剔除（如重放后需要以 PG 为准重建时）。"""
        key = self._key(station_id)
        for member in self.r.zrange(key, 0, -1):
            if json.loads(member)["obs_id"] in obs_ids:
                self.r.zrem(key, member)

    def window_size(self, station_id: str) -> int:
        return self.r.zcard(self._key(station_id))
