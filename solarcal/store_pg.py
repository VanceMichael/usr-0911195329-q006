"""PostgreSQL 可追溯存储。

所有结算相关结果只追加、不改写：
- 批次一经 settle 即不可变，迟到观测只能产生 revision 批次（revision_of 串成链）；
- 模型版本 + 校准参数（params_hash）一经用于结算即冻结，
  同一模型版本不得以不同参数再次结算；
- 告警以 dedup_key 唯一约束保证重试不重复发布。
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from .domain import KIND_ORIGINAL, KIND_REVISION, STATUS_OPEN, STATUS_SETTLED

SCHEMA = """
CREATE TABLE IF NOT EXISTS stations (
  station_id TEXT PRIMARY KEY,
  timezone TEXT NOT NULL,
  capacity_kw DOUBLE PRECISION NOT NULL,
  interval_minutes INT NOT NULL DEFAULT 15,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS model_versions (
  model_version TEXT NOT NULL,
  params_hash TEXT NOT NULL,
  params JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','frozen')),
  frozen_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (model_version, params_hash)
);
CREATE TABLE IF NOT EXISTS observations (
  station_id TEXT NOT NULL,
  obs_id TEXT NOT NULL,
  ts_utc TIMESTAMPTZ NOT NULL,
  irradiance_wm2 DOUBLE PRECISION,
  power_kw DOUBLE PRECISION,
  source TEXT NOT NULL DEFAULT 'scada',
  ts_quality TEXT NOT NULL DEFAULT 'ok',
  received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (station_id, obs_id)
);
CREATE TABLE IF NOT EXISTS forecasts (
  station_id TEXT NOT NULL,
  model_version TEXT NOT NULL,
  ts_utc TIMESTAMPTZ NOT NULL,
  power_kw DOUBLE PRECISION NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (station_id, model_version, ts_utc)
);
CREATE TABLE IF NOT EXISTS device_statuses (
  station_id TEXT NOT NULL,
  ts_utc TIMESTAMPTZ NOT NULL,
  status TEXT NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (station_id, ts_utc)
);
CREATE TABLE IF NOT EXISTS calibration_batches (
  batch_id UUID PRIMARY KEY,
  station_id TEXT NOT NULL REFERENCES stations(station_id),
  window_start_utc TIMESTAMPTZ NOT NULL,
  window_end_utc TIMESTAMPTZ NOT NULL,
  model_version TEXT NOT NULL,
  params_hash TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('original','revision')),
  revision_of UUID REFERENCES calibration_batches(batch_id),
  status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','settled')),
  reasons JSONB NOT NULL,
  metrics JSONB NOT NULL,
  slots JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  settled_at TIMESTAMPTZ
);
CREATE UNIQUE INDEX IF NOT EXISTS one_original_per_window
  ON calibration_batches (station_id, window_start_utc, window_end_utc, model_version)
  WHERE kind = 'original';
CREATE UNIQUE INDEX IF NOT EXISTS revision_chain_linear
  ON calibration_batches (revision_of) WHERE revision_of IS NOT NULL;
CREATE TABLE IF NOT EXISTS alerts (
  alert_id UUID PRIMARY KEY,
  dedup_key TEXT NOT NULL UNIQUE,
  batch_id UUID NOT NULL,
  alert_type TEXT NOT NULL,
  payload JSONB NOT NULL,
  published_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS tasks (
  task_id UUID PRIMARY KEY,
  task_type TEXT NOT NULL,
  payload JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','running','done','failed')),
  attempts INT NOT NULL DEFAULT 0,
  max_attempts INT NOT NULL DEFAULT 5,
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


class FrozenError(Exception):
    """违反冻结语义（已结算的模型版本/参数被变更，或批次已结算）。"""


class ConflictError(Exception):
    """状态冲突（如批次不存在、已结算批次被重复结算）。"""


class PgStore:
    def __init__(self, dsn: str):
        self.pool = ConnectionPool(dsn, min_size=1, max_size=8, kwargs={"row_factory": dict_row})
        with self.pool.connection() as conn:
            conn.execute(SCHEMA)

    def close(self):
        self.pool.close()

    # ---------- 站点与模型版本 ----------

    def upsert_station(self, station_id: str, tz: str, capacity_kw: float, interval_minutes: int):
        with self.pool.connection() as conn:
            conn.execute(
                """INSERT INTO stations (station_id, timezone, capacity_kw, interval_minutes)
                   VALUES (%s,%s,%s,%s)
                   ON CONFLICT (station_id) DO UPDATE
                   SET timezone=EXCLUDED.timezone, capacity_kw=EXCLUDED.capacity_kw,
                       interval_minutes=EXCLUDED.interval_minutes""",
                (station_id, tz, capacity_kw, interval_minutes),
            )

    def get_station(self, station_id: str) -> dict | None:
        with self.pool.connection() as conn:
            return conn.execute(
                "SELECT * FROM stations WHERE station_id=%s", (station_id,)
            ).fetchone()

    def register_model_version(self, model_version: str, params_hash: str, params: dict):
        """注册模型版本+参数组合。已冻结的组合不允许改参数内容。"""
        with self.pool.connection() as conn:
            existing = conn.execute(
                "SELECT * FROM model_versions WHERE model_version=%s AND params_hash=%s",
                (model_version, params_hash),
            ).fetchone()
            if existing:
                if existing["params"] != params:
                    raise FrozenError(
                        f"模型版本 {model_version} 的参数组合 {params_hash} 已注册，内容不可变更"
                    )
                return existing
            conn.execute(
                "INSERT INTO model_versions (model_version, params_hash, params) VALUES (%s,%s,%s)",
                (model_version, params_hash, Jsonb(params)),
            )
            return conn.execute(
                "SELECT * FROM model_versions WHERE model_version=%s AND params_hash=%s",
                (model_version, params_hash),
            ).fetchone()

    def get_model_version(self, model_version: str, params_hash: str) -> dict | None:
        with self.pool.connection() as conn:
            return conn.execute(
                "SELECT * FROM model_versions WHERE model_version=%s AND params_hash=%s",
                (model_version, params_hash),
            ).fetchone()

    # ---------- 观测 / 预测 / 设备状态（幂等写入） ----------

    def insert_observation(self, station_id: str, obs: dict) -> bool:
        """返回 True 表示新插入；False 表示重放重复（幂等忽略）。"""
        with self.pool.connection() as conn:
            row = conn.execute(
                """INSERT INTO observations (station_id, obs_id, ts_utc, irradiance_wm2, power_kw, source, ts_quality)
                   VALUES (%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (station_id, obs_id) DO NOTHING
                   RETURNING obs_id""",
                (
                    station_id,
                    obs["obs_id"],
                    obs["ts_utc"],
                    obs.get("irradiance_wm2"),
                    obs.get("power_kw"),
                    obs.get("source", "scada"),
                    obs.get("ts_quality", "ok"),
                ),
            ).fetchone()
            return row is not None

    def insert_forecast(self, station_id: str, model_version: str, ts_utc, power_kw: float):
        with self.pool.connection() as conn:
            conn.execute(
                """INSERT INTO forecasts (station_id, model_version, ts_utc, power_kw)
                   VALUES (%s,%s,%s,%s)
                   ON CONFLICT (station_id, model_version, ts_utc)
                   DO UPDATE SET power_kw=EXCLUDED.power_kw""",
                (station_id, model_version, ts_utc, power_kw),
            )

    def insert_device_status(self, station_id: str, ts_utc, status: str):
        with self.pool.connection() as conn:
            conn.execute(
                """INSERT INTO device_statuses (station_id, ts_utc, status)
                   VALUES (%s,%s,%s)
                   ON CONFLICT (station_id, ts_utc) DO UPDATE SET status=EXCLUDED.status""",
                (station_id, ts_utc, status),
            )

    def observations_between(self, station_id: str, start, end) -> list[dict]:
        with self.pool.connection() as conn:
            return conn.execute(
                """SELECT * FROM observations
                   WHERE station_id=%s AND ts_utc >= %s AND ts_utc < %s
                   ORDER BY obs_id""",
                (station_id, start, end),
            ).fetchall()

    def forecasts_between(self, station_id: str, model_version: str, start, end) -> list[dict]:
        with self.pool.connection() as conn:
            return conn.execute(
                """SELECT * FROM forecasts
                   WHERE station_id=%s AND model_version=%s AND ts_utc >= %s AND ts_utc < %s
                   ORDER BY ts_utc""",
                (station_id, model_version, start, end),
            ).fetchall()

    def device_statuses_until(self, station_id: str, end) -> list[dict]:
        with self.pool.connection() as conn:
            return conn.execute(
                "SELECT * FROM device_statuses WHERE station_id=%s AND ts_utc <= %s ORDER BY ts_utc",
                (station_id, end),
            ).fetchall()

    # ---------- 批次：冻结语义的核心 ----------

    def _batch_chain(self, conn, station_id, start, end, model_version) -> list[dict]:
        return conn.execute(
            """SELECT * FROM calibration_batches
               WHERE station_id=%s AND window_start_utc=%s AND window_end_utc=%s AND model_version=%s
               ORDER BY created_at""",
            (station_id, start, end, model_version),
        ).fetchall()

    def save_batch(
        self,
        station_id: str,
        window_start,
        window_end,
        model_version: str,
        params_hash: str,
        content: dict,
    ) -> dict:
        """保存校准批次。

        - 同窗口无批次 → original；
        - 链尾为 open → 就地更新内容（冻结前允许重算，乱序重放到齐后结果收敛）；
        - 链尾已 settled → 新建 revision 批次挂在链尾（迟到观测的唯一出路）。
        """
        with self.pool.connection() as conn:
            with conn.transaction():
                chain = self._batch_chain(conn, station_id, window_start, window_end, model_version)
                slots = Jsonb(content["slots"])
                reasons = Jsonb(content["reasons"])
                metrics = Jsonb(content["metrics"])

                if not chain:
                    batch_id = uuid.uuid4()
                    conn.execute(
                        """INSERT INTO calibration_batches
                           (batch_id, station_id, window_start_utc, window_end_utc,
                            model_version, params_hash, kind, status, reasons, metrics, slots)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (
                            batch_id, station_id, window_start, window_end,
                            model_version, params_hash, KIND_ORIGINAL, STATUS_OPEN,
                            reasons, metrics, slots,
                        ),
                    )
                elif chain[-1]["status"] == STATUS_OPEN:
                    batch_id = chain[-1]["batch_id"]
                    conn.execute(
                        """UPDATE calibration_batches
                           SET params_hash=%s, reasons=%s, metrics=%s, slots=%s
                           WHERE batch_id=%s AND status='open'""",
                        (params_hash, reasons, metrics, slots, batch_id),
                    )
                else:
                    batch_id = uuid.uuid4()
                    conn.execute(
                        """INSERT INTO calibration_batches
                           (batch_id, station_id, window_start_utc, window_end_utc,
                            model_version, params_hash, kind, revision_of, status,
                            reasons, metrics, slots)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (
                            batch_id, station_id, window_start, window_end,
                            model_version, params_hash, KIND_REVISION, chain[-1]["batch_id"],
                            STATUS_OPEN, reasons, metrics, slots,
                        ),
                    )
                return conn.execute(
                    "SELECT * FROM calibration_batches WHERE batch_id=%s", (batch_id,)
                ).fetchone()

    def settle_batch(self, batch_id: str) -> dict:
        """结算批次：冻结对应 (模型版本, 参数) 组合，批次变为不可变。"""
        with self.pool.connection() as conn:
            with conn.transaction():
                batch = conn.execute(
                    "SELECT * FROM calibration_batches WHERE batch_id=%s FOR UPDATE",
                    (batch_id,),
                ).fetchone()
                if not batch:
                    raise ConflictError(f"批次不存在: {batch_id}")
                if batch["status"] == STATUS_SETTLED:
                    raise ConflictError(f"批次 {batch_id} 已结算，不可重复结算")

                mv = batch["model_version"]
                ph = batch["params_hash"]
                frozen_other = conn.execute(
                    """SELECT params_hash FROM model_versions
                       WHERE model_version=%s AND status='frozen' AND params_hash<>%s""",
                    (mv, ph),
                ).fetchone()
                if frozen_other:
                    raise FrozenError(
                        f"模型版本 {mv} 已用参数 {frozen_other['params_hash']} 结算并冻结，"
                        f"不得以参数 {ph} 再次结算"
                    )
                conn.execute(
                    """UPDATE model_versions SET status='frozen', frozen_at=now()
                       WHERE model_version=%s AND params_hash=%s""",
                    (mv, ph),
                )
                conn.execute(
                    "UPDATE calibration_batches SET status='settled', settled_at=now() WHERE batch_id=%s",
                    (batch_id,),
                )
                return conn.execute(
                    "SELECT * FROM calibration_batches WHERE batch_id=%s", (batch_id,)
                ).fetchone()

    def get_batch(self, batch_id: str) -> dict | None:
        with self.pool.connection() as conn:
            return conn.execute(
                "SELECT * FROM calibration_batches WHERE batch_id=%s", (batch_id,)
            ).fetchone()

    def list_batches(self, station_id: str) -> list[dict]:
        with self.pool.connection() as conn:
            return conn.execute(
                """SELECT batch_id, station_id, window_start_utc, window_end_utc,
                          model_version, params_hash, kind, revision_of, status,
                          reasons, metrics, created_at, settled_at
                   FROM calibration_batches WHERE station_id=%s
                   ORDER BY window_start_utc, created_at""",
                (station_id,),
            ).fetchall()

    def settled_batch_covering(self, station_id: str, ts_utc) -> dict | None:
        """覆盖某时刻的最新已结算批次（用于判定迟到观测）。"""
        with self.pool.connection() as conn:
            return conn.execute(
                """SELECT batch_id, kind, status FROM calibration_batches
                   WHERE station_id=%s AND status='settled'
                         AND window_start_utc <= %s AND window_end_utc > %s
                   ORDER BY created_at DESC LIMIT 1""",
                (station_id, ts_utc, ts_utc),
            ).fetchone()

    # ---------- 告警（幂等发布）与后台任务 ----------

    def publish_alert(self, dedup_key: str, batch_id: str, alert_type: str, payload: dict) -> bool:
        """返回 True 表示本次真正发布；False 表示已存在（重试/重放，跳过）。"""
        with self.pool.connection() as conn:
            row = conn.execute(
                """INSERT INTO alerts (alert_id, dedup_key, batch_id, alert_type, payload)
                   VALUES (%s,%s,%s,%s,%s)
                   ON CONFLICT (dedup_key) DO NOTHING
                   RETURNING alert_id""",
                (uuid.uuid4(), dedup_key, batch_id, alert_type, Jsonb(payload)),
            ).fetchone()
            return row is not None

    def list_alerts(self, station_id: str | None = None) -> list[dict]:
        with self.pool.connection() as conn:
            if station_id:
                return conn.execute(
                    """SELECT a.* FROM alerts a
                       JOIN calibration_batches b ON b.batch_id = a.batch_id
                       WHERE b.station_id=%s ORDER BY a.published_at""",
                    (station_id,),
                ).fetchall()
            return conn.execute("SELECT * FROM alerts ORDER BY published_at").fetchall()

    def enqueue_task(self, task_type: str, payload: dict, max_attempts: int = 5) -> str:
        task_id = str(uuid.uuid4())
        with self.pool.connection() as conn:
            conn.execute(
                "INSERT INTO tasks (task_id, task_type, payload, max_attempts) VALUES (%s,%s,%s,%s)",
                (task_id, task_type, Jsonb(payload), max_attempts),
            )
        return task_id

    def claim_task(self) -> dict | None:
        """取一个到期任务；并发下由行锁保证同一任务只被一个 worker 拿走。"""
        with self.pool.connection() as conn:
            with conn.transaction():
                row = conn.execute(
                    """SELECT task_id FROM tasks
                       WHERE status='pending' AND next_attempt_at <= now()
                       ORDER BY created_at LIMIT 1
                       FOR UPDATE SKIP LOCKED"""
                ).fetchone()
                if not row:
                    return None
                conn.execute(
                    "UPDATE tasks SET status='running', attempts=attempts+1 WHERE task_id=%s",
                    (row["task_id"],),
                )
                return conn.execute(
                    "SELECT * FROM tasks WHERE task_id=%s", (row["task_id"],)
                ).fetchone()

    def finish_task(self, task_id: str):
        with self.pool.connection() as conn:
            conn.execute("UPDATE tasks SET status='done' WHERE task_id=%s", (task_id,))

    def fail_task(self, task_id: str, attempts: int, max_attempts: int, error: str, retry_delay_secs: float):
        with self.pool.connection() as conn:
            if attempts >= max_attempts:
                conn.execute(
                    "UPDATE tasks SET status='failed', last_error=%s WHERE task_id=%s",
                    (error, task_id),
                )
            else:
                conn.execute(
                    """UPDATE tasks SET status='pending', last_error=%s,
                       next_attempt_at = now() + make_interval(secs => %s)
                       WHERE task_id=%s""",
                    (error, retry_delay_secs, task_id),
                )

    def task_counts(self) -> dict:
        with self.pool.connection() as conn:
            rows = conn.execute(
                "SELECT status, count(*) AS n FROM tasks GROUP BY status"
            ).fetchall()
            return {r["status"]: r["n"] for r in rows}
