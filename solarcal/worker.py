"""后台任务 worker：从 tasks 表取任务执行，失败按指数退避重试。

告警发布以 alerts.dedup_key 唯一约束为幂等屏障：
任务重试、worker 重启、重复投递都不会产生第二条告警。
"""
from __future__ import annotations

import logging
import threading
import time

log = logging.getLogger("solarcal.worker")

TASK_PUBLISH_ALERT = "publish_alert"


class Alerter:
    """告警出口。真实环境可接 webhook/消息队列；此处落日志，发布事实以 PG alerts 表为准。"""

    def send(self, alert_type: str, payload: dict) -> None:
        log.warning("ALERT %s %s", alert_type, payload)


class Worker:
    def __init__(self, store, alerter: Alerter | None = None, poll_secs: float = 0.5):
        self.store = store
        self.alerter = alerter or Alerter()
        self.poll_secs = poll_secs
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def execute(self, task: dict) -> None:
        """执行单个任务。发布动作幂等：dedup_key 已存在则视为成功跳过。"""
        if task["task_type"] == TASK_PUBLISH_ALERT:
            p = task["payload"]
            published = self.store.publish_alert(
                dedup_key=p["dedup_key"],
                batch_id=p["batch_id"],
                alert_type=p["alert_type"],
                payload=p["alert_payload"],
            )
            if published:
                self.alerter.send(p["alert_type"], p["alert_payload"])
            return
        raise ValueError(f"未知任务类型: {task['task_type']}")

    def run_once(self) -> bool:
        """处理一个任务；返回是否取到了任务。"""
        task = self.store.claim_task()
        if not task:
            return False
        try:
            self.execute(task)
        except Exception as exc:  # noqa: BLE001 —— 任何失败都进入重试
            delay = min(2 ** task["attempts"], 60)
            self.store.fail_task(
                task["task_id"], task["attempts"], task["max_attempts"], str(exc), delay
            )
            log.error("任务 %s 第 %d 次失败: %s", task["task_id"], task["attempts"], exc)
        else:
            self.store.finish_task(task["task_id"])
        return True

    def _loop(self):
        while not self._stop.is_set():
            if not self.run_once():
                self._stop.wait(self.poll_secs)

    def start(self):
        self._thread = threading.Thread(target=self._loop, name="solarcal-worker", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def drain(self, timeout_secs: float = 10.0) -> None:
        """阻塞直到没有 pending 任务（测试与优雅停机用）。"""
        deadline = time.monotonic() + timeout_secs
        while time.monotonic() < deadline:
            if not self.run_once():
                counts = self.store.task_counts()
                if not counts.get("pending"):
                    return
            time.sleep(0.01)
        raise TimeoutError("drain 超时：仍有 pending 任务")
