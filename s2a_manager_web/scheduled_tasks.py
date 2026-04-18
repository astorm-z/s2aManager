from __future__ import annotations

import json
import sqlite3
import threading
import time
from datetime import datetime, timezone, tzinfo
from pathlib import Path
from typing import Any, Callable

from .errors import AppError

TASK_TYPE_BULK_UPDATE = "bulk_update_accounts"

TASK_STATUS_PENDING = "pending"
TASK_STATUS_RUNNING = "running"
TASK_STATUS_SUCCESS = "success"
TASK_STATUS_FAILED = "failed"
TASK_STATUS_CANCELLED = "cancelled"
TASK_STATUSES = (
    TASK_STATUS_PENDING,
    TASK_STATUS_RUNNING,
    TASK_STATUS_SUCCESS,
    TASK_STATUS_FAILED,
    TASK_STATUS_CANCELLED,
)

SCHEDULE_MODE_IMMEDIATE = "immediate"
SCHEDULE_MODE_DELAY = "delay_minutes"
SCHEDULE_MODE_RUN_AT = "run_at"
SCHEDULE_MODES = (
    SCHEDULE_MODE_IMMEDIATE,
    SCHEDULE_MODE_DELAY,
    SCHEDULE_MODE_RUN_AT,
)

STATUS_LABELS = {
    TASK_STATUS_PENDING: "待执行",
    TASK_STATUS_RUNNING: "执行中",
    TASK_STATUS_SUCCESS: "成功",
    TASK_STATUS_FAILED: "失败",
    TASK_STATUS_CANCELLED: "已取消",
}

SCHEDULE_MODE_LABELS = {
    SCHEDULE_MODE_DELAY: "延时分钟",
    SCHEDULE_MODE_RUN_AT: "指定时间",
}


def utc_now_ts() -> int:
    return int(time.time())


def parse_local_datetime_input(raw_value: str, tz: tzinfo, *, label: str = "指定执行时间") -> datetime:
    value = str(raw_value or "").strip()
    if not value:
        raise AppError(f"{label} 不能为空")
    normalized = value.replace(" ", "T")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise AppError(f"{label} 格式无效，请使用类似 2026-04-18T23:30 的时间") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=tz)
    else:
        parsed = parsed.astimezone(tz)
    return parsed


def format_timestamp_local(timestamp: Any, tz: tzinfo) -> str:
    if not isinstance(timestamp, int) or timestamp <= 0:
        return "-"
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).astimezone(tz).strftime("%Y-%m-%d %H:%M:%S")


def summarize_task_result(task: dict[str, Any]) -> str:
    status = str(task.get("status") or "")
    if status == TASK_STATUS_SUCCESS:
        result = task.get("result_json")
        if isinstance(result, dict):
            success = int(result.get("updated_success") or 0)
            failed = int(result.get("updated_failed") or 0)
            matched = int(result.get("matched") or 0)
            return f"命中 {matched}，成功 {success}，失败 {failed}"
        return "执行成功"
    if status == TASK_STATUS_FAILED:
        return str(task.get("error_message") or "执行失败")
    if status == TASK_STATUS_CANCELLED:
        return "任务已取消"
    return "-"


def summarize_task_updates(task: dict[str, Any]) -> str:
    updates = task.get("updates")
    if not isinstance(updates, dict) or not updates:
        return "-"
    keys = list(updates.keys())
    preview = "，".join(keys[:4])
    if len(keys) > 4:
        preview += f" 等 {len(keys)} 项"
    return preview


def build_task_view(task: dict[str, Any], tz: tzinfo) -> dict[str, Any]:
    result = dict(task)
    result["status_label"] = STATUS_LABELS.get(result["status"], result["status"])
    schedule_mode = str(result.get("schedule_mode") or "")
    result["schedule_mode_label"] = SCHEDULE_MODE_LABELS.get(schedule_mode, schedule_mode)
    result["run_at_display"] = format_timestamp_local(result.get("run_at_utc"), tz)
    result["created_at_display"] = format_timestamp_local(result.get("created_at_utc"), tz)
    result["started_at_display"] = format_timestamp_local(result.get("started_at_utc"), tz)
    result["finished_at_display"] = format_timestamp_local(result.get("finished_at_utc"), tz)
    result["summary_text"] = summarize_task_result(result)
    result["updates_summary"] = summarize_task_updates(result)
    updates = result.get("updates")
    result["updates_preview"] = json.dumps(updates, ensure_ascii=False, indent=2) if isinstance(updates, dict) else ""
    result["can_run_now"] = result["status"] == TASK_STATUS_PENDING
    result["can_delete"] = result["status"] != TASK_STATUS_RUNNING
    return result


class ScheduledTaskRepository:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._lock = threading.RLock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS scheduled_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    site_key TEXT NOT NULL,
                    task_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    schedule_mode TEXT NOT NULL,
                    delay_minutes INTEGER,
                    run_at_utc INTEGER NOT NULL,
                    run_at_timezone TEXT NOT NULL,
                    filters_json TEXT NOT NULL,
                    updates_json TEXT NOT NULL,
                    matched_count_preview INTEGER,
                    result_json TEXT,
                    error_message TEXT,
                    created_at_utc INTEGER NOT NULL,
                    started_at_utc INTEGER,
                    finished_at_utc INTEGER
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_scheduled_tasks_status_run_at ON scheduled_tasks(status, run_at_utc)"
            )

    @staticmethod
    def _row_to_task(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        result = dict(row)
        for field in ("filters_json", "updates_json", "result_json"):
            value = result.get(field)
            if isinstance(value, str) and value:
                result[field] = json.loads(value)
            else:
                result[field] = None
        result["filters"] = result.pop("filters_json")
        result["updates"] = result.pop("updates_json")
        result["result_json"] = result.get("result_json")
        return result

    def create_bulk_update_task(
        self,
        *,
        site_key: str,
        schedule_mode: str,
        delay_minutes: int | None,
        run_at_utc: int,
        run_at_timezone: str,
        filters: dict[str, Any],
        updates: dict[str, Any],
        matched_count_preview: int | None,
    ) -> dict[str, Any]:
        created_at_utc = utc_now_ts()
        with self._lock, self._connect() as connection:
            cursor = connection.execute(
                """
                INSERT INTO scheduled_tasks (
                    site_key, task_type, status, schedule_mode, delay_minutes, run_at_utc, run_at_timezone,
                    filters_json, updates_json, matched_count_preview, created_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    site_key,
                    TASK_TYPE_BULK_UPDATE,
                    TASK_STATUS_PENDING,
                    schedule_mode,
                    delay_minutes,
                    run_at_utc,
                    run_at_timezone,
                    json.dumps(filters, ensure_ascii=False),
                    json.dumps(updates, ensure_ascii=False),
                    matched_count_preview,
                    created_at_utc,
                ),
            )
            task_id = int(cursor.lastrowid)
        task = self.get_task(task_id)
        if task is None:
            raise AppError("创建延时任务失败")
        return task

    def get_task(self, task_id: int) -> dict[str, Any] | None:
        with self._lock, self._connect() as connection:
            row = connection.execute("SELECT * FROM scheduled_tasks WHERE id = ?", (task_id,)).fetchone()
        return self._row_to_task(row)

    def list_tasks(
        self,
        *,
        site_key: str | None = None,
        status: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if site_key:
            clauses.append("site_key = ?")
            params.append(site_key)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"""
            SELECT * FROM scheduled_tasks
            {where_sql}
            ORDER BY
                CASE status
                    WHEN '{TASK_STATUS_RUNNING}' THEN 0
                    WHEN '{TASK_STATUS_PENDING}' THEN 1
                    WHEN '{TASK_STATUS_FAILED}' THEN 2
                    WHEN '{TASK_STATUS_SUCCESS}' THEN 3
                    ELSE 4
                END,
                run_at_utc ASC,
                id DESC
            LIMIT ?
        """
        params.append(limit)
        with self._lock, self._connect() as connection:
            rows = connection.execute(sql, tuple(params)).fetchall()
        return [task for task in (self._row_to_task(row) for row in rows) if task is not None]

    def reset_running_tasks(self) -> None:
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE scheduled_tasks
                SET status = ?, started_at_utc = NULL, error_message = NULL
                WHERE status = ?
                """,
                (TASK_STATUS_PENDING, TASK_STATUS_RUNNING),
            )

    def claim_due_tasks(self, *, now_utc: int, limit: int = 5) -> list[dict[str, Any]]:
        with self._lock, self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                """
                SELECT id FROM scheduled_tasks
                WHERE status = ? AND run_at_utc <= ?
                ORDER BY run_at_utc ASC, id ASC
                LIMIT ?
                """,
                (TASK_STATUS_PENDING, now_utc, limit),
            ).fetchall()
            task_ids = [int(row["id"]) for row in rows]
            if not task_ids:
                connection.commit()
                return []
            started_at_utc = utc_now_ts()
            placeholders = ",".join("?" for _ in task_ids)
            connection.execute(
                f"""
                UPDATE scheduled_tasks
                SET status = ?, started_at_utc = ?, finished_at_utc = NULL, error_message = NULL
                WHERE id IN ({placeholders}) AND status = ?
                """,
                (TASK_STATUS_RUNNING, started_at_utc, *task_ids, TASK_STATUS_PENDING),
            )
            connection.commit()
        tasks: list[dict[str, Any]] = []
        for task_id in task_ids:
            task = self.get_task(task_id)
            if task is not None and task.get("status") == TASK_STATUS_RUNNING:
                tasks.append(task)
        return tasks

    def mark_success(self, task_id: int, result: dict[str, Any]) -> None:
        finished_at_utc = utc_now_ts()
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE scheduled_tasks
                SET status = ?, result_json = ?, error_message = NULL, finished_at_utc = ?
                WHERE id = ?
                """,
                (TASK_STATUS_SUCCESS, json.dumps(result, ensure_ascii=False), finished_at_utc, task_id),
            )

    def mark_failed(self, task_id: int, error_message: str) -> None:
        finished_at_utc = utc_now_ts()
        with self._lock, self._connect() as connection:
            connection.execute(
                """
                UPDATE scheduled_tasks
                SET status = ?, error_message = ?, finished_at_utc = ?
                WHERE id = ?
                """,
                (TASK_STATUS_FAILED, error_message, finished_at_utc, task_id),
            )

    def run_now(self, task_id: int) -> dict[str, Any]:
        with self._lock, self._connect() as connection:
            current = connection.execute("SELECT status FROM scheduled_tasks WHERE id = ?", (task_id,)).fetchone()
            if current is None:
                raise AppError("延时任务不存在")
            if str(current["status"]) != TASK_STATUS_PENDING:
                raise AppError("只有待执行任务才能立即执行")
            connection.execute(
                """
                UPDATE scheduled_tasks
                SET run_at_utc = ?, error_message = NULL
                WHERE id = ?
                """,
                (utc_now_ts(), task_id),
            )
        task = self.get_task(task_id)
        if task is None:
            raise AppError("延时任务不存在")
        return task

    def cancel_task(self, task_id: int) -> dict[str, Any]:
        with self._lock, self._connect() as connection:
            current = connection.execute("SELECT status FROM scheduled_tasks WHERE id = ?", (task_id,)).fetchone()
            if current is None:
                raise AppError("延时任务不存在")
            if str(current["status"]) == TASK_STATUS_RUNNING:
                raise AppError("任务执行中，暂不支持删除")
            connection.execute(
                """
                UPDATE scheduled_tasks
                SET status = ?, finished_at_utc = COALESCE(finished_at_utc, ?)
                WHERE id = ?
                """,
                (TASK_STATUS_CANCELLED, utc_now_ts(), task_id),
            )
        task = self.get_task(task_id)
        if task is None:
            raise AppError("延时任务不存在")
        return task


class ScheduledTaskRunner:
    def __init__(
        self,
        repository: ScheduledTaskRepository,
        executor: Callable[[dict[str, Any]], dict[str, Any]],
        *,
        poll_interval_seconds: float = 2.0,
    ) -> None:
        self.repository = repository
        self.executor = executor
        self.poll_interval_seconds = max(poll_interval_seconds, 0.5)
        self._stop_event = threading.Event()
        self._wake_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self.repository.reset_running_tasks()
        self._stop_event.clear()
        self._wake_event.clear()
        self._thread = threading.Thread(target=self._run_loop, name="scheduled-task-runner", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._wake_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)

    def wake(self) -> None:
        self._wake_event.set()

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            due_tasks = self.repository.claim_due_tasks(now_utc=utc_now_ts())
            for task in due_tasks:
                if self._stop_event.is_set():
                    return
                task_id = int(task["id"])
                try:
                    result = self.executor(task)
                except Exception as exc:
                    self.repository.mark_failed(task_id, str(exc))
                else:
                    self.repository.mark_success(task_id, result)
            self._wake_event.wait(self.poll_interval_seconds)
            self._wake_event.clear()
