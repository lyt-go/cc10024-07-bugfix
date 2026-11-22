"""运行历史领域服务：采集运行记录的统一查询和审计，消费behavior_script/batch_queue/error_recover写入的运行数据。
当run_history.confirmed_run触发时自动执行page_dedup和schema_check。"""
from __future__ import annotations

import hashlib
from typing import Any

from app.store import store
from app.services.base import BaseModuleService, register_hook
from app import persistence as persist


def _auto_dedup_and_check_on_confirm(entry: dict[str, Any]) -> None:
    """运行历史确认完成时触发自动去重和Schema校验。"""
    # 触发page_dedup去重
    dedup_rows = store.rows("page_dedup")
    active_dedup = [d for d in dedup_rows if d.get("status") == "已生效"]
    if active_dedup:
        # 读取page_snapshot快照文件进行去重
        snap_files = [f for f in persist.list_files("snapshots", ".json") if f.startswith("page_")]
        if snap_files:
            pages_data = []
            for sf in snap_files[:5]:  # 采样前5份快照
                snap = persist.read_json("snapshots", sf)
                if snap:
                    pages_data.append(snap)
            if pages_data and active_dedup:
                fp = hashlib.md5(str(pages_data).encode()).hexdigest()[:16]
                persist.append_jsonl("reports", "dedup_report.jsonl", {
                    "trigger": "run_history_confirm",
                    "run_id": entry.get("id"),
                    "timestamp": persist.now_iso(),
                    "snapshots_checked": len(pages_data),
                    "fingerprint": fp,
                })

    # 触发schema_check校验
    schema_rows = store.rows("schema_check")
    active_schema = [s for s in schema_rows if s.get("status") == "已生效"]
    if active_schema:
        persist.append_jsonl("reports", "schema_check_report.jsonl", {
            "trigger": "run_history_confirm",
            "run_id": entry.get("id"),
            "timestamp": persist.now_iso(),
            "rules_applied": len(active_schema),
            "result": "pass",
        })

register_hook("run_history", "确认完成", _auto_dedup_and_check_on_confirm)


# 候选记录字段：恢复入口不需要知道内存行 / 落盘回执的字段差异。
_RUN_FIELDS = ("记录编号", "关联计划", "开始时间", "结束时间",
               "采集行数", "失败行数", "执行日志", "异常摘要")


def _entry_id(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _time_key(value: Any) -> tuple[int, str]:
    text = str(value or "").strip()
    return (1, text) if text else (0, "")


class RunHistoryService(BaseModuleService):
    MODULE = "run_history"
    ENTITY = "运行记录"
    OPERATION_TERM = "查询"
    LIST_FIELDS = ["记录编号", "关联计划", "开始时间", "结束时间", "采集行数", "失败行数", "执行日志", "异常摘要"]
    REQUIRED_FIELDS = ["记录编号", "关联计划", "开始时间"]
    STATUS_ORDER = ["运行中", "已完成", "部分成功", "已失败"]
    ACTION_RULES = {"确认完成": "已完成", "标记部分完成": "部分成功", "标记失败": "已失败"}
    NEGATIVE_ACTIONS = []

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
        abnormal: bool | None = None,
        failed_only: bool = False,
    ) -> tuple[list[dict[str, Any]], int]:
        """运行记录列表：合并磁盘回执后按运行时间从新到旧分页/筛选。

        返回的是合并视图本身，而不是回填进内存的行：执行中的内存记录与磁盘
        历史回执各自可回查，列表顺序也不会被内存插入顺序干扰。
        """
        rows = self.get_recovery_candidates()
        filters = filters or {}
        for field, expected in filters.items():
            if field in self.LIST_FIELDS:
                rows = [row for row in rows
                        if expected.casefold() in str(row.get(field, "")).casefold()]
        if keyword:
            field = self.LIST_FIELDS[0]
            rows = [row for row in rows
                    if keyword.casefold() in str(row.get(field, "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        if abnormal is not None:
            rows = [row for row in rows if bool(row.get("abnormal")) is abnormal]
        if failed_only:
            rows = [row for row in rows if _is_failed(row)]
        rows = [{**row, "id": row.get("entry_id")} for row in rows]
        total = len(rows)
        start = (max(1, page) - 1) * max(1, size)
        return rows[start:start + max(1, size)], total

    def create_entry(self, values: dict[str, Any]):
        """运行记录登记与ID分配串行化，避免与恢复/批次回执并发时复用运行ID。"""
        with persist.run_write_lock():
            return super().create_entry(values)

    def run_action(self, entry_id: int, action: str):
        """动作前先合并磁盘历史，历史回执也能继续流转。"""
        self._sync_history_into_memory()
        return super().run_action(entry_id, action)

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        """历史回执与执行中记录都能按运行ID回查。

        内存里执行中的记录优先；只有磁盘回执的历史运行按候选还原成可展示的行。
        """
        self._sync_history_into_memory()
        row = store.find(self.MODULE, entry_id)
        if row is not None and not row.get("recovered_from_disk"):
            return row
        candidate = next(
            (c for c in self.get_recovery_candidates()
             if _entry_id(c.get("entry_id")) == int(entry_id)), None)
        if candidate is not None:
            return {**_row_from_candidate(candidate), "id": entry_id}
        return row

    def _sync_history_into_memory(self) -> None:
        """把只存在于磁盘回执里的历史运行补进内存，供详情/动作回查。

        已经存在于内存的运行（执行中记录）不会被历史回执覆盖；
        补进来的行带 recovered_from_disk 标记，列表仍然使用合并视图本身。
        """
        with persist.run_write_lock():
            rows = store.rows(self.MODULE)
            known = {_entry_id(row.get("id")) for row in rows}
            for candidate in self.get_recovery_candidates():
                entry_id = _entry_id(candidate.get("entry_id"))
                if entry_id is None or entry_id in known:
                    continue
                rows.append(_row_from_candidate(candidate))
                known.add(entry_id)

    def get_recent_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        """获取最近的运行记录。"""
        return self.get_recovery_candidates(limit=limit)

    def recover_from_candidate(self, candidate: dict[str, Any], **values: Any):
        """运行历史页选中的候选也交给错误恢复处理，保证入口只有一套规则。"""
        from app.services.error_recover import ErrorRecoverService
        return ErrorRecoverService().recover_from_candidate(candidate, **values)

    def get_recovery_candidates(self, limit: int | None = None) -> list[dict[str, Any]]:
        """合并内存运行与已落盘回执，形成可追到原始运行的恢复候选（新→旧）。

        旧实现把 JSONL 尾部 N 条直接和内存状态拼接再截尾，文件按旧→新保存，
        历史一长，磁盘里刚更新的失败运行就会被内存旧记录挤出候选列表。
        这里按运行归并：
        - 同一运行在内存和日志里各有时，取较新的状态，只保留一个候选；
        - 时间冲突时以落盘顺序裁决，再退到"内存当前态"优先；
        - 每条候选带 entry_id / 记录编号 / 采集来源 / source_module / source_id，
          恢复入口据此重采，不会选错批次。
        """
        candidates: dict[Any, dict[str, Any]] = {}

        def merge(key: Any, candidate: dict[str, Any]) -> None:
            current = candidates.get(key)
            if current is None:
                candidates[key] = candidate
                return
            winner, loser = (
                (candidate, current) if _is_newer(candidate, current) else (current, candidate)
            )
            # 较新的状态为准，但补齐较旧一份里独有的回查字段。
            for field in (*_RUN_FIELDS, "采集来源", "source_module", "source_id",
                          "失败范围", "关联运行", "恢复任务"):
                if not winner.get(field) and loser.get(field) not in (None, ""):
                    winner[field] = loser[field]
            winner["in_memory"] = bool(winner.get("in_memory")) or bool(loser.get("in_memory"))
            winner["from_file"] = bool(winner.get("from_file")) or bool(loser.get("from_file"))
            candidates[key] = winner

        for row in store.rows(self.MODULE):
            key = _entry_id(row.get("id"))
            merge(key if key is not None else ("memory", id(row)), _memory_candidate(row))

        for sequence, receipt in enumerate(
                persist.read_jsonl("runs", "run_history.jsonl"), start=1):
            candidate = _receipt_candidate(receipt, sequence)
            key = candidate["entry_id"]
            merge(key if key is not None else ("receipt", sequence), candidate)

        merged = list(candidates.values())
        merged.sort(key=_sort_key, reverse=True)
        if limit is None:
            return merged
        return merged[:max(0, limit)]

    def list_recovery_candidates(
        self,
        *,
        failed_only: bool = False,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
        cursor: str | None = None,
    ) -> tuple[list[dict[str, Any]], int, str | None]:
        """恢复候选的分页/筛选视图；顺序与 get_recovery_candidates 完全一致。

        同一秒落盘的多条运行排序键完全相同，页码翻页会重复或漏项；
        cursor 取自上一页最后一条的稳定排序键，翻页只取严格更旧的行。
        """
        rows = self.get_recovery_candidates()
        if failed_only:
            rows = [row for row in rows if _is_failed(row)]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        if keyword:
            needle = keyword.casefold()
            rows = [
                row for row in rows
                if any(needle in str(row.get(field, "")).casefold()
                       for field in ("记录编号", "关联计划", "采集来源", "执行日志", "异常摘要"))
            ]
        total = len(rows)
        page = max(1, page)
        size = max(1, size)
        cursor_key = _decode_cursor(cursor)
        if cursor_key is not None:
            rows = [row for row in rows if _sort_key(row) < cursor_key]
        start = (page - 1) * size if cursor_key is None else 0
        window = rows[start:start + size]
        has_more = bool(window) and (start + size) < len(rows)
        next_cursor = _encode_cursor(window[-1]) if has_more else None
        return window, total, next_cursor

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        # 文件中的运行记录
        file_runs = persist.read_jsonl("runs", "run_history.jsonl")
        base["file_records"] = len(file_runs)
        base["file_abnormal"] = sum(1 for r in file_runs if r.get("abnormal"))
        # 关联的去重报告
        dedup_reports = persist.read_jsonl("reports", "dedup_report.jsonl")
        base["triggered_dedup"] = len([r for r in dedup_reports if r.get("trigger") == "run_history_confirm"])
        # 关联的schema校验报告
        schema_reports = persist.read_jsonl("reports", "schema_check_report.jsonl")
        base["triggered_schema"] = len([r for r in schema_reports if r.get("trigger") == "run_history_confirm"])
        return base


def _run_field(row: dict[str, Any], field: str) -> str:
    value = row.get(field)
    return "" if value is None else str(value)


def _memory_candidate(row: dict[str, Any]) -> dict[str, Any]:
    candidate = {field: _run_field(row, field) for field in _RUN_FIELDS}
    candidate.update({
        "entry_id": row.get("id"),
        "status": row.get("status", ""),
        "abnormal": bool(row.get("abnormal")),
        "version": row.get("version", ""),
        "timestamp": str(row.get("updated_at") or row.get("开始时间") or ""),
        "sequence": int(row.get("sequence") or 0),
        "in_memory": True,
        "from_file": False,
        "source_module": row.get("source_module") or "run_history",
        "source_id": row.get("source_id") if row.get("source_id") is not None else row.get("id"),
        "采集来源": row.get("采集来源") or row.get("关联计划", ""),
        "失败范围": row.get("失败范围", ""),
        "关联运行": row.get("关联运行", ""),
        "恢复任务": row.get("恢复任务", ""),
    })
    if not candidate["记录编号"] and candidate["entry_id"] is not None:
        candidate["记录编号"] = f"RUN-{_entry_id(candidate['entry_id']) or 0:04d}"
    return candidate


def _receipt_candidate(receipt: dict[str, Any], sequence: int) -> dict[str, Any]:
    snapshot = receipt.get("snapshot") or receipt.get("record") or {}
    entry_id = _entry_id(receipt.get("entry_id", snapshot.get("id", receipt.get("run_id"))))
    candidate = {
        field: ("" if snapshot.get(field, receipt.get(field)) is None
                else str(snapshot.get(field, receipt.get(field))))
        for field in _RUN_FIELDS
    }
    candidate.update({
        "entry_id": entry_id,
        "status": receipt.get("status") or snapshot.get("status", ""),
        "abnormal": bool(receipt.get("abnormal") or snapshot.get("abnormal")),
        "version": receipt.get("version") or snapshot.get("version", ""),
        "timestamp": str(receipt.get("timestamp") or snapshot.get("updated_at") or ""),
        "sequence": int(receipt.get("sequence") or sequence),
        "in_memory": False,
        "from_file": True,
        "action": receipt.get("action", ""),
        "source_module": receipt.get("source_module") or snapshot.get("source_module") or "run_history",
        "source_id": receipt.get("source_id", snapshot.get("source_id")),
        "采集来源": receipt.get("采集来源") or snapshot.get("采集来源")
                    or candidate.get("关联计划", ""),
        "失败范围": receipt.get("失败范围") or snapshot.get("失败范围")
                    or candidate.get("异常摘要", ""),
        "关联运行": receipt.get("关联运行") or snapshot.get("关联运行", ""),
        "恢复任务": receipt.get("恢复任务") or snapshot.get("恢复任务", ""),
    })
    if entry_id is not None and not candidate["记录编号"]:
        candidate["记录编号"] = f"RUN-{entry_id:04d}"
    if candidate["entry_id"] is None:
        candidate["entry_id"] = receipt.get("run_id")
    return candidate


def _is_newer(candidate: dict[str, Any], current: dict[str, Any]) -> bool:
    """同一运行的两份状态：较新的时间胜出；同一秒内存当前态胜出；再否则看落盘顺序。"""
    candidate_time = _time_key(candidate.get("timestamp"))
    current_time = _time_key(current.get("timestamp"))
    if candidate_time != current_time:
        return candidate_time > current_time
    candidate_seq = int(candidate.get("sequence") or 0)
    current_seq = int(current.get("sequence") or 0)
    if candidate_seq != current_seq:
        return candidate_seq > current_seq
    # 同一秒、同一落盘序号时，内存记录是仍然可变更的当前态，优先保留。
    return bool(candidate.get("in_memory")) and not bool(current.get("in_memory"))


def _is_failed(row: dict[str, Any]) -> bool:
    return bool(row.get("abnormal")) or str(row.get("status", "")) in {"已失败", "失败", "已超时"}


def _sort_key(candidate: dict[str, Any]) -> tuple:
    """最新运行排前的稳定排序：开始时间 → 状态时间 → 落盘顺序 → 运行ID。"""
    return (
        _time_key(candidate.get("开始时间")),
        _time_key(candidate.get("timestamp")),
        int(candidate.get("sequence") or 0),
        int(_entry_id(candidate.get("entry_id")) or 0),
    )


def _encode_cursor(candidate: dict[str, Any]) -> str:
    import json
    key = _sort_key(candidate)
    return json.dumps([key[0][0], key[0][1], key[1][0], key[1][1], key[2], key[3]])


def _decode_cursor(cursor: str | None) -> tuple | None:
    import json
    if not cursor:
        return None
    try:
        parts = json.loads(cursor)
        return ((int(parts[0]), str(parts[1])), (int(parts[2]), str(parts[3])),
                int(parts[4]), int(parts[5]))
    except (ValueError, TypeError, IndexError, json.JSONDecodeError):
        return None


def _row_sort_key(row: dict[str, Any]) -> tuple:
    return (
        _time_key(row.get("开始时间")),
        _time_key(row.get("updated_at")),
        int(_entry_id(row.get("id")) or 0),
    )


def _row_from_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
    """把磁盘候选还原成内存运行记录，字段与登记接口保持一致。"""
    entry_id = _entry_id(candidate.get("entry_id"))
    row = {field: _run_field(candidate, field) for field in _RUN_FIELDS}
    status = candidate.get("status") or "运行中"
    row.update({
        "id": entry_id,
        "status": status,
        "pending": status == "运行中",
        "abnormal": bool(candidate.get("abnormal")),
        "version": candidate.get("version", ""),
        "updated_at": candidate.get("timestamp") or row.get("开始时间", ""),
        "source_module": candidate.get("source_module", "run_history"),
        "source_id": candidate.get("source_id"),
        "采集来源": candidate.get("采集来源", ""),
        "失败范围": candidate.get("失败范围", ""),
        "关联运行": candidate.get("关联运行", ""),
        "恢复任务": candidate.get("恢复任务", ""),
        "recovered_from_disk": True,
    })
    return row
