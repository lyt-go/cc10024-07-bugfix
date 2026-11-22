"""错误恢复领域服务：采集失败后的重试与恢复，消费batch_queue失败信息，写入run_history。"""
from __future__ import annotations

from typing import Any

from app.store import store
from app.services.base import BaseModuleService, register_hook
from app.services.run_history import RunHistoryService
from app import persistence as persist


def _record_recovery_to_history(entry: dict[str, Any]) -> None:
    """恢复任务确认恢复后写入run_history，并把本次可确认的失败范围关联到新运行。"""
    with persist.run_write_lock():
        _append_recovery_run(entry)


def _append_recovery_run(entry: dict[str, Any]) -> None:
    run_rows = store.rows("run_history")
    new_id = persist.next_run_entry_id(run_rows)
    # 恢复要沿用候选记录里的实际采集来源，而不是重新猜一个批次。
    source_module = entry.get("采集来源模块") or entry.get("source_module") or "error_recover"
    source_id = entry.get("采集来源ID", entry.get("source_id"))
    if source_id is None:
        source_id = entry.get("来源运行ID", entry.get("来源运行"))
    linked_run = entry.get("来源运行ID", entry.get("来源运行"))
    failed_scope = entry.get("失败范围") or entry.get("失败步骤") or ""
    now = persist.now_iso()
    record = {
        "id": new_id,
        "记录编号": f"RUN-RECOVER-{new_id:04d}",
        "关联计划": entry.get("关联计划", ""),
        "开始时间": now,
        "结束时间": "",
        "采集行数": "恢复重采",
        "失败行数": entry.get("已重试次数", "0"),
        "执行日志": f"从error_recover任务{entry.get('任务编号')}恢复",
        "异常摘要": entry.get("错误类型", ""),
        "status": "运行中",
        "pending": True,
        "abnormal": False,
        "version": "",
        "updated_at": now,
        "采集来源": entry.get("采集来源") or entry.get("关联计划", ""),
        "source_module": source_module,
        "source_id": source_id,
        "关联运行": linked_run if linked_run is not None else "",
        "parent_run_id": linked_run if linked_run is not None else "",
        "recovery_of": linked_run if linked_run is not None else "",
        "失败范围": failed_scope,
        "恢复任务": entry.get("任务编号", ""),
    }
    run_rows.append(record)
    snapshot = {
        key: record.get(key, "") for key in RunHistoryService.LIST_FIELDS
    } | {
        "采集来源": record["采集来源"], "source_module": source_module,
        "source_id": source_id, "关联运行": record["关联运行"],
        "失败范围": failed_scope, "恢复任务": record["恢复任务"],
    }
    persist.append_jsonl("runs", "run_history.jsonl", {
        "module": "run_history", "entity": "运行记录", "entry_id": new_id,
        "status": "运行中", "version": "", "action": "auto_create_from_recovery",
        "timestamp": now, "abnormal": False, "sequence": persist.next_sequence(),
        "source_module": source_module, "source_id": source_id,
        "采集来源": record["采集来源"],
        "关联运行": record["关联运行"], "失败范围": failed_scope,
        "recovery_task_id": entry.get("id"),
        "snapshot": snapshot,
    })

register_hook("error_recover", "确认恢复", _record_recovery_to_history)


class ErrorRecoverService(BaseModuleService):
    MODULE = "error_recover"
    ENTITY = "恢复任务"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["任务编号", "关联计划", "失败步骤", "错误类型", "重试边界", "回滚策略", "已重试次数", "恢复状态"]
    REQUIRED_FIELDS = ["任务编号", "关联计划", "失败步骤"]
    STATUS_ORDER = ["待处理", "重试中", "已恢复", "已放弃"]
    ACTION_RULES = {"发起重试": "重试中", "确认恢复": "已恢复", "标记放弃": "已放弃"}
    NEGATIVE_ACTIONS = []

    def recover_from_candidate(self, candidate: dict[str, Any], **values: Any) -> tuple[dict[str, Any] | None, str]:
        """按恢复候选记录发起重采：来源取自候选本身，不重新猜批次。"""
        source_module = candidate.get("source_module") or "run_history"
        source_id = candidate.get("source_id")
        if source_id is None:
            source_id = candidate.get("entry_id")
        payload = {
            "任务编号": values.get("任务编号") or f"ERRO-RECOVER-{source_id or 'NEW'}",
            "关联计划": values.get("关联计划") or candidate.get("关联计划", ""),
            "失败步骤": values.get("失败步骤") or candidate.get("失败范围")
            or candidate.get("失败步骤") or candidate.get("异常摘要")
            or candidate.get("记录编号") or "运行记录",
            "错误类型": values.get("错误类型") or candidate.get("异常摘要", ""),
            "重试边界": values.get("重试边界", ""),
            "回滚策略": values.get("回滚策略", ""),
            "已重试次数": values.get("已重试次数", "0"),
            "恢复状态": values.get("恢复状态", ""),
        }
        entry, missing = self.create_entry(payload)
        if entry is None:
            return None, f"缺少必填字段：{'、'.join(missing)}"
        entry["采集来源"] = candidate.get("采集来源") or candidate.get("关联计划", "")
        entry["采集来源模块"] = source_module
        entry["采集来源ID"] = source_id
        entry["来源运行ID"] = candidate.get("entry_id")
        entry["来源运行"] = candidate.get("entry_id")
        entry["来源运行记录"] = candidate.get("记录编号", "")
        entry["失败范围"] = candidate.get("失败范围") or candidate.get("失败步骤") \
            or candidate.get("异常摘要") or ""
        # 默认走“确认恢复”，让钩子把本次重采登记成一条新的运行记录，
        # 并把候选里的失败范围和来源运行一起挂上去。
        action = values.get("action") or "确认恢复"
        if action in self.ACTION_RULES:
            self.run_action(entry["id"], action)
        return entry, f"{self.ENTITY}已按{candidate.get('记录编号') or source_id}发起重采"

    def get_recovery_candidates(self) -> list[dict[str, Any]]:
        """错误恢复视角的候选：直接复用运行历史的合并结果，保证选中的是同一运行。"""
        return RunHistoryService().get_recovery_candidates()

    def list_recovery_candidates(self, **kwargs: Any):
        return RunHistoryService().list_recovery_candidates(**kwargs)

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        tasks = store.rows(self.MODULE)
        # 关联的batch_queue统计数据
        batches = store.rows("batch_queue")
        plan_ids = {r.get("关联计划") for r in tasks}
        base["linked_batches"] = sum(1 for b in batches if b.get("关联计划") in plan_ids)
        # 恢复关联的run_history
        runs = persist.read_jsonl("runs", "run_history.jsonl")
        recovery_runs = [r for r in runs if "recover" in str(r.get("action", ""))]
        base["recovery_runs"] = len(recovery_runs)
        base["retry_total"] = sum(
            int(str(r.get("已重试次数") or "0")) for r in tasks if str(r.get("已重试次数") or "").isdigit())
        return base
