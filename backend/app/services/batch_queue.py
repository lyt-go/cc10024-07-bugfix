"""批次队列领域服务：采集任务批次管理，消费script_schedule调度命令，写入run_history运行记录。"""
from __future__ import annotations

from typing import Any

from app.store import store
from app.services.base import BaseModuleService, register_hook
from app import persistence as persist


def _complete_batch_to_history(entry: dict[str, Any]) -> None:
    """批次确认完成时写入run_history。"""
    with persist.run_write_lock():
        _append_batch_run(entry)


def _append_batch_run(entry: dict[str, Any]) -> None:
    run_rows = store.rows("run_history")
    new_id = persist.next_run_entry_id(run_rows)
    now = persist.now_iso()
    record = {
        "id": new_id,
        "记录编号": f"RUN-BATCH-{new_id:04d}",
        "关联计划": entry.get("关联计划", ""),
        "开始时间": now,
        "结束时间": now,
        "采集行数": str(entry.get("任务数量", 0)),
        "失败行数": str(entry.get("失败数", 0)),
        "执行日志": f"批次{entry.get('批次编号')}执行完成",
        "异常摘要": "",
        "status": "已完成",
        "pending": False,
        "abnormal": False,
        "version": "",
        "updated_at": now,
        "采集来源": entry.get("关联计划", ""),
        "source_module": "batch_queue",
        "source_id": entry.get("id"),
        "关联运行": new_id,
        "失败范围": entry.get("失败数", "0"),
    }
    run_rows.append(record)
    persist.append_jsonl("runs", "run_history.jsonl", {
        "module": "run_history", "entity": "运行记录", "entry_id": new_id,
        "status": "已完成", "version": "", "action": "auto_create_from_batch",
        "timestamp": now, "abnormal": False, "sequence": persist.next_sequence(),
        "source_module": "batch_queue", "source_id": entry.get("id"),
        "采集来源": record["采集来源"],
        "snapshot": {key: record.get(key, "") for key in (
            "记录编号", "关联计划", "开始时间", "结束时间", "采集行数",
            "失败行数", "执行日志", "异常摘要")} | {
            "采集来源": record["采集来源"],
            "source_module": "batch_queue", "source_id": entry.get("id"),
        },
    })

register_hook("batch_queue", "确认完成", _complete_batch_to_history)


class BatchQueueService(BaseModuleService):
    MODULE = "batch_queue"
    ENTITY = "采集批次"
    OPERATION_TERM = "调度"
    LIST_FIELDS = ["批次编号", "关联计划", "任务数量", "完成数", "失败数", "批次状态", "截止时间", "优先级"]
    REQUIRED_FIELDS = ["批次编号", "关联计划", "任务数量"]
    STATUS_ORDER = ["排队中", "执行中", "已完成", "已超时"]
    ACTION_RULES = {"发起执行": "执行中", "确认完成": "已完成", "标记超时": "已超时"}
    NEGATIVE_ACTIONS = []

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        batches = store.rows(self.MODULE)
        total_tasks = sum(int(r.get("任务数量", 0)) for r in batches)
        base["total_tasks"] = total_tasks
        # 关联的run_history
        runs = persist.read_jsonl("runs", "run_history.jsonl")
        batch_runs = [r for r in runs if "batch" in str(r.get("action", ""))]
        base["batch_runs"] = len(batch_runs)
        # 关联的error_recover
        errors = store.rows("error_recover")
        batch_plans = {r.get("关联计划") for r in batches}
        base["recovery_linked"] = sum(1 for e in errors if e.get("关联计划") in batch_plans)
        return base