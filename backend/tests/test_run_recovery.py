"""运行历史合并查询与错误恢复候选的回归测试。

覆盖：磁盘新失败运行不被内存旧记录挤出、同运行去重、时间冲突时最新运行与
另一条较新的失败采集竞争末位、并发恢复只占一个名额、同秒稳定翻页、
历史回执与执行中记录各自回查、恢复按候选实际采集来源发起并关联失败范围。
"""
from __future__ import annotations

import os
import sys
import threading

backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from app import persistence as persist
from app.seed import SEED_ROWS
from app.store import store
from app.services.error_recover import ErrorRecoverService
from app.services.run_history import RunHistoryService


def _reset(clean_disk: bool = True) -> None:
    """模拟进程重启：内存回到种子数据（运行历史为空），可保留磁盘回执。"""
    store._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}
    store.rows("run_history").clear()
    if clean_disk:
        for sub in ["runs", "snapshots", "reports", "exports"]:
            path = persist.data_path(sub, "")
            if os.path.isdir(path):
                for name in os.listdir(path):
                    os.remove(os.path.join(path, name))


class _Clock:
    def __init__(self, value: str) -> None:
        self.value = value

    def __call__(self) -> str:
        return self.value


def _with_clock(value: str):
    original = persist.now_iso
    persist.now_iso = _Clock(value)
    return original


def _restore_clock(original) -> None:
    persist.now_iso = original


def _new_run(clock_value: str, code: str, plan: str, *, status: str = "运行中") -> int:
    original = _with_clock(clock_value)
    try:
        svc = RunHistoryService()
        entry, missing = svc.create_entry({
            "记录编号": code, "关联计划": plan, "开始时间": clock_value,
        })
        assert entry is not None, missing
        if status == "已失败":
            svc.run_action(entry["id"], "标记失败")
        elif status != "运行中":
            raise AssertionError(status)
        return int(entry["id"])
    finally:
        _restore_clock(original)


def test_disk_failure_not_lost_after_memory_records():
    """磁盘里更新的失败运行不会因为内存里有旧记录而被截掉。"""
    _reset()
    # 内存只留一条旧运行
    memory_id = _new_run("2026-01-01T00:00:00", "RUN-OLD-MEMORY", "PLAN-OLD")
    # 重新模拟重启：内存回到种子，磁盘保留刚才的回执
    _reset(clean_disk=False)

    failed_ids = []
    for index in range(25):
        rid = _new_run(
            f"2026-02-{index + 1:02d}T10:00:00",
            f"RUN-DISK-{index:02d}", f"PLAN-DISK-{index:02d}",
            status="已失败",
        )
        failed_ids.append(rid)

    recent = RunHistoryService().get_recent_runs(limit=5)
    codes = [item.get("记录编号") for item in recent]
    assert f"RUN-DISK-24" in codes, codes
    assert "RUN-OLD-MEMORY" not in codes, codes
    # 最新的失败运行排在最前，并保留来源信息
    assert recent[0]["entry_id"] == failed_ids[-1]
    assert recent[0].get("from_file") is True

    _reset()


def test_old_memory_record_kept_while_new_runs_persist():
    """一条旧记录留在内存，接连提交几个有落盘事件的新运行：旧记录不顶掉新失败运行。"""
    _reset()
    old_id = _new_run("2026-01-15T00:00:00", "RUN-MEMORY-OLD", "PLAN-MEMORY-OLD")
    assert old_id is not None

    new_ids = []
    for index in range(4):
        new_ids.append(_new_run(
            f"2026-01-2{index}T12:00:00",
            f"RUN-FRESH-{index}", f"PLAN-FRESH-{index}",
            status="已失败",
        ))

    recent = RunHistoryService().get_recent_runs(limit=3)
    entry_ids = [item["entry_id"] for item in recent]
    assert entry_ids == [new_ids[3], new_ids[2], new_ids[1]], entry_ids
    assert old_id not in entry_ids
    # 每个运行只有一个候选，不会因为内存 + 落盘各有一份而重复占名额
    all_candidates = RunHistoryService().get_recovery_candidates()
    assert len({c["entry_id"] for c in all_candidates}) == len(all_candidates)
    assert old_id in {c["entry_id"] for c in all_candidates}
    _reset()


def test_same_run_memory_and_log_merged_to_one_slot():
    """同一运行在内存和日志里时间冲突时只占一个候选，且取较新的状态。"""
    _reset()
    svc = RunHistoryService()
    original = _with_clock("2026-03-01T09:00:00")
    try:
        entry, _ = svc.create_entry({
            "记录编号": "RUN-CONFLICT", "关联计划": "PLAN-CONFLICT",
            "开始时间": "2026-03-01T09:00:00",
        })
        run_id = int(entry["id"])
        # 内存里推进到失败状态（写入磁盘回执）
        svc.run_action(run_id, "标记失败")
    finally:
        _restore_clock(original)

    # 磁盘中再追加一条更早的同运行回执，模拟日志与内存时间冲突
    persist.append_jsonl("runs", "run_history.jsonl", {
        "module": "run_history", "entity": "运行记录", "entry_id": run_id,
        "status": "运行中", "version": "", "action": "create",
        "timestamp": "2026-03-01T08:00:00", "abnormal": False,
        "snapshot": {"记录编号": "RUN-CONFLICT", "关联计划": "PLAN-CONFLICT",
                     "开始时间": "2026-03-01T08:00:00"},
    })

    candidates = RunHistoryService().get_recovery_candidates()
    matches = [c for c in candidates if c.get("entry_id") == run_id]
    assert len(matches) == 1, matches
    assert matches[0]["status"] == "已失败"
    assert matches[0]["记录编号"] == "RUN-CONFLICT"

    # 列表中同样只出现一次
    rows, total = RunHistoryService().list_entries(filters={}, keyword=None, status=None,
                                                   page=1, size=200)
    assert sum(1 for row in rows if int(row["id"]) == run_id) == 1
    _reset()


def test_latest_run_competes_with_newer_failed_capture_for_last_slot():
    """同秒冲突：最新运行候选与另一条较新的失败采集竞争最后一个名额，排序稳定。"""
    _reset()
    # 内存里一条正在运行的最新记录
    newest_id = _new_run("2026-04-01T12:00:00", "RUN-NEWEST", "PLAN-NEWEST")
    # 磁盘上一条时间略晚、已经失败的采集（通过回执直接落盘）
    persist.append_jsonl("runs", "run_history.jsonl", {
        "module": "run_history", "entity": "运行记录", "entry_id": 9001,
        "status": "已失败", "version": "", "action": "mark_failed",
        "timestamp": "2026-04-01T12:00:00", "abnormal": True, "sequence": 1,
        "source_module": "batch_queue", "source_id": 7, "采集来源": "PLAN-FAILED",
        "snapshot": {"记录编号": "RUN-FAILED", "关联计划": "PLAN-FAILED",
                     "开始时间": "2026-04-01T11:59:59"},
    })
    # 一条更早的历史运行
    persist.append_jsonl("runs", "run_history.jsonl", {
        "module": "run_history", "entity": "运行记录", "entry_id": 9002,
        "status": "已完成", "version": "", "action": "create",
        "timestamp": "2026-03-01T00:00:00", "abnormal": False, "sequence": 2,
        "snapshot": {"记录编号": "RUN-EARLIER", "关联计划": "PLAN-EARLIER",
                     "开始时间": "2026-03-01T00:00:00"},
    })

    top = RunHistoryService().get_recovery_candidates(limit=2)
    ids = [item["entry_id"] for item in top]
    assert ids == [newest_id, 9001], ids

    # 换一个进程视角（内存重置）后，同一批候选应保持一致
    _reset(clean_disk=False)
    again = RunHistoryService().get_recovery_candidates(limit=2)
    assert [item["entry_id"] for item in again] == ids
    _reset()


def test_concurrent_recovery_and_query_single_slot_and_stable_pages():
    """重采确认与历史查询并发：同一运行只占一个名额，同秒也能稳定翻到下一页。"""
    _reset()
    # 三条运行，开始时间全部相同
    ids = [
        _new_run("2026-05-01T08:00:00", f"RUN-SAME-{i}", f"PLAN-SAME-{i}")
        for i in range(3)
    ]

    errors: list[BaseException] = []

    def query_loop() -> None:
        try:
            for _ in range(20):
                seen: list[int] = []
                cursor = None
                while True:
                    items, _, cursor = RunHistoryService().list_recovery_candidates(
                        page=1, size=2, cursor=cursor)
                    seen.extend(int(item["entry_id"]) for item in items)
                    assert len(seen) == len(set(seen)), seen
                    if not cursor:
                        break
                # 并发写入的运行可以新出现，但原有三条不能丢也不能重复
                assert set(ids).issubset(set(seen)), (ids, seen)
        except BaseException as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    def recover_loop() -> None:
        try:
            original = _with_clock("2026-05-01T08:00:00")
            try:
                for index in range(5):
                    svc = RunHistoryService()
                    entry, _ = svc.create_entry({
                        "记录编号": f"RUN-CONC-{index}",
                        "关联计划": f"PLAN-CONC-{index}",
                        "开始时间": "2026-05-01T08:00:00",
                    })
                    svc.run_action(int(entry["id"]), "标记失败")
            finally:
                _restore_clock(original)
        except BaseException as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=query_loop) for _ in range(3)]
    threads.append(threading.Thread(target=recover_loop))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not errors, errors

    candidates = RunHistoryService().get_recovery_candidates()
    entry_ids = [c["entry_id"] for c in candidates]
    assert len(entry_ids) == len(set(entry_ids))
    _reset()


def test_history_and_executing_records_queryable():
    """历史回执与执行中记录都能按ID回查。"""
    _reset()
    executing_id = _new_run("2026-06-01T08:00:00", "RUN-EXEC", "PLAN-EXEC")
    _reset(clean_disk=False)  # 重启后内存里只剩种子数据
    svc = RunHistoryService()
    row = svc.get_entry(executing_id)
    assert row is not None and row.get("记录编号") == "RUN-EXEC"
    assert row.get("status") == "运行中"
    assert row.get("recovered_from_disk") is True
    # 动作仍能作用到回查出来的运行
    acted, _ = svc.run_action(executing_id, "标记失败")
    assert acted is not None and acted["status"] == "已失败"
    _reset()


def test_recovery_uses_candidate_source_and_links_failed_scope():
    """错误恢复按候选的实际采集来源发起，并把可确认失败范围关联到本次运行。"""
    _reset()
    persist.append_jsonl("runs", "run_history.jsonl", {
        "module": "run_history", "entity": "运行记录", "entry_id": 4001,
        "status": "已失败", "version": "", "action": "mark_failed",
        "timestamp": "2026-07-01T10:00:00", "abnormal": True, "sequence": 1,
        "source_module": "batch_queue", "source_id": 42,
        "采集来源": "BATC-0042",
        "snapshot": {"记录编号": "RUN-BROKEN", "关联计划": "PLAN-BROKEN",
                     "开始时间": "2026-07-01T10:00:00",
                     "异常摘要": "selector timeout", "采集来源": "BATC-0042",
                     "source_module": "batch_queue", "source_id": 42},
    })
    svc = ErrorRecoverService()
    candidate, total = svc.list_recovery_candidates(failed_only=True)[:2]
    selected = next(item for item in candidate if item["entry_id"] == 4001)

    entry, message = svc.recover_from_candidate(selected, action="确认恢复")
    assert entry is not None, message
    assert entry["采集来源模块"] == "batch_queue"
    assert entry["采集来源ID"] == 42
    assert entry["来源运行ID"] == 4001
    assert entry["采集来源"] == "BATC-0042"

    runs = persist.read_jsonl("runs", "run_history.jsonl")
    recovery = [r for r in runs if r.get("action") == "auto_create_from_recovery"]
    assert len(recovery) == 1
    assert recovery[0]["source_module"] == "batch_queue"
    assert recovery[0]["source_id"] == 42
    assert recovery[0]["关联运行"] == 4001
    assert recovery[0]["失败范围"] == "selector timeout"
    assert recovery[0]["snapshot"]["失败范围"] == "selector timeout"
    _reset()


def test_restart_paging_filtering_and_selected_run_match():
    """重启前后分页、筛选失败记录并选中后交给错误恢复，匹配选中的那次运行。"""
    _reset()
    _new_run("2026-08-01T08:00:00", "RUN-OLD-MEMORY", "PLAN-OLD")
    _reset(clean_disk=False)  # 旧记录留在磁盘，模拟只在内存里登记的旧记录先消失

    for index in range(6):
        _new_run(
            f"2026-09-{index + 1:02d}T09:00:00",
            f"RUN-NEW-{index}", f"PLAN-NEW-{index}",
            status="已失败" if index % 2 == 1 else "运行中",
        )

    # 重启前：分页 + 筛选失败
    before_page1, total_before, cursor = RunHistoryService().list_recovery_candidates(
        failed_only=True, page=1, size=2)
    before_page2, _, _ = RunHistoryService().list_recovery_candidates(
        failed_only=True, page=1, size=2, cursor=cursor)
    before = before_page1 + before_page2
    assert total_before == 3, total_before
    assert [item["记录编号"] for item in before] == [
        "RUN-NEW-5", "RUN-NEW-3", "RUN-NEW-1"]
    selected_before = before[1]

    # 重启：内存回到种子，磁盘保留全部回执
    _reset(clean_disk=False)
    after_page1, total_after, cursor = RunHistoryService().list_recovery_candidates(
        failed_only=True, page=1, size=2)
    after_page2, _, _ = RunHistoryService().list_recovery_candidates(
        failed_only=True, page=1, size=2, cursor=cursor)
    after = after_page1 + after_page2
    assert total_after == 3, total_after
    assert [item["entry_id"] for item in after] == [item["entry_id"] for item in before]
    selected_after = after[1]
    assert selected_after["entry_id"] == selected_before["entry_id"]

    # 交给错误恢复：必须匹配选中的那次运行（RUN-NEW-3）
    entry, message = ErrorRecoverService().recover_from_candidate(
        selected_after, action="确认恢复")
    assert entry is not None, message
    assert entry["来源运行ID"] == selected_after["entry_id"]
    assert entry["来源运行记录"] == "RUN-NEW-3"
    assert entry["关联计划"] == "PLAN-NEW-3"

    run = next(r for r in store.rows("run_history")
               if r.get("恢复任务") == entry["任务编号"])
    assert run["recovery_of"] == selected_after["entry_id"]
    assert run["关联运行"] == selected_after["entry_id"]
    _reset()


if __name__ == "__main__":
    test_disk_failure_not_lost_after_memory_records()
    print("PASS: test_disk_failure_not_lost_after_memory_records")
    test_old_memory_record_kept_while_new_runs_persist()
    print("PASS: test_old_memory_record_kept_while_new_runs_persist")
    test_same_run_memory_and_log_merged_to_one_slot()
    print("PASS: test_same_run_memory_and_log_merged_to_one_slot")
    test_latest_run_competes_with_newer_failed_capture_for_last_slot()
    print("PASS: test_latest_run_competes_with_newer_failed_capture_for_last_slot")
    test_concurrent_recovery_and_query_single_slot_and_stable_pages()
    print("PASS: test_concurrent_recovery_and_query_single_slot_and_stable_pages")
    test_history_and_executing_records_queryable()
    print("PASS: test_history_and_executing_records_queryable")
    test_recovery_uses_candidate_source_and_links_failed_scope()
    print("PASS: test_recovery_uses_candidate_source_and_links_failed_scope")
    test_restart_paging_filtering_and_selected_run_match()
    print("PASS: test_restart_paging_filtering_and_selected_run_match")
    print("\nALL RECOVERY TESTS PASSED")
