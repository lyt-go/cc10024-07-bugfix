"""共享文件持久化：跨模块数据产物读写。
data/runs/     → 运行历史JSONL
data/snapshots/ → 页面快照JSON
data/reports/   → 质量报告JSON
data/exports/   → 导出文件
"""
from __future__ import annotations

import json
import os
import threading
import time
from typing import Any

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_DATA_ROOT = os.path.normpath(os.path.join(_THIS_DIR, "..", "..", "data"))

# 采集运行回执与历史查询可能并发发生：文件读写和序号分配共用一把可重入锁，
# 保证 append/read 不会看到半行 JSON，也保证同一时刻只产生一个回执序号。
_LOCK = threading.RLock()
_SEQ = 0


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def data_path(sub: str, filename: str) -> str:
    p = os.path.join(_DATA_ROOT, sub, filename)
    _ensure_dir(os.path.dirname(p))
    return p


def write_json(sub: str, filename: str, data: dict[str, Any]) -> str:
    path = data_path(sub, filename)
    with _LOCK:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    return path


def read_json(sub: str, filename: str) -> dict[str, Any] | None:
    path = data_path(sub, filename)
    if not os.path.exists(path):
        return None
    with _LOCK:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)


def append_jsonl(sub: str, filename: str, record: dict[str, Any]) -> str:
    path = data_path(sub, filename)
    with _LOCK:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
    return path


def read_jsonl(sub: str, filename: str) -> list[dict[str, Any]]:
    """读取JSONL；跳过并发写入时可能出现的空行/半行，避免一条坏记录拖垮历史查询。"""
    path = data_path(sub, filename)
    if not os.path.exists(path):
        return []
    records: list[dict[str, Any]] = []
    with _LOCK:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(item, dict):
                    records.append(item)
    return records


def list_files(sub: str, suffix: str = "") -> list[str]:
    path = os.path.join(_DATA_ROOT, sub)
    if not os.path.isdir(path):
        return []
    names = sorted(os.listdir(path))
    if suffix:
        names = [n for n in names if n.endswith(suffix)]
    return names


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def next_sequence() -> int:
    """进程内单调序号：用于同一秒内多次落盘时的稳定先后顺序。"""
    global _SEQ
    with _LOCK:
        _SEQ += 1
        return _SEQ


def _coerce_entry_id(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def max_run_entry_id(memory_rows: list[dict[str, Any]] | None = None) -> int:
    """运行记录ID分配要同时看内存和已落盘回执。

    否则进程重启后内存示例ID从1开始，会重新使用磁盘里已经存在的运行ID，
    导致两次不同的采集在合并查询里被误认为同一条运行。
    """
    ids: list[int] = []
    for row in memory_rows or []:
        entry_id = _coerce_entry_id(row.get("id"))
        if entry_id is not None:
            ids.append(entry_id)
    for record in read_jsonl("runs", "run_history.jsonl"):
        snapshot = record.get("snapshot") or record.get("record") or {}
        for value in (record.get("entry_id"), record.get("run_id"),
                      snapshot.get("id"), snapshot.get("entry_id")):
            entry_id = _coerce_entry_id(value)
            if entry_id is not None:
                ids.append(entry_id)
    return max(ids, default=0)


def next_run_entry_id(memory_rows: list[dict[str, Any]] | None = None) -> int:
    return max_run_entry_id(memory_rows) + 1


def run_write_lock() -> threading.RLock:
    """运行记录“分配ID + 写入内存 + 追加回执”需要在同一把锁内完成。"""
    return _LOCK
