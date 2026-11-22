"""统一资源接口：查询、导出、登记和动作按模块 key 分派到领域服务。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult, RecoverPayload
from app.services import service_for

router = APIRouter(prefix="/api", tags=["业务资源"])


def require_service(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


@router.get("/{module}", response_model=PageResult[dict])
def list_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, total = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{module}/recovery-candidates", response_model=PageResult[dict])
def list_recovery_candidates(
    module: str,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    failed_only: bool = Query(default=False),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
    cursor: str | None = Query(default=None),
) -> PageResult[dict]:
    """恢复候选：内存运行 + 已落盘回执合并去重后的稳定分页查询。"""
    service = require_service(module)
    lister = getattr(service, "list_recovery_candidates", None)
    if lister is None:
        raise HTTPException(status_code=404, detail=f"模块 {module} 不支持恢复候选查询")
    items, total, next_cursor = lister(
        failed_only=failed_only, keyword=keyword, status=status,
        page=page, size=size, cursor=cursor)
    return PageResult(items=items, total=total, page=page, size=size,
                      next_cursor=next_cursor)


@router.get("/{module}/export")
def export_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, _ = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=1, size=1_000_000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *service.LIST_FIELDS, "status"])
    for item in items:
        values = [item.get("id"), *(item.get(field, "") for field in service.LIST_FIELDS),
                  item.get("status", "")]
        writer.writerow([
            "'" + str(value) if str(value or "").startswith(("=", "+", "-", "@")) else value
            for value in values
        ])
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{module}.csv"'},
    )


@router.get("/{module}/{entry_id}", response_model=dict)
def get_entry(module: str, entry_id: int) -> dict[str, Any]:
    service = require_service(module)
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"{service.ENTITY} {entry_id} 不存在或已归档")
    return entry


@router.post("/{module}", response_model=ActionResult)
def create_entry(module: str, payload: EntryPayload) -> ActionResult:
    service = require_service(module)
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message=f"{service.ENTITY}已登记", entry=entry)


@router.post("/{module}/recover", response_model=ActionResult)
def recover_from_candidate(module: str, payload: RecoverPayload) -> ActionResult:
    """按选中的恢复候选发起重采；拒绝与选中运行不一致的候选。"""
    service = require_service(module)
    handler = getattr(service, "recover_from_candidate", None)
    if handler is None:
        raise HTTPException(status_code=404, detail=f"模块 {module} 不支持错误恢复")
    candidate = dict(payload.candidate or {})
    if payload.entry_id is not None:
        if candidate.get("entry_id") is not None \
                and int(candidate["entry_id"]) != int(payload.entry_id):
            return ActionResult(ok=False, message="候选运行与提交的运行ID不一致")
        candidate["entry_id"] = payload.entry_id
    if payload.record_code:
        if candidate.get("记录编号") and candidate["记录编号"] != payload.record_code:
            return ActionResult(ok=False, message="候选记录与提交的记录编号不一致")
        candidate["记录编号"] = payload.record_code
    if not candidate:
        return ActionResult(ok=False, message="未选择要恢复的运行记录")
    values = dict(payload.values)
    values.setdefault("action", payload.action)
    entry, message = handler(candidate, **values)
    return ActionResult(ok=entry is not None, message=message, entry=entry)


@router.post("/{module}/{entry_id}/actions", response_model=ActionResult)
def run_action(module: str, entry_id: int, payload: ActionPayload) -> ActionResult:
    service = require_service(module)
    entry, message = service.run_action(entry_id, payload.action.strip())
    return ActionResult(ok=entry is not None, message=message, entry=entry)
