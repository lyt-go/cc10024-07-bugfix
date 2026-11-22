"""通用接口契约；模块字段由领域服务持有，不再为每个模块复制一份模型类。"""
from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class PageResult(BaseModel, Generic[T]):
    items: list[T]
    total: int
    page: int = 1
    size: int = 20
    # 恢复候选使用稳定排序键做游标翻页：同秒写入的记录也能可靠取到下一页。
    next_cursor: str | None = None


class ActionResult(BaseModel):
    ok: bool
    message: str
    entry: dict[str, Any] | None = None


class EntryPayload(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)
    remark: str | None = None


class ActionPayload(BaseModel):
    action: str
    remark: str | None = None


class RecoverPayload(BaseModel):
    """错误恢复入口：可以直接提交选中的候选，也可以只提交候选运行ID。"""

    candidate: dict[str, Any] | None = None
    entry_id: int | None = None
    record_code: str | None = None
    action: str = "确认恢复"
    values: dict[str, Any] = Field(default_factory=dict)
