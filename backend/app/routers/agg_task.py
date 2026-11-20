"""聚合任务 时间线接口：返回作业时间序列结构。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, RecoveryBatchPayload
from app.services.agg_recovery import AggRecoveryService
from app.services.agg_task import AggTaskService

router = APIRouter(prefix="/api/agg_task", tags=["聚合任务"])
service = AggTaskService()
recovery_service = AggRecoveryService()
LIST_FIELDS = ["任务编号", "目标指标", "聚合维度", "计算周期", "依赖上游", "并行度", "超时分钟", "重试上限"]
STATUSES = ["待调度", "计算中", "已完成", "计算失败"]


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


@router.get("")
def list_entries() -> dict[str, Any]:
    items = service.get_timeline()
    return {"items": items}


@router.post("/recovery-batches")
def submit_recovery_batch(payload: RecoveryBatchPayload) -> ActionResult:
    """一次提交多条失败聚合任务，形成真正触发调度计算的恢复批次。"""
    batch, message = recovery_service.submit_batch(payload.task_ids)
    if batch is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=batch)


@router.get("/recovery-batches")
def list_recovery_batches() -> dict[str, Any]:
    """监控端：按批次查看每轮真实指标数据与仍停留在失败态的任务。"""
    items = recovery_service.list_batches()
    return {"items": items, "total": len(items)}


@router.get("/recovery-batches/{batch_id}")
def get_recovery_batch(batch_id: str) -> dict[str, Any]:
    batch = recovery_service.get_batch(batch_id)
    if batch is None:
        raise HTTPException(status_code=404, detail=f"恢复批次 {batch_id} 不存在")
    return batch


@router.get("/export")
def export_entries() -> StreamingResponse:
    items = service.get_timeline()
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    all_keys = ["id"]
    if items:
        all_keys = list(items[0].keys())
    writer.writerow(all_keys)
    for item in items:
        writer.writerow([csv_cell(item.get(k, "")) for k in all_keys])
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition": "attachment; filename=agg_task.csv"})


@router.get("/{entry_id}")
def get_entry(entry_id: int) -> dict:
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"聚合计算 {entry_id} 不存在")
    return entry


@router.post("")
def create_entry(payload: EntryPayload) -> ActionResult:
    entry_or_none, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="聚合计算已登记", entry=entry_or_none)


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    entry_or_none, message = service.run_action(entry_id, payload.action)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)
