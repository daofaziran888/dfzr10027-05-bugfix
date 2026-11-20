"""聚合任务 时间线服务：生成作业时间序列和示例数据。"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from app.services.agg_recovery import RERUNNABLE_STATUS, AggRecoveryService
from app.store import store

MODULE = "agg_task"
REQUIRED_FIELDS = ["任务编号", "目标指标", "聚合维度"]
STATUS_ORDER = ["待调度", "计算中", "已完成", "计算失败"]
# 普通动作只做状态流转；「重跑失败」是带真实计算的恢复动作，单独处理
ACTION_RULES = {"提交调度": "计算中", "取消任务": "已完成"}
RERUN_ACTION = "重跑失败"
NEGATIVE_ACTIONS = []


class AggTaskService:
    def get_timeline(self) -> list[dict[str, Any]]:
        rows = store.rows(MODULE)
        today = date.today()
        items: list[dict] = []
        for i, row in enumerate(rows):
            start = (today - timedelta(days=7 - i * 2)).isoformat()
            end = (today + timedelta(days=3 - i)).isoformat()
            items.append({
                "id": int(row.get("id", i + 1)),
                "name": str(row.get(list(row.keys())[1] if len(row) > 1 else "name", "")),
                "start": start,
                "end": end,
                "status": str(row.get("status", STATUS_ORDER[i % len(STATUS_ORDER)])),
                "logs": f"[{start}] 作业启动\n[{start}] 正在处理...\n[{end}] 任务{'完成' if row.get('status') == STATUS_ORDER[-1] else '进行中'}",
            })
        return items

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [f for f in REQUIRED_FIELDS if not str(values.get(f) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
        entry.update({f: values.get(f) for f in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"聚合计算 {entry_id} 不存在或已归档"

        # 「重跑失败」必须真实触发调度计算，不再把任务写回计算失败
        if action == RERUN_ACTION:
            return self._rerun_failed(entry_id, entry)

        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于聚合任务可执行范围"
        target = ACTION_RULES[action]
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"聚合计算已{action}"

    def _rerun_failed(
        self, entry_id: int, entry: dict[str, Any]
    ) -> tuple[dict[str, Any] | None, str]:
        # 调用计算前确认原运行仍处于可重跑的失败状态，已成功任务不能重新执行
        if entry.get("status") != RERUNNABLE_STATUS:
            return None, (
                f"聚合计算 {entry_id} 当前状态为「{entry.get('status')}」，"
                f"仅计算失败的任务可执行重跑"
            )
        # 单条重跑同样走恢复批次，保证输入绑定、回写与监控口径一致
        batch, message = AggRecoveryService().submit_batch([entry_id])
        if batch is None:
            return None, message
        item = batch["items"][0]
        if item["status"] == RERUNNABLE_STATUS:
            return entry, f"重跑未开跑：{item.get('error') or '计算失败'}"
        return store.find(MODULE, entry_id), f"聚合计算已重跑（恢复批次 {batch['batch_id']}）"
