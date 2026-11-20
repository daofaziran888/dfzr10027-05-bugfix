"""聚合任务失败恢复批次：一次提交多条失败任务，形成真正触发调度计算的恢复批次。

设计约束（对应运维交接场景）：
- 只有仍处于「计算失败」的原运行允许重跑；已成功/进行中的任务在提交前即被剔除，
  绝不重新执行，已成功任务不会产生第二份新的成功结果。
- 批次内每条任务绑定各自的指标输入（指标编号 + 提交时刻的指标定义快照），
  聚合执行端用这份绑定输入调用计算，而不是读任务表上可能被改动的旧字段。
- 找不到原指标定义的任务无法开跑，批次允许部分失败：其余任务照常复算。
- 本轮真实算出的指标数据与未开跑原因都回写到批次条目，监控端按批次即可区分
  「本轮取得新指标数据」与「仍停留在上次失败」，不用原有状态冒充本轮计算完毕。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.metric_compute import batch_compute_for_agg_task, find_metric_def
from app.store import store

BATCH_TABLE = "_agg_recovery_batches"
RERUNNABLE_STATUS = "计算失败"
RUNNING_STATUS = "计算中"
SUCCESS_STATUS = "已完成"


class AggRecoveryService:
    """聚合任务恢复批次编排：提交校验 → 绑定输入 → 调度计算 → 回写批次。"""

    def list_batches(self) -> list[dict[str, Any]]:
        """按时间倒序返回全部恢复批次，供监控端核对每轮结果。"""
        return [dict(batch) for batch in reversed(store.rows(BATCH_TABLE))]

    def get_batch(self, batch_id: str) -> dict[str, Any] | None:
        for batch in store.rows(BATCH_TABLE):
            if batch.get("batch_id") == batch_id:
                return dict(batch)
        return None

    def submit_batch(self, task_ids: list[int]) -> tuple[dict[str, Any] | None, str]:
        """提交一组失败聚合任务，同步执行恢复计算并返回恢复批次。

        返回 (批次记录, 提示信息)；没有任何可重跑任务时批次记录为 None。
        """
        if not task_ids:
            return None, "恢复批次至少需要包含一条聚合任务"

        # 同一批次内去重，重叠的提交范围只开一次计算
        unique_ids: list[int] = []
        seen: set[int] = set()
        for raw in task_ids:
            tid = int(raw)
            if tid not in seen:
                seen.add(tid)
                unique_ids.append(tid)

        items: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        for tid in unique_ids:
            task = store.find("agg_task", tid)
            if task is None:
                skipped.append({"task_id": tid, "task_code": None,
                                "reason": "聚合任务不存在或已归档"})
                continue

            # 调用计算前确认原运行仍处于可重跑的失败状态
            if task.get("status") != RERUNNABLE_STATUS:
                prior_batch = self._find_success_batch(tid)
                reason = (
                    f"任务已在恢复批次 {prior_batch} 复算成功，禁止重复执行"
                    if prior_batch
                    else f"当前状态为「{task.get('status')}」，仅计算失败的任务可重跑"
                )
                skipped.append({"task_id": tid, "task_code": task.get("任务编号"), "reason": reason})
                continue

            metric_code = str(task.get("目标指标", ""))
            # 绑定提交时刻的指标输入；定义暂缺时快照为 None，开跑即按未开跑处理
            metric_def = find_metric_def(metric_code)
            items.append({
                "task_id": tid,
                "task_code": task.get("任务编号"),
                "metric_code": metric_code,
                "metric_input": {"metric_code": metric_code, "metric_def": metric_def},
                "status": RERUNNABLE_STATUS,
                "invoked": False,
                "metric_data": None,
                "error": None,
            })

        if not items:
            reasons = "；".join(f"#{s['task_id']} {s['reason']}" for s in skipped)
            return None, f"没有可重跑的失败任务：{reasons}"

        batch_id = self._next_batch_id()
        now = datetime.now().isoformat()
        for item in items:
            item["batch_id"] = batch_id
        record: dict[str, Any] = {
            "batch_id": batch_id,
            "created_at": now,
            "updated_at": now,
            "status": "待执行",
            "items": items,
            "skipped": skipped,
            "succeeded_task_ids": [],
            "failed_task_ids": [],
        }

        # 进入调度：失败运行先转到计算中，确保状态机可观察
        for item in items:
            task = store.find("agg_task", item["task_id"])
            task["status"] = RUNNING_STATUS
            task["pending"] = True
            task["abnormal"] = False

        succeeded: list[int] = []
        failed: list[int] = []
        for item in items:
            task = store.find("agg_task", item["task_id"])
            # 传入该任务绑定的指标输入与批次条目，计算结果原地回写到批次
            result = batch_compute_for_agg_task(
                item["task_id"], item["metric_input"], item
            )
            if "error" in result:
                # 未开跑/计算失败：保留计算失败状态，绝不用旧结果冒充本轮完成
                task["status"] = RERUNNABLE_STATUS
                task["pending"] = True
                task["abnormal"] = True
                item["status"] = RERUNNABLE_STATUS
                item["error"] = result["error"]
                item["metric_data"] = None
                failed.append(item["task_id"])
            else:
                item["status"] = SUCCESS_STATUS
                succeeded.append(item["task_id"])

        record["succeeded_task_ids"] = succeeded
        record["failed_task_ids"] = failed
        record["status"] = (
            "已完成" if not failed else "部分失败" if succeeded else "计算失败"
        )
        record["updated_at"] = datetime.now().isoformat()
        store.rows(BATCH_TABLE).append(record)

        message = (
            f"恢复批次 {batch_id} 已提交：{len(succeeded)} 条复算成功，"
            f"{len(failed)} 条仍失败，{len(skipped)} 条跳过未执行"
        )
        return record, message

    def _find_success_batch(self, task_id: int) -> str | None:
        """同一失败运行若已在历史批次复算成功，返回该批次编号（重叠提交护栏）。"""
        for batch in store.rows(BATCH_TABLE):
            for item in batch.get("items", []):
                if int(item.get("task_id", 0)) == task_id and item.get("status") == SUCCESS_STATUS:
                    return str(batch.get("batch_id"))
        return None

    def _next_batch_id(self) -> str:
        existing = store.rows(BATCH_TABLE)
        seq = len(existing) + 1
        today = datetime.now().strftime("%Y%m%d")
        return f"RBA-{today}-{seq:03d}"
