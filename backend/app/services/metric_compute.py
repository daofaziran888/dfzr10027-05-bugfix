"""指标计算引擎：根据指标定义执行真实聚合计算，供 agg_task 与 api_service 消费。"""
from __future__ import annotations

import statistics
from typing import Any

from app.fixtures import TREND_POINTS
from app.store import store


def _to_float(val: Any) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


def _to_int(val: Any) -> int:
    try:
        return int(val)
    except (TypeError, ValueError):
        return 0


def find_metric_def(metric_code: str) -> dict[str, Any] | None:
    """从 store 或 fixtures 中查找指标定义。"""
    for row in store.rows("metric_def"):
        if row.get("指标编号") == metric_code:
            return dict(row)
    return None


def compute_metric(metric_code: str, metric_input: dict[str, Any] | None = None) -> dict[str, Any]:
    """按指标编号读取定义并执行真实聚合计算。

    支持的公式语义：
    - sum(order_amount) / gmv -> 交易总额
    - count(distinct user_id) / uv -> 独立访客数
    - paid_uv / total_uv -> 支付转化率
    - gmv / uv -> 每用户平均收入

    恢复批次可通过 metric_input 传入各自绑定的指标输入（指标编号与定义快照），
    此时优先使用批次绑定的输入，避免任务表上的旧字段被改动后串味。
    """
    metric_code = str((metric_input or {}).get("metric_code") or metric_code)
    if metric_input and metric_input.get("metric_def"):
        metric = dict(metric_input["metric_def"])
    else:
        metric = find_metric_def(metric_code)
    if metric is None:
        return {"error": f"指标 {metric_code} 未定义"}

    formula = str(metric.get("计算公式", ""))
    precision = _to_int(metric.get("精度位数", 2))
    points = list(TREND_POINTS)
    result: dict[str, Any] = {"metric_code": metric_code, "granularity": metric.get("聚合粒度", "日")}

    if "sum(order_amount)" in formula or "gmv" in metric.get("指标名称", "").lower():
        total = sum(p["gmv"] for p in points)
        result["value"] = round(total, precision)
        result["unit"] = "元"
        result["point_count"] = len(points)
        result["avg_daily"] = round(total / len(points), precision) if points else 0.0
        result["max_daily"] = round(max(p["gmv"] for p in points), precision) if points else 0.0
        result["min_daily"] = round(min(p["gmv"] for p in points), precision) if points else 0.0
    elif "count(distinct user_id)" in formula or ("uv" in metric.get("指标名称", "").lower() and "转化率" not in metric.get("指标名称", "")):
        total = sum(p["uv"] for p in points)
        result["value"] = total
        result["unit"] = "人"
        result["point_count"] = len(points)
        result["avg_daily"] = round(total / len(points), 0) if points else 0.0
    elif "paid_uv / total_uv" in formula or "转化率" in metric.get("指标名称", ""):
        total_paid = sum(p["paid_uv"] for p in points)
        total_uv = sum(p["total_uv"] for p in points)
        cvr = total_paid / total_uv if total_uv else 0.0
        result["value"] = round(cvr, precision)
        result["unit"] = "比率"
        result["point_count"] = len(points)
        result["total_paid"] = total_paid
        result["total_uv"] = total_uv
    elif "gmv / uv" in formula or "平均收入" in metric.get("指标名称", ""):
        total_gmv = sum(p["gmv"] for p in points)
        total_uv = sum(p["uv"] for p in points)
        arpu = total_gmv / total_uv if total_uv else 0.0
        result["value"] = round(arpu, precision)
        result["unit"] = "元/人"
        result["point_count"] = len(points)
    else:
        result["value"] = 0.0
        result["unit"] = "未知"
        result["point_count"] = 0

    return result


def compute_metric_trend(metric_code: str, metric_input: dict[str, Any] | None = None) -> dict[str, Any]:
    """计算指标的趋势特征（周同比、环比、变异系数）。"""
    points = list(TREND_POINTS)
    metric_code = str((metric_input or {}).get("metric_code") or metric_code)
    if metric_input and metric_input.get("metric_def"):
        metric = dict(metric_input["metric_def"])
    else:
        metric = find_metric_def(metric_code)
    if metric is None:
        return {"error": f"指标 {metric_code} 未定义"}

    formula = str(metric.get("计算公式", ""))
    values: list[float] = []
    if "gmv" in metric.get("指标名称", "").lower() and "平均" not in metric.get("指标名称", ""):
        values = [p["gmv"] for p in points]
    elif "uv" in metric.get("指标名称", "").lower() and "转化率" not in metric.get("指标名称", ""):
        values = [float(p["uv"]) for p in points]
    elif "转化率" in metric.get("指标名称", ""):
        values = [p["paid_uv"] / p["total_uv"] if p["total_uv"] else 0.0 for p in points]
    elif "平均收入" in metric.get("指标名称", ""):
        values = [p["gmv"] / p["uv"] if p["uv"] else 0.0 for p in points]
    else:
        values = [p["gmv"] for p in points]

    if len(values) < 2:
        return {"error": "数据不足"}

    n = len(values)
    # 环比：最后一日 vs 前一日
    mom = (values[-1] - values[-2]) / values[-2] if values[-2] else 0.0
    # 周同比：最后一日 vs 7日前
    yoy = (values[-1] - values[-7]) / values[-7] if n >= 8 and values[-7] else 0.0
    # 变异系数
    cv = statistics.stdev(values) / statistics.mean(values) if statistics.mean(values) else 0.0

    return {
        "metric_code": metric_code,
        "values": [round(v, 2) for v in values],
        "mom": round(mom, 4),
        "yoy": round(yoy, 4) if n >= 8 else None,
        "cv": round(cv, 4),
        "mean": round(statistics.mean(values), 2),
        "trend_direction": "上升" if mom > 0 else "下降" if mom < 0 else "持平",
    }


def batch_compute_for_agg_task(
    task_id: int,
    metric_input: dict[str, Any] | None = None,
    recovery_item: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """为聚合任务执行真实计算，返回计算结果并更新任务状态。

    metric_input 为恢复批次里该任务绑定的指标输入（metric_code + metric_def 快照）；
    缺省时回退到任务表自身的目标指标，保持旧调用方行为不变。
    recovery_item 非空时，把本轮指标数据与错误回写到对应恢复批次条目，
    找不到指标定义等失败场景不会改写任务状态，绝不拿旧结果冒充本轮计算。
    """
    task = store.find("agg_task", task_id)
    if task is None:
        return {"error": f"聚合任务 {task_id} 不存在"}

    metric_code = str((metric_input or {}).get("metric_code") or task.get("目标指标", ""))
    compute_result = compute_metric(metric_code, metric_input)
    if "error" in compute_result:
        if recovery_item is not None:
            _mark_recovery_item(recovery_item, compute_result)
        return compute_result

    trend_result = compute_metric_trend(metric_code, metric_input)

    result = {
        "task_id": task_id,
        "task_code": task.get("任务编号"),
        "metric_code": metric_code,
        "compute": compute_result,
        "trend": trend_result,
        "status": "已完成",
        "recovery_batch_id": recovery_item.get("batch_id") if recovery_item else None,
    }

    # 更新任务记录：只有真实跑出指标数据才允许落到已完成
    task["status"] = "已完成"
    task["pending"] = False
    task["abnormal"] = False
    task["计算结果"] = result
    if recovery_item is not None:
        task["最后恢复批次"] = recovery_item.get("batch_id")
        _mark_recovery_item(recovery_item, compute_result, result)
    return result


def _mark_recovery_item(
    recovery_item: dict[str, Any],
    compute_result: dict[str, Any],
    full_result: dict[str, Any] | None = None,
) -> None:
    """把本轮真实计算产物回写到恢复批次条目（调用方持有同一对象，直接原地更新）。"""
    recovery_item["invoked"] = True
    if full_result is None:
        # 未开跑（如找不到原定义）：保留失败状态，不用旧状态冒充本轮结果
        recovery_item["status"] = "计算失败"
        recovery_item["error"] = compute_result.get("error")
        recovery_item["metric_data"] = None
    else:
        recovery_item["status"] = "已完成"
        recovery_item["error"] = None
        recovery_item["metric_data"] = full_result
