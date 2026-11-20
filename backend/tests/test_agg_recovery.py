"""聚合任务失败恢复批次测试。

覆盖运维交接场景：
- 构造两条失败记录 + 一条已成功记录，并让其中一条失败任务暂时缺指标定义；
- 一次提交整组任务，形成真正触发调度计算的恢复批次（部分失败）；
- 重复提交同一组任务，已成功的不能再开跑，重叠范围不产生第二份成功结果；
- 恢复缺失输入后再提交，核对真正调用的任务、两轮指标数据与上次成功记录；
- 单条「重跑失败」动作不再把任务写回失败状态，而是真实触发计算。
"""
from __future__ import annotations

import pytest

from app.services import agg_recovery
from app.services.agg_recovery import AggRecoveryService
from app.services.agg_task import AggTaskService
from app.store import store

FAIL_GMV = 101
FAIL_MISSING = 102
OK_CVR = 103


def _build_failed_and_success_rows() -> None:
    """两条失败（一条缺指标定义）+ 一条已成功（带上次成功记录）。"""
    rows = store.rows("agg_task")
    del rows[:]
    rows.extend([
        {
            "id": FAIL_GMV, "status": "计算失败", "pending": True, "abnormal": True,
            "任务编号": "AGG-FAIL-GMV", "目标指标": "METR-GMV", "聚合维度": "DIM-REGION",
        },
        {
            "id": FAIL_MISSING, "status": "计算失败", "pending": True, "abnormal": True,
            "任务编号": "AGG-FAIL-MISSING", "目标指标": "METR-MISSING", "聚合维度": "DIM-CHANNEL",
        },
        {
            "id": OK_CVR, "status": "已完成", "pending": False, "abnormal": False,
            "任务编号": "AGG-OK-CVR", "目标指标": "METR-CVR", "聚合维度": "DIM-REGION",
            "计算结果": {"task_id": OK_CVR, "status": "已完成", "marker": "上次成功记录"},
        },
    ])


def _add_missing_metric_def() -> None:
    store.rows("metric_def").append({
        "id": 99, "status": "已发布", "pending": False, "abnormal": False,
        "指标编号": "METR-MISSING", "指标名称": "独立访客数",
        "计算公式": "count(distinct user_id)", "聚合粒度": "日",
        "时间窗口": "7d", "口径版本": "v1.0", "数据来源": "page_views", "精度位数": "0",
    })


@pytest.fixture
def recovery_setup():
    _build_failed_and_success_rows()
    return AggRecoveryService()


class TestRecoveryBatchPartialFailure:
    """整组失败任务一次提交：部分失败，其他任务照常复算。"""

    def test_batch_partial_failure_and_skips_success(self, recovery_setup):
        batch, message = recovery_setup.submit_batch(
            [FAIL_GMV, FAIL_MISSING, OK_CVR, FAIL_GMV]  # 含重叠 id，验证组内去重
        )
        assert batch is not None
        assert batch["status"] == "部分失败"
        assert batch["succeeded_task_ids"] == [FAIL_GMV]
        assert batch["failed_task_ids"] == [FAIL_MISSING]

        # 组内去重：批次条目只有两条失败任务，已成功任务进入 skipped
        assert len(batch["items"]) == 2
        skipped = {s["task_id"]: s for s in batch["skipped"]}
        assert set(skipped) == {OK_CVR}
        assert "已完成" in skipped[OK_CVR]["reason"]

    def test_each_item_uses_its_own_bound_metric_input(self, recovery_setup):
        batch, _ = recovery_setup.submit_batch([FAIL_GMV, FAIL_MISSING])
        by_task = {item["task_id"]: item for item in batch["items"]}

        gmv_item = by_task[FAIL_GMV]
        assert gmv_item["metric_input"]["metric_code"] == "METR-GMV"
        assert gmv_item["metric_input"]["metric_def"]["指标编号"] == "METR-GMV"
        # 真实调用了计算并写回本轮指标数据
        assert gmv_item["invoked"] is True
        assert gmv_item["status"] == "已完成"
        assert gmv_item["metric_data"]["compute"]["unit"] == "元"
        assert gmv_item["metric_data"]["compute"]["value"] > 0

        missing_item = by_task[FAIL_MISSING]
        # 缺定义：绑定输入里快照为 None，任务无法开跑，停留在上次失败
        assert missing_item["metric_input"]["metric_def"] is None
        assert missing_item["invoked"] is True
        assert missing_item["status"] == "计算失败"
        assert "METR-MISSING" in missing_item["error"]
        assert missing_item["metric_data"] is None

    def test_task_table_reflects_real_outcomes(self, recovery_setup):
        recovery_setup.submit_batch([FAIL_GMV, FAIL_MISSING, OK_CVR])
        assert store.find("agg_task", FAIL_GMV)["status"] == "已完成"
        assert store.find("agg_task", FAIL_MISSING)["status"] == "计算失败"
        assert store.find("agg_task", FAIL_MISSING)["abnormal"] is True
        # 已成功任务未被重新执行：上次成功记录原样保留
        ok_task = store.find("agg_task", OK_CVR)
        assert ok_task["status"] == "已完成"
        assert ok_task["计算结果"]["marker"] == "上次成功记录"
        assert "最后恢复批次" not in ok_task

    def test_no_fake_completion_without_real_compute(self, recovery_setup):
        """未开跑任务不能用原有状态冒充本轮计算完毕。"""
        batch, _ = recovery_setup.submit_batch([FAIL_MISSING])
        item = batch["items"][0]
        assert item["metric_data"] is None
        assert item["error"] is not None
        task = store.find("agg_task", FAIL_MISSING)
        assert task["status"] == "计算失败"
        assert "计算结果" not in task


class TestRecoveryBatchIdempotency:
    """同一组多条任务重复提交：不能产生两份新的成功结果。"""

    def test_resubmit_does_not_rerun_succeeded_task(self, recovery_setup):
        first, _ = recovery_setup.submit_batch([FAIL_GMV, FAIL_MISSING])
        first_result = next(i for i in first["items"] if i["task_id"] == FAIL_GMV)

        # 用 spy 包住计算引擎，核对第二轮真正调用了哪些任务
        invoked: list[int] = []
        original = agg_recovery.batch_compute_for_agg_task

        def spy(task_id, metric_input=None, recovery_item=None):
            invoked.append(task_id)
            return original(task_id, metric_input, recovery_item)

        agg_recovery.batch_compute_for_agg_task = spy
        try:
            second, message = recovery_setup.submit_batch([FAIL_GMV, FAIL_MISSING, OK_CVR])
        finally:
            agg_recovery.batch_compute_for_agg_task = original

        # 第二轮只有仍失败的 102 进入计算；101、103 都被跳过
        assert invoked == [FAIL_MISSING]
        assert second["succeeded_task_ids"] == []
        assert second["failed_task_ids"] == [FAIL_MISSING]
        skipped_ids = {s["task_id"] for s in second["skipped"]}
        assert skipped_ids == {FAIL_GMV, OK_CVR}
        skip_101 = next(s for s in second["skipped"] if s["task_id"] == FAIL_GMV)
        assert first["batch_id"] in skip_101["reason"]

        # 第一轮的成功结果没有被第二轮覆盖，仍是唯一一份成功产物
        persisted = next(i for i in first["items"] if i["task_id"] == FAIL_GMV)
        assert persisted["metric_data"] == first_result["metric_data"]
        success_batches = [
            b["batch_id"] for b in recovery_setup.list_batches()
            if FAIL_GMV in b["succeeded_task_ids"]
        ]
        assert success_batches == [first["batch_id"]]

    def test_overlapping_scope_same_failed_run_single_success(self, recovery_setup):
        """两个范围重叠、指向同一失败运行的提交，只允许一份成功结果。"""
        first, _ = recovery_setup.submit_batch([FAIL_GMV])
        second, _ = recovery_setup.submit_batch([FAIL_GMV, FAIL_MISSING])
        # 第二次提交里 101 已成功，被跳过；不会再产出一份成功
        assert second["succeeded_task_ids"] == []
        assert all(i["task_id"] != FAIL_GMV for i in second["items"])
        assert store.find("agg_task", FAIL_GMV)["最后恢复批次"] == first["batch_id"]


class TestRecoveryAfterInputRestored:
    """恢复缺失输入后再提交，核对两轮结果差异。"""

    def test_second_round_succeeds_after_definition_restored(self, recovery_setup):
        first, _ = recovery_setup.submit_batch([FAIL_GMV, FAIL_MISSING])
        _add_missing_metric_def()
        second, message = recovery_setup.submit_batch([FAIL_MISSING])

        assert second["status"] == "已完成"
        item = second["items"][0]
        assert item["task_id"] == FAIL_MISSING
        assert item["invoked"] is True
        assert item["status"] == "已完成"
        # 本轮取得新的指标数据
        assert item["metric_data"]["metric_code"] == "METR-MISSING"
        assert item["metric_data"]["compute"]["unit"] == "人"
        assert item["metric_data"]["compute"]["value"] > 0

        task = store.find("agg_task", FAIL_MISSING)
        assert task["status"] == "已完成"
        assert task["最后恢复批次"] == second["batch_id"]

        # 监控视角：两个批次各自留痕，可区分哪轮拿到新数据、哪轮仍失败
        batches = recovery_setup.list_batches()
        assert [b["batch_id"] for b in batches] == [second["batch_id"], first["batch_id"]]
        first_missing = next(i for i in first["items"] if i["task_id"] == FAIL_MISSING)
        assert first_missing["metric_data"] is None
        assert second["items"][0]["metric_data"] is not None


class TestSingleRerunAction:
    """单条「重跑失败」动作：真实触发计算，不再写回失败状态。"""

    def test_rerun_failed_task_recomputes(self, recovery_setup):
        entry, message = AggTaskService().run_action(FAIL_GMV, "重跑失败")
        assert entry["status"] == "已完成"
        assert entry["计算结果"]["status"] == "已完成"
        assert "RBA-" in message
        # 单条重跑同样沉淀为恢复批次
        batches = recovery_setup.list_batches()
        assert len(batches) == 1
        assert batches[0]["succeeded_task_ids"] == [FAIL_GMV]

    def test_rerun_rejects_non_failed_task(self, recovery_setup):
        entry, message = AggTaskService().run_action(OK_CVR, "重跑失败")
        assert entry is None
        assert "仅计算失败的任务可执行重跑" in message
        # 已成功任务的上次成功记录不受影响
        assert store.find("agg_task", OK_CVR)["计算结果"]["marker"] == "上次成功记录"

    def test_rerun_missing_definition_stays_failed(self, recovery_setup):
        entry, message = AggTaskService().run_action(FAIL_MISSING, "重跑失败")
        # 未开跑：仍停留在计算失败，且不会伪造计算结果
        assert entry["status"] == "计算失败"
        assert "未开跑" in message
        assert "计算结果" not in entry

    def test_other_actions_unchanged(self, recovery_setup):
        entry, message = AggTaskService().run_action(FAIL_GMV, "提交调度")
        assert entry["status"] == "计算中"
        assert message == "聚合计算已提交调度"


class TestRecoveryBatchValidation:
    def test_empty_submission_rejected(self, recovery_setup):
        batch, message = recovery_setup.submit_batch([])
        assert batch is None
        assert "至少需要" in message

    def test_unknown_task_rejected_without_batch(self, recovery_setup):
        batch, message = recovery_setup.submit_batch([999])
        assert batch is None
        assert "不存在" in message
        assert recovery_setup.list_batches() == []

    def test_get_batch_and_unknown_id(self, recovery_setup):
        created, _ = recovery_setup.submit_batch([FAIL_GMV])
        fetched = recovery_setup.get_batch(created["batch_id"])
        assert fetched["batch_id"] == created["batch_id"]
        assert recovery_setup.get_batch("RBA-NONE-000") is None
