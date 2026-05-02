import importlib
import unittest


models = importlib.import_module("drama_pipeline.3_models")


class WorkflowTests(unittest.TestCase):
    def test_today_offline_pipeline_filters_and_scores_candidates(self):
        today = importlib.import_module("drama_pipeline.6_today_recommend")
        candidates = [
            models.DramaRecord(title="A", language="英语", theater="ShortMax", source="mobo_recommend", rank=1),
            models.DramaRecord(title="CEO Sexual Contract", language="英语", theater="DramaBox", source="beidou_new", rank=1),
        ]

        result = today.build_offline_recommendation(
            candidates,
            published=[],
            material_results={("英语", "a", "ShortMax"): 12},
        )

        self.assertEqual(len(result.recommendations), 1)
        self.assertEqual(result.recommendations[0].title, "A")
        self.assertTrue(result.recommendations[0].layer)
        self.assertEqual(result.filters[0].reason, "成人标题拦截")
        self.assertTrue(result.candidates[0].layer)
        self.assertEqual(result.recommendations[0].pre_rank_summary, "Mobo推荐第1")
        self.assertIn("推荐分层", result.recommendations[0].recommend_reason)
        self.assertIn("推荐前命中 Mobo推荐第1", result.recommendations[0].recommend_reason)
        self.assertIn("素材达标 12/12", result.recommendations[0].recommend_reason)
        self.assertEqual(result.stats["fetched"]["all_candidates"], 2)
        self.assertEqual(result.stats["filtered"]["成人标题拦截"], 1)
        self.assertEqual(result.stats["final"]["推荐结果"], 1)

    def test_order_aggregation_groups_same_platform_language_theater_title(self):
        orders = importlib.import_module("drama_pipeline.7_yesterday_orders")
        rows = [
            models.OrderRecord(
                date="2026-04-18",
                title="A",
                platform="Mobo",
                language="英语",
                theater="MoboReels",
                order_count=1,
                amount=2.5,
                account="a1",
            ),
            models.OrderRecord(
                date="2026-04-18",
                title="A",
                platform="Mobo",
                language="英语",
                theater="MoboReels",
                order_count=2,
                amount=3.5,
                account="a2",
            ),
        ]

        aggregated = orders.aggregate_orders(rows)

        self.assertEqual(len(aggregated), 1)
        self.assertEqual(aggregated[0].order_count, 3)
        self.assertEqual(aggregated[0].amount, 6.0)
        self.assertEqual(aggregated[0].account, "a1,a2")

    def test_today_pipeline_merges_same_language_duplicate_titles_before_selection(self):
        today = importlib.import_module("drama_pipeline.6_today_recommend")
        candidates = [
            models.DramaRecord(title="Same Title", language="英语", theater="ShortMax", source="mobo_recommend", rank=1),
            models.DramaRecord(title="Same Title", language="英语", theater="FlareFlow", source="beidou_hot", rank=2),
        ]

        result = today.build_offline_recommendation(
            candidates,
            published=[],
            material_results={
                ("英语", "same title", "ShortMax"): 12,
                ("英语", "same title", "FlareFlow"): 12,
            },
        )

        self.assertEqual(len(result.recommendations), 1)
        self.assertEqual(result.recommendations[0].title, "Same Title")
        self.assertEqual(result.recommendations[0].theater, "ShortMax,FlareFlow")
        self.assertFalse(any(item.reason == "同语言剧名重复" for item in result.filters))

    def test_today_pipeline_filters_language_conflicts_from_source_fields(self):
        today = importlib.import_module("drama_pipeline.6_today_recommend")
        candidates = [
            models.DramaRecord(
                title="Conflict Title",
                language="英语",
                theater="ShortMax",
                source="duole_recommend",
                rank=1,
                raw={"语言": "法语"},
            )
        ]

        result = today.build_offline_recommendation(
            candidates,
            published=[],
            material_results={},
        )

        self.assertEqual(result.recommendations, [])
        self.assertTrue(any(item.reason == "语言识别冲突" for item in result.filters))

    def test_run_checks_returns_success_for_offline_checks(self):
        checks = importlib.import_module("drama_pipeline.9_run_checks")

        result = checks.run_offline_checks()

        self.assertTrue(result["ok"])

    def test_run_live_smoke_checks_accepts_injected_clients(self):
        checks = importlib.import_module("drama_pipeline.9_run_checks")

        class FakeMobo:
            def fetch_drama_page(self, **kwargs):
                return {"data": {"list": [{"seriesName": "A"}]}}

        class FakeBeidou:
            def fetch_task_page(self, **kwargs):
                return {"body": {"data": [{"title": "B"}]}}

        class FakeFeishu:
            def fetch_token(self):
                return "tenant"

        class FakeDuole:
            def fetch_web_sheet_rows(self):
                return {"2.推荐剧单": [{"title": "A"}], "12.DramaBox英语": [{"title": "B"}]}

        result = checks.run_live_smoke_checks(
            {
                "mobo_client": FakeMobo(),
                "beidou_client": FakeBeidou(),
                "feishu_client": FakeFeishu(),
                "duole_client": FakeDuole(),
            }
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["mobo_new_count"], 1)
        self.assertEqual(result["beidou_new_count"], 1)
        self.assertEqual(result["duole_live_count"], 2)


if __name__ == "__main__":
    unittest.main()
