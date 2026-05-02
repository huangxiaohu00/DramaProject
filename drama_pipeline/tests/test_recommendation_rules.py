import importlib
import unittest


config = importlib.import_module("drama_pipeline.2_config")
models = importlib.import_module("drama_pipeline.3_models")


class RecommendationRulesTests(unittest.TestCase):
    def test_published_filter_uses_exact_language_and_title_match(self):
        rules = importlib.import_module("drama_pipeline.8_recommendation_rules")
        candidates = [
            models.DramaRecord(title="Same Title", language="英语", theater="ShortMax"),
            models.DramaRecord(title="Same Title 2", language="英语", theater="ShortMax"),
            models.DramaRecord(title="Same Title", language="德语", theater="ShortMax"),
        ]
        published = [models.PublishedRecord(title="Same Title", language="英语", theater="ShortMax")]

        kept, blocked = rules.filter_published(candidates, published)

        self.assertEqual([item.title for item in kept], ["Same Title 2", "Same Title"])
        self.assertEqual(blocked[0].reason, "飞书已发布")

    def test_adult_blocking_and_ai_anime_use_soft_multipliers(self):
        rules = importlib.import_module("drama_pipeline.8_recommendation_rules")
        adult = models.DramaRecord(title="CEO Sexual Contract", language="英语", theater="DramaBox")
        adult_tag = models.DramaRecord(title="Clean Tag", language="英语", theater="DramaBox", tags=["成人剧情"])
        adult_content = models.DramaRecord(
            title="Clean Content",
            language="英语",
            theater="DramaBox",
            raw={rules.ALL_CONTENT_TAG_VALUES_RAW_KEY: ["裸露"]},
        )
        ai = models.DramaRecord(title="Clean Title", language="英语", theater="ShortMax", tags=["版权剧", "AI转绘"])
        anime = models.DramaRecord(title="Anime Title", language="英语", theater="ShortMax", tags=["动漫"])
        both = models.DramaRecord(title="Mixed Title", language="英语", theater="ShortMax", tags=["AI漫剧"])
        normal = models.DramaRecord(title="Clean Title", language="英语", theater="ShortMax", tags=["爱情"])

        self.assertEqual(rules.classify_block_reason(adult), "成人标题拦截")
        self.assertEqual(rules.classify_block_reason(adult_tag), "成人标题拦截")
        self.assertEqual(rules.classify_block_reason(adult_content), "成人标题拦截")
        self.assertEqual(rules.classify_block_reason(ai), "")
        self.assertEqual(rules.classify_block_reason(anime), "")
        self.assertEqual(rules.classify_block_reason(both), "")
        self.assertEqual(rules.classify_block_reason(normal), "")
        kept, blocked = rules.filter_blocked_content([adult, adult_tag, adult_content, ai, anime, both, normal])
        self.assertEqual([item.reason for item in blocked], ["成人标题拦截", "成人标题拦截", "成人标题拦截"])
        self.assertEqual(len(kept), 4)
        self.assertEqual(rules.content_type_multiplier(ai), config.AI_CONTENT_MULTIPLIER)
        self.assertEqual(rules.content_type_multiplier(anime), config.ANIME_CONTENT_MULTIPLIER)
        self.assertEqual(rules.content_type_multiplier(both), config.ANIME_CONTENT_MULTIPLIER)
        self.assertEqual(rules.content_type_multiplier(normal), 1.0)

    def test_ai_detection_only_uses_tag_like_fields(self):
        rules = importlib.import_module("drama_pipeline.8_recommendation_rules")
        false_positive = models.DramaRecord(
            title="False Positive",
            language="英语",
            theater="ShortMax",
            tags=["Fairytale"],
            raw={
                "description": "A wheelchair reclaim story",
                "download_url": "https://pan.baidu.com/s/abc",
            },
        )
        tagged = models.DramaRecord(
            title="Tagged AI",
            language="英语",
            theater="ShortMax",
            raw={"tag": "AI真人"},
        )

        self.assertFalse(rules.is_ai_content(false_positive))
        self.assertTrue(rules.is_ai_content(tagged))

    def test_anime_detection_accepts_tag_like_raw_fields(self):
        rules = importlib.import_module("drama_pipeline.8_recommendation_rules")
        anime = models.DramaRecord(
            title="Anime",
            language="英语",
            theater="ShortMax",
            raw={"seriesTypeList": ["动态漫"]},
        )

        self.assertTrue(rules.is_anime_content(anime))

    def test_quota_skips_theater_after_limit(self):
        rules = importlib.import_module("drama_pipeline.8_recommendation_rules")
        counts = {"英语": {"MoboReels": 2}}
        record = models.DramaRecord(title="A", language="英语", theater="MoboReels")

        self.assertFalse(rules.is_theater_quota_available(record, counts))

    def test_quota_uses_normalized_theater_name_variants(self):
        rules = importlib.import_module("drama_pipeline.8_recommendation_rules")
        counts = {"英语": {"SnackShort": 1}}
        record = models.DramaRecord(title="A", language="英语", theater="Snackshort ios")

        self.assertTrue(rules.is_theater_quota_available(record, counts))

    def test_theater_quota_block_reason_distinguishes_unsupported_and_full(self):
        rules = importlib.import_module("drama_pipeline.8_recommendation_rules")
        unsupported = models.DramaRecord(title="A", language="英语", theater="DramaBox")
        full = models.DramaRecord(title="B", language="英语", theater="MoboReels")

        self.assertEqual(rules.theater_quota_block_reason(unsupported, {}), "剧场不在语言配额内")
        self.assertEqual(rules.theater_quota_block_reason(full, {"英语": {"MoboReels": 2}}), "剧场配额已满")

    def test_dedupe_keeps_best_rank_and_merges_tags(self):
        rules = importlib.import_module("drama_pipeline.8_recommendation_rules")
        records = [
            models.DramaRecord(title="A!", language="英语", theater="ShortMax", source="beidou_new", rank=5, tags=["爱情"]),
            models.DramaRecord(title="A", language="英语", theater="ShortMax", source="mobo_recommend", rank=1, tags=["总裁"]),
        ]

        deduped = rules.dedupe_candidates(records)

        self.assertEqual(len(deduped), 1)
        self.assertEqual(deduped[0].rank, 1)
        self.assertEqual(deduped[0].source, "beidou_new,mobo_recommend")
        self.assertEqual(deduped[0].tags, ["爱情", "总裁"])
        self.assertEqual(deduped[0].source_rank_details, {"beidou_new": 5, "mobo_recommend": 1})

    def test_dedupe_merges_cross_theater_rows_without_moboreels_using_earliest_publish_theater_as_primary(self):
        rules = importlib.import_module("drama_pipeline.8_recommendation_rules")
        records = [
            models.DramaRecord(
                title="A",
                language="英语",
                theater="ShortMax",
                source="beidou_new",
                rank=2,
                publish_at="2026-04-01 10:00:00",
                tags=["爱情"],
            ),
            models.DramaRecord(
                title="A",
                language="英语",
                theater="DramaBox",
                source="duole_recommend",
                rank=8,
                raw={"上架时间": "2026-04-10", "推荐理由": "冲量"},
                tags=["总裁"],
            ),
        ]

        deduped = rules.dedupe_candidates(records)

        self.assertEqual(len(deduped), 1)
        self.assertEqual(deduped[0].theater, "ShortMax,DramaBox")
        self.assertEqual(rules.primary_theater(deduped[0]), "ShortMax")
        self.assertEqual(deduped[0].publish_at, "2026-04-01 10:00:00")
        self.assertEqual(deduped[0].tags, ["爱情", "总裁"])
        self.assertEqual(deduped[0].raw["primary_theater"], "ShortMax")
        self.assertEqual(deduped[0].raw["merged_theaters"], ["ShortMax", "DramaBox"])
        self.assertEqual(deduped[0].raw["earliest_publish_at"], "2026-04-01 10:00:00")
        self.assertEqual(deduped[0].raw["latest_publish_at"], "2026-04-10")

    def test_dedupe_prefers_moboreels_theater_and_uses_any_theater_for_ai_detection(self):
        rules = importlib.import_module("drama_pipeline.8_recommendation_rules")
        records = [
            models.DramaRecord(
                title="A",
                language="英语",
                theater="ShortMax",
                source="beidou_new",
                rank=1,
                publish_at="2026-04-01 10:00:00",
                tags=["爱情"],
            ),
            models.DramaRecord(
                title="A",
                language="英语",
                theater="MoboReels",
                source="mobo_new",
                rank=5,
                publish_at="2026-04-10 13:00:00",
                tags=["总裁"],
                raw={"audioTypeName": "原声剧", "localTypeName": "本土剧"},
            ),
            models.DramaRecord(
                title="A",
                language="英语",
                theater="DramaBox",
                source="beidou_new",
                rank=2,
                publish_at="2026-04-05 10:00:00",
                tags=["AI配音"],
            ),
        ]

        deduped = rules.dedupe_candidates(records)

        self.assertEqual(len(deduped), 1)
        self.assertEqual(rules.primary_theater(deduped[0]), "MoboReels")
        self.assertEqual(deduped[0].publish_at, "2026-04-10 13:00:00")
        self.assertEqual(deduped[0].raw["moboreels_publish_at"], "2026-04-10 13:00:00")
        self.assertEqual(deduped[0].raw["earliest_publish_at"], "2026-04-01 10:00:00")
        self.assertTrue(rules.is_ai_content(deduped[0]))

    def test_dedupe_normalizes_theater_name_variants(self):
        rules = importlib.import_module("drama_pipeline.8_recommendation_rules")
        records = [
            models.DramaRecord(title="A", language="英语", theater="Moboreels", source="beidou_new", rank=5),
            models.DramaRecord(title="A", language="英语", theater="MoboReels", source="mobo_recommend", rank=1),
        ]

        deduped = rules.dedupe_candidates(records)

        self.assertEqual(len(deduped), 1)
        self.assertEqual(deduped[0].theater, "MoboReels")

    def test_published_filter_blocks_after_cross_theater_merge(self):
        rules = importlib.import_module("drama_pipeline.8_recommendation_rules")
        deduped = rules.dedupe_candidates(
            [
                models.DramaRecord(title="A", language="英语", theater="ShortMax", source="beidou_new", rank=5),
                models.DramaRecord(title="A", language="英语", theater="DramaBox", source="duole_recommend", rank=1),
            ]
        )

        kept, blocked = rules.filter_published(deduped, [models.PublishedRecord(title="A", language="英语", theater="ShortMax")])

        self.assertEqual(kept, [])
        self.assertEqual(len(blocked), 1)
        self.assertEqual(blocked[0].reason, "飞书已发布")
        self.assertEqual(blocked[0].theater, "DramaBox,ShortMax")

    def test_source_weights_use_new_business_values(self):
        self.assertNotIn("mobo_hot", config.SOURCE_WEIGHTS)
        self.assertEqual(config.SOURCE_WEIGHTS["mobo_new"], 1.0)
        self.assertEqual(config.SOURCE_WEIGHTS["beidou_new"], 1.0)
        self.assertEqual(config.SOURCE_WEIGHTS["mobo_recommend"], 1.5)
        self.assertEqual(config.SOURCE_WEIGHTS["beidou_hot"], 1.0)
        self.assertEqual(config.SOURCE_WEIGHTS["beidou_income"], 2.0)
        self.assertEqual(config.SOURCE_WEIGHTS["duole_recommend"], 1.5)

    def test_rank_score_uses_log_decay_range(self):
        rules = importlib.import_module("drama_pipeline.8_recommendation_rules")

        self.assertEqual(rules.rank_score(1, "mobo_new"), 1.0)
        self.assertAlmostEqual(rules.rank_score(200, "mobo_new"), 0.35, places=4)
        self.assertGreater(rules.rank_score(10, "mobo_new"), rules.rank_score(100, "mobo_new"))

    def test_score_candidates_prioritizes_layer_before_final_score(self):
        rules = importlib.import_module("drama_pipeline.8_recommendation_rules")

        class FixedTagScorer:
            def score_record(self, record):
                tag_scoring = importlib.import_module("drama_pipeline.8_tag_scoring")
                return tag_scoring.DramaTagScore(
                    score=1.2 if record.title == "Old Winner" else 0.8,
                    status="标签命中",
                    matched_tags=["总裁"],
                    details={},
                )

        records = [
            models.DramaRecord(
                title="Fresh Challenger",
                language="英语",
                theater="ShortMax",
                source="mobo_recommend",
                rank=1,
                publish_at="2026-04-20 10:00:00",
                source_rank_details={"mobo_recommend": 1},
            ),
            models.DramaRecord(
                title="Old Winner",
                language="英语",
                theater="ShortMax",
                source="beidou_income,mobo_recommend,duole_recommend",
                rank=1,
                publish_at="2025-01-01 10:00:00",
                source_rank_details={"beidou_income": 1, "mobo_recommend": 1},
                source_date_details={"duole_recommend:2.推荐剧单": "2026-04-21"},
            ),
        ]

        scored = rules.score_candidates(records, reference_date="2026-04-22", tag_scorer=FixedTagScorer())

        self.assertEqual(scored[0].title, "Fresh Challenger")
        self.assertEqual(scored[0].layer, "A")
        self.assertEqual(scored[1].layer, "F")
        self.assertLess(scored[0].score, scored[1].score)

    def test_score_candidates_uses_documented_layer_formula(self):
        rules = importlib.import_module("drama_pipeline.8_recommendation_rules")

        class FixedTagScorer:
            def score_record(self, record):
                tag_scoring = importlib.import_module("drama_pipeline.8_tag_scoring")
                return tag_scoring.DramaTagScore(
                    score=1.1,
                    status="标签命中",
                    matched_tags=["总裁"],
                    details={},
                )

        record = models.DramaRecord(
            title="A",
            language="英语",
            theater="ShortMax",
            source="beidou_income,duole_recommend,mobo_recommend",
            rank=1,
            publish_at="2026-04-10 10:00:00",
            source_rank_details={"beidou_income": 300, "mobo_recommend": 60},
            source_date_details={"duole_recommend:2.推荐剧单": "2026-04-20"},
        )

        scored = rules.score_candidates([record], reference_date="2026-04-22", tag_scorer=FixedTagScorer())

        self.assertEqual(scored[0].layer, "A")
        self.assertEqual(scored[0].age_bucket, "新")
        self.assertEqual(scored[0].explosion_bucket, "新爆")
        self.assertAlmostEqual(scored[0].explosion_recommend_score, 79.0, places=4)
        self.assertAlmostEqual(scored[0].freshness_score, 100.0, places=4)
        self.assertAlmostEqual(scored[0].revenue_validation_score, 70.0, places=4)
        self.assertAlmostEqual(scored[0].layer_score, 84.55, places=4)
        self.assertAlmostEqual(scored[0].score, 93.005, places=4)

    def test_score_candidates_applies_content_multiplier_within_same_layer(self):
        rules = importlib.import_module("drama_pipeline.8_recommendation_rules")

        class FixedTagScorer:
            def score_record(self, record):
                tag_scoring = importlib.import_module("drama_pipeline.8_tag_scoring")
                return tag_scoring.DramaTagScore(
                    score=1.0,
                    status="默认",
                    matched_tags=[],
                    details={},
                )

        normal = models.DramaRecord(
            title="A",
            language="英语",
            theater="ShortMax",
            source="mobo_recommend",
            rank=1,
            publish_at="2026-04-20 10:00:00",
            source_rank_details={"mobo_recommend": 1},
        )
        ai = models.DramaRecord(
            title="B",
            language="英语",
            theater="ShortMax",
            source="mobo_recommend",
            rank=1,
            publish_at="2026-04-20 10:00:00",
            source_rank_details={"mobo_recommend": 1},
            tags=["AI转绘"],
        )
        anime = models.DramaRecord(
            title="C",
            language="英语",
            theater="ShortMax",
            source="mobo_recommend",
            rank=1,
            publish_at="2026-04-20 10:00:00",
            source_rank_details={"mobo_recommend": 1},
            tags=["动漫"],
        )

        scored = rules.score_candidates([normal, ai, anime], reference_date="2026-04-22", tag_scorer=FixedTagScorer())
        scored_by_title = {item.title: item for item in scored}

        base_score = scored_by_title["A"].score
        self.assertEqual(scored_by_title["A"].layer, "A")
        self.assertAlmostEqual(scored_by_title["B"].score, base_score * config.AI_CONTENT_MULTIPLIER, places=4)
        self.assertAlmostEqual(scored_by_title["C"].score, base_score * config.ANIME_CONTENT_MULTIPLIER, places=4)
        self.assertEqual(scored_by_title["B"].content_multiplier, config.AI_CONTENT_MULTIPLIER)
        self.assertEqual(scored_by_title["C"].content_multiplier, config.ANIME_CONTENT_MULTIPLIER)

    def test_score_candidates_orders_g_layer_by_newest_promotion_time(self):
        rules = importlib.import_module("drama_pipeline.8_recommendation_rules")

        class FixedTagScorer:
            def score_record(self, record):
                tag_scoring = importlib.import_module("drama_pipeline.8_tag_scoring")
                return tag_scoring.DramaTagScore(score=1.0, status="默认", matched_tags=[], details={})

        older = models.DramaRecord(
            title="Older",
            language="英语",
            theater="ShortMax",
            source="mobo_new",
            rank=1,
            publish_at="2025-01-01 10:00:00",
            source_rank_details={"mobo_new": 1},
        )
        newer = models.DramaRecord(
            title="Newer",
            language="英语",
            theater="ShortMax",
            source="mobo_new",
            rank=2,
            publish_at="2025-02-01 10:00:00",
            source_rank_details={"mobo_new": 2},
        )

        scored = rules.score_candidates([older, newer], reference_date="2026-04-22", tag_scorer=FixedTagScorer())

        self.assertEqual([item.layer for item in scored], ["G", "G"])
        self.assertEqual(scored[0].title, "Newer")


if __name__ == "__main__":
    unittest.main()
