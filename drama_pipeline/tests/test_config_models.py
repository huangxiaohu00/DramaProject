import importlib
import unittest
from pathlib import Path


class ConfigAndModelsTests(unittest.TestCase):
    def test_language_and_theater_config_contains_required_values(self):
        config = importlib.import_module("drama_pipeline.2_config")

        self.assertEqual(len(config.LANGUAGE_CONFIG), 7)
        self.assertEqual(
            set(config.LANGUAGE_CONFIG.values()),
            {"英语", "法语", "葡萄牙语", "德语", "繁体中文", "俄语", "意大利语"},
        )
        for theater in [
            "MoboReels",
            "DramaBox",
            "ShortMax",
            "FlareFlow",
            "ReelShort",
            "HoneyReels",
            "SnackShort",
            "StarShort",
        ]:
            self.assertIn(theater, config.THEATER_NAMES.values())

    def test_starshort_beidou_app_id_uses_official_key(self):
        config = importlib.import_module("drama_pipeline.2_config")

        self.assertEqual(config.THEATER_NAME_TO_ID["StarShort"], "starshort")

    def test_output_directories_are_inside_drama_pipeline(self):
        config = importlib.import_module("drama_pipeline.2_config")

        root = Path(config.PIPELINE_DIR)
        self.assertTrue(Path(config.TODAY_OUTPUT_ROOT).is_relative_to(root))
        self.assertTrue(Path(config.ORDER_OUTPUT_ROOT).is_relative_to(root))

    def test_drama_record_match_key_uses_language_and_normalized_title(self):
        models = importlib.import_module("drama_pipeline.3_models")

        record = models.DramaRecord(title="  The CEO's Wife! ", language="英语", theater="ShortMax")

        self.assertEqual(record.match_key, ("英语", "the ceos wife"))

    def test_config_missing_checks_report_live_requirements(self):
        config = importlib.import_module("drama_pipeline.2_config")
        old_values = (
            config.MOBO_DRAMA_AUTH,
            config.MOBO_AUTHS,
            config.BEIDOU_DRAMA_AUTH,
            config.FEISHU_APP_ID,
            config.FEISHU_APP_SECRET,
            config.FEISHU_APP_TOKEN,
            config.DUOLE_COOKIE,
            config.MATERIAL_COOKIE,
            config.BEIDOU_AUTHS,
        )
        try:
            config.MOBO_DRAMA_AUTH = ""
            config.MOBO_AUTHS = []
            config.BEIDOU_DRAMA_AUTH = ""
            config.FEISHU_APP_ID = ""
            config.FEISHU_APP_SECRET = ""
            config.FEISHU_APP_TOKEN = ""
            config.DUOLE_COOKIE = ""
            config.MATERIAL_COOKIE = ""
            config.BEIDOU_AUTHS = []

            today_missing = config.missing_today_config()
            order_missing = config.missing_order_config()

            self.assertIn("MOBO_DRAMA_AUTH", today_missing)
            self.assertIn("BEIDOU_DRAMA_AUTH", today_missing)
            self.assertIn("FEISHU_APP_ID", today_missing)
            self.assertIn("BEIDOU_AUTHS", order_missing)
        finally:
            (
                config.MOBO_DRAMA_AUTH,
                config.MOBO_AUTHS,
                config.BEIDOU_DRAMA_AUTH,
                config.FEISHU_APP_ID,
                config.FEISHU_APP_SECRET,
                config.FEISHU_APP_TOKEN,
                config.DUOLE_COOKIE,
                config.MATERIAL_COOKIE,
                config.BEIDOU_AUTHS,
            ) = old_values

    def test_migrated_config_has_required_live_values(self):
        config = importlib.import_module("drama_pipeline.2_config")
        runtime = importlib.import_module("drama_pipeline.10_runtime")

        self.assertTrue(runtime.default_config_path().exists())
        self.assertIn("MOBO_DRAMA_AUTH", config.missing_today_config())
        self.assertIn("MOBO_AUTHS", config.missing_order_config())


if __name__ == "__main__":
    unittest.main()
