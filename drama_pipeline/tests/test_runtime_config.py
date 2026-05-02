import importlib
import contextlib
import io
import shutil
import unittest
import uuid
from pathlib import Path
from unittest import mock


runtime = importlib.import_module("drama_pipeline.10_runtime")
config = importlib.import_module("drama_pipeline.2_config")


@contextlib.contextmanager
def _workspace_tempdir():
    base = Path("D:/project/DramaProject/drama_pipeline/.tmp_tests")
    base.mkdir(parents=True, exist_ok=True)
    path = base / f"tmp_{uuid.uuid4().hex}"
    path.mkdir(parents=True, exist_ok=True)
    try:
        yield str(path)
    finally:
        shutil.rmtree(path, ignore_errors=True)


class RuntimeConfigTests(unittest.TestCase):
    def test_pipeline_logger_prints_info_to_console(self):
        logger = runtime.PipelineLogger("today_recommend", app_dir=Path("."), run_date="2026-04-22")
        stream = io.StringIO()

        with contextlib.redirect_stdout(stream):
            logger.info("窗口日志")

        output = stream.getvalue()
        self.assertIn("[INFO] 窗口日志", output)

    def test_configure_utf8_runtime_enables_immediate_stdout_writes(self):
        class FakeStream:
            def __init__(self):
                self.calls = []

            def reconfigure(self, **kwargs):
                self.calls.append(kwargs)

        stdin = FakeStream()
        stdout = FakeStream()
        stderr = FakeStream()

        with mock.patch.object(runtime.sys, "stdin", stdin), mock.patch.object(
            runtime.sys, "stdout", stdout
        ), mock.patch.object(runtime.sys, "stderr", stderr):
            runtime.configure_utf8_runtime()

        self.assertEqual(stdout.calls[-1]["encoding"], "utf-8")
        self.assertTrue(stdout.calls[-1]["line_buffering"])
        self.assertTrue(stdout.calls[-1]["write_through"])
        self.assertTrue(stderr.calls[-1]["line_buffering"])

    def test_pause_before_exit_waits_only_for_frozen_exe(self):
        stream = io.StringIO()

        with mock.patch.object(runtime.sys, "frozen", True, create=True), mock.patch.object(
            runtime.time, "sleep", return_value=None
        ) as sleep_mock, contextlib.redirect_stdout(stream):
            runtime.pause_before_exit(seconds=5)

        sleep_mock.assert_called_once_with(5)
        self.assertIn("5", stream.getvalue())

        with mock.patch.object(runtime.sys, "frozen", False, create=True), mock.patch.object(
            runtime.time, "sleep", return_value=None
        ) as sleep_mock:
            runtime.pause_before_exit(seconds=5)

        sleep_mock.assert_not_called()

    def test_normalize_language_name_maps_portuguese_aliases_to_full_name(self):
        self.assertEqual(runtime.normalize_language_name("葡语"), "葡萄牙语")
        self.assertEqual(runtime.normalize_language_name("pt"), "葡萄牙语")

    def test_runtime_workbook_roundtrip_and_apply(self):
        data = runtime.RuntimeConfig(
            mobo_drama_auth="Bearer drama",
            mobo_auths=["m1", "m2"],
            beidou_drama_auth="beidou-drama",
            beidou_auths=["b1"],
            beidou_agent_id=123,
            feishu_app_id="app",
            feishu_app_secret="secret",
            feishu_app_token="token",
            feishu_tables={"英语": "tbl-en", "法语": "tbl-fr"},
            feishu_beidou_hot_spreadsheet_token="sheet-token",
            feishu_beidou_hot_sheet_name="每日爆款短剧",
            duole_cookie="cookie",
            duole_share_url="https://example.com/duole",
            duole_target_sheets=["2.推荐剧单", "13.DramaBox小语种"],
            duole_sheet_limits={"2.推荐剧单": 1000, "13.DramaBox小语种": 400},
            material_check_enabled=True,
            material_cookie="material-cookie",
            material_lookback_days=365,
            video_threshold=12,
            material_shortlist_multiplier=5,
            material_prefetch_chunk_size=5,
            material_prefetch_workers=5,
            material_prefetch_pause_min_seconds=0.6,
            material_prefetch_pause_max_seconds=1.2,
            today_collect_workers=4,
            max_ai_anime_per_language=3,
            english_local_translated_ratio=0.5,
            language_theater_quotas={"英语": {"ShortMax": 3, "FlareFlow": 2}},
        )

        old_values = (
            config.TODAY_OUTPUT_ROOT,
            config.ORDER_OUTPUT_ROOT,
            config.MOBO_DRAMA_AUTH,
            list(config.MOBO_AUTHS),
            config.BEIDOU_DRAMA_AUTH,
            list(config.BEIDOU_AUTHS),
            config.MATERIAL_CHECK_ENABLED,
            config.MATERIAL_COOKIE,
            config.MATERIAL_LOOKBACK_DAYS,
            config.VIDEO_THRESHOLD,
            config.MATERIAL_SHORTLIST_MULTIPLIER,
            config.MATERIAL_PREFETCH_CHUNK_SIZE,
            config.MATERIAL_PREFETCH_WORKERS,
            config.MATERIAL_PREFETCH_PAUSE_MIN_SECONDS,
            config.MATERIAL_PREFETCH_PAUSE_MAX_SECONDS,
            config.TODAY_COLLECT_WORKERS,
            config.MAX_AI_ANIME_PER_LANGUAGE,
            config.ENGLISH_LOCAL_TRANSLATED_RATIO,
            list(config.DUOLE_TARGET_SHEETS),
            dict(config.DUOLE_SHEET_LIMITS),
            dict(config.FEISHU_TABLES),
            dict(config.LANGUAGE_THEATER_QUOTAS),
        )
        with _workspace_tempdir() as tmp:
            try:
                workbook_path = Path(tmp) / runtime.WORKBOOK_NAME
                runtime.create_runtime_workbook(workbook_path, data)
                loaded = runtime.load_runtime_config(workbook_path)
                runtime.apply_runtime_config(loaded, config_module=config, app_dir=Path(tmp))

                self.assertEqual(loaded.mobo_drama_auth, "Bearer drama")
                self.assertEqual(loaded.mobo_auths, ["m1", "m2"])
                self.assertEqual(loaded.feishu_tables["英语"], "tbl-en")
                self.assertEqual(config.TODAY_OUTPUT_ROOT, Path(tmp) / "today_recommend")
                self.assertEqual(config.ORDER_OUTPUT_ROOT, Path(tmp))
                self.assertEqual(config.DUOLE_TARGET_SHEETS, ["2.推荐剧单", "13.DramaBox小语种"])
                self.assertEqual(config.MAX_AI_ANIME_PER_LANGUAGE, 3)
                self.assertEqual(config.MATERIAL_SHORTLIST_MULTIPLIER, 5)
                self.assertEqual(config.MATERIAL_PREFETCH_CHUNK_SIZE, 5)
                self.assertEqual(config.MATERIAL_PREFETCH_WORKERS, 5)
                self.assertAlmostEqual(config.MATERIAL_PREFETCH_PAUSE_MIN_SECONDS, 0.6)
                self.assertAlmostEqual(config.MATERIAL_PREFETCH_PAUSE_MAX_SECONDS, 1.2)
                self.assertEqual(config.TODAY_COLLECT_WORKERS, 4)
                self.assertAlmostEqual(config.ENGLISH_LOCAL_TRANSLATED_RATIO, 0.5)
                self.assertEqual(config.LANGUAGE_THEATER_QUOTAS["英语"]["ShortMax"], 3)
            finally:
                (
                    config.TODAY_OUTPUT_ROOT,
                    config.ORDER_OUTPUT_ROOT,
                    config.MOBO_DRAMA_AUTH,
                    config.MOBO_AUTHS,
                    config.BEIDOU_DRAMA_AUTH,
                    config.BEIDOU_AUTHS,
                    config.MATERIAL_CHECK_ENABLED,
                    config.MATERIAL_COOKIE,
                    config.MATERIAL_LOOKBACK_DAYS,
                    config.VIDEO_THRESHOLD,
                    config.MATERIAL_SHORTLIST_MULTIPLIER,
                    config.MATERIAL_PREFETCH_CHUNK_SIZE,
                    config.MATERIAL_PREFETCH_WORKERS,
                    config.MATERIAL_PREFETCH_PAUSE_MIN_SECONDS,
                    config.MATERIAL_PREFETCH_PAUSE_MAX_SECONDS,
                    config.TODAY_COLLECT_WORKERS,
                    config.MAX_AI_ANIME_PER_LANGUAGE,
                    config.ENGLISH_LOCAL_TRANSLATED_RATIO,
                    config.DUOLE_TARGET_SHEETS,
                    config.DUOLE_SHEET_LIMITS,
                    config.FEISHU_TABLES,
                    config.LANGUAGE_THEATER_QUOTAS,
                ) = old_values

    def test_pipeline_logger_only_writes_file_on_problem(self):
        with _workspace_tempdir() as tmp:
            logger = runtime.PipelineLogger("today_recommend", app_dir=Path(tmp), run_date="2026-04-22")
            logger.info("ok")
            self.assertIsNone(logger.flush_if_needed())

            logger.warning("problem")
            log_path = logger.flush_if_needed()
            self.assertIsNotNone(log_path)
            self.assertTrue(Path(log_path).exists())


if __name__ == "__main__":
    unittest.main()
