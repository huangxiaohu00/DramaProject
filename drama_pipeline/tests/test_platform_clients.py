import contextlib
import importlib
import shutil
import unittest
import uuid
from unittest import mock
from pathlib import Path

import openpyxl


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.posts = []
        self.gets = []

    def post(self, url, headers=None, json=None, data=None, timeout=None):
        self.posts.append({"url": url, "headers": headers, "json": json, "data": data, "timeout": timeout})
        return FakeResponse(self.responses.pop(0))

    def get(self, url, headers=None, params=None, timeout=None):
        self.gets.append({"url": url, "headers": headers, "params": params, "timeout": timeout})
        return FakeResponse(self.responses.pop(0))


class FakeDriver:
    def __init__(self):
        self.get_calls = []
        self.cdp_calls = []

    def get(self, url):
        self.get_calls.append(url)

    def execute_cdp_cmd(self, name, params):
        self.cdp_calls.append((name, params))
        return {}

    def quit(self):
        return None


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


class PlatformClientTests(unittest.TestCase):
    def test_platform_clients_package_exports_clients_and_legacy_module_matches(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        package = importlib.import_module("drama_pipeline.platform_clients")

        for name in ("BeidouClient", "FeishuClient", "MaterialClient"):
            self.assertIs(getattr(package, name), getattr(clients, name))
        self.assertTrue(issubclass(clients.MoboClient, package.MoboClient))
        self.assertTrue(issubclass(clients.DuoleClient, package.DuoleClient))

    def test_platform_client_factory_builds_default_today_and_order_clients(self):
        factory = importlib.import_module("drama_pipeline.platform_clients.factory")
        clients = importlib.import_module("drama_pipeline.platform_clients")

        today_clients = factory.build_today_clients(material_enabled=False)
        mobo_orders, beidou_orders = factory.build_order_clients()

        self.assertIsInstance(today_clients["mobo_client"], clients.MoboClient)
        self.assertIsInstance(today_clients["beidou_client"], clients.BeidouClient)
        self.assertIsInstance(today_clients["feishu_client"], clients.FeishuClient)
        self.assertIsInstance(today_clients["duole_client"], clients.DuoleClient)
        self.assertIsNone(today_clients["material_client"])
        self.assertIsInstance(mobo_orders, list)
        self.assertIsInstance(beidou_orders, list)

    def test_mobo_new_request_uses_moboreels_platform_and_new_order_type(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        client = clients.MoboClient(authorization="Bearer token")

        url, headers, payload = client.build_drama_request(language_code=3, order_type=0, platform=6833)

        self.assertTrue(url.endswith("/api/v1/res/getlistpc"))
        self.assertEqual(headers["Authorization"], "Bearer token")
        self.assertEqual(payload["lang"], 3)
        self.assertEqual(payload["orderType"], 0)
        self.assertEqual(payload["platform"], 6833)
        self.assertEqual(payload["localType"], 0)

    def test_mobo_request_can_scope_to_known_platform_id(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        client = clients.MoboClient(authorization="Bearer token")

        _url, _headers, payload = client.build_drama_request(language_code=3, order_type=1, platform=1331)

        self.assertEqual(payload["orderType"], 1)
        self.assertEqual(payload["platform"], 1331)

    def test_beidou_new_request_uses_publish_sort_and_language(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        client = clients.BeidouClient(authorization="token", agent_id=2851723045)

        url, headers, params = client.build_task_page_request(language=2, page_size=200)

        self.assertIn("task/page", url)
        self.assertEqual(headers["Authorization"], "token")
        self.assertEqual(params["language"], 2)
        self.assertEqual(params["order_field"], "publish_at")
        self.assertEqual(params["order_dir"], "desc")
        self.assertEqual(params["campaign_status"], 0)

    def test_beidou_new_request_can_scope_to_theater_app_id(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        client = clients.BeidouClient(authorization="token", agent_id=2851723045)

        _url, _headers, params = client.build_task_page_request(language=2, app_id="dramabox")

        self.assertEqual(params["language"], 2)
        self.assertEqual(params["app_id"], "dramabox")

    def test_feishu_records_request_contains_app_and_table_id(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        client = clients.FeishuClient(app_id="app", app_secret="secret", app_token="token", tables={"英语": "tbl"})

        url, headers, params = client.build_records_request("tenant-token", "tbl", page_token="next")

        self.assertIn("/bitable/v1/apps/token/tables/tbl/records", url)
        self.assertEqual(headers["Authorization"], "Bearer tenant-token")
        self.assertEqual(params["page_size"], 500)
        self.assertEqual(params["page_token"], "next")

    def test_duole_target_sheet_config_is_exposed(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        client = clients.DuoleClient(cookie="cookie")

        self.assertEqual(client.target_sheets, ["2.推荐剧单", "12.DramaBox英语", "13.DramaBox小语种"])
        self.assertEqual(client.sheet_limits["2.推荐剧单"], 600)

    def test_duole_web_sheet_configs_follow_layout_limits(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        client = clients.DuoleClient(cookie="cookie")

        configs = clients._build_duole_web_sheet_configs(client.target_sheets[:2])

        self.assertEqual(configs[0]["used_cols"], 7)
        self.assertEqual(configs[0]["limit_rows"], 1004)
        self.assertEqual(configs[1]["used_cols"], 9)
        self.assertEqual(configs[1]["limit_rows"], 1001)

    def test_inject_duole_cookie_uses_cdp_only_without_bootstrap_navigation(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        driver = FakeDriver()

        clients._inject_duole_cookie(driver, "a=1; b=2")

        self.assertEqual(driver.get_calls, [])
        self.assertEqual(driver.cdp_calls[0][0], "Network.enable")
        self.assertEqual(driver.cdp_calls[1][0], "Network.setCookie")
        self.assertEqual(driver.cdp_calls[2][0], "Network.setCookie")

    def test_inject_duole_cookie_supports_structured_cookie_payload(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        driver = FakeDriver()
        payload = '[{"name":"kso_sid","value":"abc","domain":".kdocs.cn","path":"/","secure":true,"httpOnly":true},{"name":"wps_sid","value":"def","domain":".wps.cn","path":"/"}]'

        clients._inject_duole_cookie(driver, payload)

        self.assertEqual(driver.cdp_calls[0][0], "Network.enable")
        self.assertEqual(driver.cdp_calls[1][1]["domain"], ".kdocs.cn")
        self.assertEqual(driver.cdp_calls[1][1]["httpOnly"], True)
        self.assertEqual(driver.cdp_calls[2][1]["domain"], ".wps.cn")

    def test_duole_fetch_web_sheet_rows_refreshes_when_cookie_missing(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        client = clients.DuoleClient(cookie="")
        client.target_sheets = ["2.推荐剧单"]

        with (
            mock.patch.object(clients, "webdriver", object()),
            mock.patch.object(clients, "EdgeOptions", object()),
            mock.patch.object(clients, "WebDriverWait", object()),
            mock.patch.object(clients, "_refresh_duole_cookie_via_playwright", return_value="refreshed-cookie") as refresh_mock,
            mock.patch.object(clients, "_save_duole_cookie_to_config") as save_mock,
            mock.patch.object(clients, "_fetch_duole_records_from_web", return_value={"2.推荐剧单": [{"剧名": "A"}]}) as fetch_mock,
        ):
            result = client.fetch_web_sheet_rows()

        self.assertEqual(result["2.推荐剧单"], [{"剧名": "A"}])
        self.assertEqual(client.cookie, "refreshed-cookie")
        refresh_mock.assert_called_once()
        save_mock.assert_called_once_with("refreshed-cookie")
        fetch_mock.assert_called_once()

    def test_duole_fetch_web_sheet_rows_retries_after_empty_fetch_and_revalidates(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        client = clients.DuoleClient(cookie="old-cookie")
        client.target_sheets = ["2.推荐剧单"]

        with (
            mock.patch.object(clients, "webdriver", object()),
            mock.patch.object(clients, "EdgeOptions", object()),
            mock.patch.object(clients, "WebDriverWait", object()),
            mock.patch.object(clients, "_refresh_duole_cookie_via_playwright", return_value="new-cookie") as refresh_mock,
            mock.patch.object(clients, "_save_duole_cookie_to_config") as save_mock,
            mock.patch.object(
                clients,
                "_fetch_duole_records_from_web",
                side_effect=[{}, {"2.推荐剧单": [{"剧名": "B"}]}],
            ) as fetch_mock,
        ):
            result = client.fetch_web_sheet_rows()

        self.assertEqual(result["2.推荐剧单"], [{"剧名": "B"}])
        self.assertEqual(client.cookie, "new-cookie")
        refresh_mock.assert_called_once()
        save_mock.assert_called_once_with("new-cookie")
        self.assertEqual(fetch_mock.call_count, 2)

    def test_material_request_contains_search_key_and_cookie(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        client = clients.MaterialClient(cookie="SESSION=abc")

        url, headers, payload = client.build_search_request("Drama", "2026-01-01", "2026-04-19")

        self.assertIn("searchCreative", url)
        self.assertEqual(headers["Cookie"], "SESSION=abc")
        self.assertEqual(headers["Content-Type"], "application/x-www-form-urlencoded;charset=UTF-8")
        self.assertEqual(payload["searchKey"], "Drama")
        self.assertEqual(payload["startDate"], "2026-01-01")
        self.assertEqual(payload["endDate"], "2026-04-19")
        self.assertEqual(payload["pageId"], 1)
        self.assertEqual(payload["pageSize"], clients.config.MATERIAL_PAGE_SIZE)
        self.assertEqual(payload["deduplicationBy"], "SMART")

    def test_material_client_counts_qualified_theater_videos(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        session = FakeSession(
            [
                {
                    "statusCode": 200,
                    "content": {
                        "totalRecord": 4,
                        "searchList": [
                            {
                                "playletName": "A",
                                "durationMillis": 180000,
                                "exposureNum": 10,
                                "videoList": ["v1"],
                                "product": {"productName": "ShortMax Android"},
                            },
                            {
                                "playletName": "A",
                                "durationMillis": 60000,
                                "exposureNum": 9,
                                "videoList": ["v2"],
                                "product": {"productName": "ShortMax Android"},
                            },
                            {
                                "playletName": "A",
                                "durationMillis": 180000,
                                "exposureNum": 8,
                                "videoList": ["v1-duplicate-duration"],
                                "product": {"productName": "ShortMax Android"},
                            },
                            {
                                "playletName": "A",
                                "durationMillis": 180000,
                                "exposureNum": 7,
                                "videoList": ["v3"],
                                "product": {"productName": "DramaBox"},
                            },
                        ],
                    },
                }
            ]
        )
        client = clients.MaterialClient(cookie="SESSION=abc", session=session)

        result = client.fetch_material_result("英语", "A", "ShortMax", "2026-01-01", "2026-04-19")

        self.assertEqual(result.language, "英语")
        self.assertEqual(result.title, "A")
        self.assertEqual(result.theater, "ShortMax")
        self.assertEqual(result.qualified_count, 1)
        self.assertEqual(result.total_count, 3)

    def test_material_client_reads_only_first_page_even_when_total_is_larger(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        session = FakeSession(
            [
                {
                    "statusCode": 200,
                    "content": {
                        "totalRecord": 800,
                        "searchList": [
                            {
                                "playletName": "A",
                                "durationMillis": 180000,
                                "exposureNum": 10,
                                "videoList": ["v1"],
                                "product": {"productName": "ShortMax Android"},
                            }
                        ],
                    },
                },
                {
                    "statusCode": 200,
                    "content": {
                        "totalRecord": 800,
                        "searchList": [
                            {
                                "playletName": "A",
                                "durationMillis": 180000,
                                "exposureNum": 8,
                                "videoList": ["v2"],
                                "product": {"productName": "ShortMax Android"},
                            }
                        ],
                    },
                },
            ]
        )
        client = clients.MaterialClient(cookie="SESSION=abc", session=session)

        materials = client.fetch_all_materials("A", "2026-01-01", "2026-04-19")

        self.assertEqual(len(session.posts), 1)
        self.assertEqual(len(materials), 1)
        self.assertEqual(materials[0]["videoUrl"], "v1")

    def test_material_prefetch_reuses_single_title_search_for_multiple_theaters(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        session = FakeSession(
            [
                {
                    "statusCode": 200,
                    "content": {
                        "totalRecord": 2,
                        "searchList": [
                            {
                                "playletName": "A",
                                "durationMillis": 180000,
                                "videoList": ["v1"],
                                "product": {"productName": "ShortMax Android"},
                            },
                            {
                                "playletName": "A",
                                "durationMillis": 180000,
                                "videoList": ["v2"],
                                "product": {"productName": "DramaBox"},
                            },
                        ],
                    },
                }
            ]
        )
        client = clients.MaterialClient(cookie="SESSION=abc", session=session)
        candidates = [
            clients.models.DramaRecord(title="A", language="英语", theater="ShortMax"),
            clients.models.DramaRecord(title="A", language="英语", theater="DramaBox"),
        ]

        results = client.prefetch_material_results(candidates, "2026-01-01", "2026-04-19")

        self.assertEqual(len(session.posts), 1)
        self.assertEqual(results[("英语", "a", "ShortMax")].qualified_count, 1)
        self.assertEqual(results[("英语", "a", "DramaBox")].qualified_count, 1)

    def test_material_client_retries_429_then_succeeds(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        session = FakeSession(
            [
                {"statusCode": 429, "message": "success"},
                {"statusCode": 429, "message": "success"},
                {
                    "statusCode": 200,
                    "content": {
                        "totalRecord": 1,
                        "searchList": [
                            {
                                "playletName": "A",
                                "durationMillis": 180000,
                                "videoList": ["v1"],
                                "product": {"productName": "ShortMax Android"},
                            }
                        ],
                    },
                },
            ]
        )
        client = clients.MaterialClient(cookie="SESSION=abc", session=session)

        with mock.patch.object(clients.time, "sleep", return_value=None):
            result = client.fetch_material_result("英语", "A", "ShortMax", "2026-01-01", "2026-04-19")

        self.assertEqual(len(session.posts), 3)
        self.assertEqual(result.qualified_count, 1)

    def test_material_client_reuses_today_cache_across_instances(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        with _workspace_tempdir() as tmp:
            cache_path = Path(tmp) / "material_cache.json"
            first_session = FakeSession(
                [
                    {
                        "statusCode": 200,
                        "content": {
                            "totalRecord": 1,
                            "searchList": [
                                {
                                    "playletName": "A",
                                    "durationMillis": 180000,
                                    "videoList": ["v1"],
                                    "product": {"productName": "ShortMax Android"},
                                }
                            ],
                        },
                    }
                ]
            )
            first_client = clients.MaterialClient(cookie="SESSION=abc", session=first_session)
            first_client.set_daily_cache_path(cache_path)
            first_result = first_client.fetch_material_result("英语", "A", "ShortMax", "2026-01-01", "2026-04-19")

            second_session = FakeSession([])
            second_client = clients.MaterialClient(cookie="SESSION=abc", session=second_session)
            second_client.set_daily_cache_path(cache_path)
            second_result = second_client.fetch_material_result("英语", "A", "ShortMax", "2026-01-01", "2026-04-19")

        self.assertEqual(len(first_session.posts), 1)
        self.assertEqual(len(second_session.posts), 0)
        self.assertEqual(first_result.qualified_count, second_result.qualified_count)

    def test_duole_web_batch_fetch_retries_before_fallback(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        driver = FakeDriver()
        batch_rows = {"2.推荐剧单": [{"剧名": "A"}]}
        old_use_batch = clients.config.DUOLE_WEB_USE_BATCH
        clients.config.DUOLE_WEB_USE_BATCH = True

        try:
            with (
                mock.patch.object(clients, "_create_duole_edge_driver", return_value=driver),
                mock.patch.object(clients, "_inject_duole_cookie"),
                mock.patch.object(
                    clients,
                    "_fetch_duole_sheet_rows_batch",
                    side_effect=[RuntimeError("timeout-1"), RuntimeError("timeout-2"), batch_rows],
                ) as batch_mock,
                mock.patch.object(clients, "_wait_for_duole_api") as wait_mock,
                mock.patch.object(clients, "_fetch_duole_sheet_index_web") as index_mock,
                mock.patch.object(clients, "_fetch_duole_sheet_matrix_web") as matrix_mock,
                mock.patch.object(clients, "_parse_duole_sheet_matrix") as parse_mock,
            ):
                result = clients._fetch_duole_records_from_web("https://example.com/share", "cookie", ["2.推荐剧单"])
        finally:
            clients.config.DUOLE_WEB_USE_BATCH = old_use_batch

        self.assertEqual(result, batch_rows)
        self.assertEqual(batch_mock.call_count, 3)
        self.assertEqual(wait_mock.call_count, 2)
        index_mock.assert_not_called()
        matrix_mock.assert_not_called()
        parse_mock.assert_not_called()

    def test_duole_web_uses_single_sheet_fallback_before_matrix(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        driver = FakeDriver()
        old_use_batch = clients.config.DUOLE_WEB_USE_BATCH
        clients.config.DUOLE_WEB_USE_BATCH = True

        try:
            with (
                mock.patch.object(clients, "_create_duole_edge_driver", return_value=driver),
                mock.patch.object(clients, "_inject_duole_cookie"),
                mock.patch.object(clients, "_fetch_duole_sheet_rows_batch", side_effect=[RuntimeError("timeout"), {"2.推荐剧单": [{"剧名": "A"}]}]) as batch_mock,
                mock.patch.object(clients, "_wait_for_duole_api") as wait_mock,
                mock.patch.object(clients, "_fetch_duole_sheet_index_web") as index_mock,
                mock.patch.object(clients, "_fetch_duole_sheet_matrix_web") as matrix_mock,
            ):
                result = clients._fetch_duole_records_from_web("https://example.com/share", "cookie", ["2.推荐剧单"], logger=None)
        finally:
            clients.config.DUOLE_WEB_USE_BATCH = old_use_batch

        self.assertEqual(result["2.推荐剧单"], [{"剧名": "A"}])
        self.assertEqual(batch_mock.call_count, 2)
        self.assertGreaterEqual(wait_mock.call_count, 1)
        index_mock.assert_not_called()
        matrix_mock.assert_not_called()

    def test_duole_web_defaults_to_single_sheet_mode(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        driver = FakeDriver()
        sheet_name = "2.\u63a8\u8350\u5267\u5355"

        with (
            mock.patch.object(clients, "_create_duole_edge_driver", return_value=driver),
            mock.patch.object(clients, "_inject_duole_cookie"),
            mock.patch.object(clients, "_wait_for_duole_api") as wait_mock,
            mock.patch.object(clients, "_fetch_duole_sheet_index_web", return_value=[{"name": sheet_name, "index": 1, "used_rows": 2, "used_cols": 3}]) as index_mock,
            mock.patch.object(clients, "_fetch_duole_sheet_matrix_web", return_value=[["\u5267\u540d"], ["A"]]) as matrix_mock,
            mock.patch.object(clients, "_parse_duole_sheet_matrix", return_value=[{"\u5267\u540d": "A"}]) as parse_mock,
            mock.patch.object(clients, "_fetch_duole_sheet_rows_batch") as batch_mock,
        ):
            result = clients._fetch_duole_records_from_web("https://example.com/share", "cookie", [sheet_name], logger=None)

        self.assertEqual(result[sheet_name], [{"\u5267\u540d": "A"}])
        batch_mock.assert_not_called()
        wait_mock.assert_called_once()
        index_mock.assert_called_once()
        matrix_mock.assert_called_once()
        parse_mock.assert_called_once()


    def test_mobo_fetch_new_dramas_parses_response_and_uses_language_mapping(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        session = FakeSession(
            [
                {
                    "data": {
                        "list": [
                            {
                                "seriesName": "A",
                                "languageName": "英语",
                                "agencyName": "MoboReels",
                                "seriesId": "1",
                                "seriesTypeList": ["总裁"],
                            }
                        ]
                    }
                }
            ]
        )
        client = clients.MoboClient(authorization="Bearer token", session=session)

        rows = client.fetch_new_dramas("英语")

        self.assertEqual(session.posts[0]["json"]["lang"], 3)
        self.assertEqual(session.posts[0]["json"]["platform"], 6833)
        self.assertEqual(session.posts[0]["json"]["pageSize"], 1000)
        self.assertEqual(session.posts[0]["json"]["pageIndex"], 1)
        self.assertEqual(rows[0].title, "A")
        self.assertEqual(rows[0].source, "mobo_new")
        stats = client.consume_fetch_stats()
        self.assertTrue(any(row["阶段"] == "原始返回" and row["来源"] == "mobo_new" and row["条数"] == 1 for row in stats))
        self.assertTrue(any(row["阶段"] == "解析成功" and row["来源"] == "mobo_new" and row["维度"] == "剧场" and row["值"] == "MoboReels" for row in stats))

    def test_mobo_fetch_new_dramas_requests_multiple_pages_until_short_page(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        session = FakeSession(
            [
                {"data": {"list": [{"seriesName": f"A{i}", "languageName": "英语", "agencyName": "MoboReels"} for i in range(1000)]}},
                {"data": {"list": [{"seriesName": "B", "languageName": "英语", "agencyName": "MoboReels"}]}},
            ]
        )
        client = clients.MoboClient(authorization="Bearer token", session=session)
        old_page_count = clients.config.NEW_DRAMA_FETCH_PAGE_COUNT
        clients.config.NEW_DRAMA_FETCH_PAGE_COUNT = 5
        try:
            rows = client.fetch_new_dramas("英语")
        finally:
            clients.config.NEW_DRAMA_FETCH_PAGE_COUNT = old_page_count

        self.assertEqual(len(session.posts), 2)
        self.assertEqual(session.posts[0]["json"]["pageIndex"], 1)
        self.assertEqual(session.posts[1]["json"]["pageIndex"], 2)
        self.assertEqual(len(rows), 1001)

    def test_beidou_fetch_new_dramas_parses_body_data(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        session = FakeSession(
            [
                {
                    "body": {
                        "data": [
                            {
                                "title": "B",
                                "language": 11,
                                "app_id": "dramabox",
                                "task_id": 9,
                                "tag": "版权剧,爱情",
                            }
                        ]
                    }
                }
            ]
        )
        client = clients.BeidouClient(authorization="token", session=session)

        rows = client.fetch_new_dramas("德语")

        self.assertEqual(session.gets[0]["params"]["language"], 11)
        self.assertEqual(session.gets[0]["params"]["page_size"], 1000)
        self.assertEqual(session.gets[0]["params"]["page_num"], 1)
        self.assertEqual(rows[0].title, "B")
        self.assertEqual(rows[0].theater, "DramaBox")
        self.assertEqual(rows[0].source, "beidou_new")

    def test_beidou_fetch_income_dramas_uses_total_income_and_source(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        session = FakeSession(
            [
                {
                    "body": {
                        "data": [
                            {
                                "title": "C",
                                "language": 5,
                                "app_id": "dramabox",
                                "task_id": 10,
                                "tag": "版权剧,爱情",
                            }
                        ]
                    }
                }
            ]
        )
        client = clients.BeidouClient(authorization="token", session=session)

        rows = client.fetch_income_dramas("法语")

        self.assertEqual(session.gets[0]["params"]["language"], 5)
        self.assertEqual(session.gets[0]["params"]["order_field"], "total_income")
        self.assertEqual(rows[0].title, "C")
        self.assertEqual(rows[0].language, "法语")
        self.assertEqual(rows[0].source, "beidou_income")

    def test_feishu_fetch_published_pages_records(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        session = FakeSession(
            [
                {"tenant_access_token": "tenant"},
                {
                    "code": 0,
                    "data": {
                        "items": [{"fields": {"剧集名称": "P", "剧场": "ShortMax"}}],
                        "has_more": False,
                    },
                },
            ]
        )
        client = clients.FeishuClient(app_id="app", app_secret="secret", app_token="token", tables={"英语": "tbl"}, session=session)

        rows = client.fetch_published("英语")

        self.assertEqual(session.posts[0]["json"]["app_id"], "app")
        self.assertEqual(session.gets[0]["params"]["page_size"], 500)
        self.assertEqual(rows[0].title, "P")
        stats = client.consume_fetch_stats()
        self.assertTrue(any(row["阶段"] == "原始返回" and row["来源"] == "published" and row["值"] == "英语" for row in stats))
        self.assertTrue(any(row["阶段"] == "解析成功" and row["来源"] == "published" and row["维度"] == "剧场" and row["值"] == "ShortMax" for row in stats))

    def test_feishu_fetch_beidou_hot_dramas_parses_date_rows_and_skips_notes(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        session = FakeSession(
            [
                {"tenant_access_token": "tenant"},
                {
                    "code": 0,
                    "data": {
                        "sheets": [
                            {
                                "sheet_id": "0jKQZS",
                                "title": "每日爆款短剧",
                                "grid_properties": {"row_count": 8, "column_count": 23},
                            },
                            {
                                "sheet_id": "sB1rjR",
                                "title": "小说",
                                "grid_properties": {"row_count": 12, "column_count": 23},
                            },
                        ]
                    },
                },
                {
                    "code": 0,
                    "data": {
                        "valueRange": {
                            "values": [
                                [[{"text": "说明备注"}], None, None, None, None, None, None],
                                [None, None, None, None, None, None, None],
                                ["推荐日期", "短剧名称", "剧场", "语言", "剧集类型", "推荐理由", "原片素材"],
                                [46132, "Love Again, My Hockey Superstar", "ShortMax", "英语", "本土剧", "周末出单TOP1", "平台直接搜索"],
                                [[{"text": "达人注意事项"}], None, None, None, None, None, None],
                                [46132, "Love Affair with EX-Wife", "MoboReels,SanckShort,Flickreels", "英语", "本土剧", "说明", "链接"],
                                [46132, "French Title", "DramaBox", "法语", "本土剧", "说明", "链接"],
                            ]
                        }
                    },
                },
            ]
        )
        client = clients.FeishuClient(app_id="app", app_secret="secret", app_token="token", tables={"英语": "tbl"}, session=session)

        rows = client.fetch_beidou_hot_dramas()

        self.assertEqual(len(session.gets), 2)
        self.assertIn("/metainfo", session.gets[0]["url"])
        self.assertEqual(session.gets[1]["url"].split("/")[-1], "0jKQZS!A1:G8")
        self.assertEqual([row.title for row in rows], [
            "Love Again, My Hockey Superstar",
            "Love Affair with EX-Wife",
            "Love Affair with EX-Wife",
            "Love Affair with EX-Wife",
        ])
        self.assertEqual([row.theater for row in rows], ["ShortMax", "MoboReels", "SnackShort", "FlickReels"])
        self.assertTrue(all(row.language == "英语" for row in rows))
        self.assertTrue(all(row.source == "beidou_hot" for row in rows))
        self.assertEqual(rows[0].publish_at, "2026-04-20")
        stats = client.consume_fetch_stats()
        self.assertTrue(any(row["阶段"] == "原始返回" and row["来源"] == "beidou_hot" and row["维度"] == "Sheet" and row["值"] == "每日爆款短剧" for row in stats))
        self.assertTrue(any(row["阶段"] == "解析成功" and row["来源"] == "beidou_hot" and row["维度"] == "剧场" and row["值"] == "SnackShort" for row in stats))

    def test_mobo_fetch_orders_parses_positive_rows(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        session = FakeSession(
            [
                {
                    "data": [
                        {"dataName": "A", "appName": "MoboReels", "languageName": "英语", "num": 2, "rmbRealIncome": 5.5},
                        {"dataName": "B", "appName": "MoboReels", "num": 0, "rmbRealIncome": 0},
                    ]
                }
            ]
        )
        client = clients.MoboClient(authorization="Bearer token", session=session)

        rows = client.fetch_orders("2026-04-18", "2026-04-18", account="m1")

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].title, "A")
        self.assertEqual(rows[0].account, "m1")
        self.assertEqual(rows[0].order_count, 2)
        self.assertEqual(rows[0].order_type, "订单金额")

    def test_mobo_fetch_orders_rejects_business_error(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        session = FakeSession([{"code": 401, "status": False, "message": "登录失效"}])
        client = clients.MoboClient(authorization="Bearer token", session=session)

        with self.assertRaisesRegex(RuntimeError, "Mobo order account invalid"):
            client.fetch_orders("2026-04-18", "2026-04-18", account="m1")

    def test_mobo_fetch_orders_parses_ad_type_one_as_ad_amount(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        session = FakeSession(
            [
                {
                    "data": [
                        {
                            "dataName": "A",
                            "appName": "MoboReels",
                            "languageName": "英语",
                            "num": 2,
                            "rmbRealIncome": 5.5,
                            "adType": 1,
                        },
                    ]
                }
            ]
        )
        client = clients.MoboClient(authorization="Bearer token", session=session)

        rows = client.fetch_orders("2026-04-18", "2026-04-18", account="m1")

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].order_type, "广告金额")
        self.assertEqual(rows[0].amount, 5.5)
        self.assertEqual(rows[0].order_count, 1)

    def test_mobo_fetch_orders_looks_up_missing_language_once_per_title(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        session = FakeSession(
            [
                {
                    "data": [
                        {"dataName": "A", "appName": "MoboReels", "num": 2, "rmbRealIncome": 5.5},
                        {"dataName": "A", "appName": "MoboReels", "num": 1, "rmbRealIncome": 1.5},
                    ]
                },
                {
                    "data": {
                        "list": [
                            {"seriesName": "A", "languageName": "德语"},
                        ]
                    }
                },
            ]
        )
        client = clients.MoboClient(authorization="Bearer token", session=session)

        rows = client.fetch_orders("2026-04-18", "2026-04-18", account="m1")

        self.assertEqual([row.language for row in rows], ["德语", "德语"])
        self.assertEqual(len(session.posts), 2)
        self.assertEqual(session.posts[1]["json"]["name"], "A")

    def test_mobo_fetch_orders_falls_back_to_beidou_language_lookup(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        session = FakeSession(
            [
                {
                    "data": [
                        {"dataName": "A", "appName": "MoboReels", "num": 2, "rmbRealIncome": 5.5},
                    ]
                },
                {"data": {"list": []}},
                {
                    "body": {
                        "data": [
                            {"title": "A", "language": 11, "app_id": "dramabox"},
                        ]
                    }
                },
            ]
        )
        client = clients.MoboClient(authorization="Bearer token", session=session)

        rows = client.fetch_orders("2026-04-18", "2026-04-18", account="m1")

        self.assertEqual(rows[0].language, "德语")
        self.assertEqual(len(session.gets), 1)
        self.assertEqual(session.gets[0]["params"]["search_title"], "A")

    def test_beidou_fetch_orders_parses_ad_amount_as_separate_type(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        session = FakeSession(
            [
                {
                    "body": {
                        "data": [
                            {
                                "serial_name": "A",
                                "language_str": "英语",
                                "app_id": "6833",
                                "task_id": "task-1",
                                "total_recharge_count": 3,
                                "total_recharge_income": 8.5,
                                "total_ad_income": 2.25,
                            }
                        ]
                    }
                }
            ]
        )
        client = clients.BeidouClient(authorization="Bearer token", session=session)

        rows = client.fetch_orders("2026-04-18", "2026-04-18", account="b1")

        self.assertEqual([row.order_type for row in rows], ["订单金额", "广告金额"])
        self.assertEqual([row.amount for row in rows], [8.5, 2.25])
        self.assertEqual(rows[1].order_count, 1)
        self.assertEqual(rows[0].platform, "北斗")
        self.assertEqual(rows[0].account, "b1")

    def test_beidou_fetch_orders_uses_page_size_100_and_reads_all_pages(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        session = FakeSession(
            [
                {
                    "code": 0,
                    "msg": "ok",
                    "body": {
                        "data": [
                            {
                                "serial_name": "A",
                                "language_str": "英语",
                                "app_id": "6833",
                                "task_id": "task-1",
                                "total_recharge_count": 1,
                                "total_recharge_income": 8.5,
                            }
                        ],
                        "page": {"current_page": 1, "page_size": 100, "total_count": 101},
                    },
                },
                {
                    "code": 0,
                    "msg": "ok",
                    "body": {
                        "data": [
                            {
                                "serial_name": "B",
                                "language_str": "德语",
                                "app_id": "6833",
                                "task_id": "task-2",
                                "total_recharge_count": 2,
                                "total_recharge_income": 18.5,
                            }
                        ],
                        "page": {"current_page": 2, "page_size": 100, "total_count": 101},
                    },
                },
            ]
        )
        client = clients.BeidouClient(authorization="Bearer token", session=session)

        rows = client.fetch_orders("2026-04-18", "2026-04-18", account="b1")

        self.assertEqual([row.title for row in rows], ["A", "B"])
        self.assertEqual(session.gets[0]["params"]["page_size"], 100)
        self.assertEqual(session.gets[0]["params"]["page_num"], 1)
        self.assertEqual(session.gets[1]["params"]["page_num"], 2)

    def test_beidou_fetch_orders_rejects_business_error(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        session = FakeSession([{"code": 401, "msg": "token expired", "body": {"data": []}}])
        client = clients.BeidouClient(authorization="Bearer token", session=session)

        with self.assertRaisesRegex(RuntimeError, "Beidou order account invalid"):
            client.fetch_orders("2026-04-18", "2026-04-18", account="b1")

    def test_beidou_fetch_orders_looks_up_missing_large_ad_language(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        session = FakeSession(
            [
                {
                    "body": {
                        "data": [
                            {
                                "serial_name": "A",
                                "language_str": "",
                                "app_id": "6833",
                                "task_id": "task-1",
                                "total_recharge_count": 0,
                                "total_recharge_income": 0,
                                "total_ad_income": 12.5,
                            }
                        ]
                    }
                },
                {
                    "body": {
                        "data": [
                            {"title": "A", "language": 11, "app_id": "6833"},
                        ]
                    }
                },
            ]
        )
        client = clients.BeidouClient(authorization="Bearer token", session=session)

        rows = client.fetch_orders("2026-04-18", "2026-04-18", account="b1")

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].order_type, "广告金额")
        self.assertEqual(rows[0].amount, 12.5)
        self.assertEqual(rows[0].language, "德语")
        self.assertEqual(len(session.gets), 2)
        self.assertEqual(session.gets[1]["params"]["search_title"], "A")

    def test_beidou_fetch_orders_fills_missing_recharge_language_from_language_code(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        session = FakeSession(
            [
                {
                    "body": {
                        "data": [
                            {
                                "serial_name": "A",
                                "language_str": "",
                                "language": 11,
                                "app_id": "6833",
                                "task_id": "task-1",
                                "total_recharge_count": 3,
                                "total_recharge_income": 8.5,
                            }
                        ]
                    }
                }
            ]
        )
        client = clients.BeidouClient(authorization="Bearer token", session=session)

        rows = client.fetch_orders("2026-04-18", "2026-04-18", account="b1")

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].order_type, "订单金额")
        self.assertEqual(rows[0].language, "德语")

    def test_beidou_fetch_orders_looks_up_missing_recharge_language(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        session = FakeSession(
            [
                {
                    "body": {
                        "data": [
                            {
                                "serial_name": "A",
                                "language_str": "",
                                "app_id": "6833",
                                "task_id": "task-1",
                                "total_recharge_count": 3,
                                "total_recharge_income": 8.5,
                            }
                        ]
                    }
                },
                {
                    "body": {
                        "data": [
                            {"title": "A", "language": 11, "app_id": "6833"},
                        ]
                    }
                },
            ]
        )
        client = clients.BeidouClient(authorization="Bearer token", session=session)

        rows = client.fetch_orders("2026-04-18", "2026-04-18", account="b1")

        self.assertEqual(rows[0].language, "德语")
        self.assertEqual(len(session.gets), 2)
        self.assertEqual(session.gets[1]["params"]["search_title"], "A")

    def test_mobo_lookup_drama_metadata_filters_by_language_and_merges_tags(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        session = FakeSession(
            [
                {
                    "data": {
                        "list": [
                            {
                                "seriesName": "A",
                                "languageName": "英语",
                                "agencyName": "DramaBox",
                                "createTime": "2026-04-10",
                                "seriesTypeList": ["爱情"],
                            },
                            {
                                "seriesName": "A",
                                "languageName": "法语",
                                "agencyName": "ShortMax",
                                "createTime": "2026-04-12",
                                "seriesTypeList": ["总裁"],
                            },
                            {
                                "seriesName": "A",
                                "languageName": "法语",
                                "agencyName": "DramaBox",
                                "createTime": "2026-04-15",
                                "seriesTypeList": ["复仇"],
                            },
                        ]
                    }
                }
            ]
        )
        client = clients.MoboClient(authorization="Bearer token", session=session)

        metadata = client.lookup_drama_metadata("A", "法语")

        self.assertEqual(metadata["publish_at"], "2026-04-15")
        self.assertEqual(metadata["theater"], "DramaBox")
        self.assertEqual(metadata["tags"], ["复仇", "总裁"])
        self.assertEqual(session.posts[0]["json"]["lang"], 6)

    def test_beidou_lookup_task_metadata_uses_search_title(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        session = FakeSession(
            [
                {
                    "body": {
                        "data": [
                            {
                                "title": "A",
                                "language": 5,
                                "app_id": "dramabox",
                                "publish_at": "2026-04-11 10:00:00",
                                "tag": "爱情,复仇",
                            }
                        ]
                    }
                }
            ]
        )
        client = clients.BeidouClient(authorization="token", session=session)

        metadata = client.lookup_task_metadata("A", "法语")

        self.assertEqual(session.gets[0]["params"]["search_title"], "A")
        self.assertEqual(session.gets[0]["params"]["language"], 5)
        self.assertEqual(metadata["publish_at"], "2026-04-11 10:00:00")
        self.assertEqual(metadata["theater"], "DramaBox")
        self.assertEqual(metadata["tags"], ["复仇", "爱情"])

    def test_duole_fetch_recommend_dramas_reads_local_workbook(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        with _workspace_tempdir() as tmp:
            path = Path(tmp) / "duole.xlsx"
            workbook = openpyxl.Workbook()
            worksheet = workbook.active
            worksheet.title = "2.推荐剧单"
            worksheet.append(["日期", "剧场", "剧名", "语言", "类型", "理由", "素材"])
            worksheet.append(["2026-04-19", "DramaBox", "A", "英语", "推荐", "reason", "url"])
            workbook.save(path)
            workbook.close()

            client = clients.DuoleClient(cookie="cookie", local_workbook=path)
            rows = client.fetch_recommend_dramas()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].title, "A")
        self.assertEqual(rows[0].language, "英语")
        self.assertEqual(rows[0].theater, "DramaBox")
        self.assertEqual(rows[0].source, "duole_recommend")
        stats = client.consume_fetch_stats()
        self.assertTrue(any(row["阶段"] == "原始返回" and row["来源"] == "duole_recommend" and row["维度"] == "Sheet" and row["值"] == "2.推荐剧单" for row in stats))
        self.assertTrue(any(row["阶段"] == "解析成功" and row["来源"] == "duole_recommend" and row["维度"] == "语言+剧场" and row["值"] == "英语 / DramaBox" for row in stats))

    def test_duole_web_sheet_configs_use_explicit_small_language_column_width(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")

        configs = clients._build_duole_web_sheet_configs(["13.DramaBox小语种"])

        self.assertEqual(configs[0]["used_cols"], 10)

    def test_duole_english_sheet_uses_fixed_columns_and_note_keywords(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        client = clients.DuoleClient(cookie="cookie")
        sheet_meta = {"index": 1, "sid": "sid", "type": "sheet", "used_rows": 2, "used_cols": 9}
        matrix = [
            ["更新时间", "上架时间", "C", "D", "外语名", "F", "G", "H", "备注"],
            ["2026-04-20", "2026-04-10", "", "", "English Title", "", "", "", "重点爆款"],
        ]

        rows = clients._parse_duole_sheet_matrix("12.DramaBox英语", sheet_meta, matrix)
        parsed = client.parse_web_sheet_rows({"12.DramaBox英语": rows})

        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0].title, "English Title")
        self.assertEqual(parsed[0].language, "英语")
        self.assertEqual(parsed[0].publish_at, "2026-04-10")
        self.assertEqual(parsed[0].raw["备注"], "重点爆款")

    def test_duole_small_language_sheet_requires_note_keyword_from_column_j(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        client = clients.DuoleClient(cookie="cookie")
        sheet_meta = {"index": 1, "sid": "sid", "type": "sheet", "used_rows": 3, "used_cols": 10}
        matrix = [
            ["更新时间", "上架时间", "语种", "D", "E", "外语名", "G", "H", "I", "备注"],
            ["2026-04-20", "2026-04-10", "法语", "", "", "French Hit", "", "", "", "热榜TOP"],
            ["2026-04-20", "2026-04-10", "德语", "", "", "German Normal", "", "", "", "普通备注"],
        ]

        rows = clients._parse_duole_sheet_matrix("13.DramaBox小语种", sheet_meta, matrix)
        parsed = client.parse_web_sheet_rows({"13.DramaBox小语种": rows})

        self.assertEqual(len(rows), 2)
        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0].title, "French Hit")
        self.assertEqual(parsed[0].language, "法语")
        self.assertEqual(parsed[0].raw["备注"], "热榜TOP")

    def test_mobo_parse_drama_items_normalizes_theater_name_variants(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        client = clients.MoboClient(authorization="Bearer token")

        rows = client.parse_drama_items(
            [{"seriesName": "A", "languageName": "英语", "agencyName": "Moboreels Android"}],
            source="mobo_new",
        )

        self.assertEqual(rows[0].theater, "MoboReels")

    def test_duole_fetch_recommend_dramas_uses_web_when_no_local_workbook(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        client = clients.DuoleClient(cookie="cookie")
        client._find_local_workbook = lambda: None
        client.fetch_web_sheet_rows = lambda: {
            "2.推荐剧单": [
                {"sheet_name": "2.推荐剧单", "剧名": "A", "语言": "英语", "剧场": "dramabox", "类型": "推荐"},
            ]
        }
        client.fetch_local_recommend_dramas = lambda: self.fail("should not use local fallback when web data exists")

        rows = client.fetch_recommend_dramas()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].title, "A")
        self.assertEqual(rows[0].theater, "DramaBox")
        self.assertEqual(rows[0].raw["sheet_name"], "2.推荐剧单")

    def test_duole_fetch_recommend_dramas_keeps_web_only_when_some_sheets_are_missing(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        client = clients.DuoleClient(cookie="cookie")
        primary_sheet = client.target_sheets[0]
        missing_sheets = client.target_sheets[1:]
        client.fetch_web_sheet_rows = lambda: {
            primary_sheet: [{"sheet_name": primary_sheet, "title": "A", "language_name": "英语", "theater_name": "DramaBox", "type": "推荐"}],
            missing_sheets[0]: [],
            missing_sheets[1]: [],
        }
        client._fetch_local_recommend_dramas_for_sheets = lambda sheet_names: self.fail("should not fallback to local workbook for missing web sheets")

        rows = client.fetch_recommend_dramas()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].title, "A")


    def test_duole_fetch_recommend_dramas_prefers_web_even_when_local_exists(self):
        clients = importlib.import_module("drama_pipeline.4_platform_clients")
        models = importlib.import_module("drama_pipeline.3_models")
        client = clients.DuoleClient(cookie="cookie")
        client._find_local_workbook = lambda: Path("D:/project/DramaProject/drama_pipeline/.tmp_tests/duole.xlsx")
        client.fetch_web_sheet_rows = lambda: {"2.鎺ㄨ崘鍓у崟": [{"dummy": "row"}]}
        client.parse_web_sheet_rows = lambda rows: [
            models.DramaRecord(title="Web A", language="鑻辫", theater="DramaBox", source="duole_recommend", rank=1)
        ]
        client.fetch_local_recommend_dramas = lambda: self.fail("should not read local workbook before web fetch")

        rows = client.fetch_recommend_dramas()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].title, "Web A")
        self.assertEqual(rows[0].theater, "DramaBox")


if __name__ == "__main__":
    unittest.main()
