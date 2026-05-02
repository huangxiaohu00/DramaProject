import importlib
import unittest
from unittest import mock


class WangpaiAbroadTaskTests(unittest.TestCase):
    def test_build_task_params_omits_empty_and_default_filters(self):
        module = importlib.import_module("drama_pipeline.wangpai_abroad_tasks")

        params = module.build_task_params(
            task_type=1,
            pay_type=0,
            filter_type=0,
            thread_name="KalosTV",
            language="全部语言",
            country="全部国家",
            title="",
            promotion_type="",
            page=1,
            page_size=500,
        )

        self.assertEqual(params, {"thread_name": "KalosTV", "page": 1, "page_size": 500})

    def test_build_task_params_keeps_real_filters(self):
        module = importlib.import_module("drama_pipeline.wangpai_abroad_tasks")

        params = module.build_task_params(
            thread_name="全部剧场",
            language="英语",
            country="美国",
            title="Mafia",
            promotion_type="self",
            pay_type=1,
            page=2,
            page_size=30,
        )

        self.assertEqual(
            params,
            {
                "pay_type": 1,
                "language": "英语",
                "page": 2,
                "page_size": 30,
                "title": "Mafia",
                "promotion_type": "self",
                "country": "美国",
            },
        )

    def test_parse_decrypted_payload_supports_double_json_parse(self):
        module = importlib.import_module("drama_pipeline.wangpai_abroad_tasks")
        inner = (
            '{"list":[{"task_id":243621,"title":"Mafia Brother","thread_name":"KalosTV",'
            '"language":"英语","online_date":"2026-04-30 16:05:07","pay_type":1,'
            '"tag_name":["甜宠"]}],"meta":{"pagination":{"current_page":1,"total":1}}}'
        )
        decrypted = module.json.dumps(inner, ensure_ascii=False)

        payload = module.parse_decrypted_payload(decrypted)

        self.assertEqual(payload["meta"]["pagination"]["total"], 1)
        self.assertEqual(payload["list"][0]["title"], "Mafia Brother")

    def test_structure_task_item_flattens_core_fields(self):
        module = importlib.import_module("drama_pipeline.wangpai_abroad_tasks")

        row = module.structure_task_item(
            {
                "task_id": 243621,
                "title": "Mafia Brother",
                "thread_name": "KalosTV",
                "language": "英语",
                "online_date": "2026-04-30 16:05:07",
                "pay_type": 1,
                "copyright": "番茄",
                "promotion_type": "self",
                "tag_name": ["甜宠", "霸总"],
                "cps_subsidy_radio": "67.2",
            }
        )

        self.assertEqual(row["task_id"], "243621")
        self.assertEqual(row["title"], "Mafia Brother")
        self.assertEqual(row["thread_name"], "KalosTV")
        self.assertEqual(row["pay_type_label"], "CPS/分成")
        self.assertEqual(row["tag_name"], ["甜宠", "霸总"])
        self.assertEqual(row["cps_subsidy_radio"], 67.2)

    def test_parse_args_uses_top_level_cookie_config_not_environment(self):
        module = importlib.import_module("drama_pipeline.wangpai_abroad_tasks")

        with mock.patch.dict(module.os.environ, {"WANGPAI_COOKIE": "env-cookie"}):
            with mock.patch.object(module, "WANGPAI_COOKIE", "configured-cookie"):
                args = module.parse_args([])

        self.assertEqual(args.cookie, "configured-cookie")

    def test_parse_args_cli_cookie_overrides_top_level_cookie_config(self):
        module = importlib.import_module("drama_pipeline.wangpai_abroad_tasks")

        with mock.patch.object(module, "WANGPAI_COOKIE", "configured-cookie"):
            args = module.parse_args(["--cookie", "cli-cookie"])

        self.assertEqual(args.cookie, "cli-cookie")

    def test_parse_args_defaults_to_income_mode_for_direct_run(self):
        module = importlib.import_module("drama_pipeline.wangpai_abroad_tasks")

        args = module.parse_args([])

        self.assertEqual(args.mode, "income")

    def test_normalize_cookie_removes_line_breaks_and_indent(self):
        module = importlib.import_module("drama_pipeline.wangpai_abroad_tasks")

        cookie = """
            a=1;
            b=17775107
            74;
            c=3
        """

        self.assertEqual(module.normalize_cookie(cookie), "a=1; b=1777510774; c=3")

    def test_parse_cookie_pairs_extracts_values(self):
        module = importlib.import_module("drama_pipeline.wangpai_abroad_tasks")

        cookie = "a=1; abroad_ticket=abc123; empty=; spaced = value"

        self.assertEqual(
            module.parse_cookie_pairs(cookie),
            {"a": "1", "abroad_ticket": "abc123", "empty": "", "spaced": "value"},
        )

    def test_build_headers_adds_ticket_header_from_abroad_ticket_cookie(self):
        module = importlib.import_module("drama_pipeline.wangpai_abroad_tasks")

        client = module.WangpaiAbroadClient.__new__(module.WangpaiAbroadClient)
        client.cookie = "2345login_fp=x; abroad_ticket=ticket-value"

        headers = client.build_headers()

        self.assertEqual(headers["ticket"], "ticket-value")

    def test_default_session_ignores_environment_proxies(self):
        module = importlib.import_module("drama_pipeline.wangpai_abroad_tasks")

        session = module.create_session()

        self.assertFalse(session.trust_env)

    def test_build_income_params_omits_empty_default_filters(self):
        module = importlib.import_module("drama_pipeline.wangpai_abroad_tasks")

        params = module.build_income_params(
            search_keyword="",
            income_sub_type=0,
            search_type=2,
            promotion_type="",
            page=1,
            page_size=10,
        )

        self.assertEqual(params, {"income_sub_type": 0, "search_type": 2, "page": 1, "page_size": 10})

    def test_build_income_params_keeps_real_filters(self):
        module = importlib.import_module("drama_pipeline.wangpai_abroad_tasks")

        params = module.build_income_params(
            search_keyword="Mafia",
            income_sub_type=1,
            search_type=1,
            promotion_type="self",
            page=2,
            page_size=30,
        )

        self.assertEqual(
            params,
            {
                "search_keyword": "Mafia",
                "income_sub_type": 1,
                "search_type": 1,
                "promotion_type": "self",
                "page": 2,
                "page_size": 30,
            },
        )

    def test_structure_income_item_maps_common_income_fields(self):
        module = importlib.import_module("drama_pipeline.wangpai_abroad_tasks")

        row = module.structure_income_item(
            {
                "income_id": 99,
                "task_id": 243621,
                "title": "Mafia Brother",
                "thread_name": "KalosTV",
                "language": "英语",
                "income_sub_type": 1,
                "income_amount": "12.34",
                "promotion_type": "self",
                "settle_date": "2026-04-30",
                "created_at": "2026-04-30 12:00:00",
            }
        )

        self.assertEqual(row["income_id"], "99")
        self.assertEqual(row["task_id"], "243621")
        self.assertEqual(row["title"], "Mafia Brother")
        self.assertEqual(row["thread_name"], "KalosTV")
        self.assertEqual(row["language"], "英语")
        self.assertEqual(row["income_sub_type"], 1)
        self.assertEqual(row["income_amount"], 12.34)
        self.assertEqual(row["promotion_type"], "self")
        self.assertEqual(row["settle_date"], "2026-04-30")

    def test_structure_income_item_maps_wangpai_income_detail_fields(self):
        module = importlib.import_module("drama_pipeline.wangpai_abroad_tasks")

        row = module.structure_income_item(
            {
                "abroad_id": 88,
                "task_id": "243621",
                "name": "Mafia Brother",
                "theater_name": "KalosTV",
                "income": "12.34",
                "ad_income": "1.23",
                "charge_num": "4",
                "ad_num": "5",
                "date": "2026-04-30",
                "recharge_date": "2026-04-29",
                "country": "US",
                "promotion_type": "self",
            }
        )

        self.assertEqual(row["income_id"], "88")
        self.assertEqual(row["title"], "Mafia Brother")
        self.assertEqual(row["thread_name"], "KalosTV")
        self.assertEqual(row["income_amount"], 12.34)
        self.assertEqual(row["settle_date"], "2026-04-30")
        self.assertEqual(row["order_date"], "2026-04-30")
        self.assertEqual(row["recharge_date"], "2026-04-29")
        self.assertEqual(row["charge_amount"], 4.0)
        self.assertEqual(row["ad_income"], 1.23)
        self.assertEqual(row["ad_num"], 5.0)
        self.assertEqual(row["pay_type"], 0)
        self.assertEqual(row["country"], "US")
        self.assertEqual(row["raw"]["ad_income"], "1.23")

    def test_payload_list_supports_income_detail_item_field(self):
        module = importlib.import_module("drama_pipeline.wangpai_abroad_tasks")

        rows = module._payload_list({"item": [{"task_id": "1"}]})

        self.assertEqual(rows, [{"task_id": "1"}])

    def test_build_console_payload_outputs_all_parsed_income_rows(self):
        module = importlib.import_module("drama_pipeline.wangpai_abroad_tasks")

        payload = module.build_console_payload(
            [{"income_id": "1"}, {"income_id": "2"}],
            mode="income",
        )

        self.assertEqual(payload, {"count": 2, "orders": [{"income_id": "1"}, {"income_id": "2"}]})
        self.assertNotIn("first", payload)


if __name__ == "__main__":
    unittest.main()
