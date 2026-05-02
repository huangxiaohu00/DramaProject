import importlib
import tempfile
import unittest
from pathlib import Path


class DefaultFactoryTests(unittest.TestCase):
    def test_today_default_clients_use_central_config(self):
        config = importlib.import_module("drama_pipeline.2_config")
        today = importlib.import_module("drama_pipeline.6_today_recommend")

        old_mobo_drama_auth = config.MOBO_DRAMA_AUTH
        old_mobo_auths = config.MOBO_AUTHS
        old_beidou_auth = config.BEIDOU_DRAMA_AUTH
        old_duole_cookie = config.DUOLE_COOKIE
        old_material_check_enabled = config.MATERIAL_CHECK_ENABLED
        try:
            config.MOBO_DRAMA_AUTH = "Bearer drama"
            config.MOBO_AUTHS = ["Bearer mobo"]
            config.BEIDOU_DRAMA_AUTH = "beidou"
            config.DUOLE_COOKIE = "duole-cookie"
            config.MATERIAL_CHECK_ENABLED = False

            clients = today.build_default_clients()

            self.assertEqual(clients["mobo_client"].authorization, "Bearer drama")
            self.assertEqual(clients["beidou_client"].authorization, "beidou")
            self.assertEqual(clients["duole_client"].cookie, "duole-cookie")
            self.assertIsNone(clients["material_client"])
        finally:
            config.MOBO_DRAMA_AUTH = old_mobo_drama_auth
            config.MOBO_AUTHS = old_mobo_auths
            config.BEIDOU_DRAMA_AUTH = old_beidou_auth
            config.DUOLE_COOKIE = old_duole_cookie
            config.MATERIAL_CHECK_ENABLED = old_material_check_enabled

    def test_order_default_clients_use_all_configured_accounts(self):
        config = importlib.import_module("drama_pipeline.2_config")
        orders = importlib.import_module("drama_pipeline.7_yesterday_orders")

        old_mobo_auths = config.MOBO_AUTHS
        old_beidou_auths = config.BEIDOU_AUTHS
        try:
            config.MOBO_AUTHS = ["m1", "m2", "m3"]
            config.BEIDOU_AUTHS = ["b1", "b2"]

            mobo_clients, beidou_clients = orders.build_default_order_clients()

            self.assertEqual([client.authorization for client in mobo_clients], ["m1", "m2", "m3"])
            self.assertEqual([client.account for client in mobo_clients], ["mobo_1", "mobo_2", "mobo_3"])
            self.assertEqual([client.authorization for client in beidou_clients], ["b1", "b2"])
            self.assertEqual([client.account for client in beidou_clients], ["beidou_1", "beidou_2"])
        finally:
            config.MOBO_AUTHS = old_mobo_auths
            config.BEIDOU_AUTHS = old_beidou_auths

    def test_run_yesterday_orders_writes_stage_files_with_injected_clients(self):
        orders = importlib.import_module("drama_pipeline.7_yesterday_orders")
        models = importlib.import_module("drama_pipeline.3_models")

        class FakeClient:
            account = "a1"

            def fetch_orders(self, begin_date, end_date, account=""):
                return [models.OrderRecord(begin_date, "A", "Mobo", "英语", "MoboReels", 1, 2.0, account)]

        with tempfile.TemporaryDirectory() as tmp:
            paths = orders.run_yesterday_orders(
                "2026-04-18",
                "2026-04-18",
                output_root=Path(tmp),
                mobo_clients=[FakeClient()],
                beidou_clients=[],
            )

            self.assertTrue(paths["summary"].exists())


if __name__ == "__main__":
    unittest.main()
