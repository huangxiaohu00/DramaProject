import importlib
import unittest


class CliTests(unittest.TestCase):
    def test_today_parse_args_accepts_language_list_and_date(self):
        today = importlib.import_module("drama_pipeline.6_today_recommend")

        args = today.parse_args(["--languages", "德语,法语", "--date", "2026-04-19"])

        self.assertEqual(args.languages, ["德语", "法语"])
        self.assertEqual(args.date, "2026-04-19")

    def test_orders_parse_args_accepts_begin_and_end(self):
        orders = importlib.import_module("drama_pipeline.7_yesterday_orders")

        args = orders.parse_args(["--begin", "2026-04-18", "--end", "2026-04-18"])

        self.assertEqual(args.begin, "2026-04-18")
        self.assertEqual(args.end, "2026-04-18")

    def test_orders_parse_args_allows_empty_for_date_dialog(self):
        orders = importlib.import_module("drama_pipeline.7_yesterday_orders")

        args = orders.parse_args([])

        self.assertEqual(args.begin, "")
        self.assertEqual(args.end, "")


if __name__ == "__main__":
    unittest.main()
