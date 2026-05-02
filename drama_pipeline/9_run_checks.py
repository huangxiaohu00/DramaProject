from __future__ import annotations

import importlib
import argparse
import sys
from pathlib import Path
from typing import Any, Dict


if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

runtime = importlib.import_module("drama_pipeline.10_runtime")


def run_offline_checks() -> Dict[str, Any]:
    config = importlib.import_module("drama_pipeline.2_config")
    models = importlib.import_module("drama_pipeline.3_models")
    rules = importlib.import_module("drama_pipeline.8_recommendation_rules")
    today = importlib.import_module("drama_pipeline.6_today_recommend")
    orders = importlib.import_module("drama_pipeline.7_yesterday_orders")

    assert len(config.LANGUAGE_CONFIG) == 7
    record = models.DramaRecord(title="A", language="英语", theater="ShortMax", source="mobo_recommend", rank=1)
    assert rules.classify_block_reason(record) == ""
    run = today.build_offline_recommendation(
        [record],
        [],
        {rules.material_key("英语", "A", "ShortMax"): config.VIDEO_THRESHOLD},
    )
    assert len(run.recommendations) == 1
    assert run.stats["fetched"]["all_candidates"] == 1
    assert run.stats["final"]["推荐结果"] == 1
    assert isinstance(run.stats.get("stage_rows"), list)
    assert any(row.get("阶段") == "最终推荐" for row in run.stats["stage_rows"])
    aggregated = orders.aggregate_orders(
        [
            models.OrderRecord("2026-04-18", "A", "Mobo", "英语", "MoboReels", 1, 1.5, "a"),
            models.OrderRecord("2026-04-18", "A", "Mobo", "英语", "MoboReels", 2, 2.5, "b"),
        ]
    )
    assert aggregated[0].order_count == 3
    return {"ok": True, "checks": ["config", "rules", "today_recommend", "today_stats", "yesterday_orders"]}


def run_live_smoke_checks(clients: Dict[str, Any] | None = None) -> Dict[str, Any]:
    config = importlib.import_module("drama_pipeline.2_config")
    today = importlib.import_module("drama_pipeline.6_today_recommend")

    if clients is None:
        missing = config.missing_today_config()
        if missing:
            return {"ok": False, "missing": missing}

    active_clients = clients or today.build_default_clients()
    result: Dict[str, Any] = {"ok": True}
    try:
        mobo_data = active_clients["mobo_client"].fetch_drama_page(
            language_code=config.MOBO_LANG_MAP["英语"],
            order_type=config.MOBO_ORDER_TYPE["new"],
            platform=config.MOBO_REELS_PLATFORM_ID,
            page_size=1,
        )
        beidou_data = active_clients["beidou_client"].fetch_task_page(
            language=config.LANGUAGE_NAME_TO_CODE["英语"],
            page_size=1,
        )
        token = active_clients["feishu_client"].fetch_token()
        result["mobo_new_count"] = _count_mobo_items(mobo_data)
        result["beidou_new_count"] = _count_beidou_items(beidou_data)
        result["feishu_token_ok"] = bool(token)
        if "duole_client" in active_clients:
            result["duole_live_count"] = _fetch_duole_live_rows(active_clients["duole_client"])
    except Exception as exc:
        result["ok"] = False
        result["error"] = f"{type(exc).__name__}: {exc}"
    return result


def _fetch_duole_live_rows(client: Any) -> int:
    if hasattr(client, "fetch_web_sheet_rows"):
        rows = client.fetch_web_sheet_rows() or {}
        if isinstance(rows, dict):
            return sum(len(value or []) for value in rows.values())
    if hasattr(client, "fetch_recommend_dramas"):
        rows = client.fetch_recommend_dramas() or []
        return len(rows)
    raise RuntimeError("Duole client missing live fetch method")


def _count_mobo_items(data: Dict[str, Any]) -> int:
    body = data.get("data") or {}
    if isinstance(body, dict):
        return len(body.get("list") or [])
    if isinstance(body, list):
        return len(body)
    return 0


def _count_beidou_items(data: Dict[str, Any]) -> int:
    body = data.get("body") or {}
    if isinstance(body, dict):
        return len(body.get("data") or [])
    if isinstance(body, list):
        return len(body)
    return 0


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="drama_pipeline checks")
    parser.add_argument("--live", action="store_true", help="run small live API smoke checks")
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    if args.live:
        runtime.bootstrap_runtime(config_module=importlib.import_module("drama_pipeline.2_config"))
        result = run_live_smoke_checks()
        if result["ok"]:
            print(
                "live smoke checks passed "
                f"(mobo_new_count={result.get('mobo_new_count')}, "
                f"beidou_new_count={result.get('beidou_new_count')}, "
                f"feishu_token_ok={result.get('feishu_token_ok')}, "
                f"duole_live_count={result.get('duole_live_count')})"
            )
        else:
            print(f"live smoke checks failed: {result}")
            raise SystemExit(1)
    else:
        result = run_offline_checks()
        if result["ok"]:
            print("offline checks passed")


if __name__ == "__main__":
    main()
