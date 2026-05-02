from __future__ import annotations

import importlib
import time

config = importlib.import_module("drama_pipeline.2_config")
models = importlib.import_module("drama_pipeline.3_models")

from drama_pipeline.platform_clients import (  # noqa: F401
    BeidouClient,
    DuoleClient,
    FeishuClient,
    MaterialClient,
    MaterialRateLimitError,
    MaterialServiceDegradedError,
    MoboClient,
    OrderAccountInvalidError,
    format_material_item,
    normalize_theater_name,
)
from drama_pipeline.platform_clients import duole as _duole
from drama_pipeline.platform_clients import mobo as _mobo
from drama_pipeline.platform_clients.beidou import _beidou_app_id_for_theater, _beidou_list
from drama_pipeline.platform_clients.common import (  # noqa: F401
    FetchStatsMixin,
    _append_fetch_stat_row,
    _append_model_stat_rows,
    _beidou_page_info,
    _ensure_beidou_order_success,
    _ensure_mobo_order_success,
    _first_positive_amount,
    _language_pair_sort_key,
    _language_sort_key,
    _log_info,
    _log_warning,
    _lookup_metadata_from_records,
    _mobo_list,
    _mobo_platform_id_for_theater,
    _safe_float,
    _safe_int,
    _titles_match,
)
from drama_pipeline.platform_clients.duole import (  # noqa: F401
    _build_duole_raw_row,
    _build_duole_web_sheet_configs,
    _choose_duole_title,
    _create_duole_edge_driver,
    _duole_fixed_value,
    _duole_note_matches,
    _fetch_duole_range_values_web,
    _fetch_duole_records_from_web,
    _fetch_duole_sheet_index_web,
    _fetch_duole_sheet_matrix_web,
    _fetch_duole_sheet_rows_batch,
    _fetch_duole_sheet_rows_one_by_one,
    _fill_down_duole_dates,
    _format_datetime,
    _format_duole_date_value,
    _get_duole_fetch_row_limit,
    _has_any_duole_sheet_rows,
    _infer_duole_language,
    _infer_duole_theater,
    _inject_duole_cookie,
    _is_duole_login_url,
    _looks_like_duole_audience,
    _looks_like_duole_id,
    _looks_like_duole_language,
    _looks_like_duole_link,
    _make_unique_headers,
    _normalize_duole_date_columns,
    _normalize_duole_sheet_row,
    _parse_cookie_pairs,
    _parse_datetime,
    _parse_duole_cookie_payload,
    _parse_duole_sheet_matrix,
    _refresh_duole_cookie_via_playwright,
    _save_duole_cookie_to_config,
    _serialize_duole_cookies,
    _wait_for_duole_api,
)
from drama_pipeline.platform_clients.feishu import (  # noqa: F401
    _excel_serial_to_date,
    _feishu_sheet_id,
    _feishu_sheet_list,
    _feishu_sheet_row_count,
    _feishu_sheet_title,
    _feishu_sheet_values,
    _parse_sheet_date,
    _split_theaters,
)
from drama_pipeline.platform_clients.material import _material_list, _material_total  # noqa: F401

# Re-export patchable browser globals for legacy tests and callers.
webdriver = _duole.webdriver
TimeoutException = _duole.TimeoutException
EdgeOptions = _duole.EdgeOptions
WebDriverWait = _duole.WebDriverWait
sync_playwright = _duole.sync_playwright

_original_fetch_duole_records_from_web = _duole._fetch_duole_records_from_web


def _sync_duole_patchables() -> None:
    _duole.webdriver = webdriver
    _duole.TimeoutException = TimeoutException
    _duole.EdgeOptions = EdgeOptions
    _duole.WebDriverWait = WebDriverWait
    _duole.sync_playwright = sync_playwright
    _duole._refresh_duole_cookie_via_playwright = _refresh_duole_cookie_via_playwright
    _duole._save_duole_cookie_to_config = _save_duole_cookie_to_config
    _duole._create_duole_edge_driver = _create_duole_edge_driver
    _duole._inject_duole_cookie = _inject_duole_cookie
    _duole._wait_for_duole_api = _wait_for_duole_api
    _duole._fetch_duole_sheet_rows_batch = _fetch_duole_sheet_rows_batch
    _duole._fetch_duole_sheet_rows_one_by_one = _fetch_duole_sheet_rows_one_by_one
    _duole._fetch_duole_sheet_index_web = _fetch_duole_sheet_index_web
    _duole._fetch_duole_sheet_matrix_web = _fetch_duole_sheet_matrix_web
    _duole._parse_duole_sheet_matrix = _parse_duole_sheet_matrix


class DuoleClient(_duole.DuoleClient):
    def fetch_web_sheet_rows(self):
        _sync_duole_patchables()
        _duole._fetch_duole_records_from_web = _fetch_duole_records_from_web
        return super().fetch_web_sheet_rows()


class MoboClient(_mobo.MoboClient):
    def _fetch_beidou_language(self, title: str) -> str:
        client = BeidouClient(
            authorization=config.BEIDOU_DRAMA_AUTH,
            agent_id=config.BEIDOU_AGENT_ID,
            session=self.session,
        )
        data = client.fetch_task_page(search_title=title, page_size=10)
        for item in _beidou_list(data):
            candidate_title = item.get("title") or item.get("serial_name") or ""
            if not _titles_match(title, candidate_title):
                continue
            language = importlib.import_module("drama_pipeline.10_runtime").normalize_language_name(item.get("language_str"))
            if language:
                return language
            language_code = item.get("language")
            if language_code in config.LANGUAGE_CONFIG:
                return importlib.import_module("drama_pipeline.10_runtime").normalize_language_name(config.LANGUAGE_CONFIG[language_code])
        return ""


def _fetch_duole_records_from_web(share_url, cookie, target_sheets, logger=None):
    _sync_duole_patchables()
    return _original_fetch_duole_records_from_web(share_url, cookie, target_sheets, logger=logger)
