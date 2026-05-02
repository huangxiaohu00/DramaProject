from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

import importlib
import requests

config = importlib.import_module("drama_pipeline.2_config")
models = importlib.import_module("drama_pipeline.3_models")
runtime = importlib.import_module("drama_pipeline.10_runtime")

from .common import (
    FetchStatsMixin,
    _beidou_app_id_for_theater,
    _beidou_list,
    _beidou_page_info,
    _ensure_beidou_order_success,
    _first_positive_amount,
    _lookup_metadata_from_records,
    _safe_float,
    _safe_int,
    normalize_theater_name,
)

class BeidouClient(FetchStatsMixin):
    def __init__(
        self,
        authorization: str = "",
        agent_id: int = config.BEIDOU_AGENT_ID,
        session: Optional[requests.Session] = None,
        logger: Any = None,
    ):
        super().__init__()
        self.authorization = authorization
        self.agent_id = agent_id
        self.session = session or requests.Session()
        self.logger = logger
        self.order_language_cache: Dict[str, str] = {}

    def build_task_page_request(
        self,
        language: Optional[int] = None,
        page_num: int = 1,
        page_size: int = 200,
        app_id: str = "",
        order_field: str = "publish_at",
        order_dir: str = "desc",
        search_title: str = "",
        campaign_status: int = 0,
    ) -> Tuple[str, Dict[str, str], Dict[str, Any]]:
        headers = {
            "Authorization": self.authorization,
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0",
        }
        params = {
            "task_type": 1,
            "page_num": page_num,
            "page_size": page_size,
            "app_id": app_id,
            "order_field": order_field,
            "order_dir": order_dir,
            "language": language if language is not None else "",
            "search_title": search_title,
            "campaign_status": campaign_status,
            "agent_id": self.agent_id,
        }
        return config.BEIDOU_TASK_PAGE_URL, headers, params

    def fetch_task_page(self, **kwargs: Any) -> Dict[str, Any]:
        url, headers, params = self.build_task_page_request(**kwargs)
        response = self.session.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    def fetch_new_dramas(self, language: str, theater: str = "") -> List[models.DramaRecord]:
        language_code = config.LANGUAGE_NAME_TO_CODE.get(language)
        app_id = _beidou_app_id_for_theater(theater)
        raw_rows: List[Dict[str, Any]] = []
        for page_num in range(1, int(config.NEW_DRAMA_FETCH_PAGE_COUNT or 1) + 1):
            data = self.fetch_task_page(
                language=language_code,
                app_id=app_id,
                page_num=page_num,
                page_size=int(config.NEW_DRAMA_FETCH_PAGE_SIZE or 200),
            )
            page_rows = _beidou_list(data)
            raw_rows.extend(page_rows)
            if len(page_rows) < int(config.NEW_DRAMA_FETCH_PAGE_SIZE or 200):
                break
        parsed = self.parse_task_items(raw_rows, language=language, source="beidou_new")
        stat_language = f"{language}/{normalize_theater_name(theater)}" if theater else language
        self._record_fetch_total("beidou_new", "原始返回", len(raw_rows), language=stat_language)
        self._record_parsed_rows("beidou_new", "解析成功", parsed)
        return parsed

    def fetch_income_dramas(self, language: str) -> List[models.DramaRecord]:
        language_code = config.LANGUAGE_NAME_TO_CODE.get(language)
        data = self.fetch_task_page(language=language_code, order_field="total_income")
        raw_rows = _beidou_list(data)
        parsed = self.parse_task_items(raw_rows, language=language, source="beidou_income")
        self._record_fetch_total("beidou_income", "原始返回", len(raw_rows), language=language)
        self._record_parsed_rows("beidou_income", "解析成功", parsed)
        return parsed

    def lookup_task_metadata(self, title: str, language: str = "") -> Dict[str, Any]:
        language_code = config.LANGUAGE_NAME_TO_CODE.get(runtime.normalize_language_name(language))
        for scoped_language in (language_code, None):
            data = self.fetch_task_page(language=scoped_language, search_title=title, page_size=20)
            records = self.parse_task_items(_beidou_list(data), source="beidou_lookup")
            metadata = _lookup_metadata_from_records(records, title, language)
            if metadata:
                return metadata
            if language_code is None:
                break
        return {}

    def fetch_english_hot_dramas(self) -> List[models.DramaRecord]:
        return self.fetch_income_dramas("英语")

    def build_order_request(self, begin_time: str, end_time: str) -> Tuple[str, Dict[str, str], Dict[str, Any]]:
        headers = {"Authorization": self.authorization, "Content-Type": "application/json"}
        params = {"start_time": begin_time, "end_time": end_time, "page_num": 1, "page_size": 100}
        return config.BEIDOU_ORDER_URL, headers, params

    def build_order_detail_request(self, begin_time: str, end_time: str, task_id: str) -> Tuple[str, Dict[str, str], Dict[str, Any]]:
        headers = {"Authorization": self.authorization, "Content-Type": "application/json"}
        params = {"start_time": begin_time, "end_time": end_time, "task_id": task_id}
        return config.BEIDOU_ORDER_DETAIL_URL, headers, params

    def resolve_order_language(self, item: Mapping[str, Any]) -> str:
        language = self._order_language_from_item(item)
        if language:
            return language
        title = str(item.get("serial_name") or item.get("title") or "")
        normalized = models.normalize_title(title)
        if not normalized:
            return ""
        if normalized in self.order_language_cache:
            return self.order_language_cache[normalized]
        try:
            metadata = self.lookup_task_metadata(title)
            language = runtime.normalize_language_name(metadata.get("language"))
        except Exception:
            language = ""
        self.order_language_cache[normalized] = language
        return language

    def _order_language_from_item(self, item: Mapping[str, Any]) -> str:
        language = runtime.normalize_language_name(item.get("language_str"))
        if language:
            return language
        for key in ("language", "language_id", "languageId", "language_code", "languageCode", "lang"):
            value = item.get(key)
            code = _safe_int(value)
            if code in config.LANGUAGE_CONFIG:
                return runtime.normalize_language_name(config.LANGUAGE_CONFIG[code])
            language = runtime.normalize_language_name(value)
            if language:
                return language
        return ""

    def fetch_orders(self, begin_date: str, end_date: str, account: str = "") -> List[models.OrderRecord]:
        raw_rows: List[Dict[str, Any]] = []
        total_count = 0
        page_size = 100
        page_num = 1
        while True:
            url, headers, params = self.build_order_request(begin_date, end_date)
            params["page_num"] = page_num
            params["page_size"] = page_size
            response = self.session.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            _ensure_beidou_order_success(data, account)
            page_rows = _beidou_list(data)
            raw_rows.extend(page_rows)
            page_info = _beidou_page_info(data)
            total_count = _safe_int(page_info.get("total_count")) or len(raw_rows)
            actual_page_size = _safe_int(page_info.get("page_size")) or page_size
            expected_pages = max(1, (total_count + actual_page_size - 1) // actual_page_size) if total_count else page_num
            if not page_rows or len(raw_rows) >= total_count or page_num >= expected_pages:
                break
            page_num += 1
        rows = []
        for item in raw_rows:
            order_count = _safe_int(item.get("total_recharge_count"))
            amount = _safe_float(item.get("total_recharge_income"))
            ad_amount = _first_positive_amount(
                item,
                "total_ad_income",
                "total_advert_income",
                "total_ad_revenue",
                "ad_income",
                "advert_income",
                "ad_amount",
                "total_cost",
                "cost",
            )
            has_order_amount = order_count > 0 and amount > 0
            if not has_order_amount and ad_amount <= 0:
                continue

            base = {
                "date": begin_date,
                "title": str(item.get("serial_name") or ""),
                "platform": "北斗",
                "language": self._order_language_from_item(item),
                "theater": normalize_theater_name(
                    config.THEATER_NAMES.get(str(item.get("app_id") or ""), str(item.get("app_id") or ""))
                ),
                "account": account,
                "task_id": str(item.get("task_id") or ""),
                "raw": dict(item),
            }
            if has_order_amount:
                rows.append(
                    models.OrderRecord(
                        **{**base, "language": base["language"] or self.resolve_order_language(item) or "全部"},
                        order_count=order_count,
                        amount=amount,
                        order_type="订单金额",
                    )
                )
            if ad_amount > 0:
                ad_language = base["language"]
                if ad_amount >= 10:
                    ad_language = ad_language or self.resolve_order_language(item)
                rows.append(
                    models.OrderRecord(
                        **{**base, "language": ad_language or "全部"},
                        order_count=1,
                        amount=ad_amount,
                        order_type="广告金额",
                    )
                )
        self.last_order_fetch_stats = {
            "raw_count": len(raw_rows),
            "parsed_count": len(rows),
            "total_count": total_count,
            "amount_sum": round(sum(float(row.amount or 0.0) for row in rows), 2),
        }
        return rows

    def parse_task_items(self, rows: Iterable[Dict[str, Any]], language: str = "", source: str = "beidou_new") -> List[models.DramaRecord]:
        parsed: List[models.DramaRecord] = []
        for index, row in enumerate(rows, 1):
            title = row.get("title") or row.get("serial_name") or ""
            if not title:
                continue
            app_id = str(row.get("app_id") or "")
            parsed.append(
                models.DramaRecord(
                    title=title,
                    language=runtime.normalize_language_name(language or config.LANGUAGE_CONFIG.get(row.get("language"), "")),
                    theater=normalize_theater_name(config.THEATER_NAMES.get(app_id, app_id)),
                    source=source,
                    rank=index,
                    publish_at=row.get("publish_at") or "",
                    task_id=str(row.get("task_id") or ""),
                    tags=[tag.strip() for tag in str(row.get("tag") or "").split(",") if tag.strip()],
                    raw=dict(row),
                )
            )
        return parsed


