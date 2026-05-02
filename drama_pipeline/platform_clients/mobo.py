from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple

import importlib
import requests

config = importlib.import_module("drama_pipeline.2_config")
models = importlib.import_module("drama_pipeline.3_models")
runtime = importlib.import_module("drama_pipeline.10_runtime")

from .common import (
    FetchStatsMixin,
    _beidou_list,
    _ensure_mobo_order_success,
    _first_positive_amount,
    _lookup_metadata_from_records,
    _mobo_list,
    _mobo_platform_id_for_theater,
    _safe_float,
    _safe_int,
    _titles_match,
    normalize_theater_name,
)
from .beidou import BeidouClient

class MoboClient(FetchStatsMixin):
    def __init__(self, authorization: str = "", session: Optional[requests.Session] = None, logger: Any = None):
        super().__init__()
        self.authorization = authorization
        self.session = session or requests.Session()
        self.language_cache: Dict[str, str] = {}
        self.logger = logger

    def build_drama_request(
        self,
        language_code: Optional[int] = None,
        order_type: int = 0,
        platform: Optional[int] = None,
        page_index: int = 1,
        page_size: int = 200,
        name: str = "",
        audio_type: int = 0,
        local_type: int = 0,
    ) -> Tuple[str, Dict[str, str], Dict[str, Any]]:
        headers = {"Authorization": self.authorization, "Content-Type": "application/json"}
        payload = {
            "name": name,
            "lang": language_code,
            "platform": platform,
            "audioType": audio_type,
            "localType": local_type,
            "orderType": order_type,
            "pageIndex": page_index,
            "projectType": 2,
            "pageSize": page_size,
        }
        return config.MOBO_URL, headers, payload

    def fetch_drama_page(self, **kwargs: Any) -> Dict[str, Any]:
        url, headers, payload = self.build_drama_request(**kwargs)
        response = self.session.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        return response.json()

    def fetch_new_dramas(self, language: str, theater: str = "") -> List[models.DramaRecord]:
        payload_language = config.MOBO_LANG_MAP.get(language)
        platform_id = _mobo_platform_id_for_theater(theater) or config.MOBO_REELS_PLATFORM_ID
        raw_rows: List[Dict[str, Any]] = []
        for page_index in range(1, int(config.NEW_DRAMA_FETCH_PAGE_COUNT or 1) + 1):
            data = self.fetch_drama_page(
                language_code=payload_language,
                order_type=config.MOBO_ORDER_TYPE["new"],
                platform=platform_id,
                page_index=page_index,
                page_size=int(config.NEW_DRAMA_FETCH_PAGE_SIZE or 200),
            )
            page_rows = _mobo_list(data)
            raw_rows.extend(page_rows)
            if len(page_rows) < int(config.NEW_DRAMA_FETCH_PAGE_SIZE or 200):
                break
        parsed = self.parse_drama_items(raw_rows, language=runtime.normalize_language_name(language), source="mobo_new")
        stat_language = f"{language}/{normalize_theater_name(theater)}" if theater else language
        self._record_fetch_total("mobo_new", "原始返回", len(raw_rows), language=stat_language)
        self._record_parsed_rows("mobo_new", "解析成功", parsed)
        return parsed

    def fetch_recommend_dramas(self, language: str, theater: str = "") -> List[models.DramaRecord]:
        payload_language = config.MOBO_LANG_MAP.get(language)
        platform_id = _mobo_platform_id_for_theater(theater) if theater else None
        data = self.fetch_drama_page(
            language_code=payload_language,
            order_type=config.MOBO_ORDER_TYPE["recommend"],
            platform=platform_id,
        )
        raw_rows = _mobo_list(data)
        parsed = self.parse_drama_items(raw_rows, language=runtime.normalize_language_name(language), source="mobo_recommend")
        stat_language = f"{language}/{normalize_theater_name(theater)}" if theater else language
        self._record_fetch_total("mobo_recommend", "原始返回", len(raw_rows), language=stat_language)
        self._record_parsed_rows("mobo_recommend", "解析成功", parsed)
        return parsed

    def lookup_drama_metadata(self, title: str, language: str = "") -> Dict[str, Any]:
        language_code = config.MOBO_LANG_MAP.get(runtime.normalize_language_name(language))
        for scoped_language in (language_code, None):
            data = self.fetch_drama_page(
                name=title,
                language_code=scoped_language,
                order_type=config.MOBO_ORDER_TYPE["new"],
                platform=None,
                page_size=20,
            )
            records = self.parse_drama_items(_mobo_list(data), source="mobo_lookup")
            metadata = _lookup_metadata_from_records(records, title, language)
            if metadata:
                return metadata
            if language_code is None:
                break
        return {}

    def build_order_request(self, begin_time: str, end_time: str) -> Tuple[str, Dict[str, str], Dict[str, Any]]:
        headers = {"Authorization": self.authorization, "Content-Type": "application/json"}
        payload = {
            "range": None,
            "beginTime": begin_time,
            "endTime": end_time,
            "appTypeList": ["683001001"],
        }
        return config.MOBO_ORDER_URL, headers, payload

    def resolve_order_language(self, title: str) -> str:
        normalized = models.normalize_title(title)
        if not normalized:
            return "未知"
        if normalized in self.language_cache:
            return self.language_cache[normalized]

        language = self._fetch_mobo_language(title)
        if not language:
            try:
                language = self._fetch_beidou_language(title)
            except Exception:
                language = ""
        language = language or "未知"
        self.language_cache[normalized] = language
        return language

    def _fetch_mobo_language(self, title: str) -> str:
        data = self.fetch_drama_page(
            name=title,
            order_type=config.MOBO_ORDER_TYPE["new"],
            platform=None,
            page_size=10,
        )
        for item in _mobo_list(data):
            series_name = item.get("seriesName") or item.get("name") or ""
            if _titles_match(title, series_name):
                language = runtime.normalize_language_name(item.get("languageName"))
                if language:
                    return language
        return ""

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
            language = runtime.normalize_language_name(item.get("language_str"))
            if language:
                return language
            language_code = item.get("language")
            if language_code in config.LANGUAGE_CONFIG:
                return runtime.normalize_language_name(config.LANGUAGE_CONFIG[language_code])
        return ""

    def fetch_orders(self, begin_date: str, end_date: str, account: str = "") -> List[models.OrderRecord]:
        url, headers, payload = self.build_order_request(begin_date, end_date)
        response = self.session.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()
        _ensure_mobo_order_success(data, account)
        raw_items = _mobo_list(data)
        rows = []
        for item in raw_items:
            order_count = _safe_int(item.get("num"))
            amount = _safe_float(item.get("rmbRealIncome"))
            ad_field_amount = _first_positive_amount(
                item,
                "rmbNonOrderIncome",
                "rmbNonOrderAmount",
                "rmbNoOrderIncome",
                "rmbNoOrderAmount",
                "rmbNotOrderIncome",
                "rmbNotOrderAmount",
                "nonOrderIncome",
                "nonOrderAmount",
                "non_order_income",
                "non_order_amount",
                "noOrderIncome",
                "noOrderAmount",
                "no_order_income",
                "no_order_amount",
                "notOrderIncome",
                "notOrderAmount",
                "not_order_income",
                "not_order_amount",
                "rmbAdIncome",
                "rmbAdvertIncome",
                "rmbAdRealIncome",
                "adIncome",
                "ad_income",
                "adAmount",
                "ad_amount",
                "rmbOtherIncome",
                "rmbOtherAmount",
                "otherIncome",
                "otherAmount",
                "rmbConsume",
                "consume",
                "cost",
            )
            is_ad_order = _safe_int(item.get("adType")) == 1
            ad_amount = amount if is_ad_order and amount > 0 else ad_field_amount
            has_order_amount = not is_ad_order and order_count > 0 and amount > 0
            has_ad_amount = ad_amount > 0 and (is_ad_order or ad_field_amount > 0)
            if not has_order_amount and not has_ad_amount:
                continue

            title = str(item.get("dataName") or "")
            language = runtime.normalize_language_name(item.get("languageName"))
            if not language:
                language = self.resolve_order_language(title)
            base = {
                "date": begin_date,
                "title": title,
                "platform": "Mobo",
                "language": runtime.normalize_language_name(language),
                "theater": normalize_theater_name(item.get("appName") or "MoboReels"),
                "account": account,
                "task_id": str(item.get("taskId") or item.get("task_id") or ""),
                "raw": dict(item),
            }
            if has_order_amount:
                rows.append(
                    models.OrderRecord(
                        **base,
                        order_count=order_count,
                        amount=amount,
                        order_type="订单金额",
                    )
                )
            if has_ad_amount:
                rows.append(
                    models.OrderRecord(
                        **base,
                        order_count=1,
                        amount=ad_amount,
                        order_type="广告金额",
                    )
                )
        self.last_order_fetch_stats = {
            "raw_count": len(raw_items),
            "parsed_count": len(rows),
            "total_count": len(raw_items),
            "amount_sum": round(sum(float(row.amount or 0.0) for row in rows), 2),
        }
        return rows

    def parse_drama_items(self, rows: Iterable[Dict[str, Any]], language: str = "", source: str = "") -> List[models.DramaRecord]:
        parsed: List[models.DramaRecord] = []
        for index, row in enumerate(rows, 1):
            title = row.get("seriesName") or row.get("name") or ""
            if not title:
                continue
            parsed.append(
                models.DramaRecord(
                    title=title,
                    language=runtime.normalize_language_name(language or row.get("languageName", "")),
                    theater=normalize_theater_name(row.get("agencyName") or row.get("appName") or ""),
                    source=source,
                    rank=index,
                    publish_at=row.get("createTime") or row.get("publishTime") or "",
                    task_id=str(row.get("seriesId") or ""),
                    tags=[str(tag) for tag in row.get("seriesTypeList") or []],
                    raw=dict(row),
                )
            )
        return parsed


