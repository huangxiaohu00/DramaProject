from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

import importlib
import requests

config = importlib.import_module("drama_pipeline.2_config")
models = importlib.import_module("drama_pipeline.3_models")
runtime = importlib.import_module("drama_pipeline.10_runtime")

from .common import FetchStatsMixin, _safe_int, normalize_theater_name

class FeishuClient(FetchStatsMixin):
    def __init__(
        self,
        app_id: str = config.FEISHU_APP_ID,
        app_secret: str = config.FEISHU_APP_SECRET,
        app_token: str = config.FEISHU_APP_TOKEN,
        tables: Optional[Dict[str, str]] = None,
        session: Optional[requests.Session] = None,
        logger: Any = None,
    ):
        super().__init__()
        self.app_id = app_id
        self.app_secret = app_secret
        self.app_token = app_token
        self.tables = tables or dict(config.FEISHU_TABLES)
        self.session = session or requests.Session()
        self.logger = logger

    def build_token_request(self) -> Tuple[str, Dict[str, str], Dict[str, str]]:
        headers = {"Content-Type": "application/json"}
        payload = {"app_id": self.app_id, "app_secret": self.app_secret}
        return config.FEISHU_TOKEN_URL, headers, payload

    def fetch_token(self) -> str:
        url, headers, payload = self.build_token_request()
        response = self.session.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data.get("tenant_access_token", "")

    def build_records_request(
        self,
        tenant_token: str,
        table_id: str,
        page_token: str = "",
    ) -> Tuple[str, Dict[str, str], Dict[str, Any]]:
        url = config.FEISHU_RECORDS_URL_TEMPLATE.format(app_token=self.app_token, table_id=table_id)
        headers = {"Authorization": f"Bearer {tenant_token}"}
        params: Dict[str, Any] = {"page_size": 500}
        if page_token:
            params["page_token"] = page_token
        return url, headers, params

    def fetch_records(self, tenant_token: str, table_id: str) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        page_token = ""
        while True:
            url, headers, params = self.build_records_request(tenant_token, table_id, page_token=page_token)
            response = self.session.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            if data.get("code", 0) != 0:
                raise RuntimeError(f"Feishu records API error: {data.get('msg')}")
            body = data.get("data") or {}
            records.extend(body.get("items") or [])
            if not body.get("has_more"):
                break
            page_token = body.get("page_token") or ""
        return records

    def build_sheet_meta_request(self, tenant_token: str, spreadsheet_token: str) -> Tuple[str, Dict[str, str], Dict[str, Any]]:
        url = config.FEISHU_SHEET_META_URL_TEMPLATE.format(spreadsheet_token=spreadsheet_token)
        headers = {"Authorization": f"Bearer {tenant_token}"}
        return url, headers, {}

    def fetch_sheet_meta(self, tenant_token: str, spreadsheet_token: str) -> Dict[str, Any]:
        url, headers, params = self.build_sheet_meta_request(tenant_token, spreadsheet_token)
        response = self.session.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        if data.get("code", 0) != 0:
            raise RuntimeError(f"Feishu sheet meta API error: {data.get('msg')}")
        return data

    def build_sheet_values_request(
        self,
        tenant_token: str,
        spreadsheet_token: str,
        range_text: str,
    ) -> Tuple[str, Dict[str, str], Dict[str, Any]]:
        url = config.FEISHU_SHEET_VALUES_URL_TEMPLATE.format(spreadsheet_token=spreadsheet_token, range_text=range_text)
        headers = {"Authorization": f"Bearer {tenant_token}"}
        return url, headers, {}

    def fetch_sheet_values(self, tenant_token: str, spreadsheet_token: str, range_text: str) -> List[List[Any]]:
        url, headers, params = self.build_sheet_values_request(tenant_token, spreadsheet_token, range_text)
        response = self.session.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        if data.get("code", 0) != 0:
            raise RuntimeError(f"Feishu sheet values API error: {data.get('msg')}")
        return _feishu_sheet_values(data)

    def fetch_published(self, language: str) -> List[models.PublishedRecord]:
        table_id = self.tables.get(language)
        if not table_id:
            return []
        token = self.fetch_token()
        records = self.fetch_records(token, table_id)
        parsed = self.parse_published_records(language, records)
        self._record_fetch_total("published", "原始返回", len(records), language=language)
        self._record_parsed_rows("published", "解析成功", parsed)
        return parsed

    def fetch_beidou_hot_dramas(self) -> List[models.DramaRecord]:
        token = self.fetch_token()
        meta = self.fetch_sheet_meta(token, config.FEISHU_BEIDOU_HOT_SPREADSHEET_TOKEN)
        sheet_meta = next(
            (sheet for sheet in _feishu_sheet_list(meta) if _feishu_sheet_title(sheet) == config.FEISHU_BEIDOU_HOT_SHEET_NAME),
            None,
        )
        if not sheet_meta:
            return []
        sheet_id = _feishu_sheet_id(sheet_meta)
        row_count = max(_feishu_sheet_row_count(sheet_meta), 1)
        range_text = f"{sheet_id}!A1:G{row_count}"
        values = self.fetch_sheet_values(token, config.FEISHU_BEIDOU_HOT_SPREADSHEET_TOKEN, range_text)
        parsed = self.parse_beidou_hot_rows(sheet_meta, values)
        self._record_fetch_total("beidou_hot", "原始返回", len(values), sheet_name=_feishu_sheet_title(sheet_meta))
        self._record_parsed_rows("beidou_hot", "解析成功", parsed)
        return parsed

    @staticmethod
    def parse_field(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, list):
            return ",".join(FeishuClient.parse_field(item) for item in value)
        if isinstance(value, dict):
            return str(value.get("text") or value.get("name") or "")
        return str(value)

    @staticmethod
    def parse_sheet_cell(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, (int, float)):
            if isinstance(value, float) and value.is_integer():
                return str(int(value))
            return str(value)
        if isinstance(value, list):
            return "".join(part for part in (FeishuClient.parse_sheet_cell(item) for item in value) if part)
        if isinstance(value, dict):
            for key in ("text", "name", "value", "link", "url"):
                text = FeishuClient.parse_sheet_cell(value.get(key))
                if text:
                    return text
        return str(value).strip()

    def parse_published_records(self, language: str, records: Iterable[Dict[str, Any]]) -> List[models.PublishedRecord]:
        parsed: List[models.PublishedRecord] = []
        for record in records:
            fields = record.get("fields", {})
            title = self.parse_field(fields.get("剧集名称") or fields.get("剧名"))
            if not title:
                continue
            parsed.append(
                models.PublishedRecord(
                    title=title,
                    language=runtime.normalize_language_name(language),
                    theater=normalize_theater_name(self.parse_field(fields.get("剧场"))),
                    raw=dict(record),
                )
            )
        return parsed

    def parse_beidou_hot_rows(self, sheet_meta: Dict[str, Any], rows: Iterable[Iterable[Any]]) -> List[models.DramaRecord]:
        parsed: List[models.DramaRecord] = []
        header_map: Dict[str, int] = {}
        rank = 0
        sheet_title = _feishu_sheet_title(sheet_meta)
        sheet_id = _feishu_sheet_id(sheet_meta)

        for row in rows:
            cells = list(row or [])
            first_value = cells[0] if cells else None
            first_text = self.parse_sheet_cell(first_value)
            if first_text == "推荐日期":
                header_map = {
                    self.parse_sheet_cell(value): index
                    for index, value in enumerate(cells)
                    if self.parse_sheet_cell(value)
                }
                continue

            recommend_date = _parse_sheet_date(first_value)
            if not recommend_date or not header_map:
                continue

            language = runtime.normalize_language_name(self._sheet_value(cells, header_map, "语言"))
            if language != "英语":
                continue

            title = self._sheet_value(cells, header_map, "短剧名称", "剧集名称", "剧名")
            theater_text = self._sheet_value(cells, header_map, "剧场")
            if not title or not theater_text:
                continue

            rank += 1
            drama_type = self._sheet_value(cells, header_map, "剧集类型")
            reason = self._sheet_value(cells, header_map, "推荐理由")
            material = self._sheet_value(cells, header_map, "原片素材")
            theaters = _split_theaters(theater_text)
            if not theaters:
                continue

            raw = {
                key: self.parse_sheet_cell(cells[index]) if index < len(cells) else ""
                for key, index in header_map.items()
            }
            raw.update(
                {
                    "recommend_date": recommend_date,
                    "sheet_name": sheet_title,
                    "sheet_id": sheet_id,
                    "theater_raw": theater_text,
                    "recommend_reason": reason,
                    "material_hint": material,
                }
            )
            tags = [drama_type] if drama_type else []

            for theater in theaters:
                normalized_theater = normalize_theater_name(theater)
                parsed.append(
                    models.DramaRecord(
                        title=title,
                        language=language,
                        theater=normalized_theater,
                        source="beidou_hot",
                        rank=rank,
                        publish_at=recommend_date,
                        tags=tags,
                        raw=dict(raw, theater=normalized_theater),
                    )
                )
        return parsed

    def _sheet_value(self, cells: List[Any], header_map: Dict[str, int], *names: str) -> str:
        for name in names:
            index = header_map.get(name)
            if index is None or index >= len(cells):
                continue
            text = self.parse_sheet_cell(cells[index]).strip()
            if text:
                return text
        return ""


def _feishu_sheet_list(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    body = data.get("data") or {}
    if isinstance(body, dict):
        sheets = body.get("sheets") or body.get("items") or []
        if isinstance(sheets, list):
            return list(sheets)
    return []


def _feishu_sheet_title(sheet: Dict[str, Any]) -> str:
    return str(sheet.get("title") or sheet.get("name") or "").strip()


def _feishu_sheet_id(sheet: Dict[str, Any]) -> str:
    return str(sheet.get("sheet_id") or sheet.get("sheetId") or sheet.get("id") or "").strip()


def _feishu_sheet_row_count(sheet: Dict[str, Any]) -> int:
    grid = sheet.get("grid_properties") or sheet.get("gridProperties") or sheet.get("properties") or {}
    if isinstance(grid, dict):
        count = _safe_int(grid.get("row_count") or grid.get("rowCount"))
        if count > 0:
            return count
    return _safe_int(sheet.get("row_count") or sheet.get("rowCount"))


def _feishu_sheet_values(data: Dict[str, Any]) -> List[List[Any]]:
    body = data.get("data") or {}
    if isinstance(body, dict):
        for key in ("valueRange", "value_range"):
            value_range = body.get(key)
            if isinstance(value_range, dict) and isinstance(value_range.get("values"), list):
                return list(value_range.get("values") or [])
        if isinstance(body.get("values"), list):
            return list(body.get("values") or [])
    return []


def _parse_sheet_date(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return _excel_serial_to_date(value)

    text = FeishuClient.parse_sheet_cell(value)
    if not text:
        return ""
    if re.fullmatch(r"\d+(?:\.\d+)?", text):
        return _excel_serial_to_date(float(text))
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d", "%Y年%m月%d日", "%m/%d/%Y", "%m-%d-%Y"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def _excel_serial_to_date(value: float | int) -> str:
    try:
        serial = float(value)
    except Exception:
        return ""
    if serial < 30000:
        return ""
    return (datetime(1899, 12, 30) + timedelta(days=serial)).strftime("%Y-%m-%d")


def _split_theaters(value: Any) -> List[str]:
    text = FeishuClient.parse_sheet_cell(value)
    if not text:
        return []
    parts = [normalize_theater_name(part) for part in re.split("[,，/、|；;]+", text) if str(part).strip()]
    deduped: List[str] = []
    for part in parts:
        if part and part not in deduped:
            deduped.append(part)
    return deduped


