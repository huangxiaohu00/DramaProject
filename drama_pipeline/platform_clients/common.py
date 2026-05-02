from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, Sequence

import importlib

config = importlib.import_module("drama_pipeline.2_config")
models = importlib.import_module("drama_pipeline.3_models")
runtime = importlib.import_module("drama_pipeline.10_runtime")

class OrderAccountInvalidError(RuntimeError):
    pass


def _log_warning(logger: Any, message: str) -> None:
    if logger is not None and hasattr(logger, "warning"):
        logger.warning(message)
        return
    print(message)


def _log_info(logger: Any, message: str) -> None:
    if logger is not None and hasattr(logger, "info"):
        logger.info(message)
        return
    print(message)


class FetchStatsMixin:
    def __init__(self):
        self.fetch_stats: List[Dict[str, Any]] = []

    def reset_fetch_stats(self) -> None:
        self.fetch_stats.clear()

    def consume_fetch_stats(self) -> List[Dict[str, Any]]:
        rows = list(self.fetch_stats)
        self.fetch_stats.clear()
        return rows

    def _record_fetch_total(self, source: str, stage: str, count: int, language: str = "", sheet_name: str = "") -> None:
        _append_fetch_stat_row(self.fetch_stats, stage, source, "全部", "全部", count)
        if language:
            _append_fetch_stat_row(self.fetch_stats, stage, source, "语言", language, count)
        if sheet_name:
            _append_fetch_stat_row(self.fetch_stats, stage, source, "Sheet", sheet_name, count)

    def _record_parsed_rows(self, source: str, stage: str, rows: Iterable[Any]) -> None:
        _append_model_stat_rows(self.fetch_stats, stage, source, rows)


def _append_fetch_stat_row(
    store: List[Dict[str, Any]],
    stage: str,
    source: str,
    dimension: str,
    value: str,
    count: int,
) -> None:
    store.append(
        {
            "阶段": stage,
            "来源": source,
            "维度": dimension,
            "值": value,
            "条数": int(count),
        }
    )


def _append_model_stat_rows(
    store: List[Dict[str, Any]],
    stage: str,
    source: str,
    rows: Iterable[Any],
) -> None:
    row_list = list(rows)
    _append_fetch_stat_row(store, stage, source, "全部", "全部", len(row_list))

    language_counts: Dict[str, int] = {}
    theater_counts: Dict[str, int] = {}
    pair_counts: Dict[str, int] = {}
    sheet_counts: Dict[str, int] = {}

    for row in row_list:
        language = str(getattr(row, "language", "") or "").strip()
        theater = normalize_theater_name(str(getattr(row, "theater", "") or "").strip())
        raw = getattr(row, "raw", {}) or {}
        sheet_name = str(raw.get("sheet_name") or "").strip()

        if language:
            language_counts[language] = language_counts.get(language, 0) + 1
        if theater:
            theater_counts[theater] = theater_counts.get(theater, 0) + 1
        if language and theater:
            pair_key = f"{language} / {theater}"
            pair_counts[pair_key] = pair_counts.get(pair_key, 0) + 1
        if sheet_name:
            sheet_counts[sheet_name] = sheet_counts.get(sheet_name, 0) + 1

    for language in sorted(language_counts, key=lambda item: (_language_sort_key(item), item)):
        _append_fetch_stat_row(store, stage, source, "语言", language, language_counts[language])
    for theater in sorted(theater_counts):
        _append_fetch_stat_row(store, stage, source, "剧场", theater, theater_counts[theater])
    for pair_key in sorted(pair_counts, key=lambda item: (_language_pair_sort_key(item), item)):
        _append_fetch_stat_row(store, stage, source, "语言+剧场", pair_key, pair_counts[pair_key])
    for sheet_name in sorted(sheet_counts):
        _append_fetch_stat_row(store, stage, source, "Sheet", sheet_name, sheet_counts[sheet_name])


def _language_sort_key(language: str) -> int:
    try:
        return config.LANGUAGE_ORDER.index(language)
    except ValueError:
        return len(config.LANGUAGE_ORDER)


def _language_pair_sort_key(pair_text: str) -> int:
    language = pair_text.split(" / ", 1)[0].strip()
    return _language_sort_key(language)


def _mobo_list(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    body = data.get("data") or {}
    if isinstance(body, dict):
        return list(body.get("list") or [])
    if isinstance(body, list):
        return list(body)
    return []


def _beidou_list(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    body = data.get("body") or {}
    if isinstance(body, dict):
        return list(body.get("data") or [])
    if isinstance(body, list):
        return list(body)
    return []


def _beidou_page_info(data: Mapping[str, Any]) -> Dict[str, Any]:
    body = data.get("body") or {}
    if isinstance(body, dict) and isinstance(body.get("page"), dict):
        return dict(body.get("page") or {})
    return {}


def _ensure_mobo_order_success(data: Mapping[str, Any], account: str = "") -> None:
    code = data.get("code")
    status = data.get("status")
    message = str(data.get("message") or data.get("msg") or data.get("statusMsg") or "").strip()
    if _safe_int(code) == 200 and status is not False:
        return
    if status is True and code in (None, ""):
        return
    if code in (None, "") and status is None and "data" in data and not _looks_like_auth_failure(message):
        return
    hint = f" account={account}" if account else ""
    raise OrderAccountInvalidError(
        f"Mobo order account invalid{hint}: code={code} status={status} message={message or 'unknown'}"
    )


def _ensure_beidou_order_success(data: Mapping[str, Any], account: str = "") -> None:
    code = data.get("code")
    message = str(data.get("msg") or data.get("message") or data.get("statusMsg") or "").strip()
    if _safe_int(code) == 0 and not _looks_like_auth_failure(message):
        return
    hint = f" account={account}" if account else ""
    raise OrderAccountInvalidError(
        f"Beidou order account invalid{hint}: code={code} message={message or 'unknown'}"
    )


def _looks_like_auth_failure(message: str) -> bool:
    lowered = str(message or "").strip().lower()
    if not lowered:
        return False
    markers = ("登录", "未登录", "失效", "过期", "无权限", "unauthor", "login", "token", "expired", "forbidden")
    return any(marker in lowered for marker in markers)


def _lookup_metadata_from_records(
    records: Sequence[models.DramaRecord],
    title: str,
    language: str = "",
) -> Dict[str, Any]:
    normalized_language = runtime.normalize_language_name(language)
    matches = [record for record in records if _titles_match(title, record.title)]
    if not matches:
        return {}
    if normalized_language:
        same_language = [
            record
            for record in matches
            if not runtime.normalize_language_name(record.language)
            or runtime.normalize_language_name(record.language) == normalized_language
        ]
        if not same_language:
            return {}
        matches = same_language
    primary = max(
        matches,
        key=lambda record: (_parse_lookup_datetime(record.publish_at) is not None, _parse_lookup_datetime(record.publish_at) or datetime.min),
    )
    publish_at = _latest_lookup_publish_at(record.publish_at for record in matches)
    tags = sorted({tag.strip() for record in matches for tag in record.tags if str(tag).strip()})
    return {
        "title": primary.title,
        "language": runtime.normalize_language_name(primary.language) or normalized_language,
        "theater": normalize_theater_name(primary.theater),
        "publish_at": publish_at,
        "tags": tags,
    }


def _parse_lookup_datetime(value: Any) -> datetime | None:
    text = "" if value is None else str(value).strip()
    if not text:
        return None
    normalized = text.replace("/", "-").replace("T", " ").strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1]
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _format_lookup_datetime(value: datetime) -> str:
    if value.hour == 0 and value.minute == 0 and value.second == 0 and value.microsecond == 0:
        return value.strftime("%Y-%m-%d")
    return value.replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")


def _latest_lookup_publish_at(values: Iterable[Any]) -> str:
    latest: datetime | None = None
    text_values: List[str] = []
    for value in values:
        text = "" if value is None else str(value).strip()
        if not text:
            continue
        text_values.append(text)
        parsed = _parse_lookup_datetime(text)
        if parsed is None:
            continue
        if latest is None or parsed > latest:
            latest = parsed
    if latest is not None:
        return _format_lookup_datetime(latest)
    return text_values[0] if text_values else ""


def _titles_match(left: Any, right: Any) -> bool:
    return models.normalize_title(left) == models.normalize_title(right)


def normalize_theater_name(value: str) -> str:
    return config.normalize_theater_name(value)


def _beidou_app_id_for_theater(theater: str) -> str:
    normalized = normalize_theater_name(theater)
    if not normalized:
        return ""
    return str(config.THEATER_NAME_TO_ID.get(normalized, "") or "")


def _mobo_platform_id_for_theater(theater: str) -> int | None:
    normalized = normalize_theater_name(theater)
    if not normalized:
        return None
    platform_id = dict(getattr(config, "MOBO_PLATFORM_IDS", {}) or {}).get(normalized)
    if platform_id is None:
        return None
    try:
        return int(platform_id)
    except (TypeError, ValueError):
        return None


def format_material_item(item: Dict[str, Any]) -> Dict[str, Any]:
    product = item.get("product") or {}
    product_name = normalize_theater_name(str(product.get("productName") or item.get("productName") or ""))
    video_list = item.get("videoList") or []
    duration_millis = _safe_float(item.get("durationMillis"))
    duration_min = round(duration_millis / 60000, 2) if duration_millis else _safe_float(item.get("durationMin"))
    return {
        "playletName": item.get("playletName", ""),
        "exposureNum": _safe_int(item.get("exposureNum")),
        "durationMin": duration_min,
        "videoUrl": video_list[0] if video_list else item.get("videoUrl", ""),
        "productName": product_name,
    }


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _first_positive_amount(row: Mapping[str, Any], *keys: str) -> float:
    for key in keys:
        value = _safe_float(row.get(key))
        if value > 0:
            return value
    return 0.0
