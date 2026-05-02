from __future__ import annotations

import importlib
import math
import re
from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Sequence, Tuple


config = importlib.import_module("drama_pipeline.2_config")
models = importlib.import_module("drama_pipeline.3_models")
tag_scoring = importlib.import_module("drama_pipeline.8_tag_scoring")


@dataclass(frozen=True)
class _DateSignalHit:
    source_key: str
    days_ago: int
    date_text: str


PRIMARY_THEATER_RAW_KEY = "primary_theater"
MERGED_THEATERS_RAW_KEY = "merged_theaters"
PRIMARY_PUBLISH_AT_RAW_KEY = "primary_publish_at"
PUBLISH_VALUES_RAW_KEY = "publish_values"
EARLIEST_PUBLISH_AT_RAW_KEY = "earliest_publish_at"
LATEST_PUBLISH_AT_RAW_KEY = "latest_publish_at"
MOBOREELS_PUBLISH_AT_RAW_KEY = "moboreels_publish_at"
FRESHNESS_PUBLISH_AT_RAW_KEY = "freshness_publish_at"
ALL_CONTENT_TAG_VALUES_RAW_KEY = "all_content_tag_values"
PRIMARY_CONTENT_TAG_VALUES_RAW_KEY = "primary_content_tag_values"


def normalize_title(title: str) -> str:
    return models.normalize_title(title)


def filter_published(
    candidates: Sequence[models.DramaRecord],
    published: Sequence[models.PublishedRecord],
) -> Tuple[List[models.DramaRecord], List[models.FilterRecord]]:
    return filter_title_blocks(candidates, published, "飞书已发布")


def filter_title_blocks(
    candidates: Sequence[models.DramaRecord],
    blocked_records: Sequence[object],
    reason: str,
) -> Tuple[List[models.DramaRecord], List[models.FilterRecord]]:
    blocked_keys = {
        item.match_key
        for item in blocked_records
        if hasattr(item, "match_key")
    }
    kept: List[models.DramaRecord] = []
    blocked: List[models.FilterRecord] = []
    for candidate in candidates:
        if candidate.match_key in blocked_keys:
            blocked.append(_blocked(candidate, reason))
        else:
            kept.append(candidate)
    return kept, blocked


def classify_block_reason(record: models.DramaRecord) -> str:
    if is_adult_title(record.title) or is_adult_content(record):
        return "\u6210\u4eba\u6807\u9898\u62e6\u622a"
    return ""


def is_adult_title(title: str) -> bool:
    normalized = normalize_title(title).lower()
    return any(keyword.lower() in normalized for keyword in config.ADULT_TITLE_KEYWORDS)


def is_adult_content(record: models.DramaRecord) -> bool:
    return any(
        _matches_content_keyword(token, keyword)
        for token in _all_content_tag_tokens(record)
        for keyword in config.ADULT_CONTENT_KEYWORDS
    )


def is_ai_or_anime(record: models.DramaRecord) -> bool:
    return is_ai_content(record) or is_anime_content(record)


def content_origin(record: models.DramaRecord) -> str:
    tokens = _priority_content_tag_tokens(record)
    origin = _content_origin_from_tokens(tokens)
    if origin != "unknown":
        return origin
    return _content_origin_from_tokens(_all_content_tag_tokens(record))


def _content_origin_from_tokens(tokens: Sequence[str]) -> str:
    has_local = any(
        _matches_content_keyword(token, keyword)
        for token in tokens
        for keyword in config.LOCAL_DRAMA_KEYWORDS
    )
    has_translated = any(
        _matches_content_keyword(token, keyword)
        for token in tokens
        for keyword in config.TRANSLATED_DRAMA_KEYWORDS
    )
    if has_local and not has_translated:
        return "local"
    if has_translated and not has_local:
        return "translated"
    if has_local:
        return "local"
    if has_translated:
        return "translated"
    return "unknown"


def is_ai_content(record: models.DramaRecord) -> bool:
    return any(
        _matches_content_keyword(token, keyword)
        for token in _all_content_tag_tokens(record)
        for keyword in config.AI_CONTENT_KEYWORDS
    )


def is_anime_content(record: models.DramaRecord) -> bool:
    return any(
        _matches_content_keyword(token, keyword)
        for token in _all_content_tag_tokens(record)
        for keyword in config.ANIME_CONTENT_KEYWORDS
    )


def content_type_multiplier(record: models.DramaRecord) -> float:
    has_ai = is_ai_content(record)
    has_anime = is_anime_content(record)
    if has_anime:
        return config.ANIME_CONTENT_MULTIPLIER
    if has_ai:
        return config.AI_CONTENT_MULTIPLIER
    return 1.0


def content_labels(record: models.DramaRecord) -> List[str]:
    labels: List[str] = []
    if is_ai_content(record):
        labels.append("AI剧")
    if is_anime_content(record):
        labels.append("漫剧")
    return labels


def filter_blocked_content(
    candidates: Sequence[models.DramaRecord],
) -> Tuple[List[models.DramaRecord], List[models.FilterRecord]]:
    kept: List[models.DramaRecord] = []
    blocked: List[models.FilterRecord] = []
    for candidate in candidates:
        reason = classify_block_reason(candidate)
        if reason:
            blocked.append(_blocked(candidate, reason))
        else:
            kept.append(candidate)
    return kept, blocked


def dedupe_candidates(candidates: Sequence[models.DramaRecord]) -> List[models.DramaRecord]:
    grouped: Dict[Tuple[str, str], List[models.DramaRecord]] = defaultdict(list)
    for candidate in candidates:
        grouped[(candidate.language, candidate.title_norm)].append(candidate)

    deduped = [_merge_candidate_group(language, rows) for (language, _title_norm), rows in grouped.items()]
    return sorted(deduped, key=lambda item: (language_sort_index(item.language), primary_theater(item), item.rank, item.title_norm))


def _merge_candidate_group(language: str, rows: Sequence[models.DramaRecord]) -> models.DramaRecord:
    moboreels_rows = [row for row in rows if normalize_theater(row.theater) == "MoboReels"]
    has_moboreels = bool(moboreels_rows)
    base_rows = moboreels_rows if has_moboreels else rows
    base_primary = _pick_group_primary_row(base_rows, prefer_mobo=has_moboreels)

    primary_theater_name = "MoboReels" if has_moboreels else normalize_theater(base_primary.theater)
    theaters = _merge_theaters_with_primary(rows, primary_theater_name)
    sources = sorted({row.source for row in rows if row.source})
    theme_tags = _merge_theme_tags(rows, primary_theater_name if has_moboreels else "")
    all_content_tags = _merge_content_tags(rows)
    primary_content_tags = _merge_content_tags(base_rows if has_moboreels else rows)
    source_rank_details = _merge_source_rank_details(rows)
    source_date_details = _merge_source_date_details(rows)
    earliest_publish_at = _earliest_datetime_text(_row_publish_time(row) for row in rows)
    latest_publish_at = _latest_datetime_text(_row_publish_time(row) for row in rows)
    moboreels_publish_at = _earliest_datetime_text(_row_publish_time(row) for row in moboreels_rows)
    if has_moboreels:
        freshness_publish_at = moboreels_publish_at or earliest_publish_at or latest_publish_at
    else:
        freshness_publish_at = earliest_publish_at or latest_publish_at
    raw = _merge_candidate_raw(
        rows=rows,
        primary=base_primary,
        theaters=theaters,
        tags=theme_tags,
        publish_at=freshness_publish_at,
        earliest_publish_at=earliest_publish_at,
        latest_publish_at=latest_publish_at,
        moboreels_publish_at=moboreels_publish_at,
        all_content_tags=all_content_tags,
        primary_content_tags=primary_content_tags,
    )
    return replace(
        base_primary,
        language=language,
        theater=",".join(theaters) if theaters else normalize_theater(base_primary.theater),
        source=",".join(sources),
        tags=theme_tags,
        raw=raw,
        rank=min((row.rank or 999 for row in rows), default=999),
        source_rank_details=source_rank_details,
        source_date_details=source_date_details,
        publish_at=freshness_publish_at,
    )


def score_candidates(
    candidates: Sequence[models.DramaRecord],
    reference_date: str | date | datetime | None = None,
    tag_scorer=None,
) -> List[models.DramaRecord]:
    scorer = tag_scorer or tag_scoring.TagWeightScorer()
    as_of = _resolve_reference_date(reference_date)
    scored: List[models.DramaRecord] = []
    for candidate in candidates:
        tag_result = scorer.score_record(candidate)
        source_signal = source_signal_score(candidate)
        content_multiplier = content_type_multiplier(candidate)
        theme_multiplier = _clamp_theme_multiplier(tag_result.score)
        publish_at = resolve_publish_at(candidate)
        age_days = _days_ago(publish_at, as_of)
        age_bucket = classify_age_bucket(age_days)
        date_hits = collect_date_signal_hits(candidate, as_of)
        date_score, date_bucket = score_date_signal_hits(date_hits)
        mobo_score, mobo_bucket = score_mobo_recommend(candidate)
        beidou_score, beidou_bucket = score_beidou_validation(candidate, age_days)
        explosion_bucket = combine_explosion_buckets(date_bucket, mobo_bucket, beidou_bucket)
        explosion_score = 0.6 * date_score + 0.25 * mobo_score + 0.15 * beidou_score
        freshness = freshness_score(age_bucket)
        layer = assign_layer(age_bucket, explosion_bucket)
        layer_score = compute_layer_score(layer, explosion_score, freshness, beidou_score)
        final_score = round(layer_score * theme_multiplier * content_multiplier, 4)
        scored.append(
            replace(
                candidate,
                publish_at=publish_at,
                score=final_score,
                source_signal_score=round(source_signal, 4),
                tag_score=theme_multiplier,
                theme_multiplier=theme_multiplier,
                content_multiplier=content_multiplier,
                tag_status=tag_result.status,
                matched_tags=tag_result.matched_tags,
                age_bucket=age_bucket,
                explosion_bucket=explosion_bucket,
                layer=layer,
                date_signal_hits=[f"{hit.source_key}@{hit.date_text}" for hit in date_hits],
                explosion_recommend_score=round(explosion_score, 4),
                freshness_score=round(freshness, 4),
                revenue_validation_score=round(beidou_score, 4),
                layer_score=round(layer_score, 4),
            )
        )
    return sorted(scored, key=layer_sort_key)


def source_signal_score(candidate: models.DramaRecord) -> float:
    details = candidate.source_rank_details or {
        source: candidate.rank for source in _split_sources(candidate.source)
    }
    score = 0.0
    for source, rank in details.items():
        weight = config.SOURCE_WEIGHTS.get(source, config.DEFAULT_SOURCE_WEIGHT)
        score += weight * rank_score(rank, source, candidate)
    return round(score, 6)


def rank_score(rank: int | None, source: str = "", candidate: models.DramaRecord | None = None) -> float:
    max_rank = max_rank_for_source(source, candidate)
    if max_rank <= 1:
        return 1.0
    value = 1 if not rank or rank <= 1 else min(int(rank), max_rank)
    if value <= 1:
        return 1.0
    score = 1.0 - (1.0 - config.RANK_SCORE_MIN) * math.log1p(value - 1) / math.log1p(max_rank - 1)
    return round(max(config.RANK_SCORE_MIN, min(1.0, score)), 6)


def max_rank_for_source(source: str, candidate: models.DramaRecord | None = None) -> int:
    if source == "duole_recommend" and candidate is not None:
        sheet_name = str((candidate.raw or {}).get("sheet_name") or "")
        if sheet_name in config.DUOLE_SHEET_LIMITS:
            return config.DUOLE_SHEET_LIMITS[sheet_name]
    return int(config.SOURCE_RANK_MAX.get(source, 200))


def is_theater_quota_available(record: models.DramaRecord, counts: Dict[str, Dict[str, int]]) -> bool:
    return theater_quota_block_reason(record, counts) == ""


def theater_quota_block_reason(record: models.DramaRecord, counts: Dict[str, Dict[str, int]]) -> str:
    quotas = normalized_language_quotas(record.language)
    if not quotas:
        return ""
    theater = primary_theater(record)
    limit = quotas.get(theater)
    if limit is None:
        return "\u5267\u573a\u4e0d\u5728\u8bed\u8a00\u914d\u989d\u5185"
    if counts.get(record.language, {}).get(theater, 0) >= limit:
        return "\u5267\u573a\u914d\u989d\u5df2\u6ee1"
    return ""


def increment_theater_quota(record: models.DramaRecord, counts: Dict[str, Dict[str, int]]) -> None:
    theater = primary_theater(record)
    counts.setdefault(record.language, {})
    counts[record.language][theater] = counts[record.language].get(theater, 0) + 1


def language_target(language: str) -> int:
    quotas = config.LANGUAGE_THEATER_QUOTAS.get(language)
    return sum(quotas.values()) if quotas else config.DEFAULT_LANGUAGE_TARGET


def language_sort_index(language: str) -> int:
    try:
        return config.LANGUAGE_ORDER.index(language)
    except ValueError:
        return len(config.LANGUAGE_ORDER)


def layer_sort_index(layer: str) -> int:
    try:
        return config.LAYER_ORDER.index(layer)
    except ValueError:
        return len(config.LAYER_ORDER)


def material_key(language: str, title: str, theater: str) -> Tuple[str, str, str]:
    return (language, normalize_title(title), normalize_theater(_first_theater_text(theater)))


def primary_theater(record: models.DramaRecord) -> str:
    raw = record.raw or {}
    if isinstance(raw, dict):
        raw_theater = _clean_text(raw.get(PRIMARY_THEATER_RAW_KEY))
        if raw_theater:
            return normalize_theater(raw_theater)
    return normalize_theater(_first_theater_text(record.theater))


def normalize_theater(theater: str) -> str:
    return config.normalize_theater_name(theater)


def normalized_language_quotas(language: str) -> Dict[str, int]:
    quotas = config.LANGUAGE_THEATER_QUOTAS.get(language, {})
    return {normalize_theater(theater): limit for theater, limit in quotas.items()}


def classify_age_bucket(age_days: int | None) -> str:
    if age_days is None:
        return config.AGE_BUCKET_UNKNOWN
    if age_days <= config.AGE_BUCKET_NEW_DAYS:
        return "\u65b0"
    if age_days <= config.AGE_BUCKET_MID_DAYS:
        return "\u4e2d"
    return "\u8001"


def collect_date_signal_hits(candidate: models.DramaRecord, as_of: date) -> List[_DateSignalHit]:
    source_dates = candidate.source_date_details or _fallback_source_date_details(candidate)
    hits_by_source: Dict[str, _DateSignalHit] = {}
    for source_key, date_text in source_dates.items():
        base_source = _base_source(source_key)
        if base_source not in {"beidou_hot", "duole_recommend"}:
            continue
        parsed = _parse_datetime(date_text)
        if parsed is None:
            continue
        days_ago = max((as_of - parsed.date()).days, 0)
        if days_ago > config.DATE_SIGNAL_MID_DAYS:
            continue
        current = hits_by_source.get(source_key)
        hit = _DateSignalHit(source_key=source_key, days_ago=days_ago, date_text=_format_datetime(parsed))
        if current is None or hit.days_ago < current.days_ago:
            hits_by_source[source_key] = hit
    return sorted(hits_by_source.values(), key=lambda item: (item.days_ago, item.source_key))


def score_date_signal_hits(hits: Sequence[_DateSignalHit]) -> Tuple[float, str]:
    recent = sum(1 for hit in hits if hit.days_ago <= config.DATE_SIGNAL_NEW_DAYS)
    mid = sum(1 for hit in hits if config.DATE_SIGNAL_NEW_DAYS < hit.days_ago <= config.DATE_SIGNAL_MID_DAYS)
    if recent >= 2:
        return config.DATE_SIGNAL_SCORES["recent_multi"], "\u65b0\u7206"
    if recent == 1:
        return config.DATE_SIGNAL_SCORES["recent_single"], "\u65b0\u7206"
    if mid >= 2:
        return config.DATE_SIGNAL_SCORES["mid_multi"], "\u4e2d\u7206"
    if mid == 1:
        return config.DATE_SIGNAL_SCORES["mid_single"], "\u4e2d\u7206"
    return config.DATE_SIGNAL_SCORES["none"], "\u65e0"


def score_mobo_recommend(candidate: models.DramaRecord) -> Tuple[float, str]:
    rank = _best_rank(candidate, ["mobo_recommend"])
    if rank <= 0:
        return config.MOBO_RECOMMEND_SCORES["\u65e0"], "\u65e0"
    if rank <= config.MOBO_RECOMMEND_NEW_MAX_RANK:
        return config.MOBO_RECOMMEND_SCORES["\u65b0\u7206"], "\u65b0\u7206"
    if rank <= config.MOBO_RECOMMEND_MID_MAX_RANK:
        return config.MOBO_RECOMMEND_SCORES["\u4e2d\u7206"], "\u4e2d\u7206"
    if rank <= config.MOBO_RECOMMEND_OLD_MAX_RANK:
        return config.MOBO_RECOMMEND_SCORES["\u8001\u7206"], "\u8001\u7206"
    return config.MOBO_RECOMMEND_SCORES["\u65e0"], "\u65e0"


def score_beidou_validation(candidate: models.DramaRecord, age_days: int | None) -> Tuple[float, str]:
    rank = _best_rank(candidate, ["beidou_income", "beidou_hot"])
    if rank <= 0:
        return config.BEIDOU_VALIDATION_SCORES["\u65e0"], "\u65e0"
    if age_days is not None and rank <= config.BEIDOU_NEW_MAX_RANK and age_days <= config.BEIDOU_NEW_MAX_DAYS:
        return config.BEIDOU_VALIDATION_SCORES["\u65b0\u7206"], "\u65b0\u7206"
    if age_days is not None and rank <= config.BEIDOU_MID_MAX_RANK and age_days <= config.BEIDOU_MID_MAX_DAYS:
        return config.BEIDOU_VALIDATION_SCORES["\u4e2d\u7206"], "\u4e2d\u7206"
    if rank < config.BEIDOU_OLD_MAX_RANK:
        return config.BEIDOU_VALIDATION_SCORES["\u8001\u7206"], "\u8001\u7206"
    return config.BEIDOU_VALIDATION_SCORES["\u65e0"], "\u65e0"


def combine_explosion_buckets(*buckets: str) -> str:
    if "\u65b0\u7206" in buckets:
        return "\u65b0\u7206"
    if "\u4e2d\u7206" in buckets:
        return "\u4e2d\u7206"
    if "\u8001\u7206" in buckets:
        return "\u8001\u7206"
    return "\u65e0"


def freshness_score(age_bucket: str) -> float:
    return float(config.FRESHNESS_SCORES.get(age_bucket, config.FRESHNESS_SCORES[config.AGE_BUCKET_UNKNOWN]))


def assign_layer(age_bucket: str, explosion_bucket: str) -> str:
    if age_bucket == "\u65b0" and explosion_bucket == "\u65b0\u7206":
        return "A"
    if age_bucket == "\u4e2d" and explosion_bucket == "\u65b0\u7206":
        return "B"
    if age_bucket == "\u65b0":
        return "C"
    if age_bucket == "\u4e2d" and explosion_bucket == "\u4e2d\u7206":
        return "D"
    if age_bucket == "\u4e2d":
        return "E"
    if age_bucket == "\u8001" and explosion_bucket in {"\u65b0\u7206", "\u4e2d\u7206", "\u8001\u7206"}:
        return "F"
    if age_bucket == config.AGE_BUCKET_UNKNOWN and explosion_bucket == "\u65b0\u7206":
        return "C"
    if age_bucket == config.AGE_BUCKET_UNKNOWN and explosion_bucket == "\u4e2d\u7206":
        return "E"
    return "G"


def compute_layer_score(layer: str, explosion_score: float, freshness: float, revenue: float) -> float:
    weights = config.LAYER_SCORE_WEIGHTS["F"] if layer == "F" else config.LAYER_SCORE_WEIGHTS["default"]
    return round(
        explosion_score * weights["explosion"]
        + freshness * weights["freshness"]
        + revenue * weights["revenue"],
        4,
    )


def layer_sort_key(candidate: models.DramaRecord) -> Tuple[object, ...]:
    layer = candidate.layer or "G"
    if layer == "F":
        return (
            layer_sort_index(layer),
            _explosion_priority(candidate.explosion_bucket),
            -candidate.score,
            -candidate.theme_multiplier,
            -candidate.content_multiplier,
            -candidate.source_signal_score,
            candidate.rank or 999,
            candidate.title_norm,
        )
    if layer == "G":
        promotion_dt = resolve_promotion_datetime(candidate) or datetime.min
        return (
            layer_sort_index(layer),
            -promotion_dt.timestamp() if promotion_dt != datetime.min else float("inf"),
            -candidate.theme_multiplier,
            -candidate.content_multiplier,
            -candidate.source_signal_score,
            candidate.rank or 999,
            candidate.title_norm,
        )
    return (
        layer_sort_index(layer),
        -candidate.score,
        -candidate.theme_multiplier,
        -candidate.content_multiplier,
        -candidate.source_signal_score,
        candidate.rank or 999,
        candidate.title_norm,
    )


def resolve_publish_at(candidate: models.DramaRecord) -> str:
    raw = candidate.raw or {}
    primary_values = [
        candidate.publish_at,
        raw.get(FRESHNESS_PUBLISH_AT_RAW_KEY),
        raw.get(PRIMARY_PUBLISH_AT_RAW_KEY),
        raw.get(MOBOREELS_PUBLISH_AT_RAW_KEY),
        raw.get(EARLIEST_PUBLISH_AT_RAW_KEY),
    ]
    preferred = _first_datetime_text(primary_values)
    if preferred:
        return preferred
    values = [
        raw.get(LATEST_PUBLISH_AT_RAW_KEY),
    ]
    values.extend(list(raw.get(PUBLISH_VALUES_RAW_KEY) or []))
    values.extend(
        [
            raw.get("publish_at"),
            raw.get("createTime"),
            raw.get("publishTime"),
            raw.get("\u53d1\u5e03\u65f6\u95f4"),
            raw.get("\u4e0a\u67b6\u65f6\u95f4"),
            raw.get("\u4e0a\u7ebf\u65f6\u95f4"),
            raw.get("\u4e0a\u7ebf\u65e5\u671f"),
        ]
    )
    latest = _latest_datetime_text(values)
    if latest:
        return latest
    for value in values:
        text = _clean_text(value)
        if text:
            return text
    return ""


def resolve_promotion_datetime(candidate: models.DramaRecord) -> datetime | None:
    values = list((candidate.source_date_details or {}).values())
    values.extend(
        [
            (candidate.raw or {}).get("recommend_date"),
            (candidate.raw or {}).get("\u65e5\u671f"),
            (candidate.raw or {}).get("\u66f4\u65b0\u65e5\u671f"),
            (candidate.raw or {}).get("\u66f4\u65b0\u65f6\u95f4"),
            candidate.publish_at,
        ]
    )
    datetimes = [_parse_datetime(value) for value in values if _parse_datetime(value) is not None]
    if not datetimes:
        return None
    return max(datetimes)


def _resolve_reference_date(reference_date: str | date | datetime | None) -> date:
    if isinstance(reference_date, datetime):
        return reference_date.date()
    if isinstance(reference_date, date):
        return reference_date
    if isinstance(reference_date, str) and reference_date.strip():
        parsed = _parse_datetime(reference_date)
        if parsed is not None:
            return parsed.date()
    return datetime.now().date()


def _days_ago(value: str, as_of: date) -> int | None:
    parsed = _parse_datetime(value)
    if parsed is None:
        return None
    return max((as_of - parsed.date()).days, 0)


def _best_rank(candidate: models.DramaRecord, sources: Sequence[str]) -> int:
    details = candidate.source_rank_details or {}
    ranks = [int(details[source]) for source in sources if details.get(source)]
    return min(ranks) if ranks else 0


def _clamp_theme_multiplier(value: float) -> float:
    return round(max(config.TAG_SCORE_FLOOR, min(config.TAG_SCORE_CAP, float(value))), 4)


def _merge_source_rank_details(rows: Sequence[models.DramaRecord]) -> Dict[str, int]:
    output: Dict[str, int] = {}
    for row in rows:
        row_details = row.source_rank_details or {source: row.rank for source in _split_sources(row.source)}
        for source, rank in row_details.items():
            normalized_rank = rank if rank and rank > 0 else 999
            output[source] = min(output.get(source, normalized_rank), normalized_rank)
    return dict(sorted(output.items()))


def _merge_source_date_details(rows: Sequence[models.DramaRecord]) -> Dict[str, str]:
    output: Dict[str, str] = {}
    for row in rows:
        source_key = _source_date_key(row)
        if not source_key:
            continue
        date_text = _row_source_date(row)
        if not date_text:
            continue
        existing = output.get(source_key)
        output[source_key] = _newer_date_text(existing, date_text)
    return dict(sorted(output.items()))


def _merge_publish_time(rows: Sequence[models.DramaRecord]) -> str:
    return _latest_datetime_text(_row_publish_time(row) for row in rows)


def _row_source_date(row: models.DramaRecord) -> str:
    raw = row.raw or {}
    if row.source == "beidou_hot":
        return _clean_text(raw.get("recommend_date") or raw.get("\u65e5\u671f"))
    if row.source == "duole_recommend":
        return _clean_text(raw.get("recommend_date") or raw.get("\u65e5\u671f") or raw.get("\u66f4\u65b0\u65e5\u671f") or raw.get("\u66f4\u65b0\u65f6\u95f4"))
    return _row_publish_time(row)


def _row_publish_time(row: models.DramaRecord) -> str:
    raw = row.raw or {}
    if row.source == "beidou_hot":
        return ""
    if row.source == "duole_recommend":
        for value in (raw.get("\u4e0a\u67b6\u65f6\u95f4"), row.publish_at, raw.get("publish_at"), raw.get("publishTime"), raw.get("\u53d1\u5e03\u65f6\u95f4")):
            text = _clean_text(value)
            if text:
                return text
        return ""
    for value in (row.publish_at, raw.get("publish_at"), raw.get("createTime"), raw.get("publishTime"), raw.get("\u53d1\u5e03\u65f6\u95f4"), raw.get("\u4e0a\u67b6\u65f6\u95f4")):
        text = _clean_text(value)
        if text:
            return text
    return ""


def _pick_primary_row(rows: Sequence[models.DramaRecord]) -> models.DramaRecord:
    return max(
        rows,
        key=lambda row: (
            _row_publish_datetime(row) is not None,
            _row_publish_datetime(row) or datetime.min,
            _parse_datetime(_row_source_date(row)) or datetime.min,
            -(row.rank or 999),
            normalize_theater(row.theater),
            row.title_norm,
        ),
    )


def _pick_group_primary_row(rows: Sequence[models.DramaRecord], prefer_mobo: bool = False) -> models.DramaRecord:
    return min(
        rows,
        key=lambda row: (
            0 if prefer_mobo and _is_mobo_source(row) else 1,
            _row_publish_datetime(row) or datetime.max,
            row.rank or 999,
            normalize_theater(row.theater),
            row.title_norm,
        ),
    )


def _merge_theaters_with_primary(rows: Sequence[models.DramaRecord], primary_theater_name: str) -> List[str]:
    normalized_primary = normalize_theater(primary_theater_name)
    others = sorted(
        {
            theater
            for row in rows
            for theater in _theaters_from_row(row)
            if theater and theater != normalized_primary
        }
    )
    ordered = [normalized_primary] if normalized_primary else []
    return ordered + others


def _merge_theaters(rows: Sequence[models.DramaRecord], primary: models.DramaRecord) -> List[str]:
    primary_name = normalize_theater(primary.theater)
    others = sorted(
        {
            theater
            for row in rows
            for theater in _theaters_from_row(row)
            if theater and theater != primary_name
        }
    )
    ordered = [primary_name] if primary_name else []
    return ordered + others


def _theaters_from_row(row: models.DramaRecord) -> List[str]:
    raw = row.raw or {}
    merged = [normalize_theater(item) for item in list(raw.get(MERGED_THEATERS_RAW_KEY) or [])]
    if not merged:
        merged = [normalize_theater(item) for item in str(row.theater or "").split(",")]
    return [theater for theater in merged if theater]


def _merge_theme_tags(rows: Sequence[models.DramaRecord], preferred_theater: str = "") -> List[str]:
    primary_tags = []
    if preferred_theater:
        primary_tags = _unique_preserve_order(
            tag
            for row in rows
            if normalize_theater(row.theater) == normalize_theater(preferred_theater)
            for tag in _theme_tags_from_row(row)
        )
    all_tags = _unique_preserve_order(tag for row in rows for tag in _theme_tags_from_row(row))
    if not primary_tags:
        return all_tags
    return primary_tags + [tag for tag in all_tags if tag not in primary_tags]


def _merge_content_tags(rows: Sequence[models.DramaRecord]) -> List[str]:
    return _unique_preserve_order(tag for row in rows for tag in _content_tags_from_row(row))


def _merge_candidate_raw(
    rows: Sequence[models.DramaRecord],
    primary: models.DramaRecord,
    theaters: Sequence[str],
    tags: Sequence[str],
    publish_at: str,
    earliest_publish_at: str,
    latest_publish_at: str,
    moboreels_publish_at: str,
    all_content_tags: Sequence[str],
    primary_content_tags: Sequence[str],
) -> Dict[str, object]:
    raw: Dict[str, object] = dict(primary.raw or {})
    for row in rows:
        for key, value in (row.raw or {}).items():
            if key not in raw or not _clean_text(raw.get(key)):
                raw[key] = value
    publish_values = []
    for row in rows:
        value = _row_publish_time(row)
        if value and value not in publish_values:
            publish_values.append(value)
    raw[PRIMARY_THEATER_RAW_KEY] = normalize_theater(primary.theater)
    raw[MERGED_THEATERS_RAW_KEY] = list(theaters)
    raw[PRIMARY_PUBLISH_AT_RAW_KEY] = publish_at
    raw[EARLIEST_PUBLISH_AT_RAW_KEY] = earliest_publish_at
    raw[LATEST_PUBLISH_AT_RAW_KEY] = latest_publish_at
    raw[MOBOREELS_PUBLISH_AT_RAW_KEY] = moboreels_publish_at
    raw[FRESHNESS_PUBLISH_AT_RAW_KEY] = publish_at
    raw[PUBLISH_VALUES_RAW_KEY] = publish_values
    raw["merged_tag_values"] = list(tags)
    raw[ALL_CONTENT_TAG_VALUES_RAW_KEY] = list(all_content_tags)
    raw[PRIMARY_CONTENT_TAG_VALUES_RAW_KEY] = list(primary_content_tags)
    return raw


def _source_date_key(row: models.DramaRecord) -> str:
    if row.source == "duole_recommend":
        sheet_name = str((row.raw or {}).get("sheet_name") or "").strip()
        if sheet_name:
            return f"{row.source}:{sheet_name}"
    return row.source


def _fallback_source_date_details(candidate: models.DramaRecord) -> Dict[str, str]:
    details: Dict[str, str] = {}
    for source in _split_sources(candidate.source):
        source_key = source
        if source == "duole_recommend":
            sheet_name = str((candidate.raw or {}).get("sheet_name") or "").strip()
            if sheet_name:
                source_key = f"{source}:{sheet_name}"
        date_text = _candidate_source_date(candidate, source)
        if date_text:
            details[source_key] = date_text
    return details


def _candidate_source_date(candidate: models.DramaRecord, source: str) -> str:
    raw = candidate.raw or {}
    if source == "beidou_hot":
        return _clean_text(raw.get("recommend_date") or raw.get("\u65e5\u671f"))
    if source == "duole_recommend":
        return _clean_text(raw.get("recommend_date") or raw.get("\u65e5\u671f") or raw.get("\u66f4\u65b0\u65e5\u671f") or raw.get("\u66f4\u65b0\u65f6\u95f4"))
    return _clean_text(candidate.publish_at or raw.get("publish_at") or raw.get("createTime") or raw.get("publishTime"))


def _row_publish_datetime(row: models.DramaRecord) -> datetime | None:
    return _parse_datetime(_row_publish_time(row))


def _latest_datetime_text(values: Iterable[object]) -> str:
    latest: datetime | None = None
    text_values: List[str] = []
    for value in values:
        text = _clean_text(value)
        if not text:
            continue
        text_values.append(text)
        parsed = _parse_datetime(text)
        if parsed is None:
            continue
        if latest is None or parsed > latest:
            latest = parsed
    if latest is not None:
        return _format_datetime(latest)
    return text_values[0] if text_values else ""


def _first_theater_text(value: object) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    for separator in (",", "，", "|", "/"):
        if separator in text:
            return text.split(separator, 1)[0].strip()
    return text


def _newer_date_text(current: str | None, candidate: str) -> str:
    current_dt = _parse_datetime(current)
    candidate_dt = _parse_datetime(candidate)
    if current_dt is None:
        return _clean_text(candidate)
    if candidate_dt is None:
        return _clean_text(current)
    return _format_datetime(max(current_dt, candidate_dt))


def _base_source(source_key: str) -> str:
    return source_key.split(":", 1)[0]


def _explosion_priority(bucket: str) -> int:
    order = {"\u65b0\u7206": 0, "\u4e2d\u7206": 1, "\u8001\u7206": 2, "\u65e0": 3}
    return order.get(bucket, len(order))


def _split_sources(source_text: str) -> List[str]:
    return [source.strip() for source in str(source_text or "").split(",") if source.strip()]


def _parse_datetime(value: object) -> datetime | None:
    text = _clean_text(value)
    if not text:
        return None
    normalized = text.replace("/", "-").replace("T", " ").strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1]
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S.%f",
    ):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _format_datetime(value: datetime) -> str:
    if value.hour == 0 and value.minute == 0 and value.second == 0 and value.microsecond == 0:
        return value.strftime("%Y-%m-%d")
    return value.replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")


def _first_datetime_text(values: Iterable[object]) -> str:
    text_values: List[str] = []
    for value in values:
        text = _clean_text(value)
        if not text:
            continue
        text_values.append(text)
        parsed = _parse_datetime(text)
        if parsed is not None:
            return _format_datetime(parsed)
    return text_values[0] if text_values else ""


def _earliest_datetime_text(values: Iterable[object]) -> str:
    earliest: datetime | None = None
    text_values: List[str] = []
    for value in values:
        text = _clean_text(value)
        if not text:
            continue
        text_values.append(text)
        parsed = _parse_datetime(text)
        if parsed is None:
            continue
        if earliest is None or parsed < earliest:
            earliest = parsed
    if earliest is not None:
        return _format_datetime(earliest)
    return text_values[0] if text_values else ""


def _unique_preserve_order(values: Iterable[str]) -> List[str]:
    output: List[str] = []
    seen = set()
    for value in values:
        text = _clean_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        output.append(text)
    return output


def _is_mobo_source(row: models.DramaRecord) -> bool:
    return any(source.startswith("mobo") for source in _split_sources(row.source))


def _theme_tags_from_row(row: models.DramaRecord) -> List[str]:
    return [tag for tag in _raw_tag_tokens_from_row(row) if not _is_distribution_tag(tag)]


def _content_tags_from_row(row: models.DramaRecord) -> List[str]:
    tokens = list(_raw_tag_tokens_from_row(row))
    raw = row.raw or {}
    if isinstance(raw, dict):
        for key in ("audioTypeName", "localTypeName", "tag", "seriesTypeList"):
            _append_content_tokens(tokens, set(), raw.get(key))
    return _unique_preserve_order(tokens)


def _raw_tag_tokens_from_row(row: models.DramaRecord) -> List[str]:
    tokens: List[str] = []
    seen: set[str] = set()
    for value in row.tags:
        _append_content_tokens(tokens, seen, value)
    raw = row.raw or {}
    if isinstance(raw, dict):
        for key, value in raw.items():
            if _is_content_tag_key(key):
                _append_content_tokens(tokens, seen, value)
    return tokens


def _is_distribution_tag(value: str) -> bool:
    text = _clean_text(value)
    if not text:
        return False
    keywords = (
        "AI",
        "配音",
        "字幕",
        "翻译",
        "本土",
        "版权剧",
        "原创",
        "自制剧",
        "真人",
        "海外",
        "中文",
        "国内剧",
        "独家",
        "非独家",
    )
    return any(keyword.lower() in text.lower() for keyword in keywords)


def _content_tag_tokens(record: models.DramaRecord) -> List[str]:
    tokens: List[str] = []
    seen: set[str] = set()
    for value in record.tags:
        _append_content_tokens(tokens, seen, value)
    raw = record.raw or {}
    if isinstance(raw, dict):
        for key, value in raw.items():
            if _is_content_tag_key(key):
                _append_content_tokens(tokens, seen, value)
    return tokens


def _all_content_tag_tokens(record: models.DramaRecord) -> List[str]:
    raw = record.raw or {}
    if isinstance(raw, dict):
        explicit = raw.get(ALL_CONTENT_TAG_VALUES_RAW_KEY)
        if explicit:
            tokens: List[str] = []
            seen: set[str] = set()
            _append_content_tokens(tokens, seen, explicit)
            return tokens
    return _content_tag_tokens(record)


def _priority_content_tag_tokens(record: models.DramaRecord) -> List[str]:
    raw = record.raw or {}
    if isinstance(raw, dict):
        explicit = raw.get(PRIMARY_CONTENT_TAG_VALUES_RAW_KEY)
        if explicit:
            tokens: List[str] = []
            seen: set[str] = set()
            _append_content_tokens(tokens, seen, explicit)
            return tokens
    return _all_content_tag_tokens(record)


def _append_content_tokens(tokens: List[str], seen: set[str], value: Any) -> None:
    if value is None:
        return
    if isinstance(value, dict):
        for nested in value.values():
            _append_content_tokens(tokens, seen, nested)
        return
    if isinstance(value, (list, tuple, set)):
        for nested in value:
            _append_content_tokens(tokens, seen, nested)
        return
    text = str(value).strip()
    if not text:
        return
    parts = [part.strip() for part in re.split(r"[;,/|、，]+", text) if part and part.strip()]
    if not parts:
        parts = [text]
    for part in parts:
        lowered = part.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        tokens.append(part)


def _is_content_tag_key(key: Any) -> bool:
    text = str(key or "").strip().lower()
    if not text:
        return False
    if any(token in text for token in ("标签", "题材", "剧集类型", "内容标签", "类型", "受众")):
        return True
    return any(token in text for token in ("tag", "label", "genre", "theme", "seriestype", "category", "type"))


def _matches_content_keyword(token: str, keyword: str) -> bool:
    token_text = str(token or "").strip()
    keyword_text = str(keyword or "").strip()
    if not token_text or not keyword_text:
        return False
    lowered_keyword = keyword_text.lower()
    lowered_token = token_text.lower()
    if lowered_keyword == "ai":
        return re.search(r"(?<![a-z])ai(?![a-z])", lowered_token) is not None
    return lowered_keyword in lowered_token


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _blocked(record: models.DramaRecord, reason: str) -> models.FilterRecord:
    return models.FilterRecord(
        title=record.title,
        language=record.language,
        theater=record.theater,
        reason=reason,
        source=record.source,
        score=record.score,
        layer=record.layer,
        age_bucket=record.age_bucket,
        explosion_bucket=record.explosion_bucket,
        raw=record.raw,
    )
