from __future__ import annotations

import json
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import importlib
import requests

config = importlib.import_module("drama_pipeline.2_config")
models = importlib.import_module("drama_pipeline.3_models")

from .common import _log_info, _log_warning, _safe_float, _safe_int, normalize_theater_name


class MaterialRateLimitError(RuntimeError):
    pass


class MaterialServiceDegradedError(RuntimeError):
    pass


class MaterialClient:
    def __init__(self, cookie: str = config.MATERIAL_COOKIE, session: Optional[requests.Session] = None, logger: Any = None):
        self.cookie = cookie
        self.session = session or requests.Session()
        self.session.trust_env = False
        self.logger = logger
        self._title_cache: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = {}
        self._result_cache: Dict[Tuple[str, str, str, str, str], models.MaterialResult] = {}
        self._cache_lock = threading.Lock()
        self._daily_cache_path: Optional[Path] = None
        self._daily_cache_loaded = False
        self._daily_cache_dirty = False
        self._failure_count = 0
        self._degraded = False
        self._degraded_reason = ""
        self._cooldown_until = 0.0

    def set_daily_cache_path(self, path: Optional[Path | str]) -> None:
        cache_path = Path(path).resolve() if path else None
        with self._cache_lock:
            self._daily_cache_path = cache_path
            self._daily_cache_loaded = False
            self._daily_cache_dirty = False
        if cache_path and getattr(config, "MATERIAL_TODAY_CACHE_ENABLED", True):
            self._load_daily_cache()

    def _result_cache_key(
        self,
        language: str,
        title: str,
        theater: str,
        start_date: str,
        end_date: str,
    ) -> Tuple[str, str, str, str, str]:
        return (
            str(language or "").strip(),
            models.normalize_title(title),
            normalize_theater_name(theater),
            str(start_date or "").strip(),
            str(end_date or "").strip(),
        )

    def _read_result_cache(
        self,
        language: str,
        title: str,
        theater: str,
        start_date: str,
        end_date: str,
    ) -> Optional[models.MaterialResult]:
        cache_key = self._result_cache_key(language, title, theater, start_date, end_date)
        with self._cache_lock:
            cached = self._result_cache.get(cache_key)
        return cached

    def _store_result_cache(
        self,
        result: models.MaterialResult,
        start_date: str,
        end_date: str,
    ) -> None:
        cache_key = self._result_cache_key(result.language, result.title, result.theater, start_date, end_date)
        with self._cache_lock:
            self._result_cache[cache_key] = result
            self._daily_cache_dirty = True

    def _load_daily_cache(self) -> None:
        with self._cache_lock:
            cache_path = self._daily_cache_path
            already_loaded = self._daily_cache_loaded
        if already_loaded or cache_path is None or not getattr(config, "MATERIAL_TODAY_CACHE_ENABLED", True):
            return
        loaded = 0
        if cache_path.exists():
            try:
                payload = json.loads(cache_path.read_text(encoding="utf-8"))
                rows = payload.get("results") if isinstance(payload, dict) else []
                if isinstance(rows, list):
                    for row in rows:
                        if not isinstance(row, dict):
                            continue
                        result = models.MaterialResult(
                            language=str(row.get("language") or "").strip(),
                            title=str(row.get("title") or "").strip(),
                            theater=normalize_theater_name(row.get("theater")),
                            qualified_count=int(_safe_int(row.get("qualified_count"))),
                            total_count=int(_safe_int(row.get("total_count"))),
                        )
                        if not result.language or not result.title or not result.theater:
                            continue
                        cache_key = self._result_cache_key(
                            result.language,
                            result.title,
                            result.theater,
                            str(row.get("start_date") or "").strip(),
                            str(row.get("end_date") or "").strip(),
                        )
                        self._result_cache[cache_key] = result
                        loaded += 1
            except Exception as exc:
                _log_warning(self.logger, f"[素材] 读取今日缓存失败: {type(exc).__name__}: {exc}")
        with self._cache_lock:
            self._daily_cache_loaded = True
            self._daily_cache_dirty = False
        if loaded:
            _log_info(self.logger, f"[素材] 已加载今日缓存: count={loaded} path={cache_path}")

    def _flush_daily_cache(self) -> None:
        with self._cache_lock:
            cache_path = self._daily_cache_path
            dirty = self._daily_cache_dirty
            if cache_path is None or not dirty or not getattr(config, "MATERIAL_TODAY_CACHE_ENABLED", True):
                return
            rows = [
                {
                    "language": result.language,
                    "title": result.title,
                    "theater": result.theater,
                    "qualified_count": int(result.qualified_count),
                    "total_count": int(result.total_count),
                    "start_date": start_date,
                    "end_date": end_date,
                }
                for (language, _title_norm, theater, start_date, end_date), result in self._result_cache.items()
                if language and theater and start_date and end_date
            ]
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(
                json.dumps({"version": 1, "results": rows}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            with self._cache_lock:
                self._daily_cache_dirty = False
        except Exception as exc:
            _log_warning(self.logger, f"[素材] 写入今日缓存失败: {type(exc).__name__}: {exc}")

    def _material_retry_limit(self) -> int:
        return max(int(getattr(config, "MATERIAL_RATE_LIMIT_RETRY_COUNT", 0) or 0), 0)

    def _material_max_failures(self) -> int:
        return max(int(getattr(config, "MATERIAL_MAX_FAILURES", 5) or 5), 1)

    def _prefetch_chunk_size(self) -> int:
        return max(int(getattr(config, "MATERIAL_PREFETCH_CHUNK_SIZE", 1) or 1), 1)

    def _prefetch_pause_range(self) -> Tuple[float, float]:
        minimum = float(getattr(config, "MATERIAL_PREFETCH_PAUSE_MIN_SECONDS", 0.5) or 0.5)
        maximum = float(getattr(config, "MATERIAL_PREFETCH_PAUSE_MAX_SECONDS", minimum) or minimum)
        if maximum < minimum:
            maximum = minimum
        return minimum, maximum

    def _cooldown_range(self) -> Tuple[float, float]:
        minimum = float(getattr(config, "MATERIAL_RATE_LIMIT_COOLDOWN_MIN_SECONDS", 2.0) or 2.0)
        maximum = float(getattr(config, "MATERIAL_RATE_LIMIT_COOLDOWN_MAX_SECONDS", minimum) or minimum)
        if maximum < minimum:
            maximum = minimum
        return minimum, maximum

    def _pause_after_chunk(self) -> None:
        minimum, maximum = self._prefetch_pause_range()
        time.sleep(random.uniform(minimum, maximum))

    def _wait_for_cooldown(self) -> None:
        delay = 0.0
        with self._cache_lock:
            if self._cooldown_until > 0:
                delay = max(self._cooldown_until - time.monotonic(), 0.0)
        if delay > 0:
            time.sleep(delay)

    def _register_rate_limit(self, context: str, attempt: int, retry_limit: int) -> None:
        minimum, maximum = self._cooldown_range()
        delay = random.uniform(minimum, maximum)
        with self._cache_lock:
            self._failure_count += 1
            self._cooldown_until = max(self._cooldown_until, time.monotonic() + delay)
            failure_count = self._failure_count
        _log_warning(
            self.logger,
            f"[素材] 接口限流，进入冷却: context={context} attempt={attempt}/{retry_limit + 1} failures={failure_count} cooldown={delay:.1f}s",
        )
        if failure_count > self._material_max_failures():
            self._degrade_material_checks(f"连续素材限流超过阈值: failures={failure_count}")

    def _degrade_material_checks(self, reason: str) -> None:
        should_log = False
        with self._cache_lock:
            if not self._degraded:
                self._degraded = True
                self._degraded_reason = reason
                should_log = True
        if should_log:
            _log_warning(self.logger, f"[素材] 已降级为跳过素材接口: {reason}")

    def _ensure_material_service_available(self) -> None:
        with self._cache_lock:
            degraded = self._degraded
            reason = self._degraded_reason
        if degraded:
            raise MaterialServiceDegradedError(reason or "material service degraded")

    def build_search_request(
        self,
        search_key: str,
        start_date: str,
        end_date: str,
        page_num: int = 1,
        page_size: int = config.MATERIAL_PAGE_SIZE,
    ) -> Tuple[str, Dict[str, str], Dict[str, Any]]:
        headers = {
            "Cookie": self.cookie,
            "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
            "User-Agent": "Mozilla/5.0",
        }
        this_times = __import__("datetime").datetime.now().timestamp()
        payload = {
            "pageId": page_num,
            "pageSize": page_size,
            "deduplicationBy": "SMART",
            "sortBy": "EXPOSURE_NUM",
            "showFlag": 0,
            "searchKey": search_key,
            "startDate": start_date,
            "endDate": end_date,
            "industry": "02",
            "searchType": 1,
            "matchType": 1,
            "thisTimes": int(this_times * 1000),
        }
        return config.MATERIAL_API_URL, headers, payload

    def search(self, search_key: str, start_date: str, end_date: str) -> Dict[str, Any]:
        url, headers, payload = self.build_search_request(search_key, start_date, end_date)
        response = self.session.post(url, headers=headers, data=payload, timeout=60)
        response.raise_for_status()
        return response.json()

    def fetch_all_materials(self, search_key: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        cache_key = (models.normalize_title(search_key), start_date, end_date)
        with self._cache_lock:
            cached = self._title_cache.get(cache_key)
        if cached is not None:
            return list(cached)
        formatted = self._fetch_all_materials_uncached(search_key, start_date, end_date)
        with self._cache_lock:
            self._title_cache[cache_key] = list(formatted)
        return list(formatted)

    def _fetch_all_materials_uncached(
        self,
        search_key: str,
        start_date: str,
        end_date: str,
        session: Optional[requests.Session] = None,
    ) -> List[Dict[str, Any]]:
        page_num = 1
        page_size = config.MATERIAL_PAGE_SIZE
        active_session = session or self.session
        self._ensure_material_service_available()
        data: Optional[Dict[str, Any]] = None
        retry_limit = self._material_retry_limit()
        for attempt in range(retry_limit + 1):
            self._wait_for_cooldown()
            url, headers, payload = self.build_search_request(
                search_key,
                start_date,
                end_date,
                page_num=page_num,
                page_size=page_size,
            )
            try:
                response = active_session.post(url, headers=headers, data=payload, timeout=60)
                status_code = int(getattr(response, "status_code", 200) or 200)
                if status_code == 429:
                    raise MaterialRateLimitError("HTTP 429")
                response.raise_for_status()
                data = response.json()
                status_code = int(_safe_int(data.get("statusCode")))
                if status_code == 429:
                    raise MaterialRateLimitError(
                        str(data.get("message") or data.get("msg") or data.get("statusMsg") or "rate limit")
                    )
                if status_code not in (0, 200):
                    message = data.get("message") or data.get("msg") or data.get("statusMsg") or "unknown error"
                    content = data.get("content")
                    if status_code in (401, 411) and isinstance(content, str) and "login" in content.lower():
                        raise RuntimeError("Material API unauthorized: material cookie expired or invalid")
                    raise RuntimeError(f"Material API error {data.get('statusCode')}: {message}")
                break
            except MaterialRateLimitError:
                context = f"title={search_key} page={page_num}"
                self._register_rate_limit(context, attempt + 1, retry_limit)
                self._ensure_material_service_available()
                if attempt >= retry_limit:
                    raise
            except requests.RequestException:
                raise
        if data is None:
            raise MaterialRateLimitError(f"Material API rate limit persisted: {search_key}")
        return [format_material_item(item) for item in _material_list(data)]

    def fetch_material_result(
        self,
        language: str,
        title: str,
        theater: str,
        start_date: str,
        end_date: str,
        persist_cache: bool = True,
    ) -> models.MaterialResult:
        normalized_theater = normalize_theater_name(theater)
        cached = self._read_result_cache(language, title, normalized_theater, start_date, end_date)
        if cached is not None:
            return cached
        materials = self.fetch_all_materials(title, start_date, end_date)
        materials = [item for item in materials if item.get("videoUrl")]
        theater_items = [item for item in materials if item.get("productName") == normalized_theater]
        duration_ok = [item for item in theater_items if _safe_float(item.get("durationMin")) >= config.MIN_VIDEO_DURATION_MINUTES]
        seen = set()
        unique = []
        for item in duration_ok:
            key = (item.get("playletName", ""), item.get("durationMin", 0))
            if key in seen:
                continue
            seen.add(key)
            unique.append(item)
        result = models.MaterialResult(
            language=language,
            title=title,
            theater=normalized_theater,
            qualified_count=len(unique),
            total_count=len(theater_items),
        )
        self._store_result_cache(result, start_date, end_date)
        if persist_cache:
            self._flush_daily_cache()
        return result

    def prefetch_material_results(
        self,
        candidates: Sequence[models.DramaRecord],
        start_date: str,
        end_date: str,
    ) -> Dict[Tuple[str, str, str], models.MaterialResult]:
        grouped: Dict[Tuple[str, str], List[Tuple[str, str, str]]] = {}
        output: Dict[Tuple[str, str, str], models.MaterialResult] = {}
        for candidate in candidates:
            language = str(candidate.language or "").strip()
            title = str(candidate.title or "").strip()
            theater = normalize_theater_name(candidate.theater)
            title_norm = models.normalize_title(title)
            if not language or not title_norm or not theater:
                continue
            cached = self._read_result_cache(language, title, theater, start_date, end_date)
            if cached is not None:
                output[(language, title_norm, theater)] = cached
                continue
            grouped.setdefault((title_norm, title), [])
            result_key = (language, title_norm, theater)
            if result_key in grouped[(title_norm, title)]:
                continue
            grouped[(title_norm, title)].append(result_key)

        if not grouped:
            return output
        try:
            self._ensure_material_service_available()
        except MaterialServiceDegradedError as exc:
            _log_warning(self.logger, f"[素材] 跳过批量预取: {exc}")
            return output
        workers = min(max(int(getattr(config, "MATERIAL_PREFETCH_WORKERS", 1) or 1), 1), 5)
        grouped_items = list(grouped.items())
        chunk_size = self._prefetch_chunk_size()
        for chunk_index in range(0, len(grouped_items), chunk_size):
            try:
                self._ensure_material_service_available()
            except MaterialServiceDegradedError as exc:
                _log_warning(self.logger, f"[素材] 跳过后续批量预取: {exc}")
                break
            chunk = grouped_items[chunk_index : chunk_index + chunk_size]
            chunk_output: Dict[Tuple[str, str, str], models.MaterialResult] = {}
            if workers <= 1 or len(chunk) <= 1:
                for (_title_norm, title), entries in chunk:
                    try:
                        self.fetch_all_materials(title, start_date, end_date)
                        for language, title_norm, theater in entries:
                            result = self.fetch_material_result(
                                language,
                                title,
                                theater,
                                start_date,
                                end_date,
                                persist_cache=False,
                            )
                            chunk_output[(language, title_norm, theater)] = result
                    except (MaterialRateLimitError, MaterialServiceDegradedError) as exc:
                        _log_warning(self.logger, f"[素材] 预取降级: title={title} reason={exc}")
                        if isinstance(exc, MaterialServiceDegradedError):
                            break
                    except Exception as exc:
                        _log_warning(self.logger, f"[素材] 预取失败: title={title} error={type(exc).__name__}: {exc}")
                output.update(chunk_output)
            else:
                with ThreadPoolExecutor(max_workers=min(workers, len(chunk))) as executor:
                    future_map = {
                        executor.submit(self._prefetch_material_group, title, entries, start_date, end_date): (title_norm, title)
                        for (title_norm, title), entries in chunk
                    }
                    for future, (_title_norm, title) in future_map.items():
                        try:
                            output.update(future.result())
                        except (MaterialRateLimitError, MaterialServiceDegradedError) as exc:
                            _log_warning(self.logger, f"[素材] 预取降级: title={title} reason={exc}")
                            if isinstance(exc, MaterialServiceDegradedError):
                                break
                        except Exception as exc:
                            _log_warning(self.logger, f"[素材] 预取失败: title={title} error={type(exc).__name__}: {exc}")
            if output:
                self._flush_daily_cache()
            if chunk_index + chunk_size < len(grouped_items) and not self._degraded:
                self._pause_after_chunk()
        return output

    def _prefetch_material_group(
        self,
        title: str,
        entries: Sequence[Tuple[str, str, str]],
        start_date: str,
        end_date: str,
    ) -> Dict[Tuple[str, str, str], models.MaterialResult]:
        session = requests.Session()
        session.trust_env = False
        try:
            materials = self._fetch_all_materials_uncached(title, start_date, end_date, session=session)
        finally:
            session.close()
        cache_key = (models.normalize_title(title), start_date, end_date)
        with self._cache_lock:
            self._title_cache[cache_key] = list(materials)
        output: Dict[Tuple[str, str, str], models.MaterialResult] = {}
        for language, title_norm, theater in entries:
            result = self.fetch_material_result(
                language,
                title,
                theater,
                start_date,
                end_date,
                persist_cache=False,
            )
            output[(language, title_norm, theater)] = result
        return output


def _material_list(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    content = data.get("content") or data.get("data") or {}
    if isinstance(content, dict):
        for key in ("searchList", "list", "records", "items"):
            if isinstance(content.get(key), list):
                return list(content.get(key) or [])
    if isinstance(content, list):
        return list(content)
    return []


def _material_total(data: Dict[str, Any]) -> int:
    content = data.get("content") or data.get("data") or {}
    if isinstance(content, dict):
        for key in ("totalRecord", "total", "totalCount"):
            if content.get(key) is not None:
                return _safe_int(content.get(key))
    return 0




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
