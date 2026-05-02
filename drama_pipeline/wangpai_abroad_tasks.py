from __future__ import annotations

import argparse
import csv
import json
import math
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence
from urllib.parse import urljoin

import requests


# PyCharm 直接运行时，把完整登录态 Cookie 填在三引号中间。
# 可以多行粘贴；程序会在发送请求前自动去掉换行和缩进。
# 也可以临时通过命令行参数 --cookie 覆盖这个值。
WANGPAI_COOKIE = """
2345login_fp=n8fSIzvEOEKrK1RZ; 2345login_fpi=n8fSIzvEOEKrK1RZ; abroad_ticket=2F083A658F3CFB6E3A39DC05483941116D9A2F820AEE3D49A4227F6AF481B85791FEF8475325865E376C9356096BBC4D; acw_tc=1bdd6bae17775558069332182ef62d76da75706eed416ddec0f9cbcef8; cdn_sec_tc=1bdd6bae17775558069332182ef62d76da75706eed416ddec0f9cbcef8
"""

TASK_LIST_URL = "https://api.yd126.com/merchant/web/abroad/task_list"
INCOME_DETAIL_URL = "https://api.yd126.com/merchant/web/abroad/income_detail"
ZMT_ENTRY_URL = "https://zmt.yd126.com/s/index.html#/"
DEFAULT_WASM_URL = "https://zmt.yd126.com/s/static/wasm/e0878ce0a6584e4b.module.wasm"
DEFAULT_CACHE_DIR = Path(__file__).resolve().parent / ".cache" / "wangpai"
DEFAULT_WASM_PATH = DEFAULT_CACHE_DIR / "e0878ce0a6584e4b.module.wasm"

ALL_THREAD = "全部剧场"
ALL_LANGUAGE = "全部语言"
ALL_COUNTRY = "全部国家"

THREAD_NAME_LIST = [
    "DreameShort",
    "Kalos",
    "KalosTV",
    "DramaBox",
    "ReelShort",
    "ShortMax",
    "GoodShort",
    "FlareFlow",
    "SnapDrama",
    "SnackShort",
    "Playlet",
    "HoneyReels",
    "TopShort",
    "FlexTV",
    "StarShort",
    "TorchShort",
    "StardustTV",
    "MoboReels",
    "TouchShort",
]

LANGUAGE_LIST = [
    "保加利亚语",
    "简体中文",
    "印尼语",
    "意大利语",
    "繁体中文",
    "捷克语",
    "西班牙语",
    "菲律宾语",
    "罗马尼亚语",
    "马来语",
    "日语",
    "德语",
    "挪威语",
    "阿拉伯语",
    "丹麦语",
    "土耳其语",
    "荷兰语",
    "越南语",
    "波兰语",
    "韩语",
    "葡语",
    "英语",
    "泰语",
    "俄语",
    "法语",
    "印地语",
    "瑞典语",
    "芬兰语",
]

CSV_FIELDS = [
    "task_id",
    "title",
    "thread_name",
    "language",
    "online_date",
    "pay_type",
    "pay_type_label",
    "copyright",
    "promotion_type",
    "country",
    "channel",
    "top_num",
    "is_new",
    "is_popular",
    "cps_subsidy_radio",
    "tag_name",
    "cover",
    "icon",
]

INCOME_CSV_FIELDS = [
    "income_id",
    "task_id",
    "title",
    "thread_name",
    "language",
    "order_date",
    "recharge_date",
    "income_sub_type",
    "income_sub_type_label",
    "income_amount",
    "charge_amount",
    "ad_income",
    "ad_num",
    "pay_type",
    "task_type",
    "is_predict",
    "promotion_type",
    "country",
    "settle_date",
    "created_at",
    "updated_at",
]


def build_task_params(
    *,
    task_type: Optional[int] = 1,
    pay_type: Optional[int] = 0,
    filter_type: Optional[int] = 0,
    thread_name: str = "",
    language: str = ALL_LANGUAGE,
    country: str = ALL_COUNTRY,
    page: int = 1,
    page_size: int = 500,
    title: str = "",
    promotion_type: str = "",
    sort_type: str = "",
    compact: bool = True,
) -> Dict[str, Any]:
    """Build query params for the Wangpai abroad task list endpoint.

    With compact=True, default "all/unlimited" filters are omitted because the
    endpoint returns the same logical result without them.
    """
    params: Dict[str, Any] = {}
    if not compact:
        params.update({"task_type": task_type, "pay_type": pay_type, "filter_type": filter_type})
    elif pay_type not in (None, "", 0, "0"):
        params["pay_type"] = pay_type

    normalized_thread = str(thread_name or "").strip()
    if normalized_thread and normalized_thread != ALL_THREAD:
        params["thread_name"] = normalized_thread

    normalized_language = str(language or "").strip()
    if normalized_language and (not compact or normalized_language != ALL_LANGUAGE):
        params["language"] = normalized_language

    normalized_country = str(country or "").strip()
    if normalized_country and (not compact or normalized_country != ALL_COUNTRY):
        params["country"] = normalized_country

    if not compact:
        params["page"] = page
        params["page_size"] = page_size
        params["title"] = title
        params["promotion_type"] = promotion_type
        return params

    params["page"] = int(page or 1)
    params["page_size"] = int(page_size or 500)
    if str(title or "").strip():
        params["title"] = str(title).strip()
    if str(promotion_type or "").strip():
        params["promotion_type"] = str(promotion_type).strip()
    if str(sort_type or "").strip():
        params["sort_type"] = str(sort_type).strip()
    return params


def build_income_params(
    *,
    search_keyword: str = "",
    income_sub_type: int = 0,
    search_type: int = 2,
    promotion_type: str = "",
    page: int = 1,
    page_size: int = 10,
) -> Dict[str, Any]:
    params: Dict[str, Any] = {
        "income_sub_type": int(income_sub_type or 0),
        "search_type": int(search_type or 2),
        "page": int(page or 1),
        "page_size": int(page_size or 10),
    }
    if str(search_keyword or "").strip():
        params["search_keyword"] = str(search_keyword).strip()
    if str(promotion_type or "").strip():
        params["promotion_type"] = str(promotion_type).strip()
    return params


def parse_decrypted_payload(decrypted_text: str) -> Dict[str, Any]:
    value: Any = decrypted_text
    for _ in range(2):
        if isinstance(value, str) and value.strip():
            value = json.loads(value)
    if not isinstance(value, dict):
        raise ValueError(f"Expected decrypted payload to be an object, got {type(value).__name__}")
    return value


def structure_task_item(item: Mapping[str, Any]) -> Dict[str, Any]:
    tags = item.get("tag_name")
    if tags is None:
        tag_value: List[str] = []
    elif isinstance(tags, list):
        tag_value = [str(tag) for tag in tags if str(tag).strip()]
    else:
        tag_value = [part.strip() for part in str(tags).replace("，", ",").split(",") if part.strip()]

    pay_type = _safe_int(item.get("pay_type"))
    return {
        "task_id": str(item.get("task_id") or ""),
        "title": str(item.get("title") or ""),
        "thread_name": str(item.get("thread_name") or ""),
        "language": str(item.get("language") or ""),
        "online_date": str(item.get("online_date") or ""),
        "pay_type": pay_type,
        "pay_type_label": _pay_type_label(pay_type),
        "copyright": str(item.get("copyright") or ""),
        "promotion_type": str(item.get("promotion_type") or ""),
        "country": str(item.get("country") or ""),
        "channel": item.get("channel"),
        "top_num": _safe_int(item.get("top_num")),
        "is_new": _safe_int(item.get("is_new")),
        "is_popular": _safe_int(item.get("is_popular")),
        "cps_subsidy_radio": _safe_float(item.get("cps_subsidy_radio")),
        "tag_name": tag_value,
        "cover": str(item.get("cover") or ""),
        "icon": str(item.get("icon") or ""),
        "raw": dict(item),
    }


def structure_income_item(item: Mapping[str, Any]) -> Dict[str, Any]:
    income_sub_type = _safe_int(item.get("income_sub_type"))
    income_amount = _first_number(
        item,
        "income_amount",
        "amount",
        "income",
        "settle_amount",
        "total_income",
        "rmb_income",
        "money",
    )
    order_date = str(item.get("date") or item.get("settle_date") or item.get("income_date") or "")
    return {
        "income_id": str(item.get("income_id") or item.get("id") or item.get("abroad_id") or ""),
        "task_id": str(item.get("task_id") or ""),
        "title": str(item.get("title") or item.get("task_title") or item.get("name") or ""),
        "thread_name": str(item.get("thread_name") or item.get("theater_name") or item.get("theater") or ""),
        "language": str(item.get("language") or ""),
        "order_date": order_date,
        "recharge_date": str(item.get("recharge_date") or ""),
        "income_sub_type": income_sub_type,
        "income_sub_type_label": _income_sub_type_label(income_sub_type),
        "income_amount": income_amount,
        "charge_amount": _first_number(item, "charge_amount", "charge_num"),
        "ad_income": _first_number(item, "ad_income"),
        "ad_num": _first_number(item, "ad_num"),
        "pay_type": _safe_int(item.get("pay_type")),
        "task_type": _safe_int(item.get("task_type")),
        "is_predict": _safe_int(item.get("is_predict")),
        "promotion_type": str(item.get("promotion_type") or ""),
        "country": str(item.get("country") or ""),
        "settle_date": order_date,
        "created_at": str(item.get("created_at") or item.get("create_time") or ""),
        "updated_at": str(item.get("updated_at") or item.get("update_time") or ""),
        "raw": dict(item),
    }


def normalize_cookie(cookie: str) -> str:
    text = str(cookie or "").strip()
    if not text:
        return ""
    parts = [part.strip() for part in text.replace("\r", "\n").split("\n") if part.strip()]
    merged = ""
    for part in parts:
        if not merged:
            merged = part
        elif merged.endswith(";"):
            merged += " " + part
        else:
            merged += part
    return " ".join(merged.split())


def parse_cookie_pairs(cookie: str) -> Dict[str, str]:
    pairs: Dict[str, str] = {}
    for part in normalize_cookie(cookie).split(";"):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        key = key.strip()
        if key:
            pairs[key] = value.strip()
    return pairs


def create_session() -> requests.Session:
    session = requests.Session()
    session.trust_env = False
    return session


class WasmtimeDecryptor:
    def __init__(self, wasm_path: Path):
        try:
            import wasmtime
        except Exception as exc:  # pragma: no cover - exercised by fallback path when dependency is absent.
            raise RuntimeError("wasmtime is not installed") from exc

        self.wasmtime = wasmtime
        self.store = wasmtime.Store()
        module = wasmtime.Module.from_file(self.store.engine, str(wasm_path))
        self.heap: List[Any] = [None] * 128 + [None, None, True, False]
        self.heap_next = len(self.heap)
        self.memory = None

        linker = wasmtime.Linker(self.store.engine)
        string_new_type = wasmtime.FuncType(
            [wasmtime.ValType.i32(), wasmtime.ValType.i32()],
            [wasmtime.ValType.i32()],
        )
        linker.define(self.store, "wbg", "__wbindgen_string_new", wasmtime.Func(self.store, string_new_type, self._string_new))
        instance = linker.instantiate(self.store, module)
        exports = instance.exports(self.store)
        self.memory = exports["memory"]
        self.malloc = exports["__wbindgen_export_0"]
        self.realloc = exports["__wbindgen_export_1"]
        self.free = exports["__wbindgen_export_2"]
        self.stack_pointer = exports["__wbindgen_add_to_stack_pointer"]
        self.decrypt_api_func = exports["decrypt_api"]

    def decrypt_api(self, encrypted_text: str) -> str:
        retptr = self.stack_pointer(self.store, -16)
        try:
            ptr, length = self._pass_string(encrypted_text)
            self.decrypt_api_func(self.store, retptr, ptr, length)
            out_ptr = self._read_i32(retptr)
            out_len = self._read_i32(retptr + 4)
            err_idx = self._read_i32(retptr + 8)
            has_error = self._read_i32(retptr + 12)
            if has_error:
                detail = self.heap[err_idx] if 0 <= err_idx < len(self.heap) else err_idx
                raise RuntimeError(f"decrypt_api failed: {detail}")
            output = self.memory.read(self.store, out_ptr, out_ptr + out_len).decode("utf-8")
            self.free(self.store, out_ptr, out_len, 1)
            return output
        finally:
            self.stack_pointer(self.store, 16)

    def _pass_string(self, value: str) -> tuple[int, int]:
        data = value.encode("utf-8")
        ptr = self.malloc(self.store, len(data), 1)
        self.memory.write(self.store, data, ptr)
        return ptr, len(data)

    def _read_i32(self, ptr: int) -> int:
        return int.from_bytes(self.memory.read(self.store, ptr, ptr + 4), "little", signed=True)

    def _string_new(self, ptr: int, length: int) -> int:
        value = self.memory.read(self.store, ptr, ptr + length).decode("utf-8")
        return self._add_heap_object(value)

    def _add_heap_object(self, value: Any) -> int:
        if self.heap_next == len(self.heap):
            self.heap.append(len(self.heap) + 1)
        index = self.heap_next
        self.heap_next = self.heap[index]
        self.heap[index] = value
        return index


class NodeDecryptor:
    def __init__(self, wasm_path: Path):
        self.wasm_path = Path(wasm_path)
        self.bridge_path = self.wasm_path.with_name("wangpai_decrypt_bridge.js")
        ensure_node_bridge(self.bridge_path)

    def decrypt_api(self, encrypted_text: str) -> str:
        try:
            result = subprocess.run(
                ["node", str(self.bridge_path), str(self.wasm_path), encrypted_text],
                check=True,
                capture_output=True,
                text=True,
                encoding="utf-8",
            )
        except FileNotFoundError as exc:
            raise RuntimeError("Node.js is required when wasmtime is unavailable") from exc
        except subprocess.CalledProcessError as exc:
            raise RuntimeError((exc.stderr or exc.stdout or "node decrypt bridge failed").strip()) from exc
        return result.stdout


class WangpaiAbroadClient:
    def __init__(
        self,
        cookie: str,
        *,
        session: Optional[requests.Session] = None,
        wasm_path: Path | str | None = None,
        timeout: int = 30,
    ):
        self.cookie = normalize_cookie(cookie)
        self.session = session or create_session()
        self.timeout = timeout
        self.wasm_path = ensure_wasm_file(Path(wasm_path) if wasm_path else DEFAULT_WASM_PATH)
        self.decryptor = create_decryptor(self.wasm_path)

    def build_headers(self) -> Dict[str, str]:
        headers = {
            "Cookie": self.cookie,
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://yd126.com",
            "Referer": "https://zmt.yd126.com/s/index.html",
            "platform": "browser",
            "appChannel": "gf_default",
            "isH5": "1",
        }
        ticket = parse_cookie_pairs(self.cookie).get("abroad_ticket")
        if ticket:
            headers["ticket"] = ticket
        return headers

    def fetch_task_page(self, **kwargs: Any) -> Dict[str, Any]:
        params = build_task_params(**kwargs)
        return self._fetch_encrypted_get(TASK_LIST_URL, params)

    def fetch_income_page(self, **kwargs: Any) -> Dict[str, Any]:
        params = build_income_params(**kwargs)
        return self._fetch_encrypted_get(INCOME_DETAIL_URL, params)

    def _fetch_encrypted_get(self, url: str, params: Mapping[str, Any]) -> Dict[str, Any]:
        response = self.session.get(url, params=params, headers=self.build_headers(), timeout=self.timeout)
        response.raise_for_status()
        outer = response.json()
        if outer.get("code") != 200:
            raise RuntimeError(f"Wangpai API failed: url={url} code={outer.get('code')} msg={outer.get('msg')}")
        encrypted = outer.get("data")
        if not isinstance(encrypted, str):
            raise RuntimeError("Wangpai API response data is not an encrypted string")
        decrypted = self.decryptor.decrypt_api(encrypted)
        return parse_decrypted_payload(decrypted)

    def fetch_structured_page(self, **kwargs: Any) -> Dict[str, Any]:
        payload = self.fetch_task_page(**kwargs)
        rows = [structure_task_item(item) for item in _payload_list(payload)]
        return {"list": rows, "meta": payload.get("meta") or {}}

    def fetch_structured_income_page(self, **kwargs: Any) -> Dict[str, Any]:
        payload = self.fetch_income_page(**kwargs)
        rows = [structure_income_item(item) for item in _payload_list(payload)]
        return {"list": rows, "meta": payload.get("meta") or {}}

    def fetch_all_tasks(
        self,
        *,
        max_pages: int = 1,
        page_size: int = 500,
        **kwargs: Any,
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        first_page = int(kwargs.pop("page", 1) or 1)
        for page in range(first_page, first_page + max_pages):
            page_payload = self.fetch_structured_page(page=page, page_size=page_size, **kwargs)
            page_rows = list(page_payload.get("list") or [])
            rows.extend(page_rows)
            pagination = ((page_payload.get("meta") or {}).get("pagination") or {})
            total_pages = _safe_int(pagination.get("total_pages"))
            per_page = _safe_int(pagination.get("per_page")) or page_size
            total = _safe_int(pagination.get("total"))
            if len(page_rows) < page_size:
                break
            if total_pages and page >= total_pages:
                break
            if total and page * per_page >= total:
                break
        return rows

    def fetch_all_income(
        self,
        *,
        max_pages: int = 1,
        page_size: int = 10,
        **kwargs: Any,
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        first_page = int(kwargs.pop("page", 1) or 1)
        for page in range(first_page, first_page + max_pages):
            page_payload = self.fetch_structured_income_page(page=page, page_size=page_size, **kwargs)
            page_rows = list(page_payload.get("list") or [])
            rows.extend(page_rows)
            pagination = ((page_payload.get("meta") or {}).get("pagination") or {})
            total_pages = _safe_int(pagination.get("total_pages"))
            per_page = _safe_int(pagination.get("per_page")) or page_size
            total = _safe_int(pagination.get("total"))
            if len(page_rows) < page_size:
                break
            if total_pages and page >= total_pages:
                break
            if total and page * per_page >= total:
                break
        return rows


def create_decryptor(wasm_path: Path) -> Any:
    try:
        return WasmtimeDecryptor(wasm_path)
    except Exception:
        return NodeDecryptor(wasm_path)


def ensure_wasm_file(path: Path = DEFAULT_WASM_PATH) -> Path:
    if path.exists() and path.stat().st_size > 0:
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    response = requests.get(DEFAULT_WASM_URL, headers={"User-Agent": "Mozilla/5.0", "Referer": ZMT_ENTRY_URL}, timeout=30)
    response.raise_for_status()
    path.write_bytes(response.content)
    return path


def ensure_node_bridge(path: Path) -> Path:
    if path.exists():
        return path
    path.write_text(NODE_BRIDGE_SOURCE, encoding="utf-8")
    return path


def write_json(rows: Sequence[Mapping[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(list(rows), ensure_ascii=False, indent=2), encoding="utf-8")


def write_csv(rows: Sequence[Mapping[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8-sig", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            csv_row = dict(row)
            if isinstance(csv_row.get("tag_name"), list):
                csv_row["tag_name"] = "|".join(str(tag) for tag in csv_row["tag_name"])
            writer.writerow(csv_row)


def write_income_csv(rows: Sequence[Mapping[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8-sig", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=INCOME_CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))


def build_console_payload(rows: Sequence[Mapping[str, Any]], *, mode: str) -> Dict[str, Any]:
    key = "orders" if mode == "income" else "tasks"
    return {"count": len(rows), key: list(rows)}


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch and decrypt Wangpai abroad drama task list.")
    parser.add_argument("--mode", choices=["tasks", "income"], default="income", help="Fetch tasks or income detail.")
    parser.add_argument("--cookie", default=normalize_cookie(WANGPAI_COOKIE), help="Login cookie. Defaults to top-level WANGPAI_COOKIE.")
    parser.add_argument("--thread-name", default="KalosTV", help="Theater name, for example KalosTV. Use 全部剧场 for all.")
    parser.add_argument("--language", default=ALL_LANGUAGE, help="Language filter. Default: 全部语言.")
    parser.add_argument("--country", default=ALL_COUNTRY, help="Country filter. Default: 全部国家.")
    parser.add_argument("--title", default="", help="Title keyword filter.")
    parser.add_argument("--promotion-type", default="", help="Promotion type filter, for example self or tto.")
    parser.add_argument("--pay-type", type=int, default=0, help="Pay type filter. 0 means all/unlimited.")
    parser.add_argument("--search-keyword", default="", help="Income keyword filter.")
    parser.add_argument("--income-sub-type", type=int, default=0, help="Income subtype filter. 0 means all.")
    parser.add_argument("--search-type", type=int, default=2, help="Income search type. The frontend currently uses 2.")
    parser.add_argument("--page", type=int, default=1, help="Start page.")
    parser.add_argument("--page-size", type=int, default=500, help="Page size.")
    parser.add_argument("--max-pages", type=int, default=1, help="Maximum pages to fetch.")
    parser.add_argument("--wasm-path", default=str(DEFAULT_WASM_PATH), help="Local decrypt wasm path.")
    parser.add_argument("--output-json", default="", help="Optional JSON output path.")
    parser.add_argument("--output-csv", default="", help="Optional CSV output path.")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    if not args.cookie.strip():
        raise SystemExit("Missing cookie. Fill WANGPAI_COOKIE at the top of this file or pass --cookie.")
    client = WangpaiAbroadClient(args.cookie, wasm_path=Path(args.wasm_path))
    if args.mode == "income":
        rows = client.fetch_all_income(
            search_keyword=args.search_keyword,
            income_sub_type=args.income_sub_type,
            search_type=args.search_type,
            promotion_type=args.promotion_type,
            page=args.page,
            page_size=args.page_size,
            max_pages=args.max_pages,
        )
    else:
        rows = client.fetch_all_tasks(
            thread_name=args.thread_name,
            language=args.language,
            country=args.country,
            title=args.title,
            promotion_type=args.promotion_type,
            pay_type=args.pay_type,
            page=args.page,
            page_size=args.page_size,
            max_pages=args.max_pages,
        )
    if args.output_json:
        write_json(rows, Path(args.output_json))
    if args.output_csv:
        if args.mode == "income":
            write_income_csv(rows, Path(args.output_csv))
        else:
            write_csv(rows, Path(args.output_csv))
    print(json.dumps(build_console_payload(rows, mode=args.mode), ensure_ascii=False, indent=2))
    return 0


def _payload_list(payload: Mapping[str, Any]) -> List[Mapping[str, Any]]:
    rows = payload.get("list")
    if rows is None:
        rows = payload.get("item") or []
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, Mapping)]
    return []


def _pay_type_label(value: int) -> str:
    if value == 1:
        return "CPS/分成"
    if value == 2:
        return "CPA/拉新"
    if value == 0:
        return "全部/不限"
    return str(value)


def _income_sub_type_label(value: int) -> str:
    if value == 0:
        return "全部"
    return str(value)


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def _safe_float(value: Any) -> float:
    try:
        number = float(value)
    except Exception:
        return 0.0
    if math.isnan(number) or math.isinf(number):
        return 0.0
    return number


def _first_number(row: Mapping[str, Any], *keys: str) -> float:
    for key in keys:
        if key in row and row.get(key) not in (None, ""):
            return _safe_float(row.get(key))
    return 0.0


NODE_BRIDGE_SOURCE = r"""const fs = require('fs');
let wasm;
const decoder = new TextDecoder('utf-8', { ignoreBOM: true, fatal: true });
decoder.decode();
const encoder = new TextEncoder('utf-8');
let vectorLen = 0;
const heap = new Array(128).fill(undefined);
heap.push(undefined, null, true, false);
let heapNext = heap.length;
function memU8() { return new Uint8Array(wasm.memory.buffer); }
function dataView() { return new DataView(wasm.memory.buffer); }
function addHeapObject(obj) {
  if (heapNext === heap.length) heap.push(heap.length + 1);
  const idx = heapNext;
  heapNext = heap[idx];
  heap[idx] = obj;
  return idx;
}
function takeObject(idx) {
  const ret = heap[idx];
  if (idx >= 132) {
    heap[idx] = heapNext;
    heapNext = idx;
  }
  return ret;
}
function getString(ptr, len) {
  return decoder.decode(memU8().subarray(ptr >>> 0, (ptr >>> 0) + len));
}
function passString(value, malloc) {
  const bytes = encoder.encode(value);
  const ptr = malloc(bytes.length, 1) >>> 0;
  memU8().subarray(ptr, ptr + bytes.length).set(bytes);
  vectorLen = bytes.length;
  return ptr;
}
function decryptApi(value) {
  const retptr = wasm.__wbindgen_add_to_stack_pointer(-16);
  try {
    const ptr = passString(value, wasm.__wbindgen_export_0);
    wasm.decrypt_api(retptr, ptr, vectorLen);
    const view = dataView();
    const r0 = view.getInt32(retptr + 0, true);
    const r1 = view.getInt32(retptr + 4, true);
    const r2 = view.getInt32(retptr + 8, true);
    const r3 = view.getInt32(retptr + 12, true);
    if (r3) throw takeObject(r2);
    const output = decoder.decode(memU8().subarray(r0 >>> 0, (r0 >>> 0) + r1).slice());
    wasm.__wbindgen_export_2(r0, r1, 1);
    return output;
  } finally {
    wasm.__wbindgen_add_to_stack_pointer(16);
  }
}
(async () => {
  const wasmPath = process.argv[2];
  const encrypted = process.argv[3];
  const imports = { wbg: { __wbindgen_string_new: (ptr, len) => addHeapObject(getString(ptr, len)) } };
  const bytes = fs.readFileSync(wasmPath);
  const result = await WebAssembly.instantiate(bytes, imports);
  wasm = result.instance.exports;
  process.stdout.write(decryptApi(encrypted));
})().catch((error) => {
  console.error(error && error.stack ? error.stack : String(error));
  process.exit(1);
});
"""


if __name__ == "__main__":
    raise SystemExit(main())
