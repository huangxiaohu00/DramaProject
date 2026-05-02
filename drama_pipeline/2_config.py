import os
import re
from pathlib import Path
from typing import Any


def _split_env_list(value: str) -> list[str]:
    text = str(value or "").replace("\r", "\n")
    parts = text.replace(";", "\n").split("\n")
    return [item.strip() for item in parts if item.strip()]


PIPELINE_DIR = Path(__file__).resolve().parent
OUTPUT_ROOT = PIPELINE_DIR / "output"
TODAY_OUTPUT_ROOT = OUTPUT_ROOT / "today_recommend"
ORDER_OUTPUT_ROOT = OUTPUT_ROOT / "yesterday_orders"


LANGUAGE_CONFIG = {
    2: "英语",
    5: "法语",
    7: "葡萄牙语",
    11: "德语",
    12: "繁体中文",
    13: "俄语",
    14: "意大利语",
}
LANGUAGE_ORDER = list(LANGUAGE_CONFIG.values())
LANGUAGE_NAME_TO_CODE = {name: code for code, name in LANGUAGE_CONFIG.items()}


THEATER_NAMES = {
    "moboreels": "MoboReels",
    "dramabox": "DramaBox",
    "shortmax": "ShortMax",
    "flareflow": "FlareFlow",
    "reelshort": "ReelShort",
    "honeyreels": "HoneyReels",
    "snackshort": "SnackShort",
    "sanckshort": "SnackShort",
    "starshort": "StarShort",
    "stashort": "StarShort",
    "kalostv": "KalosTV",
    "dreameshort": "DreamShort",
    "goodshort": "GoodShort",
    "flickreels": "FlickReels",
    "flextv": "FlexTV",
    "footage": "Footage",
    "topshort": "TopShort",
}
THEATER_NAME_TO_ID = {name: key for key, name in THEATER_NAMES.items()}
THEATER_NAME_TO_ID["SnackShort"] = "snackshort"
THEATER_NAME_TO_ID["StarShort"] = "starshort"


MOBO_URL = "https://kocserver-cn.cdreader.com/api/v1/res/getlistpc"
MOBO_ORDER_URL = "https://kocserver-cn.cdreader.com/api/Report/GetMDetailsReport"
MOBO_REELS_PLATFORM_ID = 6833
MOBO_PLATFORM_IDS = {
    "MoboReels": 6833,
    "SnackShort": 1281,
    "KalosTV": 1211,
    "ShortMax": 1331,
    "HoneyReels": 1291,
    "FlexTV": 1341,
    "Footage": 1251,
    "TopShort": 1321,
    "FlickReels": 1311,
}
MOBO_LANG_MAP = {
    "英语": 3,
    "法语": 6,
    "葡萄牙语": 5,
    "德语": 16,
    "繁体中文": 2,
    "俄语": 7,
    "意大利语": 8,
}
MOBO_ORDER_TYPE = {
    "new": 0,
    "recommend": 1,
    "hot": 2,
}
NEW_DRAMA_FETCH_PAGE_SIZE = 1000
NEW_DRAMA_FETCH_PAGE_COUNT = 1
MOBO_DRAMA_AUTH = os.environ.get("MOBO_DRAMA_AUTH", "")
MOBO_AUTHS = _split_env_list(os.environ.get("MOBO_AUTHS", ""))


BEIDOU_TASK_PAGE_URL = "https://api-scenter.inbeidou.cn/agent/v1/task/page"
BEIDOU_ORDER_URL = "https://api-scenter.inbeidou.cn/agent/v1/sett/order/promotion_code_page"
BEIDOU_ORDER_DETAIL_URL = "https://api-scenter.inbeidou.cn/agent/v1/sett/order/promotion_code_detail"
BEIDOU_AGENT_ID = 2851723045
BEIDOU_AUTHS = _split_env_list(os.environ.get("BEIDOU_AUTHS", ""))
BEIDOU_DRAMA_AUTH = os.environ.get("BEIDOU_DRAMA_AUTH", "")


FEISHU_TOKEN_URL = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
FEISHU_RECORDS_URL_TEMPLATE = "https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
FEISHU_SHEET_META_URL_TEMPLATE = "https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/metainfo"
FEISHU_SHEET_VALUES_URL_TEMPLATE = "https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values/{range_text}"
FEISHU_APP_ID = os.environ.get("FEISHU_APP_ID", "")
FEISHU_APP_SECRET = os.environ.get("FEISHU_APP_SECRET", "")
FEISHU_APP_TOKEN = os.environ.get("FEISHU_APP_TOKEN", "")
FEISHU_TABLES = {
    "英语": "tblwLRWnyBuY0i8m",
    "法语": "tblgEyMgNlGDAQPx",
    "葡萄牙语": "tbl9yAsugolc2eZ4",
    "德语": "tblJ1WePzAyzGEMu",
    "意大利语": "tblOP3ttyc4u2eKh",
    "繁体中文": "tblJZylft8kBnOdh",
    "俄语": "tblqxeb6xtqJzYYO",
}
FEISHU_BEIDOU_HOT_URL = "https://inbeidou.feishu.cn/sheets/JzA8s0HIhhVKmAtpMm8cBAUKn8g?from=from_copylink"
FEISHU_BEIDOU_HOT_SPREADSHEET_TOKEN = "JzA8s0HIhhVKmAtpMm8cBAUKn8g"
FEISHU_BEIDOU_HOT_SHEET_NAME = "每日爆款短剧"


DUOLE_SHARE_URL = "https://www.kdocs.cn/l/cfKncYEvVVGd"
DUOLE_COOKIE = os.environ.get("DUOLE_COOKIE", "")
DUOLE_TARGET_SHEETS = ["2.推荐剧单", "12.DramaBox英语", "13.DramaBox小语种"]
DUOLE_SHEET_LIMITS = {
    "2.推荐剧单": 600,
    "12.DramaBox英语": 200,
    "13.DramaBox小语种": 400,
}
DUOLE_SHEET_LAYOUTS = {
    "2.推荐剧单": {
        "header_row": 4,
        "max_data_rows": 1000,
        "headers": ["日期", "剧场", "剧名", "语言", "类型", "理由", "素材"],
        "title_column": "剧名",
        "language_column": "语言",
        "theater_column": "剧场",
        "recommend_column": "日期",
    },
    "12.DramaBox英语": {
        "header_row": 1,
        "max_data_rows": 1000,
        "used_cols": 9,
        "headers": ["更新日期", "上架时间", "col_C", "col_D", "外语名", "col_F", "col_G", "col_H", "备注"],
        "date_columns": ["更新日期", "上架时间"],
        "title_column": "外语名",
        "publish_column": "上架时间",
        "recommend_column": "更新日期",
        "note_column": "备注",
        "note_filter_required": True,
        "fixed_columns": {
            "recommend": "A",
            "publish_at": "B",
            "title": "E",
            "note": "I",
        },
        "language_name": "英语",
        "theater_name": "DramaBox",
    },
    "13.DramaBox小语种": {
        "header_row": 1,
        "max_data_rows": 1000,
        "used_cols": 10,
        "headers": ["更新时间", "上架时间", "语种", "col_D", "col_E", "外语名", "col_G", "col_H", "col_I", "备注"],
        "date_columns": ["更新时间", "上架时间"],
        "title_column": "外语名",
        "publish_column": "上架时间",
        "recommend_column": "更新时间",
        "language_column": "语种",
        "note_column": "备注",
        "note_filter_required": True,
        "fixed_columns": {
            "recommend": "A",
            "publish_at": "B",
            "language": "C",
            "title": "F",
            "note": "J",
        },
        "theater_name": "DramaBox",
    },
}
DUOLE_RECOMMEND_NOTE_KEYWORDS = ("重点", "爆款", "高优", "排期", "热榜", "优先", "TOP", "充值")
DUOLE_LOCAL_CANDIDATES = [
    "多乐.xlsx",
    "多乐推荐剧单.xlsx",
    "【多乐】平台常用信息汇总.xlsx",
    "cfKncYEvVVGd.xlsx",
]
DUOLE_USE_LOCAL_FIRST = False
DUOLE_WEB_CHUNK_SIZE = 700
DUOLE_WEB_WAIT_SECONDS = 20
DUOLE_WEB_BATCH_RETRIES = 3
DUOLE_WEB_SHEET_RETRIES = 3
DUOLE_WEB_USE_BATCH = False


MATERIAL_API_URL = "https://oversea-v2.dataeye.com/api/playlet/creative/searchCreative"
MATERIAL_COOKIE = os.environ.get("MATERIAL_COOKIE", "")
VIDEO_THRESHOLD = 10
MIN_VIDEO_DURATION_MINUTES = 2.0
MATERIAL_LOOKBACK_DAYS = 730
MATERIAL_PAGE_SIZE = 1000
MATERIAL_CHECK_ENABLED = False
MATERIAL_SHORTLIST_MULTIPLIER = 5
MATERIAL_PREFETCH_CHUNK_SIZE = 5
MATERIAL_PREFETCH_ENABLED = True
MATERIAL_PREFETCH_WORKERS = 5
MATERIAL_PREFETCH_PAUSE_MIN_SECONDS = 1.0
MATERIAL_PREFETCH_PAUSE_MAX_SECONDS = 2.0
MATERIAL_MAX_EXPANSION_WAVES_PER_POOL = 20
MATERIAL_POST_VALIDATION_ENABLED = False
MATERIAL_FAILED_COOLDOWN_DAYS = 7
MATERIAL_RATE_LIMIT_RETRY_COUNT = 2
MATERIAL_RATE_LIMIT_COOLDOWN_MIN_SECONDS = 2.0
MATERIAL_RATE_LIMIT_COOLDOWN_MAX_SECONDS = 4.0
MATERIAL_MAX_FAILURES = 5
MATERIAL_TODAY_CACHE_ENABLED = True
TODAY_COLLECT_WORKERS = 4
MAX_AI_ANIME_PER_LANGUAGE = 3
ENGLISH_LOCAL_TRANSLATED_RATIO = 0.5


LANGUAGE_THEATER_QUOTAS = {
    "德语": {"MoboReels": 2, "ShortMax": 2, "FlareFlow": 2, "ReelShort": 2, "DramaBox": 4},
    "葡萄牙语": {"MoboReels": 2, "ShortMax": 2, "FlareFlow": 2, "ReelShort": 2, "HoneyReels": 2},
    "英语": {"MoboReels": 2, "ShortMax": 2, "FlareFlow": 2, "SnackShort": 2, "StarShort": 2},
    "意大利语": {"MoboReels": 4, "ShortMax": 4, "ReelShort": 2},
    "俄语": {"MoboReels": 4, "ShortMax": 2},
    "繁体中文": {"MoboReels": 4, "ShortMax": 2},
    "法语": {"MoboReels": 2, "ShortMax": 2, "HoneyReels": 2, "ReelShort": 2, "DramaBox": 4},
}
DEFAULT_LANGUAGE_TARGET = 10

AGE_BUCKET_NEW_DAYS = 14
AGE_BUCKET_MID_DAYS = 28
AGE_BUCKET_UNKNOWN = "未知"

DATE_SIGNAL_NEW_DAYS = 14
DATE_SIGNAL_MID_DAYS = 28

MOBO_RECOMMEND_NEW_MAX_RANK = 50
MOBO_RECOMMEND_MID_MAX_RANK = 100
MOBO_RECOMMEND_OLD_MAX_RANK = 500

BEIDOU_NEW_MAX_RANK = 200
BEIDOU_NEW_MAX_DAYS = 28
BEIDOU_MID_MAX_RANK = 400
BEIDOU_MID_MAX_DAYS = 60
BEIDOU_OLD_MAX_RANK = 1000

DATE_SIGNAL_SCORES = {
    "recent_multi": 100.0,
    "recent_single": 85.0,
    "mid_multi": 70.0,
    "mid_single": 55.0,
    "none": 0.0,
}
MOBO_RECOMMEND_SCORES = {
    "新爆": 100.0,
    "中爆": 70.0,
    "老爆": 40.0,
    "无": 0.0,
}
BEIDOU_VALIDATION_SCORES = {
    "新爆": 100.0,
    "中爆": 70.0,
    "老爆": 40.0,
    "无": 0.0,
}
FRESHNESS_SCORES = {
    "新": 100.0,
    "中": 70.0,
    "老": 30.0,
    AGE_BUCKET_UNKNOWN: 30.0,
}
LAYER_ORDER = ("A", "B", "C", "D", "E", "F", "G")
LAYER_SCORE_WEIGHTS = {
    "default": {"explosion": 0.45, "freshness": 0.35, "revenue": 0.20},
    "F": {"explosion": 0.55, "freshness": 0.15, "revenue": 0.30},
}


SOURCE_WEIGHTS = {
    "mobo_new": 1.0,
    "beidou_new": 1.0,
    "mobo_recommend": 1.5,
    "beidou_hot": 1.0,
    "beidou_income": 2.0,
    "duole_recommend": 1.5,
}
DEFAULT_SOURCE_WEIGHT = 0.5
RANK_SCORE_MIN = 0.35
SOURCE_RANK_MAX = {
    "mobo_new": 200,
    "beidou_new": 200,
    "mobo_recommend": 200,
    "beidou_hot": 200,
    "beidou_income": 200,
    "duole_recommend": 600,
}

TAG_SCORING_WORKBOOK = PIPELINE_DIR / "basic_data" / "drama_theme_tag_analysis.xlsx"
TAG_SCORE_FLOOR = 0.8
TAG_DEFAULT_SCORE = 1.0
TAG_AMOUNT_WEIGHT = 0.85
TAG_ORDER_WEIGHT = 0.15
TAG_SAMPLE_CONFIDENCE_ORDER_CAP = 30
TAG_SCORE_CAP = 1.2
TAG_COMBINE_DECAYS = (1.0, 0.5, 0.25)
TAG_LEVEL_CONFIDENCE = {
    "language_theater": 1.0,
    "language": 0.8,
    "theater": 0.6,
    "global": 0.5,
    "default": 0.3,
}
TAG_EXCLUDED_BROAD_TAGS = (
    "古代",
    "现代",
    "年代",
    "女频",
    "男频",
    "现实",
    "都市",
    "古代言情",
    "现代言情",
    "都市情感",
)


ADULT_TITLE_KEYWORDS = (
    "sex",
    "sexual",
    "sexuel",
    "sessual",
    "seduce",
    "seduc",
    "sedurr",
    "禁忌",
    "裸露",
)
ADULT_CONTENT_KEYWORDS = tuple(dict.fromkeys(ADULT_TITLE_KEYWORDS + ("成人", "adult")))
AI_CONTENT_KEYWORDS = ("ai", "转绘")
ANIME_CONTENT_KEYWORDS = ("漫剧", "动漫", "动态漫")
AI_ANIME_KEYWORDS = AI_CONTENT_KEYWORDS + ANIME_CONTENT_KEYWORDS
AI_CONTENT_MULTIPLIER = 0.8
ANIME_CONTENT_MULTIPLIER = 0.6
LOCAL_DRAMA_KEYWORDS = ("本土剧",)
TRANSLATED_DRAMA_KEYWORDS = ("翻译剧", "版权剧")


def normalize_theater_name(value: Any) -> str:
    text = "" if value is None else str(value).strip()
    if not text:
        return ""
    compact = re.sub(r"[\s_-]+", "", text).lower()
    direct = THEATER_NAMES.get(compact)
    if direct:
        return direct
    for alias, display in THEATER_NAMES.items():
        if alias in compact or compact in alias:
            return display
    return text

MATERIAL_COOKIE = os.environ.get("MATERIAL_COOKIE") or MATERIAL_COOKIE


def missing_today_config() -> list[str]:
    missing = []
    if not (MOBO_DRAMA_AUTH or MOBO_AUTHS):
        missing.append("MOBO_DRAMA_AUTH")
    if not BEIDOU_DRAMA_AUTH:
        missing.append("BEIDOU_DRAMA_AUTH")
    if not FEISHU_APP_ID:
        missing.append("FEISHU_APP_ID")
    if not FEISHU_APP_SECRET:
        missing.append("FEISHU_APP_SECRET")
    if not FEISHU_APP_TOKEN:
        missing.append("FEISHU_APP_TOKEN")
    if MATERIAL_CHECK_ENABLED and not MATERIAL_COOKIE:
        missing.append("MATERIAL_COOKIE")
    return missing


def missing_order_config() -> list[str]:
    missing = []
    if not MOBO_AUTHS:
        missing.append("MOBO_AUTHS")
    if not BEIDOU_AUTHS:
        missing.append("BEIDOU_AUTHS")
    return missing
