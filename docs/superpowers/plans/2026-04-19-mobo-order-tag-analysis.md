# Mobo Order Tag Analysis Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 基于 `出单数据.xlsx` 和 Mobo 全量剧集接口，给订单补齐 `Mobo标签`，并输出按语言、按剧场、按语言+剧场的标签偏好排序 Excel。

**Architecture:** 新增一个独立脚本 `mobo_order_tag_analysis.py`，分成订单读取、Mobo 分页抓取、剧名/语言/剧场标准化、标签中文归一化、订单匹配、偏好聚合、Excel 导出七个步骤。测试集中放在一个 `unittest` 文件里，先锁定标准化、分页和聚合行为，再补真实请求链路。

**Tech Stack:** Python 3.13、requests、openpyxl、unittest

---

### Task 1: 建立脚本骨架并锁定标准化规则

**Files:**
- Create: `mobo_order_tag_analysis.py`
- Create: `tests/test_mobo_order_tag_analysis.py`

- [ ] **Step 1: Write the failing tests**

```python
import unittest

from mobo_order_tag_analysis import (
    normalize_order_language,
    normalize_theater_name,
    normalize_mobo_tags,
    build_match_key,
)


class NormalizationTests(unittest.TestCase):
    def test_language_and_theater_are_normalized_for_matching(self):
        self.assertEqual(normalize_order_language("翻译英语"), "英语")
        self.assertEqual(normalize_theater_name("shortmax"), "ShortMax")
        self.assertEqual(
            build_match_key("翻译英语", "shortmax", "[Synchron] Danke für den Rauswurf: Jetzt kaufe ich eure Firma!"),
            build_match_key("英语", "ShortMax", "Danke für den Rauswurf: Jetzt kaufe ich eure Firma!"),
        )

    def test_foreign_and_traditional_tags_are_normalized_to_chinese(self):
        normalized, checks = normalize_mobo_tags(
            ["CEO", "Verborgene Identität", "現代言情", " Dolce Romanzo "]
        )
        self.assertEqual(normalized, ["总裁", "隐藏身份", "现代言情", "甜宠"])
        self.assertEqual([item["normalized_tag"] for item in checks], ["总裁", "隐藏身份", "现代言情", "甜宠"])
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m unittest tests.test_mobo_order_tag_analysis -v`

Expected: FAIL with `ModuleNotFoundError` or missing function import.

- [ ] **Step 3: Write the minimal implementation**

```python
LANGUAGE_ALIASES = {"翻译英语": "英语"}
THEATER_ALIASES = {"shortmax": "ShortMax"}
TAG_ALIASES = {
    "CEO": "总裁",
    "Verborgene Identität": "隐藏身份",
    "現代言情": "现代言情",
    "Dolce Romanzo": "甜宠",
}
```

Add the first-pass implementations for:

- `normalize_order_language()`
- `normalize_theater_name()`
- `normalize_tag_text()`
- `normalize_mobo_tags()`
- `build_match_key()`

- [ ] **Step 4: Run the test to verify it passes**

Run: `python -m unittest tests.test_mobo_order_tag_analysis -v`

Expected: PASS for both normalization tests.

### Task 2: 实现 Mobo 全量分页抓取与剧集索引

**Files:**
- Modify: `mobo_order_tag_analysis.py`
- Modify: `tests/test_mobo_order_tag_analysis.py`

- [ ] **Step 1: Add the failing pagination and dedupe tests**

```python
from mobo_order_tag_analysis import fetch_all_mobo_records, build_mobo_index


def fake_fetch_page(payload):
    page = payload["pageIndex"]
    if page == 1:
        return [{"languageName": "德语", "agencyName": "ShortMax", "seriesName": "A", "seriesId": "1", "seriesTypeList": ["CEO"]}] * 10000
    if page == 2:
        return [{"languageName": "德语", "agencyName": "ShortMax", "seriesName": "A", "seriesId": "1", "seriesTypeList": ["Verborgene Identität"]}]
    return []


class FetchTests(unittest.TestCase):
    def test_fetch_all_records_stops_when_page_smaller_than_page_size(self):
        rows = fetch_all_mobo_records({"德语": 16}, fetch_page_fn=fake_fetch_page)
        self.assertEqual(len(rows), 10001)

    def test_build_mobo_index_merges_duplicate_drama_tags(self):
        rows = [
            {"languageName": "德语", "agencyName": "ShortMax", "seriesName": "A", "seriesId": "1", "seriesTypeList": ["CEO"]},
            {"languageName": "德语", "agencyName": "ShortMax", "seriesName": "A", "seriesId": "1", "seriesTypeList": ["Verborgene Identität"]},
        ]
        index, checks = build_mobo_index(rows)
        matched = index[("德语", "ShortMax", "a")]
        self.assertEqual(matched["Mobo标签"], "总裁,隐藏身份")
        self.assertEqual(matched["Mobo剧集ID"], "1")
        self.assertEqual(len(checks), 2)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m unittest tests.test_mobo_order_tag_analysis -v`

Expected: FAIL because pagination and index functions are not implemented yet.

- [ ] **Step 3: Implement the fetch and index layer**

Add these functions to `mobo_order_tag_analysis.py`:

- `build_mobo_payload(lang_code: int, page_index: int) -> dict`
- `fetch_mobo_page(payload: dict) -> list[dict]`
- `fetch_all_mobo_records(language_map: dict[str, int], fetch_page_fn=fetch_mobo_page) -> list[dict]`
- `build_mobo_index(rows: list[dict]) -> tuple[dict, list[dict]]`

Use these exact request parameters:

```python
{
    "name": "",
    "lang": lang_code,
    "platform": None,
    "audioType": 0,
    "localType": 0,
    "orderType": 0,
    "pageIndex": page_index,
    "projectType": 2,
    "pageSize": 10000,
}
```

For indexing, key by:

```python
(language_name, theater_name, normalize_title(series_name))
```

and store:

```python
{
    "Mobo剧集ID": str(series_id),
    "Mobo剧名": series_name,
    "Mobo剧场": theater_name,
    "Mobo标签": ",".join(sorted(normalized_tags)),
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python -m unittest tests.test_mobo_order_tag_analysis -v`

Expected: PASS for normalization and fetch/index tests.

### Task 3: 读取订单并完成打标

**Files:**
- Modify: `mobo_order_tag_analysis.py`
- Modify: `tests/test_mobo_order_tag_analysis.py`

- [ ] **Step 1: Add the failing order matching test**

```python
from mobo_order_tag_analysis import annotate_order_rows


class MatchTests(unittest.TestCase):
    def test_annotate_orders_adds_mobo_columns(self):
        orders = [{
            "语言": "翻译英语",
            "剧场": "shortmax",
            "剧名": "[Synchron] Danke für den Rauswurf: Jetzt kaufe ich eure Firma!",
            "订单数": 1,
            "金额": 67.68,
        }]
        index = {
            ("英语", "ShortMax", "danke für den rauswurf jetzt kaufe ich eure firma"): {
                "Mobo剧集ID": "1001",
                "Mobo剧名": "Danke für den Rauswurf: Jetzt kaufe ich eure Firma!",
                "Mobo剧场": "ShortMax",
                "Mobo标签": "总裁,现代言情",
            }
        }

        tagged_rows, unmatched_rows = annotate_order_rows(orders, index)

        self.assertEqual(tagged_rows[0]["标准化语言"], "英语")
        self.assertEqual(tagged_rows[0]["标准化剧场"], "ShortMax")
        self.assertEqual(tagged_rows[0]["Mobo标签"], "总裁,现代言情")
        self.assertEqual(unmatched_rows, [])
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m unittest tests.test_mobo_order_tag_analysis -v`

Expected: FAIL because order annotation functions are not implemented yet.

- [ ] **Step 3: Implement order loading and tagging**

Add these functions:

- `read_order_rows(input_path: str) -> list[dict]`
- `annotate_order_rows(order_rows: list[dict], mobo_index: dict) -> tuple[list[dict], list[dict]]`

Required output columns for each tagged row:

- original order columns
- `标准化语言`
- `标准化剧场`
- `标准化剧名`
- `Mobo标签`
- `Mobo剧集ID`
- `Mobo剧名`
- `Mobo剧场`

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python -m unittest tests.test_mobo_order_tag_analysis -v`

Expected: PASS for matching and all previous tests.

### Task 4: 实现标签偏好聚合与 Excel 输出

**Files:**
- Modify: `mobo_order_tag_analysis.py`
- Modify: `tests/test_mobo_order_tag_analysis.py`

- [ ] **Step 1: Add the failing aggregation/export tests**

```python
from mobo_order_tag_analysis import aggregate_preferences, write_analysis_workbook


class AggregateTests(unittest.TestCase):
    def test_aggregate_preferences_sorts_by_amount_then_orders(self):
        rows = [
            {"语言": "德语", "剧场": "ShortMax", "Mobo标签": "总裁,现代言情", "订单数": 3, "金额": 147.28},
            {"语言": "德语", "剧场": "ShortMax", "Mobo标签": "总裁", "订单数": 1, "金额": 67.68},
        ]
        by_language, by_theater, by_language_theater = aggregate_preferences(rows)
        self.assertEqual(by_language[0]["标签"], "总裁")
        self.assertEqual(by_language[0]["金额"], 214.96)
        self.assertEqual(by_language[0]["订单数"], 4)
        self.assertEqual(by_language[1]["标签"], "现代言情")

    def test_write_analysis_workbook_creates_expected_sheets(self):
        write_analysis_workbook(
            output_path,
            tagged_rows=[{"语言": "德语", "剧场": "ShortMax", "剧名": "A", "Mobo标签": "总裁"}],
            unmatched_rows=[],
            language_rows=[{"语言": "德语", "标签": "总裁", "金额": 100.0, "订单数": 2, "排名": 1}],
            theater_rows=[{"剧场": "ShortMax", "标签": "总裁", "金额": 100.0, "订单数": 2, "排名": 1}],
            language_theater_rows=[{"语言": "德语", "剧场": "ShortMax", "标签": "总裁", "金额": 100.0, "订单数": 2, "排名": 1}],
            tag_check_rows=[{"raw_tag": "CEO", "normalized_tag": "总裁", "is_chinese": 1, "count": 2}],
        )
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m unittest tests.test_mobo_order_tag_analysis -v`

Expected: FAIL because aggregation and export are not implemented yet.

- [ ] **Step 3: Implement the aggregation and workbook writer**

Add these functions:

- `split_tags(tag_text: str) -> list[str]`
- `aggregate_preferences(tagged_rows: list[dict]) -> tuple[list[dict], list[dict], list[dict]]`
- `write_analysis_workbook(...) -> None`

Workbook sheets and order must be:

1. `订单打标`
2. `按语言偏好标签`
3. `按剧场偏好标签`
4. `按语言剧场偏好标签`
5. `未匹配订单`
6. `标签归一化检查`

Sort rules:

- group by the requested dimension
- within each group, sort by `金额 desc`, then `订单数 desc`, then `标签`

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python -m unittest tests.test_mobo_order_tag_analysis -v`

Expected: PASS for all tests.

### Task 5: 串起主流程并做真实验证

**Files:**
- Modify: `mobo_order_tag_analysis.py`

- [ ] **Step 1: Implement `main()`**

Wire together:

```python
order_rows = read_order_rows("出单数据.xlsx")
mobo_rows = fetch_all_mobo_records(language_map)
mobo_index, tag_check_rows = build_mobo_index(mobo_rows)
tagged_rows, unmatched_rows = annotate_order_rows(order_rows, mobo_index)
language_rows, theater_rows, language_theater_rows = aggregate_preferences(tagged_rows)
write_analysis_workbook(...)
```

Default output path:

```python
platform_catalog_output/mobo_order_tag_analysis.xlsx
```

- [ ] **Step 2: Run the unit tests one more time**

Run: `python -m unittest tests.test_mobo_order_tag_analysis -v`

Expected: PASS

- [ ] **Step 3: Run the real script**

Run: `python mobo_order_tag_analysis.py`

Expected:

- real Mobo API pagination succeeds
- workbook is generated
- script prints output path and row counts

- [ ] **Step 4: Verify the workbook contents**

Run a readback command and confirm:

- `订单打标` has `Mobo标签`
- `按语言偏好标签` exists
- `按剧场偏好标签` exists
- `按语言剧场偏好标签` exists
- `标签归一化检查` exists

### Self-Review

- spec coverage: 单口径 Mobo 分页、`pageSize=10000`、中文标签归一化、订单打标、三类偏好排序、检查表都已覆盖
- placeholder scan: 无 `TODO/TBD`
- type consistency: 匹配键固定为 `标准化语言 + 标准化剧场 + 标准化剧名`，导出字段固定为 `Mobo标签` 等中文列名

## Execution Handoff

Plan written for inline execution in the current session. If you want delegated execution instead, ask explicitly for subagents; otherwise continue inline here.
