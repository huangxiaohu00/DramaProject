# Mobo / 北斗接口简版说明

本文档只记录当前项目代码实际使用到的接口、核心参数、核心返回字段和使用注意事项。

## 通用说明

- 鉴权方式：
  - Mobo、北斗通过 `Authorization` 请求头传入账号 token。
  - 王牌平台通过有效登录态 Cookie 访问，并需要把 Cookie 中的 `abroad_ticket` 同步放入 `ticket` 请求头；不要把完整 Cookie 写入文档或提交到代码。
- 日期格式：订单接口使用 `YYYY-MM-DD`。
- 当前配置来源：
  - 平台 URL、语言映射、剧场映射：`drama_pipeline/2_config.py`
  - 请求构造和解析逻辑：`drama_pipeline/platform_clients/`；`drama_pipeline/4_platform_clients.py` 仅保留兼容导出
  - 王牌平台剧集抓取和解密脚本：`drama_pipeline/wangpai_abroad_tasks.py`

## Mobo 平台

### 1. 剧集列表接口

- 方法：`POST`
- URL：`https://kocserver-cn.cdreader.com/api/v1/res/getlistpc`
- 用途：
  - 获取 Mobo 新剧
  - 获取 Mobo 推荐榜
  - 按剧名回查语言、剧场、标签等元数据

核心请求头：

| 参数 | 说明 |
| --- | --- |
| `Authorization` | Mobo 平台 token，通常为 `Bearer ...` |
| `Content-Type` | `application/json` |

核心请求体：

| 参数 | 说明 |
| --- | --- |
| `name` | 剧名搜索；空字符串表示不按剧名过滤 |
| `lang` | 语言编码；见 Mobo 语言映射 |
| `platform` | 剧场/产品 ID；为空表示不限定剧场 |
| `audioType` | 声音类型；当前默认 `0` |
| `localType` | 本土/翻译类型；当前默认 `0` |
| `orderType` | 榜单类型：`0` 新剧，`1` 推荐，`2` 热门 |
| `pageIndex` | 页码，从 `1` 开始 |
| `pageSize` | 每页数量；当前新剧使用 `1000` |
| `projectType` | 当前固定 `2` |

当前 Mobo 语言映射：

| 语言    | lang  |
|-------|-------|
| 简体中文  | 1     |
| 繁体中文  | 2     |
| 英语    | 3     |
| 西班牙语  | 4     | 
| 葡萄牙语  | 5     |
| 法语    | 6     |
| 俄语    | 7     |
| 意大利语  | 8     |
| 日语    | 9     |
| 阿拉伯语  | 10    |
| 印尼    | 11    |
| 泰语    | 12    |
| 越南语   | 13    |
| 韩语    | 14    |
| 菲律宾语  | 15    |
| 德语    | 16    |
| 印地语   | 17    |
| 马来西亚语 | 21    |
| 土耳其语  | 22    |


当前 Mobo 剧场 ID：

| 剧场 | platform |
| --- | --- |
| MoboReels | 6833 |
| FlickReels | 1311 |
| SnackShort | 1281 |
| KalosTV | 1211 |
| ShortMax | 1331 |
| HoneyReels | 1291 |
| FlexTV | 1341 |
| Footage | 1251 |
| TopShort | 1321 |


核心返回字段：

| 字段 | 说明 |
| --- | --- |
| `data.list` 或 `data` | 剧集列表 |
| `seriesName` / `name` | 剧名 |
| `languageName` | 语言名称 |
| `agencyName` / `appName` | 剧场 |
| `createTime` / `publishTime` | 上新/发布时间 |
| `seriesId` | 剧集 ID |
| `seriesTypeList` | 题材/标签列表 |

项目内使用方式：

- 新剧：`orderType=0`，按语言和剧场抓取。
- 推荐榜：`orderType=1`，可按语言和剧场抓取。
- 元数据回查：按 `name` 搜索，用于补语言、标签、剧场等信息。

### 2. 订单明细接口

- 方法：`POST`
- URL：`https://kocserver-cn.cdreader.com/api/Report/GetMDetailsReport`
- 用途：获取 Mobo 昨日订单和广告金额。

核心请求体：

| 参数 | 说明 |
| --- | --- |
| `range` | 当前固定 `null` |
| `beginTime` | 开始日期 |
| `endTime` | 结束日期 |
| `appTypeList` | 当前固定 `["683001001"]` |

业务状态校验：

- 正常：`code=200` 且 `status` 不为 `False`。
- 异常：登录失效、token 过期、无权限等会作为账号异常抛出，避免静默当作空数据。

核心返回字段：

| 字段 | 说明 |
| --- | --- |
| `data` | 订单/广告记录列表 |
| `dataName` | 剧名 |
| `appName` | 剧场 |
| `languageName` | 语言；缺失时项目会按剧名回查 |
| `num` | 订单数量 |
| `rmbRealIncome` | 金额 |
| `adType` | `1` 表示广告金额记录，其他为订单金额记录 |
| `taskId` | 任务 ID |

解析规则：

- `adType=1` 且 `rmbRealIncome>0`：记为 `广告金额`，订单数按 `1`。
- 非广告记录且 `num>0`、`rmbRealIncome>0`：记为 `订单金额`。
- 兼容若干广告金额字段，如 `rmbAdIncome`、`adIncome`、`cost` 等。

## 北斗平台

### 1. 任务/剧集分页接口

- 方法：`GET`
- URL：`https://api-scenter.inbeidou.cn/agent/v1/task/page`
- 用途：
  - 获取北斗新剧
  - 获取北斗收入榜
  - 按剧名回查语言、剧场、标签等元数据

核心 Query 参数：

| 参数 | 说明 |
| --- | --- |
| `task_type` | 当前固定 `1` |
| `page_num` | 页码，从 `1` 开始 |
| `page_size` | 每页数量 |
| `app_id` | 剧场 ID；为空表示不限剧场 |
| `order_field` | 排序字段；新剧用 `publish_at`，收入榜用 `total_income` |
| `order_dir` | 排序方向；当前一般为 `desc` |
| `language` | 语言编码；见北斗语言映射 |
| `search_title` | 剧名搜索 |
| `campaign_status` | 当前默认 `0` |
| `agent_id` | 北斗代理 ID，当前默认 `2851723045` |

当前北斗语言映射：

| 语言    | language |
|-------|----------|
| 英语    | 2        |
| 印尼    | 3        |
| 西班牙语  | 4        |
| 法语    | 5        |
| 泰语    | 6        |
| 葡萄牙语  | 7        |
| 韩语    | 8        |
| 日语    | 9        |
| 阿拉伯语  | 10       |
| 德语    | 11       |
| 繁体中文  | 12       |
| 俄语    | 13       |
| 意大利语  | 14       |
| 菲律宾语  | 15       |
| 越南语   | 16       |
| 印地语   | 17       |
| 马来西亚语 | 18       |
| 土耳其语  | 19       |
| 罗马尼亚语 | 21       |
| 波兰语   | 22       |
| 捷克语   | 24       |


当前北斗剧场 ID：

| 剧场         | app_id |
|------------| --- |
| DramaBox   | `dramabox` |
| FlareFlow  | `flareflow` |
| ShortMax   | `shortmax` |
| FlickReels | `flickreels` |
| ReelShort  | `reelshort` |
| GoodShort  | `goodshort` |
| MoboReels  | `moboreels` |
| KalosTV    | `kalostv` |
| SnackShort | `snackshort` |
| TouchShort | `touchshort` |
| DreamShort | `dreameshort` |
| HoneyReels | `honeyreels` |
| Pancake    | `pancake` |
| StarShort  | `starshort` |
| Sereal     | `sereal` |
| Playlet    | `playlet` |
| TopShort | `topshort` |


核心返回字段：

| 字段 | 说明 |
| --- | --- |
| `body.data` | 剧集列表 |
| `body.page` | 分页信息 |
| `title` / `serial_name` | 剧名 |
| `language` / `language_str` | 语言编码/语言名称 |
| `app_id` | 剧场 ID |
| `publish_at` | 上新时间 |
| `task_id` | 任务 ID |
| `tag` / 标签相关字段 | 标签、题材、内容标记，项目可用于 AI/漫剧/成人判定 |
| `total_income` | 收入榜排序相关字段 |

项目内使用方式：

- 新剧：`order_field=publish_at`，`order_dir=desc`，按语言和剧场抓取。
- 收入榜：`order_field=total_income`，`order_dir=desc`，按语言抓取。
- 元数据回查：使用 `search_title` 搜索剧名。

### 2. 订单分页接口

- 方法：`GET`
- URL：`https://api-scenter.inbeidou.cn/agent/v1/sett/order/promotion_code_page`
- 用途：获取北斗订单金额和广告金额。

核心 Query 参数：

| 参数 | 说明 |
| --- | --- |
| `start_time` | 开始日期 |
| `end_time` | 结束日期 |
| `page_num` | 页码，从 `1` 开始 |
| `page_size` | 每页数量；当前项目固定使用 `100` |

业务状态校验：

- 正常：`code=0`。
- 异常：非 `0` 或返回消息包含登录失效、token 过期、无权限等，会作为账号异常抛出。

分页规则：

- 当前代码按 `page_num` 自动翻页。
- 通过 `body.page.total_count` 判断是否已取完。
- 当前每页 `page_size=100`，比平台默认 `20` 更不容易漏数据。

核心返回字段：

| 字段 | 说明 |
| --- | --- |
| `body.data` | 订单/广告记录列表 |
| `body.page.current_page` | 当前页 |
| `body.page.page_size` | 每页数量 |
| `body.page.total_count` | 总记录数 |
| `serial_name` | 剧名 |
| `app_id` | 剧场 ID |
| `language` / `language_str` | 语言 |
| `task_id` | 任务 ID |
| `total_recharge_count` | 充值订单数 |
| `total_recharge_income` | 充值订单金额 |
| `total_ad_income` | 广告金额 |

解析规则：

- `total_recharge_count>0` 且 `total_recharge_income>0`：记为 `订单金额`。
- `total_ad_income>0`：记为 `广告金额`，订单数按 `1`。
- 如果语言缺失：
  - 订单金额会按剧名回查语言。
  - 广告金额大于等于 10 元时会按剧名回查语言。
  - 小额广告金额后续会按平台汇总为 `全部 / 全部 / 其它`。

### 3. 订单详情接口

- 方法：`GET`
- URL：`https://api-scenter.inbeidou.cn/agent/v1/sett/order/promotion_code_detail`
- 当前项目只保留了请求构造，主流程暂未重点使用。

核心 Query 参数：

| 参数 | 说明 |
| --- | --- |
| `start_time` | 开始日期 |
| `end_time` | 结束日期 |
| `task_id` | 任务 ID |

## 王牌平台

### 1. 海外短剧任务/剧集列表接口

- 方法：`GET`
- URL：`https://api.yd126.com/merchant/web/abroad/task_list`
- 前端入口：`https://zmt.yd126.com/s/index.html#/`
- 用途：
  - 获取王牌平台海外短剧任务列表。
  - 按剧场、语言、国家、剧名、推广类型筛选剧集。
  - 解密后可落成结构化剧集表。

核心请求头：

| 参数 | 说明 |
| --- | --- |
| `Cookie` | 有效登录态 Cookie，至少需要能访问海外短剧任务接口的登录态 |
| `ticket` | 从 Cookie 的 `abroad_ticket` 提取；缺少该头时收入接口会返回 `code=401`、`msg=登录已失效` |
| `User-Agent` | 建议传浏览器 UA |
| `Accept` | `application/json, text/plain, */*` |
| `Origin` / `Referer` | 建议分别传 `https://yd126.com`、`https://zmt.yd126.com/s/index.html` |
| `platform` / `appChannel` / `isH5` | 前端请求会带这些头，脚本默认使用 `browser`、`gf_default`、`1` |

核心 Query 参数：

| 参数 | 说明 |
| --- | --- |
| `thread_name` | 剧场名，如 `KalosTV`；可传 `全部剧场` 表示不限剧场；不传也等价于不限 |
| `page` | 页码，从 `1` 开始；不传时平台默认返回第 1 页 |
| `page_size` | 每页数量；前端默认 `12`，接口未传时实测默认 `50`，批量抓取建议显式传 `500` |
| `language` | 语言筛选，如 `英语`；`全部语言` 可省略 |
| `country` | 国家筛选；`全部国家` 可省略 |
| `title` | 剧名关键词；空字符串可省略 |
| `promotion_type` | 推广类型，如 `self`、`tto`；空字符串可省略 |
| `pay_type` | 分成/结算筛选；`0` 表示全部/不限，可省略 |
| `task_type` | 前端当前传 `1`；默认场景可省略 |
| `filter_type` | 前端当前传 `0`；默认场景可省略 |
| `sort_type` | 排序类型；前端部分场景会传，按需使用 |

最小可用请求：

```text
GET https://api.yd126.com/merchant/web/abroad/task_list?thread_name=KalosTV&page=1&page_size=500
```

前端完整默认请求中的这些参数可以减少：

| 可减少参数 | 原因 |
| --- | --- |
| `title=` | 空筛选，无需传 |
| `promotion_type=` | 空筛选，无需传 |
| `language=全部语言` | 默认不限语言，无需传 |
| `country=全部国家` | 默认不限国家，无需传 |
| `pay_type=0` | 默认不限结算类型，无需传 |
| `filter_type=0` | 默认过滤值，无需传 |
| `task_type=1` | 当前海外短剧列表默认任务类型；保守兼容时可继续传 |
| `thread_name=全部剧场` | 不限剧场时可直接省略 |

当前已知剧场名：

```text
DreameShort / Kalos / KalosTV / DramaBox / ReelShort / ShortMax / GoodShort /
FlareFlow / SnapDrama / SnackShort / Playlet / HoneyReels / TopShort /
FlexTV / StarShort / TorchShort / StardustTV / MoboReels / TouchShort
```

当前已知语言：

```text
保加利亚语 / 简体中文 / 印尼语 / 意大利语 / 繁体中文 / 捷克语 / 西班牙语 /
菲律宾语 / 罗马尼亚语 / 马来语 / 日语 / 德语 / 挪威语 / 阿拉伯语 /
丹麦语 / 土耳其语 / 荷兰语 / 越南语 / 波兰语 / 韩语 / 葡语 / 英语 /
泰语 / 俄语 / 法语 / 印地语 / 瑞典语 / 芬兰语
```

### 2. 响应结构和解密

HTTP 外层是 JSON，但业务 `data` 是加密字符串：

| 外层字段 | 说明 |
| --- | --- |
| `code` | `200` 表示请求成功 |
| `msg` | 文案，如 `成功` |
| `data` | 加密字符串，需要前端 WASM 的 `decrypt_api` 解密 |

前端解密流程：

1. 前端 axios 使用 `responseType: "text"`。
2. axios 解析外层响应后，如果 `code=200`，调用 WASM 模块的 `decrypt_api(data)`。
3. `decrypt_api` 返回的内容是一个 JSON 字符串字面量。
4. 前端连续 `JSON.parse` 两次，得到业务对象。

解密后的业务结构：

| 字段 | 说明 |
| --- | --- |
| `list` | 剧集/任务列表 |
| `meta.pagination.current_page` | 当前页 |
| `meta.pagination.total_pages` | 总页数 |
| `meta.pagination.per_page` | 每页数量 |
| `meta.pagination.total` | 总条数 |

`list` 单条核心字段：

| 字段 | 说明 |
| --- | --- |
| `task_id` | 任务 ID |
| `title` | 剧名 |
| `thread_name` | 剧场名 |
| `language` | 语言 |
| `online_date` | 上线时间，格式如 `YYYY-MM-DD HH:mm:ss` |
| `pay_type` | 结算类型；实测 `1` 表示 CPS/分成 |
| `copyright` | 版权来源，如 `番茄` |
| `promotion_type` | 推广类型，如 `self`、`tto` |
| `country` | 国家/地区，可能为空 |
| `channel` | 渠道分类 ID |
| `tag_name` | 标签列表，可能为 `null` |
| `top_num` | 榜单/置顶相关数字 |
| `is_new` | 是否新剧标记 |
| `is_popular` | 是否热门标记 |
| `cps_subsidy_radio` | CPS 分成/补贴比例 |
| `cover` | 封面 URL |
| `icon` | 剧场图标 URL |

脚本结构化规则：

- 保留 `raw` 原始单条数据，方便后续字段补充。
- `task_id` 统一转为字符串，避免 Excel/CSV 精度或格式问题。
- `tag_name` 统一转为字符串列表。
- `pay_type_label` 额外补充人类可读标签：`1=CPS/分成`、`2=CPA/拉新`、`0=全部/不限`。
- CSV 导出时 `tag_name` 使用 `|` 拼接。

### 3. 订单/收入明细接口

- 方法：`GET`
- URL：`https://api.yd126.com/merchant/web/abroad/income_detail`
- 前端包装路径：`/merchant/abroad/income_detail`
- 用途：
  - 获取王牌平台海外短剧收入/订单明细。
  - 按关键词、收入子类型、搜索类型、推广类型分页筛选。
  - 解密后可落成收入明细表。

核心请求头：

| 参数 | 说明 |
| --- | --- |
| `Cookie` | 有效登录态 Cookie，至少需要能访问海外短剧收入明细接口的登录态 |
| `ticket` | 必传，取 Cookie 中 `abroad_ticket` 的值；只传 Cookie 不传该头时实测返回 `code=401`、`msg=登录已失效` |
| `User-Agent` | 建议传浏览器 UA |
| `Accept` | `application/json, text/plain, */*` |
| `Origin` / `Referer` | 建议分别传 `https://yd126.com`、`https://zmt.yd126.com/s/index.html` |
| `platform` / `appChannel` / `isH5` | 脚本默认使用 `browser`、`gf_default`、`1` |

核心 Query 参数：

| 参数 | 说明 |
| --- | --- |
| `search_keyword` | 搜索关键词；空字符串可省略 |
| `income_sub_type` | 收入子类型；前端默认 `0`，表示全部 |
| `search_type` | 搜索类型；前端当前使用 `2` |
| `promotion_type` | 推广类型；空字符串可省略 |
| `page` | 页码，从 `1` 开始 |
| `page_size` | 每页数量；示例接口使用 `10` |

最小可用请求：

```text
GET https://api.yd126.com/merchant/web/abroad/income_detail?income_sub_type=0&search_type=2&page=1&page_size=10
```

前端完整默认请求中的这些参数可以减少：

| 可减少参数 | 原因 |
| --- | --- |
| `search_keyword=` | 空筛选，无需传 |
| `promotion_type=` | 空筛选，无需传 |

响应和解密方式与剧集列表接口一致：

1. HTTP 外层返回 JSON。
2. `code=200` 时，`data` 是加密字符串。
3. 使用同一个前端 WASM `decrypt_api(data)` 解密。
4. 解密结果连续 `JSON.parse` 两次，得到业务对象。

解密后的通用业务结构：

| 字段 | 说明 |
| --- | --- |
| `item` | 收入/订单明细列表；收入接口实测使用该字段 |
| `meta.pagination.current_page` | 当前页 |
| `meta.pagination.total_pages` | 总页数 |
| `meta.pagination.per_page` | 每页数量 |
| `meta.pagination.total` | 总条数 |

脚本当前兼容结构化字段：

| 字段 | 说明 |
| --- | --- |
| `income_id` | 收入明细 ID；兼容 `income_id` / `id` / `abroad_id` |
| `task_id` | 任务 ID |
| `title` | 剧名；兼容 `title` / `task_title` / `name` |
| `thread_name` | 剧场名；兼容 `thread_name` / `theater_name` / `theater` |
| `language` | 语言 |
| `income_sub_type` | 收入子类型 |
| `income_sub_type_label` | 收入子类型文案；`0` 记为 `全部`，其他保留原值 |
| `income_amount` | 收入金额；兼容 `income_amount`、`amount`、`income`、`settle_amount`、`total_income`、`rmb_income`、`money` |
| `promotion_type` | 推广类型 |
| `settle_date` | 结算/收入日期；兼容 `settle_date` / `date` / `income_date` |
| `created_at` | 创建时间；兼容 `created_at` / `create_time` |
| `updated_at` | 更新时间；兼容 `updated_at` / `update_time` |
| `raw` | 原始单条明细 |

当前实测单条 `item` 原始字段包括：`date`、`name`、`theater_name`、`charge_num`、`is_predict`、`income`、`img`、`recharge_date`、`task_id`、`pay_type`、`abroad_id`、`theater_icon`、`task_type`、`ad_num`、`ad_income`、`country`、`promotion_type`。

注意：

- 如果浏览器能访问但脚本返回 `code=401`、`msg=登录已失效`，优先检查请求头是否带了 `ticket: <abroad_ticket>`；前端 axios 封装会自动补这个头。
- 脚本保留 `raw`，即使平台字段名与当前兼容字段不同，也不会丢失原始数据。

### 4. Python 脚本用法

脚本路径：

```text
drama_pipeline/wangpai_abroad_tasks.py
```

推荐在脚本顶部填入 `WANGPAI_COOKIE`，也可以运行时用 `--cookie` 临时覆盖。

```powershell
python -m drama_pipeline.wangpai_abroad_tasks --thread-name KalosTV --page-size 500 --max-pages 1 --output-json drama_pipeline/output/wangpai_kalostv.json --output-csv drama_pipeline/output/wangpai_kalostv.csv
```

收入明细导出示例：

```powershell
python -m drama_pipeline.wangpai_abroad_tasks --mode income --page-size 10 --max-pages 1 --output-json drama_pipeline/output/wangpai_income.json --output-csv drama_pipeline/output/wangpai_income.csv
```

常用参数：

| 参数 | 说明 |
| --- | --- |
| `--mode` | `tasks` 抓剧集列表，`income` 抓收入明细；默认 `tasks` |
| `--cookie` | 登录态 Cookie；不传则读取脚本顶部 `WANGPAI_COOKIE` |
| `--thread-name` | 剧场名；默认 `KalosTV` |
| `--language` | 语言筛选；默认 `全部语言`，脚本会省略该默认值 |
| `--country` | 国家筛选；默认 `全部国家`，脚本会省略该默认值 |
| `--title` | 剧名关键词 |
| `--promotion-type` | 推广类型 |
| `--pay-type` | 结算类型；默认 `0`，脚本会省略该默认值 |
| `--search-keyword` | 收入明细关键词筛选，仅 `--mode income` 使用 |
| `--income-sub-type` | 收入子类型，仅 `--mode income` 使用，默认 `0` |
| `--search-type` | 收入搜索类型，仅 `--mode income` 使用，默认 `2` |
| `--page` | 起始页 |
| `--page-size` | 每页数量 |
| `--max-pages` | 最多抓取页数 |
| `--output-json` | JSON 输出路径 |
| `--output-csv` | CSV 输出路径 |
| `--wasm-path` | 解密 WASM 本地路径；默认自动缓存到 `drama_pipeline/.cache/wangpai/` |

实现注意：

- 脚本默认下载公开前端 WASM 文件并复用 `decrypt_api`，不硬编码密钥。
- 优先使用 Python `wasmtime` 调用 WASM；如果缺少 `wasmtime`，会退回 Node.js 解密桥。
- Cookie 来自脚本顶部 `WANGPAI_COOKIE` 或命令行参数 `--cookie`；脚本会自动从 `abroad_ticket` 提取 `ticket` 请求头。
- 脚本创建的默认 Session 会关闭系统代理环境变量读取，避免 PyCharm 或本机代理配置把请求转到错误代理。

## 当前项目关键注意事项

- Mobo、北斗和王牌的语言/剧场表达方式不同，不能混用。
- Mobo 的剧场用数字 `platform`，北斗的剧场用字符串 `app_id`。
- 王牌的剧场和语言直接使用中文/英文名称字符串。
- 北斗订单接口默认每页可能只有 20 条，必须显式传 `page_size=100` 并分页。
- 各平台都可能出现 `HTTP 200` 但业务层失败，必须检查业务字段。
- 王牌平台 `HTTP 200` 后还需要解密 `data`，再解析业务分页对象。
- 订单流程日志应关注：
  - `raw_count`：接口原始返回行数
  - `parsed_count`：解析后有效订单/广告行数
  - `page_total`：接口分页总数
  - `amount_sum`：该账号解析金额合计
