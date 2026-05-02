from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple


def normalize_title(title: Any) -> str:
    text = "" if title is None else str(title)
    text = text.strip().lower()
    text = re.sub(r"[^\w\s\u4e00-\u9fff]", "", text)
    text = re.sub(r"_+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


@dataclass
class DramaRecord:
    title: str
    language: str
    theater: str
    source: str = ""
    rank: int = 999
    score: float = 0.0
    publish_at: str = ""
    task_id: str = ""
    tags: List[str] = field(default_factory=list)
    raw: Dict[str, Any] = field(default_factory=dict)
    source_rank_details: Dict[str, int] = field(default_factory=dict)
    source_date_details: Dict[str, str] = field(default_factory=dict)
    source_signal_score: float = 0.0
    tag_score: float = 0.0
    theme_multiplier: float = 1.0
    content_multiplier: float = 1.0
    tag_status: str = ""
    matched_tags: List[str] = field(default_factory=list)
    age_bucket: str = ""
    explosion_bucket: str = ""
    layer: str = ""
    date_signal_hits: List[str] = field(default_factory=list)
    explosion_recommend_score: float = 0.0
    freshness_score: float = 0.0
    revenue_validation_score: float = 0.0
    layer_score: float = 0.0

    @property
    def title_norm(self) -> str:
        return normalize_title(self.title)

    @property
    def match_key(self) -> Tuple[str, str]:
        return (self.language, self.title_norm)

    @property
    def theater_key(self) -> Tuple[str, str, str]:
        return (self.language, self.title_norm, self.theater)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "语言": self.language,
            "层级": self.layer,
            "剧龄分段": self.age_bucket,
            "爆发标签": self.explosion_bucket,
            "剧场": self.theater,
            "剧名": self.title,
            "来源": self.source,
            "排名": self.rank,
            "综合得分": round(self.score, 4),
            "层内主分": round(self.layer_score, 4),
            "爆发推荐分": round(self.explosion_recommend_score, 4),
            "新鲜度分": round(self.freshness_score, 4),
            "收入验证分": round(self.revenue_validation_score, 4),
            "来源信号分": round(self.source_signal_score, 4),
            "题材系数": round(self.theme_multiplier, 4),
            "标签分": round(self.tag_score or self.theme_multiplier, 4),
            "内容系数": round(self.content_multiplier, 4),
            "标签状态": self.tag_status,
            "命中标签": ",".join(self.matched_tags),
            "日期型爆发命中": " | ".join(self.date_signal_hits),
            "来源排名明细": ",".join(f"{source}:{rank}" for source, rank in sorted(self.source_rank_details.items())),
            "来源时间明细": ",".join(
                f"{source}:{date_text}" for source, date_text in sorted(self.source_date_details.items())
            ),
            "发布时间": self.publish_at,
            "任务ID": self.task_id,
            "标签": ",".join(self.tags),
        }


@dataclass
class PublishedRecord:
    title: str
    language: str
    theater: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)

    @property
    def title_norm(self) -> str:
        return normalize_title(self.title)

    @property
    def match_key(self) -> Tuple[str, str]:
        return (self.language, self.title_norm)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "语言": self.language,
            "剧场": self.theater,
            "剧名": self.title,
        }


@dataclass
class TitleBlockRecord:
    title: str
    language: str
    theater: str = ""
    source: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)

    @property
    def title_norm(self) -> str:
        return normalize_title(self.title)

    @property
    def match_key(self) -> Tuple[str, str]:
        return (self.language, self.title_norm)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "语言": self.language,
            "剧场": self.theater,
            "剧名": self.title,
            "来源": self.source,
        }


@dataclass
class FilterRecord:
    title: str
    language: str
    theater: str
    reason: str
    source: str = ""
    score: float = 0.0
    layer: str = ""
    age_bucket: str = ""
    explosion_bucket: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "语言": self.language,
            "层级": self.layer,
            "剧龄分段": self.age_bucket,
            "爆发标签": self.explosion_bucket,
            "剧场": self.theater,
            "剧名": self.title,
            "拦截原因": self.reason,
            "来源": self.source,
            "综合得分": round(self.score, 4),
        }


@dataclass
class MaterialResult:
    language: str
    title: str
    theater: str
    qualified_count: int
    total_count: int = 0

    @property
    def title_norm(self) -> str:
        return normalize_title(self.title)

    @property
    def theater_key(self) -> Tuple[str, str, str]:
        return (self.language, self.title_norm, self.theater)


@dataclass
class RecommendationResult:
    title: str
    language: str
    theater: str
    rank: int
    score: float
    layer: str = ""
    qualified_count: int = 0
    total_count: int = 0
    content_label: str = ""
    source: str = ""
    promotion_time: str = ""
    pre_rank_summary: str = ""
    recommend_reason: str = ""
    score_breakdown: str = ""
    rule_hits: str = ""
    filter_trace: str = ""
    source_dates: str = ""
    quality_flags: str = ""

    def to_export_dict(self) -> Dict[str, Any]:
        return {
            "语言": self.language,
            "剧场": self.theater,
            "剧名": self.title,
            "排名": self.rank,
            "综合得分": round(self.score, 4),
            "达标视频": self.qualified_count,
            "内容标签": self.content_label,
            "推荐语": self.recommend_reason,
            "分数明细": self.score_breakdown,
            "命中规则": self.rule_hits,
            "过滤链路": self.filter_trace,
            "来源时间": self.source_dates,
            "质量提示": self.quality_flags,
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            "语言": self.language,
            "层级": self.layer,
            "排名": self.rank,
            "剧名": self.title,
            "剧场": self.theater,
            "推广时间": self.promotion_time,
            "推荐前排位": self.pre_rank_summary,
            "推荐语": self.recommend_reason,
            "综合得分": round(self.score, 4),
            "达标视频数": self.qualified_count,
            "总视频数": self.total_count,
            "内容标签": self.content_label,
            "来源": self.source,
            "分数明细": self.score_breakdown,
            "命中规则": self.rule_hits,
            "过滤链路": self.filter_trace,
            "来源时间": self.source_dates,
            "质量提示": self.quality_flags,
        }


@dataclass
class TodayRecommendationRun:
    recommendations: List[RecommendationResult]
    filters: List[FilterRecord]
    candidates: List[DramaRecord]
    stats: Dict[str, Any] = field(default_factory=dict)


@dataclass
class OrderRecord:
    date: str
    title: str
    platform: str
    language: str
    theater: str
    order_count: int
    amount: float
    account: str = ""
    task_id: str = ""
    order_type: str = "订单金额"
    raw: Dict[str, Any] = field(default_factory=dict)

    @property
    def match_key(self) -> Tuple[str, str, str, str, str, str]:
        return (self.date, self.platform, self.language, self.theater, normalize_title(self.title), self.order_type)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "语言": self.language,
            "剧场": self.theater,
            "剧名": self.title,
            "订单数": self.order_count,
            "金额": round(self.amount, 2),
            "平台": self.platform,
            "类型": self.order_type,
        }
