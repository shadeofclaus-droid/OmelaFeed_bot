#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generic HTML news collector without RSS.

This module defines a flexible scraper that extracts news entries from
arbitrary websites using CSS selectors defined in a YAML configuration.
It supports basic rate limiting per source, unique URL filtering and
optional date range filtering. Extracted entries are returned as
dictionaries with keys: url, title, summary, source, published_at.

Example YAML (sources.yaml) structure:

```
sources:
  - name: "Мінсоцполітики — новини"
    list_urls:
      - "https://www.msp.gov.ua/press-center/news"
    link_selector: "a[href*='/news/']"
    max_per_source: 10
    detail:
      title_selectors: ["h1", ".page-title"]
      summary_selectors: ["article p", ".content p"]
      date_selectors: ["time[datetime]", ".date"]
      date_attr: "datetime"
```

The collector will visit each list_url, find all links matching
link_selector and then fetch each article page to extract title,
summary and published date based on the provided selectors. If
`date_from` or `date_to` arguments are provided, entries outside that
range are skipped.

"""
from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable, List, Optional, Dict
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
import yaml

# Use a session for connection reuse and header customization
SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
        ),
        "Accept-Language": "uk,ru;q=0.8,en;q=0.6",
    }
)

# Utility dataclasses to describe source configuration

@dataclass
class DetailRules:
    title_selectors: List[str] = field(default_factory=list)
    summary_selectors: List[str] = field(default_factory=list)
    date_selectors: List[str] = field(default_factory=list)
    date_attr: Optional[str] = None
    date_formats: List[str] = field(default_factory=list)


@dataclass
class SourceCfg:
    name: str
    list_urls: List[str]
    link_selector: str
    base_url: Optional[str] = None
    sleep: float = 0.7
    max_per_source: int = 10
    detail: DetailRules = field(default_factory=DetailRules)


def _get(url: str) -> Optional[BeautifulSoup]:
    """Fetch a URL and return a BeautifulSoup document or None on error."""
    try:
        resp = SESSION.get(url, timeout=15)
        if resp.status_code >= 400:
            return None
        return BeautifulSoup(resp.text, "html.parser")
    except Exception:
        return None


def _abs(base: str, href: str) -> str:
    return urljoin(base, href)


def _text_of(node) -> str:
    if not node:
        return ""
    return " ".join(node.stripped_strings)


# Common meta tags used to store publication dates
META_DATE_SELECTORS = [
    ("meta", {"property": "article:published_time"}, "content"),
    ("meta", {"name": "article:published_time"}, "content"),
    ("meta", {"property": "og:article:published_time"}, "content"),
    ("meta", {"name": "pubdate"}, "content"),
    ("meta", {"itemprop": "datePublished"}, "content"),
    ("time", {}, "datetime"),
]


def _parse_date_any(value: str) -> Optional[datetime]:
    """Parse a date string using dateutil; return None on failure."""
    try:
        return dateparser.parse(value)
    except Exception:
        return None


def _extract_date(doc: BeautifulSoup, rules: DetailRules) -> Optional[datetime]:
    """Try to extract a date from meta tags or custom selectors."""
    # 1) meta/time tags
    for tag, attrs, attr in META_DATE_SELECTORS:
        el = doc.find(tag, attrs=attrs)
        if el and el.get(attr):
            dt = _parse_date_any(el.get(attr))
            if dt:
                return dt
    # 2) custom selectors from config
    for css in rules.date_selectors:
        el = doc.select_one(css)
        if not el:
            continue
        if rules.date_attr and el.get(rules.date_attr):
            dt = _parse_date_any(el.get(rules.date_attr))
        else:
            dt = _parse_date_any(_text_of(el))
        if dt:
            return dt
    return None


def _extract_title(doc: BeautifulSoup, rules: DetailRules) -> str:
    for css in rules.title_selectors or ["h1", "h1.entry-title", "h1.article-title"]:
        el = doc.select_one(css)
        if el:
            return _text_of(el)
    og = doc.find("meta", {"property": "og:title"})
    if og and og.get("content"):
        return og["content"]
    return ""


def _extract_summary(doc: BeautifulSoup, rules: DetailRules) -> str:
    for css in rules.summary_selectors or ["article p", ".entry-content p", ".article-content p"]:
        el = doc.select_one(css)
        if el:
            return _text_of(el)
    md = doc.find("meta", {"name": "description"})
    if md and md.get("content"):
        return md["content"]
    return ""


def _pick_links(list_url: str, link_selector: str, base_url: Optional[str]) -> List[str]:
    doc = _get(list_url)
    if not doc:
        return []
    links = []
    for a in doc.select(link_selector):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        full = _abs(base_url or list_url, href)
        # ignore anchors and obvious pagination
        if "#" in full:
            full = full.split("#")[0]
        if not full:
            continue
        links.append(full)
    # unique while preserving order
    seen = set()
    out = []
    for u in links:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def _extract_article(url: str, rules: DetailRules, tz) -> Optional[Dict]:
    doc = _get(url)
    if not doc:
        return None
    title = _extract_title(doc, rules).strip()
    summary = _extract_summary(doc, rules).strip()
    dt = _extract_date(doc, rules)
    if not dt:
        dt = datetime.utcnow()
    # timezone-aware
    if not dt.tzinfo:
        dt = dt.replace(tzinfo=dateparser.tz.UTC)
    published_at = dt.astimezone(tz).isoformat()
    return {
        "url": url,
        "title": title or "Без назви",
        "summary": summary,
        "source": urlparse(url).netloc,
        "published_at": published_at,
    }


def _within_range(iso_ts: str, date_from: Optional[datetime], date_to: Optional[datetime]) -> bool:
    if not (date_from or date_to):
        return True
    dt = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
    if date_from and dt < date_from:
        return False
    if date_to and dt >= date_to:
        return False
    return True


def collect_nonrss(
    yaml_path: str,
    tz,
    remaining: int = 20,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
) -> Iterable[Dict]:
    """
    Iterate through configured sources and yield article dictionaries.

    Args:
        yaml_path: path to YAML config with a `sources` key.
        tz: timezone object for published_at conversion.
        remaining: maximum number of items to return overall.
        date_from: lower inclusive bound (aware datetime) or None.
        date_to: upper exclusive bound (aware datetime) or None.
    Yields:
        dicts with keys: url, title, summary, source, published_at
    """
    with open(yaml_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    sources_cfg = cfg.get("sources", [])
    count = 0
    for src_cfg in sources_cfg:
        src = SourceCfg(
            name=src_cfg.get("name", ""),
            list_urls=src_cfg.get("list_urls") or [],
            link_selector=src_cfg.get("link_selector") or "",
            base_url=src_cfg.get("base_url"),
            sleep=float(src_cfg.get("sleep", 0.7)),
            max_per_source=int(src_cfg.get("max_per_source", 10)),
            detail=DetailRules(
                title_selectors=src_cfg.get("detail", {}).get("title_selectors", []),
                summary_selectors=src_cfg.get("detail", {}).get("summary_selectors", []),
                date_selectors=src_cfg.get("detail", {}).get("date_selectors", []),
                date_attr=src_cfg.get("detail", {}).get("date_attr"),
                date_formats=src_cfg.get("detail", {}).get("date_formats", []),
            ),
        )
        per_source = 0
        for list_url in src.list_urls:
            for link in _pick_links(list_url, src.link_selector, src.base_url):
                art = _extract_article(link, src.detail, tz)
                if not art:
                    continue
                if not _within_range(art["published_at"], date_from, date_to):
                    continue
                yield art
                count += 1
                per_source += 1
                if count >= remaining or per_source >= src.max_per_source:
                    break
                time.sleep(src.sleep + random.random() * 0.3)
            if count >= remaining or per_source >= src.max_per_source:
                break
        if count >= remaining:
            break