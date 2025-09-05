#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import re, time, json, logging
from dataclasses import dataclass, field
from urllib.parse import urljoin, urldefrag
from datetime import datetime
from zoneinfo import ZoneInfo

import requests, yaml
from bs4 import BeautifulSoup
from readability import Document
import urllib.robotparser as robotparser

from utils_normalize import canonical_url, parse_ua_date, clamp

TZ = ZoneInfo("Europe/Kyiv")
UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124 Safari/537.36 (news-bot)")

log = logging.getLogger("nonrss")
logging.basicConfig(level=logging.INFO)

@dataclass
class Site:
    name: str
    base: str
    start_urls: list[str]
    allow: list[str]
    deny: list[str] = field(default_factory=list)
    keywords: list[str] = field(default_factory=list)
    title_selector: str | None = None
    summary_selector: str | None = None
    date_selector: str | None = None

    def allowed(self, href: str) -> bool:
        if not href: return False
        u = canonical_url(urldefrag(href)[0])
        if any(re.search(rx, u) for rx in self.deny):
            return False
        return any(re.search(rx, u) for rx in self.allow)


def load_sites_from_yaml(path: str) -> list[Site]:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or []
    return [Site(**row) for row in raw]


def robots_ok(site: Site, url: str) -> bool:
    try:
        rp = robotparser.RobotFileParser()
        rp.set_url(urljoin(site.base, "/robots.txt"))
        rp.read()
        return rp.can_fetch(UA, url)
    except Exception:
        return True


def session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept-Language": "uk,ru;q=0.8,en;q=0.7"})
    adapter = requests.adapters.HTTPAdapter(max_retries=3)
    s.mount("http://", adapter); s.mount("https://", adapter)
    return s


def extract_links(html_text: str, base: str) -> list[str]:
    soup = BeautifulSoup(html_text, "lxml")
    out = []
    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if not href or href.startswith("#"): continue
        if href.startswith("/"): href = urljoin(base, href)
        out.append(canonical_url(urldefrag(href)[0]))
    return list(dict.fromkeys(out))


def extract_meta_date(soup: BeautifulSoup) -> str | None:
    for sel in [
        'meta[property="article:published_time"]',
        'meta[property="og:updated_time"]',
        'meta[name="pubdate"]',
        'time[datetime]'
    ]:
        el = soup.select_one(sel)
        if el:
            dt = el.get("content") or el.get("datetime")
            if dt:
                try:
                    return datetime.fromisoformat(dt.replace("Z","+00:00")).astimezone(TZ).isoformat()
                except Exception:
                    pass
    return parse_ua_date(soup.get_text(" "))


def extract_article(html_text: str, url: str, site: Site) -> tuple[str, str]:
    soup_full = BeautifulSoup(html_text, "lxml")
    if site.title_selector:
        h = soup_full.select_one(site.title_selector)
        title = (h.get_text(" ").strip() if h else "")
    else:
        title = Document(html_text).short_title() or ""

    if site.summary_selector:
        body = soup_full.select_one(site.summary_selector)
        summary = clamp(body.get_text(" ").strip()) if body else ""
    else:
        content_html = Document(html_text).summary(html_partial=True)
        soup = BeautifulSoup(content_html, "lxml")
        summary = clamp(soup.get_text(" ").strip())
    return title, summary


def collect_site(site: Site, max_items: int, sess: requests.Session) -> list[dict]:
    items, seen = [], set()
    for start in site.start_urls:
        try:
            if not robots_ok(site, start):
                log.info("robots disallow: %s", start); continue
            r = sess.get(start, timeout=25)
            if r.status_code != 200: continue
            for href in extract_links(r.text, site.base):
                if href in seen: continue
                seen.add(href)
                if not site.allowed(href): continue
                if not robots_ok(site, href): continue
                rr = sess.get(href, timeout=30)
                if rr.status_code != 200: continue
                soup = BeautifulSoup(rr.text, "lxml")
                text_all = soup.get_text(" ").lower()
                if site.keywords and not any(k in text_all for k in site.keywords):
                    continue
                title, summary = extract_article(rr.text, href, site)
                pub = None
                if site.date_selector:
                    el = soup.select_one(site.date_selector)
                    if el:
                        pub = extract_meta_date(BeautifulSoup(str(el), "lxml"))
                if not pub:
                    pub = extract_meta_date(soup)
                items.append({
                    "url": href,
                    "title": title or "",
                    "summary": summary or "",
                    "source": site.name,
                    "published_at": pub or datetime.now(TZ).isoformat(),
                })
                if len(items) >= max_items: return items
                time.sleep(0.8)
        except Exception as e:
            log.warning("%s: %s", site.name, e)
    return items


def collect_nonrss(config_path: str = "sources.yaml", max_items_total: int = 15) -> list[dict]:
    sites = load_sites_from_yaml(config_path)
    sess = session()
    per_site = max(1, max_items_total // max(1, len(sites)))
    out = []
    for st in sites:
        out.extend(collect_site(st, per_site, sess))
    return out

if __name__ == "__main__":
    print(json.dumps(collect_nonrss("sources.yaml", 12), ensure_ascii=False, indent=2))
