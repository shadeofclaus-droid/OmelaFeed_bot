#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Утіліти: канонізація URL, парсинг дат українською, обрізання тексту."""
from __future__ import annotations
import re
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
from datetime import datetime
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Europe/Kyiv")

UTM_KEYS = {"utm_source","utm_medium","utm_campaign","utm_term","utm_content","fbclid","gclid","mc_cid","mc_eid"}

UA_MONTHS = {
    "січня":1, "лютого":2, "березня":3, "квітня":4, "травня":5, "червня":6,
    "липня":7, "серпня":8, "вересня":9, "жовтня":10, "листопада":11, "грудня":12,
}

DATE_PAT = re.compile(r"(\d{1,2})\s+([А-Яа-яІіЇїЄє]+)\s+(\d{4})")


def canonical_url(url: str) -> str:
    s = urlsplit(url)
    q = [(k,v) for k,v in parse_qsl(s.query, keep_blank_values=True) if k not in UTM_KEYS]
    query = urlencode(q, doseq=True)
    return urlunsplit((s.scheme, s.netloc, s.path, query, ""))


def parse_ua_date(text: str) -> str | None:
    m = DATE_PAT.search(text)
    if not m:
        return None
    day, mon_ua, year = m.groups()
    months = {k.lower():v for k,v in UA_MONTHS.items()}
    mon = months.get(mon_ua.lower())
    if not mon:
        return None
    try:
        dt = datetime(int(year), mon, int(day), tzinfo=TZ)
        return dt.isoformat()
    except Exception:
        return None


def clamp(text: str, limit: int = 750) -> str:
    t = " ".join(text.split())
    return t[:limit].rstrip() + ("…" if len(t) > limit else "")
