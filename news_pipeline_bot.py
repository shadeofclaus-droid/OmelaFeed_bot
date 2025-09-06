#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import logging
import sqlite3
import html as htmlmod
from datetime import datetime, timedelta, time as dtime
from datetime import timezone as tzmod
from email.utils import parsedate_to_datetime as _rfc_parse
from zoneinfo import ZoneInfo

import feedparser
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, constants, BotCommand
from telegram.constants import MessageEntityType
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ---- utils_normalize fallbacks ----
try:
    from utils_normalize import canonical_url, clamp  # type: ignore
except Exception:
    def canonical_url(url: str) -> str:  # type: ignore
        return url or ""
    def clamp(text: str, limit: int = 750) -> str:  # type: ignore
        t = (text or "")
        return t if len(t) <= limit else (t[:limit].rstrip() + "…")

load_dotenv()

# ---------- Config ----------
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")
TIMEZONE = os.getenv("TIMEZONE", "Europe/Kyiv")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "").strip() or None
ADMINS = [int(x) for x in os.getenv("ADMINS", "").replace(" ", "").split(",") if x]
DB_PATH = os.getenv("DB_PATH", "news.db")

SOURCES_ENV = os.getenv("SOURCES", "")
RSS_SOURCES = [s.strip() for s in SOURCES_ENV.split(",") if s.strip()] or [
    "https://www.kmu.gov.ua/rss",
    "https://www.pfu.gov.ua/feed/",
    # якщо 404 — прибери або заміни на робочий фід
    "https://mva.gov.ua/ua/rss.xml",
]

MAX_ITEMS_PER_RUN = int(os.getenv("MAX_ITEMS_PER_RUN", "10"))
SOURCES_YAML = os.getenv("SOURCES_YAML", "sources.yaml")

# optional non-RSS collector
try:
    from collectors_nonrss import collect_nonrss  # type: ignore
except Exception:
    collect_nonrss = None

TZ = ZoneInfo(TIMEZONE)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("newsbot")

# ------ Date parsing helpers for RSS ------
def parse_entry_datetime(entry, tz: ZoneInfo) -> str:
    for key in ("published_parsed", "updated_parsed"):
        t = getattr(entry, key, None)
        if t:
            try:
                dt = datetime(*t[:6], tzinfo=tzmod.utc).astimezone(tz)
                return dt.isoformat()
            except Exception:
                pass

    def _get(key):
        if hasattr(entry, key):
            return getattr(entry, key, None)
        if isinstance(entry, dict):
            return entry.get(key)
        return None

    for key in ("published", "updated", "dc_date"):
        v = _get(key)
        if not v:
            continue
        try:
            dt = _rfc_parse(v)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=tzmod.utc)
            return dt.astimezone(tz).isoformat()
        except Exception:
            pass
        try:
            dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=tzmod.utc)
            return dt.astimezone(tz).isoformat()
        except Exception:
            pass

    return datetime.now(tz).isoformat()

# ---------- DB ----------
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = db()
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS news (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          url TEXT UNIQUE,
          title TEXT,
          summary TEXT,
          source TEXT,
          published_at TEXT,
          status TEXT,
          created_at TEXT,
          approved_by INTEGER,
          scheduled_for TEXT,
          channel_message_id INTEGER
        )
        """
    )
    c.execute("CREATE INDEX IF NOT EXISTS idx_news_status ON news(status)")
    conn.commit()
    conn.close()

def add_item(url: str, title: str, summary: str, source: str, published_at: str):
    if not url or not title:
        return None
    try:
        url = canonical_url(url)
    except Exception:
        pass
    title = (title or "").strip()
    summary = clamp((summary or "").strip(), 750)
    conn = db()
    c = conn.cursor()
    try:
        c.execute(
            """
            INSERT INTO news (
                url, title, summary, source, published_at, status, created_at
            )
            VALUES (?, ?, ?, ?, ?, 'PENDING', ?)
            """,
            (
                url,
                title,
                summary,
                source,
                published_at or datetime.now(TZ).isoformat(),
                datetime.now(TZ).isoformat(),
            ),
        )
        conn.commit()
        return c.lastrowid
    except sqlite3.IntegrityError:
        return None
    finally:
        conn.close()

def get_next_pending():
    conn = db()
    c = conn.cursor()
    c.execute(
        "SELECT * FROM news WHERE status='PENDING' ORDER BY COALESCE(published_at, created_at) DESC, id ASC LIMIT 1"
    )
    row = c.fetchone()
    conn.close()
    return row

def mark_status(item_id: int, status: str, approved_by: int | None = None,
                scheduled_for: str | None = None, channel_message_id: int | None = None):
    conn = db()
    c = conn.cursor()
    sets = ["status=?"]
    params: list = [status]
    if approved_by is not None:
        sets.append("approved_by=?")
        params.append(approved_by)
    if scheduled_for is not None:
        sets.append("scheduled_for=?")
        params.append(scheduled_for)
    if channel_message_id is not None:
        sets.append("channel_message_id=?")
        params.append(channel_message_id)
    params.append(item_id)
    c.execute(f"UPDATE news SET {', '.join(sets)} WHERE id=?", params)
    conn.commit()
    conn.close()

# ---------- formatting ----------
def make_post_text(row: sqlite3.Row) -> str:
    title = htmlmod.escape(row["title"] or "")
    summary = htmlmod.escape(row["summary"] or "")
    url = row["url"]
    source = row["source"] or ""
    dt_str = (row["published_at"] or row["created_at"] or "")[:16].replace("T", " ")
    parts = [f"<b>{title}</b>"]
    if summary:
        parts.append(summary)
    meta = []
    if source:
        meta.append(source)
    if dt_str:
        meta.append(dt_str)
    parts.append(" • ".join(meta))
    parts.append(url)
    return "\n\n".join(parts)

# ---------- publishing ----------
async def publish_item_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    item_id = context.job.data["item_id"]
    conn = db(); c = conn.cursor()
    c.execute("SELECT * FROM news WHERE id=?", (item_id,))
    row = c.fetchone()
    conn.close()
    if not row or row["status"] == "PUBLISHED":
        return
    text = make_post_text(row)
    msg = await context.bot.send_message(
        chat_id=CHANNEL_ID,
        text=text,
        parse_mode=constants.ParseMode.HTML,
        disable_web_page_preview=False,
    )
    mark_status(item_id, "PUBLISHED", channel_message_id=msg.message_id)

async def schedule_0900(context: ContextTypes.DEFAULT_TYPE, item_id: int) -> None:
    now = datetime.now(TZ)
    target = datetime.combine(now.date(), dtime(9, 0, 0, tzinfo=TZ))
    if target <= now:
        target = target + timedelta(days=1)
    delay = (target - now).total_seconds()
    context.job_queue.run_once(
        publish_item_job,
        when=delay,
        data={"item_id": item_id},
        name=f"publish_{item_id}",
    )
    mark_status(item_id, "APPROVED", scheduled_for=target.isoformat())

async def restore_scheduled_jobs(app):
    try:
        conn = db(); c = conn.cursor()
        c.execute("""
            SELECT id, scheduled_for FROM news
            WHERE status='APPROVED' AND scheduled_for IS NOT NULL
        """)
        rows = c.fetchall()
        now = datetime.now(TZ)
        restored = 0
        for r in rows:
            ts = r["scheduled_for"]
            if not ts:
                continue
            try:
                target = datetime.fromisoformat(ts)
                if target.tzinfo is None:
                    target = target.replace(tzinfo=TZ)
                if target > now:
                    delay = (target - now).total_seconds()
                    app.job_queue.run_once(
                        publish_item_job,
                        when=delay,
                        data={"item_id": r["id"]},
                        name=f"publish_{r['id']}",
                    )
                    restored += 1
            except Exception as exc:
                log.error("Failed to restore job for item %s: %s", r["id"], exc, exc_info=True)
        conn.close()
        log.info("Restored %d scheduled jobs from DB", restored)
    except Exception as exc:
        log.error("Error while restoring scheduled jobs: %s", exc, exc_info=True)

# ---------- commands ----------
def is_admin_context(update: Update) -> bool:
    chat_ok = (ADMIN_CHAT_ID and str(update.effective_chat.id) == str(ADMIN_CHAT_ID))
    user_ok = (update.effective_user and update.effective_user.id in ADMINS)
    return bool(chat_ok or user_ok)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Привіт! Я — бот модерації новин. Команди:\n"
        "/collect — зібрати новини зараз\n"
        "/review — показати наступну новину до перевірки\n"
        "/search YYYY-MM-DD[..YYYY-MM-DD] — знайти новини за датою\n"
        "/collect_range YYYY-MM-DD[..YYYY-MM-DD] — зібрати новини за діапазоном дат (non-RSS)\n"
        "/stats — коротка статистика"
    )

async def review(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin_context(update):
        return await update.message.reply_text("Доступ лише для адмінів.")
    row = get_next_pending()
    if not row:
        return await update.message.reply_text("Немає нових новин у черзі.")
    await send_review_card(context, row)

async def send_review_card(context: ContextTypes.DEFAULT_TYPE, row: sqlite3.Row) -> None:
    kb = [
        [
            InlineKeyboardButton("✅ Публікувати зараз", callback_data=f"pub:{row['id']}"),
            InlineKeyboardButton("🕘 Запланувати 09:00", callback_data=f"sch:{row['id']}"),
        ],
        [
            InlineKeyboardButton("⏭ Пропустити", callback_data=f"skip:{row['id']}"),
            InlineKeyboardButton("🗑 Відхилити", callback_data=f"rej:{row['id']}"),
        ],
    ]
    text = make_post_text(row)
    await context.bot.send_message(
        chat_id=ADMIN_CHAT_ID or context._chat_id,  # безпечний fallback
        text=text,
        parse_mode=constants.ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(kb),
        disable_web_page_preview=False,
    )

async def cb_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    action, _, sid = data.partition(":")
    try:
        item_id = int(sid)
    except Exception:
        return
    if action == "pub":
        context.job_queue.run_once(publish_item_job, when=0, data={"item_id": item_id}, name=f"publish_{item_id}")
        mark_status(item_id, "APPROVED", approved_by=update.effective_user.id)
        await q.edit_message_text("✅ Заплановано до негайної публікації.")
        row = get_next_pending()
        if row:
            await send_review_card(context, row)
    elif action == "sch":
        await schedule_0900(context, item_id)
        await q.edit_message_text("🕘 Заплановано на 09:00 (Київ).")
        row = get_next_pending()
        if row:
            await send_review_card(context, row)
    elif action == "rej":
        mark_status(item_id, "REJECTED", approved_by=update.effective_user.id)
        await q.edit_message_text("🗑 Відхилено.")
        row = get_next_pending()
        if row:
            await send_review_card(context, row)
    elif action == "skip":
        conn = db(); c = conn.cursor()
        c.execute("UPDATE news SET created_at=? WHERE id=?", (datetime.now(TZ).isoformat(), item_id))
        conn.commit(); conn.close()
        await q.edit_message_text("⏭ Пропущено (залишилось у черзі).")
        row = get_next_pending()
        if row:
            await send_review_card(context, row)

# ---------- collect ----------
async def _do_collect(context: ContextTypes.DEFAULT_TYPE) -> str:
    log.info("Collect: %d RSS sources", len(RSS_SOURCES))
    added = 0; skipped = 0

    for src in RSS_SOURCES:
        try:
            feed = feedparser.parse(src)
            source_name = (feed.feed.get("title") if hasattr(feed, "feed") else None) or src
            for entry in feed.entries:
                url = canonical_url(getattr(entry, "link", "") or (entry.get("link") if isinstance(entry, dict) else ""))
                title = (getattr(entry, "title", "") or (entry.get("title") if isinstance(entry, dict) else "") or "").strip()
                summary = ""
                if hasattr(entry, "summary") and entry.summary:
                    summary = entry.summary
                elif isinstance(entry, dict):
                    summary = entry.get("summary") or ""
                if not summary:
                    try:
                        summary = entry.content[0].value  # type: ignore
                    except Exception:
                        summary = ""
                summary = clamp(BeautifulSoup(summary, "html.parser").get_text(" ", strip=True), 750)
                published_at = parse_entry_datetime(entry, TZ)

                rid = add_item(url, title, summary, source_name, published_at)
                if rid:
                    added += 1
                else:
                    skipped += 1
        except Exception as e:
            log.error("Collect RSS failed for %s: %s", src, e, exc_info=True)

    nonrss_added = 0
    if collect_nonrss and os.path.exists(SOURCES_YAML):
        try:
            remaining = max(0, MAX_ITEMS_PER_RUN - added) if MAX_ITEMS_PER_RUN else 0
            for item in collect_nonrss(SOURCES_YAML, TZ, remaining=remaining):
                rid = add_item(
                    canonical_url(item["url"]),
                    (item.get("title") or "").strip(),
                    clamp((item.get("summary") or "").strip(), 750),
                    item.get("source") or "non-rss",
                    item.get("published_at") or datetime.now(TZ).isoformat(),
                )
                if rid:
                    nonrss_added += 1
        except Exception as e:
            log.error("Collect non-RSS failed: %s", e, exc_info=True)
    else:
        log.info("Non-RSS collector disabled or sources.yaml not found")

    msg = f"Збір завершено: RSS added={added}, skipped={skipped}; nonRSS added={nonrss_added}"
    log.info(msg)
    return msg

async def collect(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update and not is_admin_context(update):
        return await update.message.reply_text("Доступ лише для адмінів.")
    msg = await _do_collect(context)
    if update and update.message:
        await update.message.reply_text(msg)

async def collect_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = await _do_collect(context)
    log.info("Daily collect: %s", msg)

# ---------- search & stats ----------
async def search_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin_context(update):
        return await update.message.reply_text("Доступ лише для адмінів.")

    arg = (context.args[0] if context.args else "").strip()
    if ".." in arg:
        left, right = [x.strip() for x in arg.split("..", 1)]
        try:
            start_d = datetime.fromisoformat(left).date()
            end_d = datetime.fromisoformat(right).date()
        except Exception:
            return await update.message.reply_text("Формат: /search YYYY-MM-DD[..YYYY-MM-DD]")
    elif arg:
        try:
            d = datetime.fromisoformat(arg).date()
        except Exception:
            return await update.message.reply_text("Формат: /search YYYY-MM-DD або /search YYYY-MM-DD..YYYY-MM-DD")
        start_d = d; end_d = d
    else:
        today = datetime.now(TZ).date()
        start_d = today - timedelta(days=6)
        end_d = today

    start_iso = datetime.combine(start_d, dtime.min, tzinfo=TZ).isoformat()
    end_iso = datetime.combine(end_d, dtime.max, tzinfo=TZ).isoformat()

    conn = db(); c = conn.cursor()
    c.execute(
        """
        SELECT * FROM news
        WHERE COALESCE(published_at, created_at) >= ?
          AND COALESCE(published_at, created_at) <= ?
        ORDER BY COALESCE(published_at, created_at) DESC, id DESC
        LIMIT 100
        """,
        (start_iso, end_iso),
    )
    rows = c.fetchall(); conn.close()

    if not rows:
        return await update.message.reply_text("За вибраний період нічого не знайдено.")

    lines = []
    for r in rows[:10]:
        d = r["published_at"] or r["created_at"]
        lines.append(f"• {r['title']} ({(d or '')[:16]})\n{r['url']}")
    suffix = "" if len(rows) <= 10 else f"\n…та ще {len(rows)-10} запис(ів)"
    await update.message.reply_text("\n\n".join(lines) + suffix)

async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin_context(update):
        return await update.message.reply_text("Доступ лише для адмінів.")
    conn = db(); c = conn.cursor()
    c.execute("SELECT COUNT(*) AS n FROM news"); total = c.fetchone()["n"]
    c.execute("SELECT COUNT(*) AS n FROM news WHERE status='PENDING'"); pending = c.fetchone()["n"]
    week_start = (datetime.now(TZ).date() - timedelta(days=6))
    start_iso = datetime.combine(week_start, dtime.min, tzinfo=TZ).isoformat()
    c.execute("SELECT COUNT(*) AS n FROM news WHERE COALESCE(published_at, created_at) >= ?", (start_iso,))
    last7 = c.fetchone()["n"]; conn.close()
    await update.message.reply_text(f"Всього в БД: {total}\nУ черзі PENDING: {pending}\nДодано за 7 днів: {last7}")

# ---------- mention panel (fixed) ----------
async def mention_panel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Працює в групах: показує панель, якщо є згадка бота
    if not update.message or not update.message.text:
        return
    entities = update.message.entities or []
    mentioned = any(e.type in (MessageEntityType.MENTION, MessageEntityType.TEXT_MENTION) for e in entities)
    if not mentioned:
        # запасний варіант: пряме входження @username
        me = await context.bot.get_me()
        if me.username and f"@{me.username}".lower() not in update.message.text.lower():
            return
    kb = [
        [InlineKeyboardButton("⚙️ Зібрати зараз", callback_data="act:collect"),
         InlineKeyboardButton("🗂 Черга", callback_data="act:review")],
        [InlineKeyboardButton("🔎 Пошук: сьогодні", callback_data="find:today"),
         InlineKeyboardButton("🔎 вчора", callback_data="find:yesterday"),
         InlineKeyboardButton("🔎 7 днів", callback_data="find:7d")],
    ]
    await update.message.reply_text("Що зробити?", reply_markup=InlineKeyboardMarkup(kb))

# ---------- group panel callbacks ----------
async def group_panel_cb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    if data == "act:collect":
        msg = await _do_collect(context)
        await q.edit_message_text(f"✅ {msg}")
    elif data == "act:review":
        row = get_next_pending()
        if row:
            await q.edit_message_text("Відкриваю картку…")
            await send_review_card(context, row)
        else:
            await q.edit_message_text("Черга порожня.")
    elif data.startswith("find:"):
        key = data.split(":", 1)[1]
        today = datetime.now(TZ).date()
        if key == "today":
            start_d = end_d = today
        elif key == "yesterday":
            start_d = end_d = today - timedelta(days=1)
        else:
            start_d = today - timedelta(days=6)
            end_d = today
        start_iso = datetime.combine(start_d, dtime.min, tzinfo=TZ).isoformat()
        end_iso = datetime.combine(end_d, dtime.max, tzinfo=TZ).isoformat()
        conn = db(); c = conn.cursor()
        c.execute(
            """
            SELECT * FROM news
            WHERE COALESCE(published_at, created_at) >= ?
              AND COALESCE(published_at, created_at) <= ?
            ORDER BY COALESCE(published_at, created_at) DESC, id DESC
            LIMIT 10
            """, (start_iso, end_iso))
        rows = c.fetchall(); conn.close()
        if not rows:
            return await q.edit_message_text("За вибраний період нічого не знайдено.")
        lines = [f"• {r['title']} ({(r['published_at'] or r['created_at'] or '')[:16]})\n{r['url']}" for r in rows]
        await q.edit_message_text("\n\n".join(lines))

# ---------- startup hooks ----------
async def set_commands(app):
    await app.bot.set_my_commands([
        BotCommand("start", "Почати роботу"),
        BotCommand("collect", "Зібрати новини"),
        BotCommand("review", "Перевірити чергу"),
        BotCommand("search", "Пошук за датою"),
        BotCommand("collect_range", "Збір non-RSS за діапазон"),
        BotCommand("stats", "Статистика"),
    ])

async def on_startup(app):
    # один хук для всіх стартових дій
    await restore_scheduled_jobs(app)
    await set_commands(app)

# ---------- main ----------
def main() -> None:
    init_db()
    if not BOT_TOKEN or not CHANNEL_ID:
        raise SystemExit("Set BOT_TOKEN and CHANNEL_ID in .env")

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(on_startup)   # 👈 тепер і відновлення, і меню команд
        .build()
    )

    # commands
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("review", review))
    app.add_handler(CommandHandler("collect", collect))
    app.add_handler(CommandHandler("collect_range", collect_range_cmd := lambda u, c: None))  # placeholder; заміни на свою реалізацію, якщо використовуєш
    app.add_handler(CommandHandler("search", search_cmd))
    app.add_handler(CommandHandler("stats", stats_cmd))

    # group panel
    app.add_handler(MessageHandler(filters.TEXT & filters.ChatType.GROUPS, mention_panel))
    app.add_handler(CallbackQueryHandler(group_panel_cb, pattern=r"^(act:|find:)"))
    app.add_handler(CallbackQueryHandler(cb_handler, pattern=r"^(pub:|sch:|rej:|skip:)"))

    # daily job
    app.job_queue.run_daily(collect_job, time=dtime(8, 45, tzinfo=TZ), name="collect_daily")

    log.info("Bot started. Press Ctrl+C to stop.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
