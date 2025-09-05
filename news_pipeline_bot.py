#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram News → Channel pipeline with human approval

Можливості:
- Збір новин з білих RSS-джерел (та опційно з HTML через collectors_nonrss, якщо є)
- Черга у SQLite (status: PENDING / APPROVED / REJECTED / PUBLISHED)
- Картка на модерацію в адмін-чат: Публікувати зараз / Запланувати 09:00 / Пропустити / Відхилити
- Команда /search для пошуку за датою або діапазоном дат
- Панель кнопок при згадці бота у групі (@BotName): швидкі дії та швидкий пошук (Сьогодні / Вчора / 7 днів)
- Щоденний автозбір о 08:45 (Europe/Kyiv)

ENV:
  BOT_TOKEN, CHANNEL_ID, TIMEZONE?, ADMIN_CHAT_ID?, ADMINS?, DB_PATH?, SOURCES?,
  MAX_ITEMS_PER_RUN?, SOURCES_YAML? (для non-RSS, якщо використовується)
"""

import os
import sqlite3
import html as htmlmod
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo

import feedparser
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, constants
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ---------- Load config ----------
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")  # @your_channel або -100xxxxxxxxxx
TIMEZONE = os.getenv("TIMEZONE", "Europe/Kyiv")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "").strip() or None  # чат адмінів (group/supergroup)
ADMINS = [int(x) for x in os.getenv("ADMINS", "").replace(" ", "").split(",") if x]  # індивідуальні адміни
DB_PATH = os.getenv("DB_PATH", "news.db")

SOURCES_ENV = os.getenv("SOURCES", "")
RSS_SOURCES = [s.strip() for s in SOURCES_ENV.split(",") if s.strip()] or [
    "https://www.kmu.gov.ua/rss",
    "https://www.pfu.gov.ua/feed/",
    "https://mva.gov.ua/ua/rss.xml",
]
MAX_ITEMS_PER_RUN = int(os.getenv("MAX_ITEMS_PER_RUN", "10"))

SOURCES_YAML = os.getenv("SOURCES_YAML", "sources.yaml")  # для HTML, якщо використовується
try:
    # Якщо у репо є модуль для не-RSS парсингу
    from collectors_nonrss import collect_nonrss  # type: ignore
except Exception:
    collect_nonrss = None  # Не обов’язково

TZ = ZoneInfo(TIMEZONE)


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
    c.execute("CREATE INDEX IF NOT EXISTS idx_news_status ON news(status);")
    conn.commit()
    conn.close()


def add_item(url, title, summary, source, published_at):
    if not url or not title:
        return None
    conn = db()
    c = conn.cursor()
    try:
        c.execute(
            """
            INSERT INTO news (url, title, summary, source, published_at, status, created_at)
            VALUES (?, ?, ?, ?, ?, 'PENDING', ?)
            """,
            (url, title, summary, source, published_at, datetime.now(TZ).isoformat()),
        )
        conn.commit()
        return c.lastrowid
    except sqlite3.IntegrityError:
        # дублікат URL — ігноруємо
        return None
    finally:
        conn.close()


def get_next_pending():
    conn = db()
    c = conn.cursor()
    c.execute(
        "SELECT * FROM news WHERE status='PENDING' ORDER BY published_at DESC, id ASC LIMIT 1"
    )
    row = c.fetchone()
    conn.close()
    return row


def mark_status(item_id, status, approved_by=None, scheduled_for=None, channel_message_id=None):
    conn = db()
    c = conn.cursor()
    c.execute(
        """
        UPDATE news SET status=?,
                        approved_by=COALESCE(?, approved_by),
                        scheduled_for=COALESCE(?, scheduled_for),
                        channel_message_id=COALESCE(?, channel_message_id)
        WHERE id=?
        """,
        (status, approved_by, scheduled_for, channel_message_id, item_id),
    )
    conn.commit()
    conn.close()


# ---------- Helpers ----------
def build_post_text(row):
    title = htmlmod.escape(row["title"] or "")
    summary = row["summary"] or ""
    # Чистимо HTML і обрізаємо
    summary_plain = " ".join(BeautifulSoup(summary, "html.parser").stripped_strings)
    if len(summary_plain) > 750:
        summary_plain = summary_plain[:750].rstrip() + "…"
    summary_plain = htmlmod.escape(summary_plain)
    source = htmlmod.escape(row["source"] or "")
    url = row["url"]

    date_str = ""
    if row["published_at"]:
        try:
            dt = datetime.fromisoformat(row["published_at"])
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=TZ)
            date_str = dt.astimezone(TZ).strftime("%d.%m.%Y")
        except Exception:
            pass

    parts = []
    if title:
        parts.append(f"<b>{title}</b>")
    if date_str or source:
        parts.append(f"<i>{source}{(' · ' + date_str) if date_str else ''}</i>")
    if summary_plain:
        parts.append(summary_plain)
    parts.append(f"Джерело: {url}")
    return "\n\n".join(parts)


def is_admin_context(update: Update) -> bool:
    """
    Дозволяє доступ, якщо:
    - повідомлення з адмін-групи ADMIN_CHAT_ID, або
    - user.id є у списку ADMINS
    Якщо нічого не налаштовано — режим розробника (дозволити всім).
    """
    try:
        # група адмінів
        if ADMIN_CHAT_ID and str(update.effective_chat.id) == str(ADMIN_CHAT_ID):
            return True
        # індивідуальні адміни
        uid = update.effective_user.id if update.effective_user else None
        if ADMINS and uid and uid in ADMINS:
            return True
        # нічого не задано — дозволити
        if not ADMIN_CHAT_ID and not ADMINS:
            return True
        return False
    except Exception:
        return False


async def send_review_card(context: ContextTypes.DEFAULT_TYPE, row):
    kb = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Публікувати зараз", callback_data=f"approve_now:{row['id']}"),
                InlineKeyboardButton("🕘 Запланувати 09:00", callback_data=f"approve_0900:{row['id']}"),
            ],
            [InlineKeyboardButton("⏭ Пропустити", callback_data=f"skip:{row['id']}")],
            [InlineKeyboardButton("🗑 Відхилити", callback_data=f"reject:{row['id']}")],
        ]
    )
    text = build_post_text(row)

    targets = []
    if ADMIN_CHAT_ID:
        targets = [ADMIN_CHAT_ID]
    elif ADMINS:
        targets = ADMINS
    else:
        return

    for chat_id in targets:
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"📰 <b>До перевірки</b> (#{row['id']})\n\n{text}",
                parse_mode=constants.ParseMode.HTML,
                disable_web_page_preview=False,
                reply_markup=kb,
            )
        except Exception as e:
            print("admin send error:", e)


# ---------- Collector ----------
def parse_feed(url):
    try:
        fp = feedparser.parse(url)
        items = []
        src_title = fp.feed.get("title", "") if getattr(fp, "feed", None) else ""
        for e in fp.entries:
            link = e.get("link")
            title = e.get("title")
            summary = e.get("summary") or e.get("description") or ""

            # Дата
            published = None
            if e.get("published_parsed"):
                published = datetime(*e.published_parsed[:6], tzinfo=TZ).isoformat()
            elif e.get("updated_parsed"):
                published = datetime(*e.updated_parsed[:6], tzinfo=TZ).isoformat()
            else:
                published = datetime.now(TZ).isoformat()

            items.append(
                {
                    "url": link,
                    "title": title,
                    "summary": summary,
                    "source": src_title,
                    "published_at": published,
                }
            )
        return items
    except Exception as e:
        print("feed error", url, e)
        return []


async def collect_job(context: ContextTypes.DEFAULT_TYPE):
    added = 0

    # 1) RSS
    for src in RSS_SOURCES:
        for itm in parse_feed(src):
            if not itm["url"] or not itm["title"]:
                continue
            inserted_id = add_item(
                itm["url"], itm["title"], itm["summary"], itm["source"], itm["published_at"]
            )
            if inserted_id:
                added += 1
                if added >= MAX_ITEMS_PER_RUN:
                    break
        if added >= MAX_ITEMS_PER_RUN:
            break

    # 2) Non-RSS (опційно)
    if collect_nonrss is not None and added < MAX_ITEMS_PER_RUN:
        try:
            for itm in collect_nonrss(SOURCES_YAML, TZ, remaining=MAX_ITEMS_PER_RUN - added):
                if not itm.get("url") or not itm.get("title"):
                    continue
                inserted_id = add_item(
                    itm["url"], itm["title"], itm.get("summary", ""), itm.get("source", ""),
                    itm.get("published_at") or datetime.now(TZ).isoformat()
                )
                if inserted_id:
                    added += 1
                    if added >= MAX_ITEMS_PER_RUN:
                        break
        except Exception as e:
            print("nonrss error:", e)

    if added:
        row = get_next_pending()
        if row:
            await send_review_card(context, row)


# ---------- Search helpers ----------
def parse_date_args(args):
    """
    Повертає (start_dt, end_dt_exclusive) у TZ.
    Підтримує:
      - YYYY-MM-DD
      - YYYY-MM-DD..YYYY-MM-DD
      - YYYY-MM-DD YYYY-MM-DD
    """
    if not args:
        raise ValueError("no args")

    raw = " ".join(args).strip()
    if ".." in raw:
        a, b = [x.strip() for x in raw.split("..", 1)]
    else:
        parts = raw.split()
        if len(parts) == 1:
            a = b = parts[0]
        else:
            a, b = parts[0], parts[1]

    d1 = datetime.strptime(a, "%Y-%m-%d").date()
    d2 = datetime.strptime(b, "%Y-%m-%d").date()
    if d2 < d1:
        d1, d2 = d2, d1

    start = datetime.combine(d1, dtime.min.replace(tzinfo=TZ))
    end_exclusive = datetime.combine(d2, dtime.max.replace(tzinfo=TZ)) + timedelta(seconds=1)
    return start, end_exclusive


def find_items_by_date(start_dt, end_dt_excl, statuses=("PENDING",)):
    conn = db()
    c = conn.cursor()
    q = f"""
        SELECT * FROM news
        WHERE status IN ({",".join("?"*len(statuses))})
          AND published_at >= ?
          AND published_at < ?
        ORDER BY published_at DESC, id ASC
    """
    params = list(statuses) + [start_dt.isoformat(), end_dt_excl.isoformat()]
    c.execute(q, params)
    rows = c.fetchall()
    conn.close()
    return rows


# ---------- Publisher ----------
async def publish_item_by_id(context: ContextTypes.DEFAULT_TYPE, item_id: int):
    conn = db()
    c = conn.cursor()
    c.execute("SELECT * FROM news WHERE id=?", (item_id,))
    row = c.fetchone()
    conn.close()
    if not row:
        return

    text = build_post_text(row)
    msg = await context.bot.send_message(
        chat_id=CHANNEL_ID,
        text=text,
        parse_mode=constants.ParseMode.HTML,
        disable_web_page_preview=False,
    )
    mark_status(item_id, "PUBLISHED", channel_message_id=msg.message_id)


async def publish_item_job(context: ContextTypes.DEFAULT_TYPE):
    """JobQueue колбек: бере item_id із context.job.data і публікує."""
    data = context.job.data or {}
    item_id = data.get("item_id")
    if item_id:
        await publish_item_by_id(context, item_id)


async def schedule_0900(context: ContextTypes.DEFAULT_TYPE, item_id: int):
    now = datetime.now(TZ)
    target = datetime.combine(now.date(), dtime(9, 0, tzinfo=TZ))
    if target < now:
        target += timedelta(days=1)
    delay = (target - now).total_seconds()

    context.job_queue.run_once(
        publish_item_job,
        when=delay,
        data={"item_id": item_id},
        name=f"publish_{item_id}"
    )
    mark_status(item_id, "APPROVED", scheduled_for=target.isoformat())


# ---------- Handlers (commands & callbacks) ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привіт! Я — бот модерації новин. Команди:\n"
        "/collect — зібрати новини зараз\n"
        "/review — показати наступну новину до перевірки\n"
        "/search YYYY-MM-DD[..YYYY-MM-DD] — знайти новини за датою"
    )


async def review(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin_context(update):
        return await update.message.reply_text("Доступ лише для адмінів.")

    row = get_next_pending()
    if not row:
        await update.message.reply_text("Немає нових новин у черзі.")
        return
    await send_review_card(context, row)


async def collect(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin_context(update):
        return await update.message.reply_text("Доступ лише для адмінів.")

    await collect_job(context)
    await update.message.reply_text("Збір завершено. Перевірте чергу: /review")


async def search_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin_context(update):
        return await update.message.reply_text("Доступ лише для адмінів.")

    if not context.args:
        return await update.message.reply_text(
            "Використання:\n"
            "/search YYYY-MM-DD\n"
            "/search YYYY-MM-DD..YYYY-MM-DD\n"
            "/search YYYY-MM-DD YYYY-MM-DD"
        )

    try:
        start_dt, end_dt_excl = parse_date_args(context.args)
    except Exception:
        return await update.message.reply_text(
            "Невірний формат дати. Приклад: 2025-09-05 або 2025-09-01..2025-09-05"
        )

    rows = find_items_by_date(start_dt, end_dt_excl, statuses=("PENDING",))
    if not rows:
        return await update.message.reply_text("Нічого не знайдено в черзі за цей період.")
    await update.message.reply_text(f"Знайдено: {len(rows)}. Надсилаю першу картку…")
    await send_review_card(context, rows[0])


async def mention_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показує швидку панель, якщо у групі згадали бота."""
    msg = update.message
    if not msg or msg.chat.type not in ("group", "supergroup"):
        return
    if not is_admin_context(update):
        return
    botname = "@" + (context.bot.username or "")
    if botname.lower() not in (msg.text or "").lower():
        return

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("⚡ Зібрати зараз", callback_data="q_collect")],
        [InlineKeyboardButton("🗂 Показати чергу", callback_data="q_review")],
        [InlineKeyboardButton("🔎 Пошук за датою", callback_data="q_search_menu")],
    ])
    await msg.reply_text("Що зробити? ↓", reply_markup=kb)


async def cb_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""

    # --- Без item_id ---
    if data in {"q_collect", "q_review", "q_search_menu", "q_search_today", "q_search_yest", "q_search_7d"}:
        if data == "q_collect":
            if not is_admin_context(update):
                return await q.edit_message_text("Доступ лише для адмінів.")
            await collect_job(context)
            return await q.edit_message_text("Збір завершено. Перевірте: /review")

        if data == "q_review":
            if not is_admin_context(update):
                return await q.edit_message_text("Доступ лише для адмінів.")
            row = get_next_pending()
            if not row:
                return await q.edit_message_text("Черга порожня.")
            await send_review_card(context, row)
            try:
                await q.edit_message_text("Надіслано наступну картку в адмін-чат.")
            except Exception:
                pass
            return

        if data == "q_search_menu":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("Сьогодні", callback_data="q_search_today")],
                [InlineKeyboardButton("Вчора", callback_data="q_search_yest")],
                [InlineKeyboardButton("Останні 7 днів", callback_data="q_search_7d")],
            ])
            return await q.edit_message_text("Оберіть діапазон:", reply_markup=kb)

        if data in {"q_search_today", "q_search_yest", "q_search_7d"}:
            now = datetime.now(TZ).date()
            if data == "q_search_today":
                d1 = d2 = now
            elif data == "q_search_yest":
                d1 = d2 = now - timedelta(days=1)
            else:
                d1, d2 = now - timedelta(days=6), now

            start_dt = datetime.combine(d1, dtime.min.replace(tzinfo=TZ))
            end_dt_excl = datetime.combine(d2, dtime.max.replace(tzinfo=TZ)) + timedelta(seconds=1)
            rows = find_items_by_date(start_dt, end_dt_excl, statuses=("PENDING",))
            if not rows:
                return await q.edit_message_text("За вибраний період нічого не знайдено.")
            await q.edit_message_text(f"Знайдено: {len(rows)}. Надсилаю першу картку…")
            await send_review_card(context, rows[0])
            return

    # --- Дії з item_id ---
    try:
        action, id_str = data.split(":")
        item_id = int(id_str)
    except Exception:
        return await q.edit_message_text("Помилка дії.")

    user_id = update.effective_user.id if update.effective_user else None

    if action == "approve_now":
        mark_status(item_id, "APPROVED", approved_by=user_id)
        await publish_item_by_id(context, item_id)
        await q.edit_message_text("✅ Опубліковано в канал.")
        row = get_next_pending()
        if row:
            await send_review_card(context, row)

    elif action == "approve_0900":
        mark_status(item_id, "APPROVED", approved_by=user_id)
        await schedule_0900(context, item_id)
        await q.edit_message_text("🕘 Заплановано на 09:00 (Київ).")
        row = get_next_pending()
        if row:
            await send_review_card(context, row)

    elif action == "reject":
        mark_status(item_id, "REJECTED", approved_by=user_id)
        await q.edit_message_text("🗑 Відхилено.")
        row = get_next_pending()
        if row:
            await send_review_card(context, row)

    elif action == "skip":
        # відсунути в черзі, оновивши created_at
        conn = db()
        c = conn.cursor()
        c.execute("UPDATE news SET created_at=? WHERE id=?", (datetime.now(TZ).isoformat(), item_id))
        conn.commit()
        conn.close()
        await q.edit_message_text("⏭ Пропущено (залишилось у черзі).")
        row = get_next_pending()
        if row:
            await send_review_card(context, row)


# ---------- App ----------
def main():
    init_db()
    if not BOT_TOKEN or not CHANNEL_ID:
        raise SystemExit("Set BOT_TOKEN and CHANNEL_ID in .env")

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Команди
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("review", review))
    app.add_handler(CommandHandler("collect", collect))
    app.add_handler(CommandHandler("search", search_cmd))

    # Панель при згадці у групі
    app.add_handler(MessageHandler(filters.TEXT & filters.ChatType.GROUPS, mention_panel))

    # Колбеки кнопок
    app.add_handler(CallbackQueryHandler(cb_handler))

    # Щоденний автозбір о 08:45 (Київ)
    app.job_queue.run_daily(collect_job, time=dtime(8, 45, tzinfo=TZ), name="collect_daily")

    print("Bot started. Press Ctrl+C to stop.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
