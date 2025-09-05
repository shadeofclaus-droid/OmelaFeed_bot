#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram News → Channel (модерація + автопост) — Збір із RSS та без-RSS (HTML)
- Черга новин у SQLite зі статусами: PENDING/APPROVED/REJECTED/PUBLISHED
- Картка модерації з кнопками: Публікувати зараз / Запланувати 09:00 / Обрати час… / Пропустити / Відхилити
- Збір:
    * RSS із .env (SOURCES)
    * HTML (без RSS) за правилами у sources.yaml (через collectors_nonrss)
- Розклад: щоденний збір 08:45 (Europe/Kyiv) + ручні /collect, /review

Налаштування:
1) Заповніть .env
2) Переконайтесь, що є файл sources.yaml (джерела без RSS)
3) pip install -r requirements.txt
4) python news_pipeline_bot.py
"""
import os
import sqlite3
import html as htmlmod
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo

import feedparser
from bs4 import BeautifulSoup
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, constants
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")  # @your_channel або -100XXXXXXXXXX
TIMEZONE = os.getenv("TIMEZONE", "Europe/Kyiv")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "").strip() or None  # опц.: один адмін/група
ADMINS = [int(x) for x in os.getenv("ADMINS", "").replace(" ", "").split(",") if x]
DB_PATH = os.getenv("DB_PATH", "news.db")
SOURCES_ENV = os.getenv("SOURCES", "")
RSS_SOURCES = [s.strip() for s in SOURCES_ENV.split(",") if s.strip()]
MAX_ITEMS_PER_RUN = int(os.getenv("MAX_ITEMS_PER_RUN", "10"))
SOURCES_YAML = os.getenv("SOURCES_YAML", "sources.yaml")

# Без-RSS колектор (може бути відсутній у дев-оточенні)
try:
    from collectors_nonrss import collect_nonrss
except Exception:
    collect_nonrss = None

TZ = ZoneInfo(TIMEZONE)

# ---------------- DB -----------------

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
        return None
    finally:
        conn.close()



def get_next_pending():
    conn = db()
    c = conn.cursor()
    c.execute("SELECT * FROM news WHERE status='PENDING' ORDER BY published_at DESC, id ASC LIMIT 1")
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


# -------------- Helpers --------------


def build_post_text(row):
    title = htmlmod.escape(row["title"] or "")
    summary = row["summary"] or ""
    summary_plain = " ".join(BeautifulSoup(summary, "html.parser").stripped_strings)
    summary_plain = summary_plain[:750].rstrip() + ("…" if len(summary_plain) >= 750 else "")
    summary_plain = htmlmod.escape(summary_plain)
    source = htmlmod.escape(row["source"] or "")
    url = row["url"]

    date_str = ""
    if row["published_at"]:
        try:
            dt = datetime.fromisoformat(row["published_at"])  # naive or aware
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=TZ)
            date_str = dt.astimezone(TZ).strftime("%d.%m.%Y")
        except Exception:
            pass

    parts = []
    if title:
        parts.append(f"<b>{title}</b>")
    if date_str or source:
        parts.append(f"<i>{source}{' · ' + date_str if date_str else ''}</i>")
    if summary_plain:
        parts.append(summary_plain)
    parts.append(f"Джерело: {url}")
    return "\n\n".join(parts)



def is_admin_context(update: Update) -> bool:
    """Дозволяємо, якщо ADMIN_CHAT_ID (група) або user id в ADMINS."""
    try:
        if ADMIN_CHAT_ID:
            return str(update.effective_chat.id) == str(ADMIN_CHAT_ID)
        if ADMINS:
            uid = update.effective_user.id if update.effective_user else None
            return bool(uid and uid in ADMINS)
        return True  # dev-режим без обмежень
    except Exception:
        return False


async def send_review_card(context: ContextTypes.DEFAULT_TYPE, row):
    kb = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Публікувати зараз", callback_data=f"approve_now:{row['id']}")],
            [InlineKeyboardButton("🕘 Запланувати 09:00", callback_data=f"approve_0900:{row['id']}")],
            [InlineKeyboardButton("⏱ Обрати час…", callback_data=f"picktime:{row['id']}")],
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


# -------------- Collectors --------------


def parse_feed(url):
    try:
        fp = feedparser.parse(url)
        items = []
        src_title = fp.feed.get("title", "") if getattr(fp, "feed", None) else ""
        for e in fp.entries:
            link = e.get("link")
            title = e.get("title")
            summary = e.get("summary") or e.get("description") or ""

            if e.get("published_parsed"):
                published = datetime(*e.published_parsed[:6], tzinfo=TZ).isoformat()
            elif e.get("updated_parsed"):
                published = datetime(*e.updated_parsed[:6], tzinfo=TZ).isoformat()
            else:
                published = datetime.now(TZ).isoformat()

            items.append({
                "url": link,
                "title": title,
                "summary": summary,
                "source": src_title,
                "published_at": published,
            })
        return items
    except Exception as e:
        print("feed error", url, e)
        return []


async def collect_job(context: ContextTypes.DEFAULT_TYPE):
    added = 0

    # 1) RSS (якщо задано у .env)
    for src in RSS_SOURCES:
        for itm in parse_feed(src):
            if not itm.get("url") or not itm.get("title"):
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

    # 2) HTML без RSS (sources.yaml)
    if collect_nonrss and added < MAX_ITEMS_PER_RUN:
        try:
            remain = MAX_ITEMS_PER_RUN - added
            for itm in collect_nonrss(SOURCES_YAML, max_items_total=remain):
                if not itm.get("url") or not itm.get("title"):
                    continue
                inserted_id = add_item(
                    itm["url"], itm["title"], itm["summary"], itm["source"], itm["published_at"]
                )
                if inserted_id:
                    added += 1
        except Exception as e:
            print("nonrss error:", e)

    if added:
        row = get_next_pending()
        if row:
            await send_review_card(context, row)


# -------------- Publisher --------------
async def publish_item(context: ContextTypes.DEFAULT_TYPE, item_id: int):
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


async def schedule_0900(context: ContextTypes.DEFAULT_TYPE, item_id: int):
    now = datetime.now(TZ)
    target = datetime.combine(now.date(), dtime(9, 0, tzinfo=TZ))
    if target < now:
        target += timedelta(days=1)
    delay = (target - now).total_seconds()
    context.job_queue.run_once(lambda ctx: publish_item(ctx, item_id), when=delay, name=f"publish_{item_id}")
    mark_status(item_id, "APPROVED", scheduled_for=target.isoformat())


# -------------- Handlers --------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привіт! Я — бот модерації новин. Команди:\n"
        "/collect — зібрати новини зараз\n"
        "/review — показати наступну новину до перевірки\n"
        "Порада: натисніть 'Обрати час…', щоб поставити публікацію на конкретну годину."
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


async def cb_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    try:
        data = q.data or ""
        action, id_str = data.split(":")
        item_id = int(id_str)
    except Exception:
        return await q.edit_message_text("Помилка дії.")

    user_id = update.effective_user.id if update.effective_user else None

    if action == "approve_now":
        mark_status(item_id, "APPROVED", approved_by=user_id)
        await publish_item(context, item_id)
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
        conn = db()
        c = conn.cursor()
        c.execute("UPDATE news SET created_at=? WHERE id=?", (datetime.now(TZ).isoformat(), item_id))
        conn.commit(); conn.close()
        await q.edit_message_text("⏭ Пропущено (залишилось у черзі).")
        row = get_next_pending()
        if row:
            await send_review_card(context, row)

    elif action == "picktime":
        await q.edit_message_text("Введіть час у форматі HH:MM (Київ), напр.: 12:30")
        context.bot_data["await_time_for"] = item_id


async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    awaiting = context.bot_data.get("await_time_for")
    if not awaiting:
        return
    try:
        hh, mm = (update.message.text or "").strip().split(":")
        hh, mm = int(hh), int(mm)
        now = datetime.now(TZ)
        target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if target < now:
            target = target + timedelta(days=1)
        delay = (target - now).total_seconds()
        context.job_queue.run_once(lambda ctx: publish_item(ctx, awaiting), when=delay, name=f"publish_{awaiting}")
        mark_status(awaiting, "APPROVED", scheduled_for=target.isoformat())
        await update.message.reply_text(f"Заплановано на {target.strftime('%d.%m %H:%M')} (Київ)")
    except Exception:
        await update.message.reply_text("Невірний формат. Приклад: 09:30")
    finally:
        context.bot_data.pop("await_time_for", None)



def main():
    init_db()
    if not BOT_TOKEN or not CHANNEL_ID:
        raise SystemExit("Set BOT_TOKEN and CHANNEL_ID in .env")

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("review", review))
    app.add_handler(CommandHandler("collect", collect))
    app.add_handler(CallbackQueryHandler(cb_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_router))

    # Щоденний збір о 08:45 (Київ)
    app.job_queue.run_daily(collect_job, time=dtime(8, 45, tzinfo=TZ), name="collect_daily")

    print("Bot started. Press Ctrl+C to stop.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
