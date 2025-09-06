#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram News → Channel pipeline with human approval

Можливості:
- Збір новин із білих RSS‑джерел (та опціонально з HTML через collectors_nonrss, якщо доступно).
- Черга у SQLite (статуси: PENDING / APPROVED / REJECTED / PUBLISHED).
- Картка для модерації в адмін‑чаті: «Публікувати зараз» / «Запланувати 09:00» / «Пропустити» / «Відхилити».
- Команда /search — знаходить новини у черзі за датою або за діапазоном дат.
- Панель швидких дій при згадці бота у групі (наприклад, @BotName): «Зібрати зараз», «Показати чергу», «Пошук за датою», а також кнопки «Сьогодні / Вчора / 7 днів».
- Команда /collect_range — сканує HTML‑джерела без RSS за заданий діапазон дат (якщо увімкнено non‑RSS колектор).
- Щоденний автозбір о 08:45 (Europe/Kyiv) + ручні /collect, /review, /search, /collect_range.

Налаштування (через змінні середовища або `.env`):
- BOT_TOKEN — токен Telegram‑бота.
- CHANNEL_ID — ID або @username каналу, куди надсилати пости.
- TIMEZONE — часовий пояс (default: Europe/Kyiv).
- ADMIN_CHAT_ID — ID групи адмінів; команди із цього чату приймаються автоматично.
- ADMINS — перелік ID користувачів‑адміністраторів (коми).
- DB_PATH — шлях до SQLite‑бази (default: news.db).
- SOURCES — список RSS‑посилань (коми). Якщо порожній, будуть використані fallback‑стрічки.
- MAX_ITEMS_PER_RUN — максимальна кількість новин, що збирається за один запуск.
- SOURCES_YAML — шлях до YAML‑конфігурації для non‑RSS сканера (default: sources.yaml).

Для запуску:
1. Встановіть залежності з requirements.txt.
2. Заповніть .env або налаштуйте змінні середовища.
3. Запустіть файл: `python news_pipeline_bot.py`.
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

# Загрузка змінних середовища
load_dotenv()

# ---------- Конфіг ----------

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")  # наприклад @channel або -100xxxx
TIMEZONE = os.getenv("TIMEZONE", "Europe/Kyiv")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "").strip() or None
ADMINS = [int(x) for x in os.getenv("ADMINS", "").replace(" ", "").split(",") if x]
DB_PATH = os.getenv("DB_PATH", "news.db")

# Стрічки RSS (через кому). Якщо не задано або порожньо — fallback
SOURCES_ENV = os.getenv("SOURCES", "")
RSS_SOURCES = [s.strip() for s in SOURCES_ENV.split(",") if s.strip()] or [
    "https://www.kmu.gov.ua/rss",
    "https://www.pfu.gov.ua/feed/",
    "https://mva.gov.ua/ua/rss.xml",
]

MAX_ITEMS_PER_RUN = int(os.getenv("MAX_ITEMS_PER_RUN", "10"))
SOURCES_YAML = os.getenv("SOURCES_YAML", "sources.yaml")

# Опціонально підключаємо HTML‑колектор
try:
    from collectors_nonrss import collect_nonrss  # type: ignore
except Exception:
    collect_nonrss = None

TZ = ZoneInfo(TIMEZONE)


# ---------- База даних ----------

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
    c.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_news_status ON news(status);
        """
    )
    conn.commit()
    conn.close()


def add_item(url: str, title: str, summary: str, source: str, published_at: str):
    """
    Додає елемент у таблицю news, якщо такого URL ще немає.
    Повертає id нового рядка або None, якщо дублікат.
    """
    if not url or not title:
        return None
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
                published_at,
                datetime.now(TZ).isoformat(),
            ),
        )
        conn.commit()
        return c.lastrowid
    except sqlite3.IntegrityError:
        # цей URL вже був
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


def mark_status(
    item_id: int,
    status: str,
    approved_by: int | None = None,
    scheduled_for: str | None = None,
    channel_message_id: int | None = None,
):
    """
    Оновлює статус елемента та повʼязані поля (approved_by, scheduled_for, channel_message_id).
    """
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


# ---------- Допоміжні функції ----------

def build_post_text(row: sqlite3.Row) -> str:
    """Формує текст поста для каналу."""
    title = htmlmod.escape(row["title"] or "")
    summary = row["summary"] or ""
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

    parts: list[str] = []
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
    Перевіряє, чи має повідомлення право на адмінські дії:
      - якщо ADMIN_CHAT_ID задано, дозволяє команди з цього чату;
      - якщо ADMINS має список числових ID, дозволяє команди від цих користувачів;
      - якщо жоден не задано (dev‑режим) — дозволяє всім.
    """
    try:
        # Група адміністраторів
        if ADMIN_CHAT_ID and str(update.effective_chat.id) == str(ADMIN_CHAT_ID):
            return True
        # Індивідуальні адміністратори
        uid = update.effective_user.id if update.effective_user else None
        if ADMINS and uid and uid in ADMINS:
            return True
        # Якщо не задано жодної умови — за промовчанням True
        if not ADMIN_CHAT_ID and not ADMINS:
            return True
        return False
    except Exception:
        return False


async def send_review_card(context: ContextTypes.DEFAULT_TYPE, row: sqlite3.Row) -> None:
    """
    Надсилає картку для модерації у всі адмін‑чати.
    """
    kb = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "✅ Публікувати зараз",
                    callback_data=f"approve_now:{row['id']}",
                ),
                InlineKeyboardButton(
                    "🕘 Запланувати 09:00",
                    callback_data=f"approve_0900:{row['id']}",
                ),
            ],
            [InlineKeyboardButton("⏭ Пропустити", callback_data=f"skip:{row['id']}")],
            [InlineKeyboardButton("🗑 Відхилити", callback_data=f"reject:{row['id']}")],
        ]
    )
    text = build_post_text(row)

    targets: list[str | int] = []
    if ADMIN_CHAT_ID:
        targets = [ADMIN_CHAT_ID]
    elif ADMINS:
        targets = ADMINS  # type: ignore[assignment]
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


# ---------- Збір новин ----------

def parse_feed(url: str) -> list[dict]:
    """Парсить RSS‑стрічку та повертає список елементів."""
    try:
        fp = feedparser.parse(url)
        items: list[dict] = []
        src_title = fp.feed.get("title", "") if getattr(fp, "feed", None) else ""
        for e in fp.entries:
            link = e.get("link")
            title = e.get("title")
            summary = e.get("summary") or e.get("description") or ""

            # Дата публікації
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


async def collect_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Основний джоб для збору новин:
    - проходиться по RSS_SOURCES, додає нові елементи;
    - потім (за потреби) сканує HTML‑джерела через collect_nonrss.
    Після збору відправляє першу картку у черзі.
    """
    added = 0

    # 1) RSS
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

    # 2) HTML‑джерела (якщо є колектор)
    if collect_nonrss and added < MAX_ITEMS_PER_RUN:
        try:
            remaining = MAX_ITEMS_PER_RUN - added
            for itm in collect_nonrss(SOURCES_YAML, TZ, remaining=remaining):
                if not itm.get("url") or not itm.get("title"):
                    continue
                inserted_id = add_item(
                    itm["url"],
                    itm["title"],
                    itm.get("summary", ""),
                    itm.get("source", ""),
                    itm.get("published_at") or datetime.now(TZ).isoformat(),
                )
                if inserted_id:
                    added += 1
                    if added >= MAX_ITEMS_PER_RUN:
                        break
        except Exception as e:
            print("nonrss error:", e)

    # Після збору — відправити першу картку
    if added:
        row = get_next_pending()
        if row:
            await send_review_card(context, row)


# ---------- Пошук та діапазони ----------

def parse_date_args(args: list[str]):
    """
    Приймає список аргументів (наприклад, з context.args) та повертає
    (start_dt, end_dt_exclusive) у TZ. Підтримує:
      • YYYY-MM-DD
      • YYYY-MM-DD..YYYY-MM-DD
      • YYYY-MM-DD YYYY-MM-DD
    Якщо end < start, міняє їх місцями.
    """
    if not args:
        raise ValueError("no args")
    raw = " ".join(args).strip()
    if ".." in raw:
        a, b = [x.strip() for x in raw.split("..", 1)]
    else:
        parts = raw.split()
        a = parts[0]
        b = parts[1] if len(parts) > 1 else parts[0]
    d1 = datetime.strptime(a, "%Y-%m-%d").date()
    d2 = datetime.strptime(b, "%Y-%m-%d").date()
    if d2 < d1:
        d1, d2 = d2, d1
    start = datetime.combine(d1, dtime.min.replace(tzinfo=TZ))
    end_excl = datetime.combine(d2, dtime.max.replace(tzinfo=TZ)) + timedelta(seconds=1)
    return start, end_excl


def find_items_by_date(start_dt: datetime, end_dt_excl: datetime, statuses=("PENDING",)):
    """
    Повертає список новин з БД зі статусами із `statuses` і діапазоном дат.
    Результат відсортований по published_at DESC, id ASC.
    """
    conn = db()
    c = conn.cursor()
    q = f"""
        SELECT * FROM news
        WHERE status IN ({','.join('?' * len(statuses))})
          AND published_at >= ?
          AND published_at < ?
        ORDER BY published_at DESC, id ASC
    """
    params = list(statuses) + [start_dt.isoformat(), end_dt_excl.isoformat()]
    c.execute(q, params)
    rows = c.fetchall()
    conn.close()
    return rows


def _parse_range_str(s: str):
    """
    Допоміжна функція: отримує рядок діапазону (формат YYYY-MM-DD або
    YYYY-MM-DD..YYYY-MM-DD) і повертає (start_dt, end_dt_exclusive).
    """
    s = s.strip()
    if ".." in s:
        a, b = [x.strip() for x in s.split("..", 1)]
    else:
        a = b = s
    d1 = datetime.strptime(a, "%Y-%m-%d").date()
    d2 = datetime.strptime(b, "%Y-%m-%d").date()
    if d2 < d1:
        d1, d2 = d2, d1
    start = datetime.combine(d1, dtime.min.replace(tzinfo=TZ))
    end_excl = datetime.combine(d2, dtime.max.replace(tzinfo=TZ)) + timedelta(seconds=1)
    return start, end_excl


# ---------- Публікація ----------

async def publish_item_by_id(context: ContextTypes.DEFAULT_TYPE, item_id: int) -> None:
    """
    Публікує новину в CHANNEL_ID і змінює статус на PUBLISHED.
    """
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


async def publish_item_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Колбек для JobQueue: викликає publish_item_by_id з context.job.data.item_id"""
    data = context.job.data or {}
    item_id = data.get("item_id")
    if item_id:
        await publish_item_by_id(context, item_id)


async def schedule_0900(context: ContextTypes.DEFAULT_TYPE, item_id: int) -> None:
    """Планує публікацію на найближче 09:00 (Europe/Kyiv)."""
    now = datetime.now(TZ)
    target = datetime.combine(now.date(), dtime(9, 0, tzinfo=TZ))
    if target < now:
        target += timedelta(days=1)
    delay = (target - now).total_seconds()
    context.job_queue.run_once(
        publish_item_job,
        when=delay,
        data={"item_id": item_id},
        name=f"publish_{item_id}",
    )
    mark_status(item_id, "APPROVED", scheduled_for=target.isoformat())


# ---------- Команди ----------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Привіт! Я — бот модерації новин. Команди:\n"
        "/collect — зібрати новини зараз\n"
        "/review — показати наступну новину до перевірки\n"
        "/search YYYY-MM-DD[..YYYY-MM-DD] — знайти новини за датою\n"
        "/collect_range YYYY-MM-DD[..YYYY-MM-DD] — зібрати новини за діапазоном дат"
    )


async def review(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin_context(update):
        return await update.message.reply_text("Доступ лише для адмінів.")
    row = get_next_pending()
    if not row:
        return await update.message.reply_text("Немає нових новин у черзі.")
    await send_review_card(context, row)


async def collect(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_admin_context(update):
        return await update.message.reply_text("Доступ лише для адмінів.")
    await collect_job(context)
    await update.message.reply_text("Збір завершено. Перевірте чергу: /review")


async def collect_range_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Команда /collect_range — збирає новини з HTML‑джерел за заданий діапазон дат.
    Формати: YYYY-MM-DD або YYYY-MM-DD..YYYY-MM-DD.
    """
    if not is_admin_context(update):
        return await update.message.reply_text("Доступ лише для адмінів.")
    if not context.args:
        return await update.message.reply_text(
            "Використання:\n"
            "/collect_range YYYY-MM-DD\n"
            "/collect_range YYYY-MM-DD..YYYY-MM-DD"
        )
    if collect_nonrss is None:
        return await update.message.reply_text("non-RSS сканер не увімкнено або відсутній.")
    try:
        start_dt, end_excl = _parse_range_str(" ".join(context.args))
    except Exception:
        return await update.message.reply_text(
            "Невірний формат. Приклад: 2025-09-01 або 2025-09-01..2025-09-05"
        )
    added = 0
    try:
        # збільшуємо лиміт, щоб за один виклик відсканувати більше сторінок
        for itm in collect_nonrss(
            SOURCES_YAML,
            TZ,
            remaining=MAX_ITEMS_PER_RUN * 3,
            date_from=start_dt,
            date_to=end_excl,
        ):
            inserted_id = add_item(
                itm["url"],
                itm["title"],
                itm.get("summary", ""),
                itm.get("source", ""),
                itm.get("published_at", datetime.now(TZ).isoformat()),
            )
            if inserted_id:
                added += 1
            if added >= MAX_ITEMS_PER_RUN:
                break
    except Exception as e:
        print("collect_range error:", e)
    if not added:
        return await update.message.reply_text("За вибраний період нічого не знайдено.")
    # після збору — відіслаємо першу картку
    row = get_next_pending()
    if row:
        await send_review_card(context, row)
    return await update.message.reply_text(f"Додано до черги: {added}. Перевірте: /review")


async def search_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
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
        start_dt, end_excl = parse_date_args(context.args)
    except Exception:
        return await update.message.reply_text(
            "Невірний формат дати. Спробуйте: 2025-09-05 або 2025-09-01..2025-09-05"
        )
    rows = find_items_by_date(start_dt, end_excl, statuses=("PENDING",))
    if not rows:
        return await update.message.reply_text("Нічого не знайдено в черзі за цей період.")
    await update.message.reply_text(f"Знайдено: {len(rows)}. Надсилаю першу картку…")
    await send_review_card(context, rows[0])


async def mention_panel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Показує панель швидких дій, якщо у групі згадали бота."""
    msg = update.message
    if not msg or msg.chat.type not in ("group", "supergroup"):
        return
    if not is_admin_context(update):
        return
    botname = "@" + (context.bot.username or "")
    if botname.lower() not in (msg.text or "").lower():
        return
    kb = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("⚡ Зібрати зараз", callback_data="q_collect")],
            [InlineKeyboardButton("🗂 Показати чергу", callback_data="q_review")],
            [InlineKeyboardButton("🔎 Пошук за датою", callback_data="q_search_menu")],
        ]
    )
    await msg.reply_text("Що зробити? ↓", reply_markup=kb)


async def cb_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    # --- дії без item_id ---
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
                await q.edit_message_text("Надіслано наступну картку в адмін‑чат.")
            except Exception:
                pass
            return
        if data == "q_search_menu":
            kb = InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("Сьогодні", callback_data="q_search_today")],
                    [InlineKeyboardButton("Вчора", callback_data="q_search_yest")],
                    [InlineKeyboardButton("Останні 7 днів", callback_data="q_search_7d")],
                ]
            )
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

    # --- дії з item_id ---
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
        c.execute(
            "UPDATE news SET created_at=? WHERE id=?",
            (datetime.now(TZ).isoformat(), item_id),
        )
        conn.commit()
        conn.close()
        await q.edit_message_text("⏭ Пропущено (залишилось у черзі).")
        row = get_next_pending()
        if row:
            await send_review_card(context, row)


# ---------- Основний цикл ----------

def main() -> None:
    init_db()
    if not BOT_TOKEN or not CHANNEL_ID:
        raise SystemExit("Set BOT_TOKEN and CHANNEL_ID in .env")
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    # Команди
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("review", review))
    app.add_handler(CommandHandler("collect", collect))
    app.add_handler(CommandHandler("collect_range", collect_range_cmd))
    app.add_handler(CommandHandler("search", search_cmd))
    # Панель при згадці у групі
    app.add_handler(MessageHandler(filters.TEXT & filters.ChatType.GROUPS, mention_panel))
    # Кнопки з inline‑callback
    app.add_handler(CallbackQueryHandler(cb_handler))
    # Щоденний автозбір о 08:45 (Київ)
    app.job_queue.run_daily(collect_job, time=dtime(8, 45, tzinfo=TZ), name="collect_daily")
    print("Bot started. Press Ctrl+C to stop.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()