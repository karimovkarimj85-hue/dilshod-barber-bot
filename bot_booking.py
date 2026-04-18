"""
✂️ Дильшод — Бот записи (Telegram Mini App + aiogram + PostgreSQL)
ТЗ: мультиуслуги, буфер, Asia/Tashkent, защита от двойной записи, напоминания.
"""

import asyncio
import hashlib
import hmac
import json
import logging
import os
import urllib.parse
from contextlib import contextmanager
from datetime import datetime, timedelta, date, time, timezone
from pathlib import Path
from typing import Optional, List, Tuple, Any, Dict
from zoneinfo import ZoneInfo

import aiohttp
import psycopg2
import psycopg2.extras

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton,
    WebAppInfo,
)
from aiohttp import web
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ═══════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════

_DEFAULT_ADMIN_ID = 1125022050
_ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", str(_DEFAULT_ADMIN_ID))

CONFIG = {
    "BOT_TOKEN": os.getenv("BOT_TOKEN", ""),
    "ADMIN_IDS": sorted(
        {int(x.strip()) for x in _ADMIN_IDS_RAW.split(",") if x.strip()} | {_DEFAULT_ADMIN_ID}
    ),
    "CHANNEL_ID": int(os.getenv("CHANNEL_ID", "-1003692525683")),

    "BARBER_NAME": "Дильшод",
    "BARBER_YEAR": 2005,
    "BARBER_EXP": "более 5 лет",
    "BARBER_PHONE": "+998 97 116 31 61",

    "SLOT_STEP_MIN": int(os.getenv("SLOT_STEP_MIN", "30")),
    "BUFFER_MIN": int(os.getenv("BUFFER_MIN", "10")),
    "MIN_ADVANCE_BOOKING_HOURS": int(os.getenv("MIN_ADVANCE_BOOKING_HOURS", "1")),
    "MAX_ADVANCE_BOOKING_DAYS": int(os.getenv("MAX_ADVANCE_BOOKING_DAYS", "30")),
    "CLIENT_CANCEL_HOURS": int(os.getenv("CLIENT_CANCEL_HOURS", "2")),
    "REMINDER_HOURS": [24, 2],

    "API_PORT": int(os.getenv("PORT", "8080")),
    "RENDER_EXTERNAL_URL": os.getenv("RENDER_EXTERNAL_URL", ""),
    "API_URL": os.getenv("RENDER_EXTERNAL_URL", "") or os.getenv("API_URL", ""),
}

TZ = ZoneInfo("Asia/Tashkent")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ═══════════════════════════════════════
#  DATABASE
# ═══════════════════════════════════════

DATABASE_URL = os.getenv("DATABASE_URL", "")


@contextmanager
def get_db():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def _migrate_legacy(c):
    """Старые колонки/статусы."""
    for stmt in (
        "ALTER TABLE bookings ADD COLUMN IF NOT EXISTS end_time TEXT",
        "ALTER TABLE bookings ADD COLUMN IF NOT EXISTS total_price INTEGER DEFAULT 60000",
        "ALTER TABLE bookings ADD COLUMN IF NOT EXISTS admin_note TEXT",
    ):
        try:
            c.execute(stmt)
        except Exception:
            pass
    c.execute("UPDATE bookings SET status='confirmed' WHERE status IS NULL OR status = ''")


def init_db():
    with get_db() as c:
        c.execute(
            """
        CREATE TABLE IF NOT EXISTS users (
            id BIGINT PRIMARY KEY,
            username TEXT DEFAULT '',
            first_name TEXT DEFAULT '',
            phone TEXT DEFAULT '',
            created_at TIMESTAMPTZ DEFAULT NOW()
        )"""
        )
        c.execute(
            """
        CREATE TABLE IF NOT EXISTS services (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            price INTEGER NOT NULL,
            duration_min INTEGER NOT NULL,
            is_active BOOLEAN DEFAULT true
        )"""
        )
        c.execute(
            """
        CREATE TABLE IF NOT EXISTS work_schedule (
            day_of_week SMALLINT PRIMARY KEY,
            start_time TIME NOT NULL,
            end_time TIME NOT NULL,
            is_working BOOLEAN DEFAULT true
        )"""
        )
        c.execute(
            """
        CREATE TABLE IF NOT EXISTS day_overrides (
            id SERIAL PRIMARY KEY,
            date DATE UNIQUE NOT NULL,
            start_time TIME,
            end_time TIME,
            is_closed BOOLEAN DEFAULT false,
            reason VARCHAR(255)
        )"""
        )
        c.execute(
            """
        CREATE TABLE IF NOT EXISTS bookings (
            id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            book_date TEXT NOT NULL,
            book_time TEXT NOT NULL,
            end_time TEXT,
            name TEXT DEFAULT '',
            phone TEXT DEFAULT '',
            total_price INTEGER DEFAULT 0,
            status TEXT DEFAULT 'confirmed',
            admin_note TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )"""
        )
        c.execute(
            """
        CREATE TABLE IF NOT EXISTS booking_services (
            booking_id INTEGER REFERENCES bookings(id) ON DELETE CASCADE,
            service_id INTEGER REFERENCES services(id),
            PRIMARY KEY (booking_id, service_id)
        )"""
        )
        c.execute(
            """
        CREATE TABLE IF NOT EXISTS blocked_slots (
            id SERIAL PRIMARY KEY,
            block_date TEXT NOT NULL,
            block_time TEXT,
            reason TEXT DEFAULT ''
        )"""
        )
        c.execute(
            """
        CREATE TABLE IF NOT EXISTS notifications (
            id SERIAL PRIMARY KEY,
            booking_id INTEGER REFERENCES bookings(id) ON DELETE CASCADE,
            type VARCHAR(50) NOT NULL,
            scheduled_at TIMESTAMPTZ NOT NULL,
            sent_at TIMESTAMPTZ
        )"""
        )
        c.execute("CREATE INDEX IF NOT EXISTS idx_bd ON bookings(book_date)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_bu ON bookings(user_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_ns ON notifications(scheduled_at) WHERE sent_at IS NULL")

        _migrate_legacy(c)

        # Сиды услуг и недели (один раз)
        c.execute("SELECT COUNT(*) FROM services")
        if c.fetchone()[0] == 0:
            seeds = [
                ("Стрижка взрослая", 60000, 60),
                ("Стрижка детская", 50000, 60),
                ("Борода", 20000, 20),
                ("Окантовка", 30000, 20),
                ("Депиляция лица воском", 20000, 20),
                ("Окрашивание волос", 15000, 15),
            ]
            for name, price, dur in seeds:
                c.execute(
                    "INSERT INTO services(name, price, duration_min) VALUES (%s,%s,%s)",
                    (name, price, dur),
                )

        c.execute("SELECT COUNT(*) FROM work_schedule")
        if c.fetchone()[0] == 0:
            # пн–сб 11:00–20:00, вс выходной
            for dow in range(7):
                if dow == 6:
                    c.execute(
                        "INSERT INTO work_schedule(day_of_week, start_time, end_time, is_working) "
                        "VALUES (%s, TIME '11:00', TIME '20:00', false)",
                        (dow,),
                    )
                else:
                    c.execute(
                        "INSERT INTO work_schedule(day_of_week, start_time, end_time, is_working) "
                        "VALUES (%s, TIME '11:00', TIME '20:00', true)",
                        (dow,),
                    )

        # Бэкфилл end_time/total_price для старых записей без услуг
        c.execute(
            """
            UPDATE bookings SET
              end_time = to_char(
                (to_timestamp(book_date || ' ' || book_time, 'YYYY-MM-DD HH24:MI')
                  + interval '60 minutes')::time,
                'HH24:MI'
              ),
              total_price = COALESCE(NULLIF(total_price, 0), 60000)
            WHERE end_time IS NULL AND status = 'confirmed'
            """
        )
        c.execute(
            """
            INSERT INTO booking_services (booking_id, service_id)
            SELECT b.id, (SELECT id FROM services WHERE name = 'Стрижка взрослая' LIMIT 1)
            FROM bookings b
            WHERE NOT EXISTS (SELECT 1 FROM booking_services bs WHERE bs.booking_id = b.id)
              AND b.status = 'confirmed'
            ON CONFLICT DO NOTHING
            """
        )


# ═══════════════════════════════════════
#  TIME / SLOTS
# ═══════════════════════════════════════

def _time_to_min(t: str) -> int:
    hh, mm = [int(x) for x in t.split(":")[:2]]
    return hh * 60 + mm


def _min_to_time(m: int) -> str:
    return f"{m // 60:02d}:{m % 60:02d}"


def _parse_local_datetime(ds: str, tm: str) -> datetime:
    return datetime.strptime(f"{ds} {tm}", "%Y-%m-%d %H:%M").replace(tzinfo=TZ)


def calc_duration_minutes(service_ids: List[int]) -> Tuple[int, int]:
    """Сумма длительностей и цены по id услуг."""
    if not service_ids:
        return 0, 0
    with get_db() as c:
        c.execute(
            "SELECT id, duration_min, price FROM services WHERE id = ANY(%s) AND is_active = true",
            (service_ids,),
        )
        rows = c.fetchall()
    by_id = {r["id"]: (r["duration_min"], r["price"]) for r in rows}
    dur = sum(by_id[i][0] for i in service_ids if i in by_id)
    price = sum(by_id[i][1] for i in service_ids if i in by_id)
    return dur, price


def booking_block_minutes(service_ids: List[int]) -> int:
    dur, _ = calc_duration_minutes(service_ids)
    return dur + CONFIG["BUFFER_MIN"]


def get_work_bounds(d: date) -> Optional[Tuple[int, int]]:
    """Начало/конец дня в минутах от полуночи локально; None = выходной."""
    ds = d.isoformat()
    with get_db() as c:
        c.execute("SELECT start_time, end_time, is_closed FROM day_overrides WHERE date = %s", (ds,))
        ovr = c.fetchone()
        if ovr and ovr["is_closed"]:
            return None
        if ovr and ovr["start_time"] and ovr["end_time"]:
            sh = ovr["start_time"].hour * 60 + ovr["start_time"].minute
            eh = ovr["end_time"].hour * 60 + ovr["end_time"].minute
            return sh, eh

        dow = d.weekday()  # пн=0
        c.execute(
            "SELECT start_time, end_time, is_working FROM work_schedule WHERE day_of_week = %s",
            (dow,),
        )
        ws = c.fetchone()
        if not ws or not ws["is_working"]:
            return None
        sh = ws["start_time"].hour * 60 + ws["start_time"].minute
        eh = ws["end_time"].hour * 60 + ws["end_time"].minute
        return sh, eh


def _blocked_set(date_str: str):
    with get_db() as c:
        c.execute("SELECT block_time FROM blocked_slots WHERE block_date=%s", (date_str,))
        rows = c.fetchall()
    s = set()
    for r in rows:
        if r[0]:
            s.add(r[0])
        else:
            return {"ALL"}
    return s


def _confirmed_intervals(c, date_str: str, exclude_booking_id: Optional[int] = None) -> List[Tuple[int, int]]:
    c.execute(
        """
        SELECT id, book_time, end_time FROM bookings
        WHERE book_date = %s AND status = 'confirmed'
        """,
        (date_str,),
    )
    out = []
    for r in c.fetchall():
        if exclude_booking_id is not None and r["id"] == exclude_booking_id:
            continue
        try:
            sm = _time_to_min(r["book_time"])
            em = _time_to_min(r["end_time"]) if r["end_time"] else sm + 60
            out.append((sm, em))
        except Exception:
            continue
    return out


def _intervals_overlap(a0: int, a1: int, b0: int, b1: int) -> bool:
    return a0 < b1 and b0 < a1


def _slot_free(
    start_m: int,
    block_m: int,
    intervals: List[Tuple[int, int]],
    bl: set,
    date_str: str,
) -> bool:
    end_m = start_m + block_m
    if "ALL" in bl:
        return False
    step = CONFIG["SLOT_STEP_MIN"]
    for tslot in range(start_m, end_m, step):
        tlabel = _min_to_time(tslot)
        if tlabel in bl:
            return False
    for sm, em in intervals:
        if _intervals_overlap(start_m, end_m, sm, em):
            return False
    return True


def day_slots(
    date_str: str,
    service_ids: List[int],
    exclude_booking_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    if not service_ids:
        return []

    d = datetime.strptime(date_str, "%Y-%m-%d").date()
    today_tz = datetime.now(TZ).date()
    if d < today_tz or d > today_tz + timedelta(days=CONFIG["MAX_ADVANCE_BOOKING_DAYS"]):
        return []

    wb = get_work_bounds(d)
    if wb is None:
        return []

    block_m = booking_block_minutes(service_ids)
    if block_m <= 0:
        return []

    step = max(1, CONFIG["SLOT_STEP_MIN"])
    start_min, end_min = wb
    latest_start = end_min - block_m

    now_tz = datetime.now(TZ)
    min_book_time = now_tz + timedelta(hours=CONFIG["MIN_ADVANCE_BOOKING_HOURS"])

    bl = _blocked_set(date_str)

    with get_db() as c:
        intervals = _confirmed_intervals(c, date_str, exclude_booking_id=exclude_booking_id)

    out = []
    for m in range(start_min, latest_start + 1, step):
        t = _min_to_time(m)
        try:
            slot_dt = _parse_local_datetime(date_str, t)
        except Exception:
            continue
        if slot_dt < min_book_time:
            st = "past"
        elif t in bl and "ALL" not in bl:
            st = "blocked"
        elif not _slot_free(m, block_m, intervals, bl, date_str):
            st = "booked"
        else:
            st = "free"
        end_t = _min_to_time(m + block_m)
        out.append({"time": t, "end": end_t, "status": st})

    return out


def calendar_data(
    service_ids: List[int], exclude_booking_id: Optional[int] = None
) -> List[Dict[str, Any]]:
    today = datetime.now(TZ).date()
    days = []
    for i in range(CONFIG["MAX_ADVANCE_BOOKING_DAYS"] + 1):
        d = today + timedelta(days=i)
        ds = d.isoformat()
        slots = day_slots(ds, service_ids, exclude_booking_id=exclude_booking_id)
        if not slots:
            off = get_work_bounds(d) is None
            days.append({"date": ds, "weekday": d.weekday(), "free": 0, "off": off})
        else:
            free = sum(1 for s in slots if s["status"] == "free")
            days.append({"date": ds, "weekday": d.weekday(), "free": free, "off": False})
    return days


def make_booking(
    uid: int,
    bdate: str,
    btime: str,
    name: str,
    phone: str,
    service_ids: List[int],
    exclude_booking_id: Optional[int] = None,
) -> Optional[int]:
    if not service_ids:
        return None
    block_m = booking_block_minutes(service_ids)
    _, total_price = calc_duration_minutes(service_ids)
    try:
        sm = _time_to_min(btime)
    except Exception:
        return None
    em = sm + block_m
    end_time = _min_to_time(em)

    d = datetime.strptime(bdate, "%Y-%m-%d").date()
    wb = get_work_bounds(d)
    if wb is None:
        return None
    start_min, end_min = wb
    if sm < start_min or sm + block_m > end_min:
        return None
    step = max(1, CONFIG["SLOT_STEP_MIN"])
    if (sm - start_min) % step != 0:
        return None

    slot_dt = _parse_local_datetime(bdate, btime)
    if slot_dt < datetime.now(TZ) + timedelta(hours=CONFIG["MIN_ADVANCE_BOOKING_HOURS"]):
        return None

    bl = _blocked_set(bdate)
    if "ALL" in bl or btime in bl:
        return None

    with get_db() as c:
        c.execute("SELECT pg_advisory_xact_lock(hashtext(%s::text))", (bdate,))
        c.execute(
            "SELECT id FROM bookings WHERE book_date=%s AND status='confirmed' FOR UPDATE",
            (bdate,),
        )
        c.fetchall()

        intervals = _confirmed_intervals(c, bdate, exclude_booking_id=exclude_booking_id)
        if not _slot_free(sm, block_m, intervals, bl, bdate):
            return None

        c.execute(
            """
            INSERT INTO bookings(user_id, book_date, book_time, end_time, name, phone, total_price, status)
            VALUES (%s,%s,%s,%s,%s,%s,%s,'confirmed') RETURNING id
            """,
            (uid, bdate, btime, end_time, name, phone, total_price),
        )
        bid = c.fetchone()[0]
        for sid in service_ids:
            c.execute(
                "INSERT INTO booking_services(booking_id, service_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                (bid, sid),
            )

    schedule_reminders(bid)
    return bid


def get_booking(bid: int) -> Optional[Dict[str, Any]]:
    with get_db() as c:
        c.execute("SELECT * FROM bookings WHERE id=%s", (bid,))
        r = c.fetchone()
        return dict(r) if r else None


def booking_service_ids(bid: int) -> List[int]:
    with get_db() as c:
        c.execute(
            "SELECT service_id FROM booking_services WHERE booking_id=%s ORDER BY service_id",
            (bid,),
        )
        return [row[0] for row in c.fetchall()]


def user_bookings(uid: int) -> List[Dict[str, Any]]:
    today = datetime.now(TZ).date().isoformat()
    with get_db() as c:
        c.execute(
            """
            SELECT id, user_id, book_date, book_time, end_time, name, phone, status,
              total_price, created_at::text
            FROM bookings
            WHERE user_id=%s AND status='confirmed' AND book_date >= %s
            ORDER BY book_date, book_time
            """,
            (uid, today),
        )
        rows = [dict(x) for x in c.fetchall()]
    for b in rows:
        b["service_ids"] = booking_service_ids(b["id"])
    return rows


def cancel_booking(bid: int, by_admin: bool = False):
    st = "cancelled_by_admin" if by_admin else "cancelled_by_client"
    with get_db() as c:
        c.execute(
            "UPDATE bookings SET status=%s WHERE id=%s AND status='confirmed'", (st, bid)
        )
        c.execute("DELETE FROM notifications WHERE booking_id=%s AND sent_at IS NULL", (bid,))


def can_modify(b: Dict[str, Any]) -> bool:
    dt = _parse_local_datetime(b["book_date"], b["book_time"])
    return (dt - datetime.now(TZ)).total_seconds() / 3600 > CONFIG["CLIENT_CANCEL_HOURS"]


def all_bookings_date(ds: str) -> List[Dict[str, Any]]:
    with get_db() as c:
        c.execute(
            """
            SELECT b.id, b.user_id, b.book_date, b.book_time, b.end_time, b.name, b.phone,
              b.status, b.total_price, b.created_at::text, u.username, u.first_name AS ufn
            FROM bookings b
            LEFT JOIN users u ON u.id=b.user_id
            WHERE b.book_date=%s AND b.status='confirmed'
            ORDER BY b.book_time
            """,
            (ds,),
        )
        rows = [dict(x) for x in c.fetchall()]
    for b in rows:
        b["service_ids"] = booking_service_ids(b["id"])
        b["services_line"] = _services_human(b["service_ids"])
    return rows


def _services_human(ids: List[int]) -> str:
    if not ids:
        return "Стрижка"
    with get_db() as c:
        c.execute("SELECT id, name FROM services WHERE id = ANY(%s)", (ids,))
        names = {r["id"]: r["name"] for r in c.fetchall()}
    return ", ".join(names.get(i, "?") for i in ids)


def block_slot(bdate: str, btime: Optional[str] = None, reason: str = ""):
    with get_db() as c:
        c.execute(
            "INSERT INTO blocked_slots(block_date,block_time,reason) VALUES(%s,%s,%s)",
            (bdate, btime, reason),
        )


def unblock_slot(bdate: str, btime: Optional[str] = None):
    with get_db() as c:
        if btime:
            c.execute(
                "DELETE FROM blocked_slots WHERE block_date=%s AND block_time=%s",
                (bdate, btime),
            )
        else:
            c.execute(
                "DELETE FROM blocked_slots WHERE block_date=%s AND block_time IS NULL",
                (bdate,),
            )


def list_services() -> List[Dict[str, Any]]:
    with get_db() as c:
        c.execute(
            "SELECT id, name, price, duration_min FROM services WHERE is_active = true ORDER BY id"
        )
        return [dict(r) for r in c.fetchall()]


def get_stats() -> Dict[str, Any]:
    tz_today = datetime.now(TZ).date()
    t = tz_today.isoformat()
    tm = (tz_today + timedelta(days=1)).isoformat()
    week_start = tz_today - timedelta(days=tz_today.weekday())

    with get_db() as c:
        c.execute(
            "SELECT COUNT(*) FROM bookings WHERE book_date=%s AND status='confirmed'", (t,)
        )
        today_n = c.fetchone()[0]
        c.execute(
            "SELECT COUNT(*) FROM bookings WHERE book_date=%s AND status='confirmed'", (tm,)
        )
        tomorrow_n = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM bookings WHERE status='confirmed'")
        total = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM users")
        clients = c.fetchone()[0]

        c.execute(
            """
            SELECT COALESCE(SUM(total_price),0) FROM bookings
            WHERE book_date = %s AND status = 'confirmed'
            """,
            (t,),
        )
        rev_day = c.fetchone()[0]
        c.execute(
            """
            SELECT COALESCE(SUM(total_price),0) FROM bookings
            WHERE book_date >= %s AND book_date <= %s AND status = 'confirmed'
            """,
            (week_start.isoformat(), (week_start + timedelta(days=6)).isoformat()),
        )
        rev_week = c.fetchone()[0]
        month_start = date(tz_today.year, tz_today.month, 1)
        if tz_today.month == 12:
            month_end = date(tz_today.year + 1, 1, 1) - timedelta(days=1)
        else:
            month_end = date(tz_today.year, tz_today.month + 1, 1) - timedelta(days=1)
        c.execute(
            """
            SELECT COALESCE(SUM(total_price),0) FROM bookings
            WHERE book_date >= %s AND book_date <= %s AND status = 'confirmed'
            """,
            (month_start.isoformat(), month_end.isoformat()),
        )
        rev_month = c.fetchone()[0]

        c.execute(
            """
            SELECT s.name, COUNT(*) AS cnt
            FROM booking_services bs
            JOIN services s ON s.id = bs.service_id
            JOIN bookings b ON b.id = bs.booking_id AND b.status = 'confirmed'
            GROUP BY s.name
            ORDER BY cnt DESC
            LIMIT 5
            """
        )
        top = [{"name": r[0], "count": r[1]} for r in c.fetchall()]

        return {
            "today": today_n,
            "tomorrow": tomorrow_n,
            "total": total,
            "clients": clients,
            "revenue_day": int(rev_day),
            "revenue_week": int(rev_week),
            "revenue_month": int(rev_month),
            "top_services": top,
        }


RU_WD = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
RU_MO = ["", "янв", "фев", "мар", "апр", "май", "июн", "июл", "авг", "сен", "окт", "ноя", "дек"]


def schedule_reminders(booking_id: int):
    b = get_booking(booking_id)
    if not b or b["status"] != "confirmed":
        return
    try:
        appt = _parse_local_datetime(b["book_date"], b["book_time"])
    except Exception:
        return
    with get_db() as c:
        for h in CONFIG["REMINDER_HOURS"]:
            when = appt - timedelta(hours=h)
            if when > datetime.now(TZ):
                typ = f"reminder_{h}h"
                c.execute(
                    """
                    INSERT INTO notifications(booking_id, type, scheduled_at)
                    VALUES (%s, %s, %s)
                    """,
                    (booking_id, typ, when.astimezone(timezone.utc)),
                )


async def process_due_notifications():
    """Отправка напоминаний."""
    now_utc = datetime.now(timezone.utc)
    with get_db() as c:
        c.execute(
            """
            SELECT n.id, n.booking_id, n.type, b.user_id, b.book_date, b.book_time, b.name
            FROM notifications n
            JOIN bookings b ON b.id = n.booking_id
            WHERE n.sent_at IS NULL AND n.scheduled_at <= %s AND b.status = 'confirmed'
            FOR UPDATE SKIP LOCKED
            """,
            (now_utc,),
        )
        rows = c.fetchall()
        for r in rows:
            try:
                await _send_reminder_message(dict(r))
                c.execute(
                    "UPDATE notifications SET sent_at = %s WHERE id = %s",
                    (now_utc, r["id"]),
                )
            except Exception as e:
                log.warning("reminder send %s: %s", r["id"], e)


async def _send_reminder_message(row: Dict[str, Any]):
    uid = row["user_id"]
    d = datetime.strptime(row["book_date"], "%Y-%m-%d").date()
    t = row["book_time"]
    typ = row["type"]
    extra = ""
    if typ == "reminder_2h" and can_modify(
        {"book_date": row["book_date"], "book_time": row["book_time"]}
    ):
        extra = f"\n\nЕсли нужно отменить — успейте до начала более {CONFIG['CLIENT_CANCEL_HOURS']} ч (через мини‑приложение «Мои записи»)."
    text = (
        f"⏰ <b>Напоминание</b>\n\n"
        f"{row['name'] or 'Клиент'}, у вас запись:\n"
        f"📅 {d.day} {RU_MO[d.month]} ({RU_WD[d.weekday()]}) в {t}\n"
        f"{extra}"
    )
    try:
        await bot.send_message(uid, text)
    except Exception as e:
        log.warning("send reminder to %s: %s", uid, e)


# ═══════════════════════════════════════
#  AUTH
# ═══════════════════════════════════════

def validate_init_data(init_data_str, detail=False):
    if not init_data_str:
        return (None, "missing_init_data") if detail else None
    try:
        vals = dict(urllib.parse.parse_qsl(init_data_str, keep_blank_values=True))
    except Exception:
        return (None, "bad_parse") if detail else None
    h = vals.pop("hash", "")
    if not h:
        return (None, "no_hash") if detail else None
    check = "\n".join(f"{k}={v}" for k, v in sorted(vals.items()))
    secret = hmac.new(b"WebAppData", CONFIG["BOT_TOKEN"].encode(), "sha256").digest()
    if hmac.new(secret, check.encode(), "sha256").hexdigest() != h:
        return (None, "bad_hash") if detail else None
    try:
        user = json.loads(vals.get("user", "{}"))
        return (user, None) if detail else user
    except Exception:
        return (None, "bad_user_json") if detail else None


async def get_user_from_request(request):
    init_data = request.headers.get("X-Init-Data", "").strip()
    if not init_data:
        try:
            body = await request.json()
            init_data = (body or {}).get("initData", "").strip()
        except Exception:
            init_data = ""
    if not init_data:
        return None
    return validate_init_data(init_data)


def require_admin(user):
    return user and user.get("id") in CONFIG["ADMIN_IDS"]


def _parse_service_ids(req, body: Optional[dict]) -> List[int]:
    raw = None
    if body:
        raw = body.get("service_ids")
    if raw is None:
        return []
    if isinstance(raw, str):
        raw = [int(x) for x in raw.split(",") if x.strip().isdigit()]
    return [int(x) for x in raw]


async def notify_new_booking(bid: int):
    b = get_booking(bid)
    if not b:
        return
    d = datetime.strptime(b["book_date"], "%Y-%m-%d").date()
    line = _services_human(booking_service_ids(bid))
    text = (
        f"✂️ <b>Новая запись #{b['id']}</b>\n\n"
        f"👤 {b['name'] or '—'}\n"
        f"📱 {b['phone'] or '—'}\n"
        f"📅 {d.day} {RU_MO[d.month]} ({RU_WD[d.weekday()]})\n"
        f"⏰ {b['book_time']}–{b.get('end_time') or '?'}\n"
        f"💇 {line}\n"
        f"💰 {b.get('total_price', 0):,} сум"
    )
    if CONFIG["CHANNEL_ID"]:
        try:
            await bot.send_message(CONFIG["CHANNEL_ID"], text)
        except Exception as e:
            log.warning("notify channel: %s", e)
    try:
        client_t = (
            f"✅ <b>Вы записаны!</b>\n\n"
            f"📅 {d.day} {RU_MO[d.month]} ({RU_WD[d.weekday()]})\n"
            f"⏰ {b['book_time']}–{b.get('end_time') or '?'}\n"
            f"💇 {line}\n"
            f"💰 {b.get('total_price', 0):,} сум\n\n"
            f"Отменить можно в «Мои записи», если до визита &gt; {CONFIG['CLIENT_CANCEL_HOURS']} ч."
        )
        await bot.send_message(b["user_id"], client_t)
    except Exception as e:
        log.warning("notify client: %s", e)


async def notify_cancel(b, to_channel: bool = True):
    d = datetime.strptime(b["book_date"], "%Y-%m-%d").date()
    text = (
        f"❌ <b>Запись #{b['id']} отменена</b>\n\n"
        f"👤 {b['name'] or '—'}\n"
        f"📅 {d.day} {RU_MO[d.month]} ({RU_WD[d.weekday()]}) {b['book_time']}"
    )
    if to_channel and CONFIG["CHANNEL_ID"]:
        try:
            await bot.send_message(CONFIG["CHANNEL_ID"], text)
        except Exception as e:
            log.warning("notify channel cancel: %s", e)
    try:
        await bot.send_message(
            b["user_id"], f"❌ Запись #{b['id']} отменена.\n{d.day} {RU_MO[d.month]}, {b['book_time']}"
        )
    except Exception:
        pass


async def notify_reschedule(old: Dict, new_bid: int):
    nb = get_booking(new_bid)
    if not nb:
        return
    od = datetime.strptime(old["book_date"], "%Y-%m-%d").date()
    nd = datetime.strptime(nb["book_date"], "%Y-%m-%d").date()
    text = (
        f"🔄 <b>Запись перенесена</b>\n\n"
        f"👤 {old['name'] or '—'}\n"
        f"📱 {old['phone'] or '—'}\n"
        f"❌ Было: {od.day} {RU_MO[od.month]} ({RU_WD[od.weekday()]}) {old['book_time']}\n"
        f"✅ Стало: {nd.day} {RU_MO[nd.month]} ({RU_WD[nd.weekday()]}) {nb['book_time']}"
    )
    if CONFIG["CHANNEL_ID"]:
        try:
            await bot.send_message(CONFIG["CHANNEL_ID"], text)
        except Exception as e:
            log.warning("notify reschedule channel: %s", e)
    try:
        await bot.send_message(
            old["user_id"],
            (
                f"🔄 Запись перенесена на {nd.day} {RU_MO[nd.month]} в {nb['book_time']}.\n"
                f"Если неудобно — отмените в «Мои записи» (пока &gt; {CONFIG['CLIENT_CANCEL_HOURS']} ч до визита)."
            ),
        )
    except Exception:
        pass


# ═══════════════════════════════════════
#  AIOHTTP API
# ═══════════════════════════════════════

@web.middleware
async def cors_mw(request, handler):
    if request.method == "OPTIONS":
        return web.Response(
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type,X-Init-Data",
            }
        )
    resp = await handler(request)
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type,X-Init-Data"
    return resp


async def api_health(req):
    return web.json_response({"status": "ok"})


async def api_auth_check(req):
    raw = req.headers.get("X-Init-Data", "")
    user, err = validate_init_data(raw, detail=True)
    if user:
        return web.json_response({"ok": True, "user_id": user.get("id"), "name": user.get("first_name", "")})
    return web.json_response({"ok": False, "error": err, "init_data_len": len(raw)})


async def api_services(req):
    return web.json_response({"ok": True, "services": list_services()})


async def api_config(req):
    ws_note = "Пн–Сб, 11:00–20:00 · вс выходной"
    return web.json_response(
        {
            "ok": True,
            "barber": CONFIG["BARBER_NAME"],
            "year": CONFIG["BARBER_YEAR"],
            "exp": CONFIG["BARBER_EXP"],
            "phone": CONFIG["BARBER_PHONE"],
            "timezone": "Asia/Tashkent",
            "slot_step_min": CONFIG["SLOT_STEP_MIN"],
            "buffer_min": CONFIG["BUFFER_MIN"],
            "min_advance_h": CONFIG["MIN_ADVANCE_BOOKING_HOURS"],
            "max_advance_days": CONFIG["MAX_ADVANCE_BOOKING_DAYS"],
            "client_cancel_h": CONFIG["CLIENT_CANCEL_HOURS"],
            "schedule_note": ws_note,
            "services": list_services(),
        }
    )


def _svc_from_query(req) -> List[int]:
    q = req.query.get("service_ids", "")
    if not q:
        return []
    return [int(x) for x in q.split(",") if x.strip().isdigit()]


def _exclude_bid_from_query(req) -> Optional[int]:
    q = req.query.get("exclude_booking_id", "")
    if q.isdigit():
        return int(q)
    return None


async def api_calendar(req):
    ids = _svc_from_query(req)
    ex = _exclude_bid_from_query(req)
    return web.json_response({"ok": True, "days": calendar_data(ids, exclude_booking_id=ex)})


async def api_schedule(req):
    ds = req.query.get("date", datetime.now(TZ).date().isoformat())
    ids = _svc_from_query(req)
    ex = _exclude_bid_from_query(req)
    return web.json_response({"ok": True, "date": ds, "slots": day_slots(ds, ids, exclude_booking_id=ex)})


async def api_book(req):
    user = await get_user_from_request(req)
    if not user:
        return web.json_response({"ok": False, "error": "missing_init_data"}, status=401)
    body = await req.json()
    uid = user["id"]
    service_ids = body.get("service_ids") or []
    if isinstance(service_ids, str):
        service_ids = [int(x) for x in service_ids.split(",") if x.strip().isdigit()]
    else:
        service_ids = [int(x) for x in service_ids]
    ensure_user(uid, user.get("username", ""), user.get("first_name", ""))
    bid = make_booking(
        uid,
        body["date"],
        body["time"],
        body.get("name", ""),
        body.get("phone", ""),
        service_ids,
    )
    if not bid:
        return web.json_response(
            {"ok": False, "error": "Слот только что заняли или недоступен — выберите другое время"}
        )
    asyncio.create_task(notify_new_booking(bid))
    return web.json_response({"ok": True, "booking_id": bid})


async def api_my_bookings(req):
    user = await get_user_from_request(req)
    if not user:
        return web.json_response({"ok": False, "error": "missing_init_data", "bookings": []}, status=401)
    bks = user_bookings(user["id"])
    for b in bks:
        b["can_cancel"] = can_modify(b)
        b["services_line"] = _services_human(b.get("service_ids") or [])
    return web.json_response({"ok": True, "bookings": bks})


async def api_last_services(req):
    user = await get_user_from_request(req)
    if not user:
        return web.json_response({"ok": False, "error": "missing_init_data"}, status=401)
    with get_db() as c:
        c.execute(
            """
            SELECT b.id FROM bookings b
            WHERE b.user_id = %s
            ORDER BY b.created_at DESC
            LIMIT 1
            """,
            (user["id"],),
        )
        row = c.fetchone()
        if not row:
            return web.json_response({"ok": True, "service_ids": []})
        last_bid = row[0]
        c.execute(
            "SELECT service_id FROM booking_services WHERE booking_id = %s ORDER BY service_id",
            (last_bid,),
        )
        ids = [r[0] for r in c.fetchall()]
    return web.json_response({"ok": True, "service_ids": ids})


async def api_cancel(req):
    user = await get_user_from_request(req)
    if not user:
        return web.json_response({"ok": False, "error": "missing_init_data"}, status=401)
    body = await req.json()
    b = get_booking(body["booking_id"])
    if not b or b["user_id"] != user["id"]:
        return web.json_response({"ok": False, "error": "Запись не найдена"})
    if not can_modify(b):
        return web.json_response(
            {
                "ok": False,
                "error": f"До записи менее {CONFIG['CLIENT_CANCEL_HOURS']} ч — отменить можно только у мастера.",
                "phone": CONFIG["BARBER_PHONE"],
            }
        )
    cancel_booking(b["id"], by_admin=False)
    asyncio.create_task(notify_cancel(b))
    return web.json_response({"ok": True})


async def api_reschedule(req):
    user = await get_user_from_request(req)
    if not user:
        return web.json_response({"ok": False, "error": "missing_init_data"}, status=401)
    body = await req.json()
    old = get_booking(body["booking_id"])
    if not old or old["user_id"] != user["id"]:
        return web.json_response({"ok": False, "error": "Запись не найдена"})
    if not can_modify(old):
        return web.json_response(
            {
                "ok": False,
                "error": f"До записи менее {CONFIG['CLIENT_CANCEL_HOURS']} ч — перенос через мастера.",
                "phone": CONFIG["BARBER_PHONE"],
            }
        )
    sids = body.get("service_ids") or booking_service_ids(old["id"])
    if isinstance(sids, str):
        sids = [int(x) for x in sids.split(",") if x.strip().isdigit()]
    else:
        sids = [int(x) for x in sids]
    if not sids:
        sids = booking_service_ids(old["id"])

    new_bid = make_booking(
        user["id"],
        body["new_date"],
        body["new_time"],
        old["name"],
        old["phone"],
        sids,
        exclude_booking_id=old["id"],
    )
    if not new_bid:
        return web.json_response(
            {"ok": False, "error": "Новое время недоступно — выберите другое"}
        )
    cancel_booking(old["id"], by_admin=False)
    asyncio.create_task(notify_reschedule(old, new_bid))
    return web.json_response({"ok": True, "new_booking_id": new_bid})


async def api_admin_bookings(req):
    user = await get_user_from_request(req)
    if not user:
        return web.json_response({"ok": False, "error": "missing_init_data"}, status=401)
    if not require_admin(user):
        return web.json_response({"ok": False, "error": "not_admin"}, status=403)
    ds = req.query.get("date", datetime.now(TZ).date().isoformat())
    # Сетка слотов для админа — макс. длина визита (все услуги + буфер) как ориентир не нужен;
    # показываем слоты для «типовой» стрижки 60 мин + буфер
    demo_services = []
    with get_db() as c:
        c.execute("SELECT id FROM services WHERE name = 'Стрижка взрослая' LIMIT 1")
        r = c.fetchone()
        if r:
            demo_services = [r[0]]
    return web.json_response(
        {
            "ok": True,
            "bookings": all_bookings_date(ds),
            "slots": day_slots(ds, demo_services),
        }
    )


async def api_admin_cancel(req):
    user = await get_user_from_request(req)
    if not user:
        return web.json_response({"ok": False, "error": "missing_init_data"}, status=401)
    if not require_admin(user):
        return web.json_response({"ok": False, "error": "not_admin"}, status=403)
    body = await req.json()
    b = get_booking(body["booking_id"])
    if b:
        cancel_booking(b["id"], by_admin=True)
        asyncio.create_task(notify_cancel(b, to_channel=True))
    return web.json_response({"ok": True})


async def api_admin_block(req):
    user = await get_user_from_request(req)
    if not user:
        return web.json_response({"ok": False, "error": "missing_init_data"}, status=401)
    if not require_admin(user):
        return web.json_response({"ok": False, "error": "not_admin"}, status=403)
    body = await req.json()
    block_slot(body["date"], body.get("time"), body.get("reason", ""))
    return web.json_response({"ok": True})


async def api_admin_unblock(req):
    user = await get_user_from_request(req)
    if not user:
        return web.json_response({"ok": False, "error": "missing_init_data"}, status=401)
    if not require_admin(user):
        return web.json_response({"ok": False, "error": "not_admin"}, status=403)
    body = await req.json()
    unblock_slot(body["date"], body.get("time"))
    return web.json_response({"ok": True})


async def api_admin_stats(req):
    user = await get_user_from_request(req)
    if not user:
        return web.json_response({"ok": False, "error": "missing_init_data"}, status=401)
    if not require_admin(user):
        return web.json_response({"ok": False, "error": "not_admin"}, status=403)
    st = get_stats()
    return web.json_response({"ok": True, **st})


def ensure_user(uid, uname="", fname=""):
    with get_db() as c:
        c.execute(
            "INSERT INTO users(id,username,first_name) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING",
            (uid, uname, fname),
        )


def setup_routes(app):
    app.router.add_get("/", api_health)
    app.router.add_get("/health", api_health)
    app.router.add_get("/api/auth-check", api_auth_check)
    app.router.add_get("/api/config", api_config)
    app.router.add_get("/api/services", api_services)
    app.router.add_get("/api/calendar", api_calendar)
    app.router.add_get("/api/schedule", api_schedule)
    app.router.add_post("/api/book", api_book)
    app.router.add_get("/api/my-bookings", api_my_bookings)
    app.router.add_post("/api/my-bookings", api_my_bookings)
    app.router.add_get("/api/last-services", api_last_services)
    app.router.add_post("/api/last-services", api_last_services)
    app.router.add_post("/api/cancel", api_cancel)
    app.router.add_post("/api/reschedule", api_reschedule)
    app.router.add_get("/api/admin/bookings", api_admin_bookings)
    app.router.add_post("/api/admin/cancel", api_admin_cancel)
    app.router.add_post("/api/admin/block", api_admin_block)
    app.router.add_post("/api/admin/unblock", api_admin_unblock)
    app.router.add_get("/api/admin/stats", api_admin_stats)
    webapp_dir = Path(__file__).parent / "webapp"
    if webapp_dir.exists():
        app.router.add_get("/webapp", lambda r: web.HTTPFound("/webapp/"))
        app.router.add_static("/webapp/", path=str(webapp_dir), show_index=True)


# ═══════════════════════════════════════
#  BOT
# ═══════════════════════════════════════

bot = Bot(token=CONFIG["BOT_TOKEN"], default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher(storage=MemoryStorage())
scheduler = AsyncIOScheduler()


def _public_base_url():
    base = CONFIG.get("API_URL") or CONFIG.get("RENDER_EXTERNAL_URL")
    if base:
        return str(base).rstrip("/")
    return f"http://localhost:{CONFIG['API_PORT']}"


def client_url():
    return f"{_public_base_url()}/webapp/index.html"


def admin_url():
    return f"{_public_base_url()}/webapp/admin.html"


def client_inline_kb():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Записаться",
                    web_app=WebAppInfo(url=client_url()),
                )
            ],
            [
                InlineKeyboardButton(
                    text="Мои записи",
                    web_app=WebAppInfo(url=client_url() + "?screen=bookings"),
                )
            ],
        ]
    )


def my_bookings_inline_kb():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Мои записи",
                    web_app=WebAppInfo(url=client_url() + "?screen=bookings"),
                )
            ],
        ]
    )


def admin_web_kb():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Панель управления",
                    web_app=WebAppInfo(url=admin_url()),
                )
            ],
        ]
    )


def main_kb():
    buttons = [[KeyboardButton(text="Мои записи"), KeyboardButton(text="О мастере")]]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


@dp.message(Command("start"))
@dp.message(CommandStart())
async def cmd_start(msg: Message):
    ensure_user(msg.from_user.id, msg.from_user.username or "", msg.from_user.first_name or "")
    name = msg.from_user.first_name or "друг"
    svc = list_services()
    price_line = ", ".join(f"{s['name'].split()[0]}… {s['price']//1000}k" for s in svc[:2])
    text = (
        f"<b>ДИЛЬШОД</b> · Барбер\n\n"
        f"Привет, {name}!\n\n"
        f"Выберите услуги в мини‑приложении (несколько можно).\n"
        f"Например: {price_line}…\n"
        f"Таймзона: Asia/Tashkent · отмена клиентом за &gt; {CONFIG['CLIENT_CANCEL_HOURS']} ч до визита.\n\n"
        f"Нажмите кнопку ниже 👇"
    )
    await msg.answer(text, reply_markup=client_inline_kb())
    await msg.answer("Меню:", reply_markup=main_kb())
    if msg.from_user.id in CONFIG["ADMIN_IDS"]:
        await msg.answer("👑 Админ: панель расписания:", reply_markup=admin_web_kb())


@dp.message(F.text == "О мастере")
async def cmd_about(msg: Message):
    age = date.today().year - CONFIG["BARBER_YEAR"]
    text = (
        f"<b>ДИЛЬШОД</b>\n\n"
        f"Год рождения: {CONFIG['BARBER_YEAR']} ({age} лет)\n"
        f"Опыт: {CONFIG['BARBER_EXP']}\n"
        f"Режим: Пн–Сб, 11:00–20:00\n"
        f"Телефон: {CONFIG['BARBER_PHONE']}"
    )
    await msg.answer(text)


@dp.message(F.text == "Мои записи")
async def cmd_my(msg: Message):
    await msg.answer("Откройте мини‑приложение «Мои записи».", reply_markup=my_bookings_inline_kb())


@dp.message(Command("admin"))
async def cmd_admin(msg: Message):
    if msg.from_user.id not in CONFIG["ADMIN_IDS"]:
        await msg.answer("Нет доступа")
        return
    await msg.answer("Админ панель:", reply_markup=admin_web_kb())


async def main():
    if not DATABASE_URL:
        log.error("DATABASE_URL не задан!")
        raise SystemExit(1)

    init_db()

    if not CONFIG["API_URL"]:
        CONFIG["API_URL"] = f"http://localhost:{CONFIG['API_PORT']}"
        log.warning("API_URL не задан — локальный режим: %s", CONFIG["API_URL"])

    log.info("Client WebApp: %s", client_url())
    log.info("Admin  WebApp: %s", admin_url())

    app = web.Application(middlewares=[cors_mw])
    setup_routes(app)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", CONFIG["API_PORT"])
    await site.start()
    log.info("API запущен на :%s", CONFIG["API_PORT"])

    if os.getenv("RENDER"):

        async def self_ping():
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(
                        f"{CONFIG['API_URL']}/health", timeout=aiohttp.ClientTimeout(total=10)
                    ):
                        pass
            except Exception:
                pass

        scheduler.add_job(self_ping, "interval", minutes=10, id="selfping")

    scheduler.add_job(process_due_notifications, "interval", minutes=1, id="notifications")

    scheduler.start()
    log.info("✂️ Бот %s запускается...", CONFIG["BARBER_NAME"])
    try:
        await dp.start_polling(bot, skip_updates=True)
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
