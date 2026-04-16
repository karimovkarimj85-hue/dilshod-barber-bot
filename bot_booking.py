"""
✂️ Дильшод — Бот записи на стрижку
Telegram Mini App + HTTP API
"""

import asyncio
import hashlib
import hmac
import json
import logging
import os
import psycopg2
import psycopg2.extras
import urllib.parse
from contextlib import contextmanager
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Optional

import aiohttp

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

# Админ по умолчанию (Telegram user_id); дополнительные — через env ADMIN_IDS через запятую
_DEFAULT_ADMIN_ID = 1125022050
_ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", str(_DEFAULT_ADMIN_ID))

CONFIG = {
    "BOT_TOKEN":   os.getenv("BOT_TOKEN", ""),
    "ADMIN_IDS":   sorted({int(x.strip()) for x in _ADMIN_IDS_RAW.split(",") if x.strip()} | {_DEFAULT_ADMIN_ID}),
    "CHANNEL_ID":  int(os.getenv("CHANNEL_ID", "-1003692525683")),

    "BARBER_NAME": "Дильшод",
    "BARBER_YEAR": 2005,
    "BARBER_EXP":  "более 5 лет",
    "BARBER_PHONE": "+998 97 116 31 61",

    "SERVICE":     "Стрижка",
    "PRICE":       60000,
    "DURATION":    60,

    "WORK_START":  11,
    "WORK_END":    20,
    "DAYS_OFF":    [6],            # 0=Пн..6=Вс
    "BOOK_AHEAD":  14,
    "MIN_CANCEL_H": 16,

    "API_PORT":    int(os.getenv("PORT", "8080")),
    "RENDER_EXTERNAL_URL": os.getenv("RENDER_EXTERNAL_URL", ""),
    "API_URL":     os.getenv("RENDER_EXTERNAL_URL", "") or os.getenv("API_URL", ""),
}

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

def init_db():
    with get_db() as c:
        c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id BIGINT PRIMARY KEY, username TEXT DEFAULT '',
            first_name TEXT DEFAULT '', phone TEXT DEFAULT '',
            created_at TIMESTAMPTZ DEFAULT NOW()
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS bookings (
            id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL, book_date TEXT NOT NULL,
            book_time TEXT NOT NULL, name TEXT DEFAULT '',
            phone TEXT DEFAULT '', status TEXT DEFAULT 'confirmed',
            created_at TIMESTAMPTZ DEFAULT NOW()
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS blocked_slots (
            id SERIAL PRIMARY KEY,
            block_date TEXT NOT NULL, block_time TEXT, reason TEXT DEFAULT ''
        )""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_bd ON bookings(book_date)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_bu ON bookings(user_id)")

# ═══════════════════════════════════════
#  DB HELPERS
# ═══════════════════════════════════════

def ensure_user(uid, uname="", fname=""):
    with get_db() as c:
        c.execute("INSERT INTO users(id,username,first_name) VALUES(%s,%s,%s) ON CONFLICT DO NOTHING",
                  (uid, uname, fname))

def _booked(date_str):
    with get_db() as c:
        c.execute("SELECT book_time FROM bookings WHERE book_date=%s AND status='confirmed'",
                  (date_str,))
        return {r[0] for r in c.fetchall()}

def _blocked(date_str):
    with get_db() as c:
        c.execute("SELECT block_time FROM blocked_slots WHERE block_date=%s",
                  (date_str,))
        rows = c.fetchall()
    s = set()
    for r in rows:
        if r[0]:
            s.add(r[0])
        else:
            return {"ALL"}
    return s

def day_slots(date_str):
    d = datetime.strptime(date_str, "%Y-%m-%d").date()
    if d.weekday() in CONFIG["DAYS_OFF"]:
        return []
    bk, bl = _booked(date_str), _blocked(date_str)
    if "ALL" in bl:
        return []
    now = datetime.now()
    out = []
    for h in range(CONFIG["WORK_START"], CONFIG["WORK_END"]):
        t = f"{h:02d}:00"
        dt = datetime.strptime(f"{date_str} {t}", "%Y-%m-%d %H:%M")
        if dt <= now:
            st = "past"
        elif t in bl:
            st = "blocked"
        elif t in bk:
            st = "booked"
        else:
            st = "free"
        out.append({"time": t, "status": st})
    return out

def calendar_data():
    today = date.today()
    days = []
    for i in range(CONFIG["BOOK_AHEAD"]):
        d = today + timedelta(days=i)
        ds = d.isoformat()
        if d.weekday() in CONFIG["DAYS_OFF"]:
            days.append({"date": ds, "weekday": d.weekday(), "free": 0, "off": True})
            continue
        slots = day_slots(ds)
        free = sum(1 for s in slots if s["status"] == "free")
        days.append({"date": ds, "weekday": d.weekday(), "free": free, "off": False})
    return days

def make_booking(uid, bdate, btime, name="", phone=""):
    if btime in _booked(bdate) or btime in _blocked(bdate):
        return None
    d = datetime.strptime(bdate, "%Y-%m-%d").date()
    if d.weekday() in CONFIG["DAYS_OFF"]:
        return None
    h = int(btime.split(":")[0])
    if h < CONFIG["WORK_START"] or h >= CONFIG["WORK_END"]:
        return None
    with get_db() as c:
        c.execute(
            "INSERT INTO bookings(user_id,book_date,book_time,name,phone) VALUES(%s,%s,%s,%s,%s) RETURNING id",
            (uid, bdate, btime, name, phone))
        return c.fetchone()[0]

def get_booking(bid):
    with get_db() as c:
        c.execute("SELECT * FROM bookings WHERE id=%s", (bid,))
        r = c.fetchone()
        return dict(r) if r else None

def user_bookings(uid):
    with get_db() as c:
        c.execute(
            "SELECT * FROM bookings WHERE user_id=%s AND status='confirmed' "
            "AND book_date>=%s ORDER BY book_date,book_time",
            (uid, date.today().isoformat()))
        rows = c.fetchall()
    return [dict(r) for r in rows]

def cancel_booking(bid):
    with get_db() as c:
        c.execute("UPDATE bookings SET status='cancelled' WHERE id=%s", (bid,))

def can_modify(b):
    dt = datetime.strptime(f"{b['book_date']} {b['book_time']}", "%Y-%m-%d %H:%M")
    return (dt - datetime.now()).total_seconds() / 3600 > CONFIG["MIN_CANCEL_H"]

def all_bookings_date(ds):
    with get_db() as c:
        c.execute(
            "SELECT b.*, u.username, u.first_name AS ufn FROM bookings b "
            "LEFT JOIN users u ON u.id=b.user_id "
            "WHERE b.book_date=%s AND b.status='confirmed' ORDER BY b.book_time",
            (ds,))
        rows = c.fetchall()
    return [dict(r) for r in rows]

def block_slot(bdate, btime=None, reason=""):
    with get_db() as c:
        c.execute("INSERT INTO blocked_slots(block_date,block_time,reason) VALUES(%s,%s,%s)",
                  (bdate, btime, reason))

def unblock_slot(bdate, btime=None):
    with get_db() as c:
        if btime:
            c.execute("DELETE FROM blocked_slots WHERE block_date=%s AND block_time=%s",
                      (bdate, btime))
        else:
            c.execute("DELETE FROM blocked_slots WHERE block_date=%s AND block_time IS NULL",
                      (bdate,))

def get_stats():
    t = date.today().isoformat()
    tm = (date.today() + timedelta(1)).isoformat()
    with get_db() as c:
        c.execute("SELECT COUNT(*) FROM bookings WHERE book_date=%s AND status='confirmed'", (t,))
        today = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM bookings WHERE book_date=%s AND status='confirmed'", (tm,))
        tomorrow = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM bookings WHERE status='confirmed'")
        total = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM users")
        clients = c.fetchone()[0]
        return {"today": today, "tomorrow": tomorrow, "total": total, "clients": clients}

RU_WD = ["Пн","Вт","Ср","Чт","Пт","Сб","Вс"]
RU_MO = ["","янв","фев","мар","апр","май","июн","июл","авг","сен","окт","ноя","дек"]

# ═══════════════════════════════════════
#  AUTH
# ═══════════════════════════════════════

def validate_init_data(init_data_str, detail=False):
    """Return user dict on success; None on failure.
    If detail=True, return (user_or_None, error_code_str)."""
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
    init_data = request.headers.get("X-Init-Data", "")
    if not init_data:
        try:
            body = await request.json()
        except Exception:
            body = {}
        init_data = (body or {}).get("initData", "")
    if not init_data:
        return None
    return validate_init_data(init_data)

def require_admin(user):
    return user and user.get("id") in CONFIG["ADMIN_IDS"]

# ═══════════════════════════════════════
#  AIOHTTP API
# ═══════════════════════════════════════

@web.middleware
async def cors_mw(request, handler):
    if request.method == "OPTIONS":
        return web.Response(headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type,X-Init-Data",
        })
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

async def api_config(req):
    return web.json_response({
        "ok": True,
        "barber": CONFIG["BARBER_NAME"],
        "year": CONFIG["BARBER_YEAR"],
        "exp": CONFIG["BARBER_EXP"],
        "phone": CONFIG["BARBER_PHONE"],
        "service": CONFIG["SERVICE"],
        "price": CONFIG["PRICE"],
        "duration": CONFIG["DURATION"],
        "work_start": CONFIG["WORK_START"],
        "work_end": CONFIG["WORK_END"],
        "days_off": CONFIG["DAYS_OFF"],
        "book_ahead": CONFIG["BOOK_AHEAD"],
        "min_cancel_h": CONFIG["MIN_CANCEL_H"],
    })

async def api_calendar(req):
    return web.json_response({"ok": True, "days": calendar_data()})

async def api_schedule(req):
    ds = req.query.get("date", date.today().isoformat())
    return web.json_response({"ok": True, "date": ds, "slots": day_slots(ds)})

async def api_book(req):
    user = await get_user_from_request(req)
    if not user:
        return web.json_response({"ok": False, "error": "missing_init_data"}, status=401)
    body = await req.json()
    uid = user["id"]
    ensure_user(uid, user.get("username", ""), user.get("first_name", ""))
    bid = make_booking(uid, body["date"], body["time"], body.get("name", ""), body.get("phone", ""))
    if not bid:
        return web.json_response({"ok": False, "error": "Это время уже занято"})
    asyncio.create_task(notify_new_booking(bid))
    return web.json_response({"ok": True, "booking_id": bid})

async def api_my_bookings(req):
    user = await get_user_from_request(req)
    if not user:
        return web.json_response({"ok": False, "error": "missing_init_data"}, status=401)
    bks = user_bookings(user["id"])
    for b in bks:
        b["can_cancel"] = can_modify(b)
    return web.json_response({"ok": True, "bookings": bks})

async def api_cancel(req):
    user = await get_user_from_request(req)
    if not user:
        return web.json_response({"ok": False, "error": "missing_init_data"}, status=401)
    body = await req.json()
    b = get_booking(body["booking_id"])
    if not b or b["user_id"] != user["id"]:
        return web.json_response({"ok": False, "error": "Запись не найдена"})
    if not can_modify(b):
        return web.json_response({"ok": False, "error": f"До записи менее {CONFIG['MIN_CANCEL_H']} ч — "
            f"самостоятельно отменить нельзя.\nСвяжитесь с мастером: {CONFIG['BARBER_PHONE']}",
            "phone": CONFIG["BARBER_PHONE"]})
    cancel_booking(b["id"])
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
        return web.json_response({"ok": False, "error": f"До записи менее {CONFIG['MIN_CANCEL_H']} ч — "
            f"самостоятельно перенести нельзя.\nСвяжитесь с мастером: {CONFIG['BARBER_PHONE']}",
            "phone": CONFIG["BARBER_PHONE"]})
    new_bid = make_booking(user["id"], body["new_date"], body["new_time"], old["name"], old["phone"])
    if not new_bid:
        return web.json_response({"ok": False, "error": "Новое время уже занято"})
    cancel_booking(old["id"])
    asyncio.create_task(notify_reschedule(old, new_bid))
    return web.json_response({"ok": True, "new_booking_id": new_bid})

# admin API
async def api_admin_bookings(req):
    user = await get_user_from_request(req)
    if not user:
        return web.json_response({"ok": False, "error": "missing_init_data"}, status=401)
    if not require_admin(user):
        return web.json_response({"ok": False, "error": "not_admin"}, status=403)
    ds = req.query.get("date", date.today().isoformat())
    return web.json_response({"ok": True, "bookings": all_bookings_date(ds), "slots": day_slots(ds)})

async def api_admin_cancel(req):
    user = await get_user_from_request(req)
    if not user:
        return web.json_response({"ok": False, "error": "missing_init_data"}, status=401)
    if not require_admin(user):
        return web.json_response({"ok": False, "error": "not_admin"}, status=403)
    body = await req.json()
    b = get_booking(body["booking_id"])
    if b:
        cancel_booking(b["id"])
        asyncio.create_task(notify_cancel(b))
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
    return web.json_response({"ok": True, **get_stats()})

async def serve_file(path):
    async def handler(req):
        try:
            return web.FileResponse(path)
        except Exception:
            return web.Response(text="Not found", status=404)
    return handler

def setup_routes(app):
    app.router.add_get("/", api_health)
    app.router.add_get("/health", api_health)
    app.router.add_get("/api/auth-check", api_auth_check)
    app.router.add_get("/api/config", api_config)
    app.router.add_get("/api/calendar", api_calendar)
    app.router.add_get("/api/schedule", api_schedule)
    app.router.add_post("/api/book", api_book)
    app.router.add_get("/api/my-bookings", api_my_bookings)
    app.router.add_post("/api/my-bookings", api_my_bookings)
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
#  CHANNEL NOTIFICATIONS
# ═══════════════════════════════════════

async def notify_new_booking(bid):
    b = get_booking(bid)
    if not b:
        return
    d = datetime.strptime(b["book_date"], "%Y-%m-%d").date()
    end_h = int(b["book_time"][:2]) + 1
    text = (
        f"✂️ <b>Новая запись #{b['id']}</b>\n\n"
        f"👤 {b['name'] or '—'}\n"
        f"📱 {b['phone'] or '—'}\n"
        f"📅 {d.day} {RU_MO[d.month]} ({RU_WD[d.weekday()]})\n"
        f"⏰ {b['book_time']}–{end_h:02d}:00\n"
        f"💰 {CONFIG['PRICE']:,} сум"
    )
    for target in ([CONFIG["CHANNEL_ID"]] if CONFIG["CHANNEL_ID"] else []) + CONFIG["ADMIN_IDS"]:
        try:
            await bot.send_message(target, text)
        except Exception as e:
            log.warning(f"notify {target}: {e}")

async def notify_cancel(b):
    d = datetime.strptime(b["book_date"], "%Y-%m-%d").date()
    text = (
        f"❌ <b>Запись #{b['id']} отменена</b>\n\n"
        f"👤 {b['name'] or '—'}\n"
        f"📅 {d.day} {RU_MO[d.month]} ({RU_WD[d.weekday()]}) {b['book_time']}"
    )
    for target in ([CONFIG["CHANNEL_ID"]] if CONFIG["CHANNEL_ID"] else []) + CONFIG["ADMIN_IDS"]:
        try:
            await bot.send_message(target, text)
        except Exception as e:
            log.warning(f"notify {target}: {e}")

async def notify_reschedule(old, new_bid):
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
    for target in ([CONFIG["CHANNEL_ID"]] if CONFIG["CHANNEL_ID"] else []) + CONFIG["ADMIN_IDS"]:
        try:
            await bot.send_message(target, text)
        except Exception as e:
            log.warning(f"notify {target}: {e}")

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

CLIENT_BOOKING_WEBAPP_URL = "https://dilshod-barber-bot.onrender.com/webapp/index.html"

def main_kb():
    buttons = [[KeyboardButton(text="📋 Мои записи"), KeyboardButton(text="ℹ️ О мастере")]]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

def booking_inline_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="✂️ Записаться на стрижку",
            web_app=WebAppInfo(url=CLIENT_BOOKING_WEBAPP_URL),
        )],
    ])

ADMIN_PANEL_WEBAPP_URL = "https://dilshod-barber-bot.onrender.com/webapp/admin.html"

def admin_web_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="Открыть админ панель",
            web_app=WebAppInfo(url=ADMIN_PANEL_WEBAPP_URL),
        )],
    ])

@dp.message(Command("start"))
@dp.message(CommandStart())
async def cmd_start(msg: Message):
    ensure_user(msg.from_user.id, msg.from_user.username or "", msg.from_user.first_name or "")
    name = msg.from_user.first_name or "друг"
    text = (
        f"✂️ <b>{CONFIG['BARBER_NAME']} — Барбер</b>\n\n"
        f"👋 Привет, {name}!\n\n"
        f"💈 {CONFIG['SERVICE']} — <b>{CONFIG['PRICE']:,} сум</b>\n"
        f"⏱ ~{CONFIG['DURATION']} мин\n"
        f"📅 Пн–Сб, {CONFIG['WORK_START']}:00–{CONFIG['WORK_END']}:00\n\n"
        f"Нажмите кнопку ниже, чтобы записаться 👇"
    )
    await msg.answer(text, reply_markup=booking_inline_kb())
    await msg.answer("Меню:", reply_markup=main_kb())
    if msg.from_user.id in CONFIG["ADMIN_IDS"]:
        await msg.answer(
            "👑 Вы администратор. Откройте панель:",
            reply_markup=admin_web_kb(),
        )

@dp.message(F.text == "ℹ️ О мастере")
async def cmd_about(msg: Message):
    age = date.today().year - CONFIG["BARBER_YEAR"]
    await msg.answer(
        f"✂️ <b>{CONFIG['BARBER_NAME']}</b>\n\n"
        f"🎂 {CONFIG['BARBER_YEAR']} г.р. ({age} лет)\n"
        f"⭐ Опыт: {CONFIG['BARBER_EXP']}\n"
        f"💈 {CONFIG['SERVICE']} — {CONFIG['PRICE']:,} сум\n"
        f"⏱ ~{CONFIG['DURATION']} мин\n"
        f"📅 Пн–Сб, {CONFIG['WORK_START']}:00–{CONFIG['WORK_END']}:00\n"
        f"📞 {CONFIG['BARBER_PHONE']}\n"
    )

@dp.message(F.text == "📋 Мои записи")
async def cmd_my(msg: Message):
    bks = user_bookings(msg.from_user.id)
    if not bks:
        await msg.answer("У вас пока нет активных записей.")
        return
    lines = ["📋 <b>Ваши записи:</b>\n"]
    for b in bks:
        d = datetime.strptime(b["book_date"], "%Y-%m-%d").date()
        can = "✅" if can_modify(b) else "🔒"
        lines.append(
            f"• {d.day} {RU_MO[d.month]} ({RU_WD[d.weekday()]}) <b>{b['book_time']}</b> {can}"
        )
    lines.append(
        f"\n✅ = можно отменить/перенести\n"
        f"🔒 = менее {CONFIG['MIN_CANCEL_H']}ч — позвоните мастеру:\n"
        f"📞 {CONFIG['BARBER_PHONE']}"
    )
    rows = []
    for b in bks:
        if can_modify(b):
            rows.append([
                InlineKeyboardButton(text=f"❌ Отменить #{b['id']}", callback_data=f"cancel:{b['id']}"),
            ])
    await msg.answer("\n".join(lines),
                     reply_markup=InlineKeyboardMarkup(inline_keyboard=rows) if rows else None)

@dp.callback_query(F.data.startswith("cancel:"))
async def cb_cancel(cb: CallbackQuery):
    bid = int(cb.data.split(":")[1])
    b = get_booking(bid)
    if not b or b["user_id"] != cb.from_user.id:
        await cb.answer("Запись не найдена", show_alert=True)
        return
    if not can_modify(b):
        await cb.answer(
            f"До записи менее {CONFIG['MIN_CANCEL_H']}ч — позвоните мастеру: {CONFIG['BARBER_PHONE']}",
            show_alert=True)
        return
    cancel_booking(bid)
    await cb.message.edit_text(f"❌ Запись #{bid} отменена.")
    asyncio.create_task(notify_cancel(b))

@dp.message(Command("admin"))
async def cmd_admin(msg: Message):
    if msg.from_user.id not in CONFIG["ADMIN_IDS"]:
        await msg.answer("Нет доступа")
        return
    await msg.answer("Админ панель:", reply_markup=admin_web_kb())

@dp.message(F.web_app_data)
async def on_webapp_data(msg: Message):
    try:
        data = json.loads(msg.web_app_data.data)
    except Exception:
        return
    action = data.get("action")
    uid = msg.from_user.id
    ensure_user(uid, msg.from_user.username or "", msg.from_user.first_name or "")

    if action == "book":
        bid = make_booking(uid, data["date"], data["time"], data.get("name", ""), data.get("phone", ""))
        if bid:
            d = datetime.strptime(data["date"], "%Y-%m-%d").date()
            await msg.answer(
                f"✅ <b>Записано!</b>\n\n"
                f"📅 {d.day} {RU_MO[d.month]} ({RU_WD[d.weekday()]}) в {data['time']}\n"
                f"💰 {CONFIG['PRICE']:,} сум"
            )
            asyncio.create_task(notify_new_booking(bid))
        else:
            await msg.answer("⚠️ Это время уже занято. Попробуйте другое.")

    elif action == "cancel":
        b = get_booking(data.get("booking_id"))
        if b and b["user_id"] == uid and can_modify(b):
            cancel_booking(b["id"])
            await msg.answer(f"❌ Запись #{b['id']} отменена.")
            asyncio.create_task(notify_cancel(b))

# ═══════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════

async def main():
    if not DATABASE_URL:
        log.error("DATABASE_URL не задан! Укажите строку подключения к PostgreSQL.")
        raise SystemExit(1)

    init_db()

    if not CONFIG["API_URL"]:
        CONFIG["API_URL"] = f"http://localhost:{CONFIG['API_PORT']}"
        log.warning(f"API_URL не задан — локальный режим: {CONFIG['API_URL']}")

    log.info(f"Client WebApp: {client_url()}")
    log.info(f"Admin  WebApp: {admin_url()}")

    app = web.Application(middlewares=[cors_mw])
    setup_routes(app)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", CONFIG["API_PORT"])
    await site.start()
    log.info(f"API запущен на :{CONFIG['API_PORT']}")

    # Self-ping every 10 min to keep Render free tier awake
    if os.getenv("RENDER"):
        async def self_ping():
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(f"{CONFIG['API_URL']}/health", timeout=aiohttp.ClientTimeout(total=10)):
                        pass
            except Exception:
                pass
        scheduler.add_job(self_ping, "interval", minutes=10, id="selfping")

    scheduler.start()
    log.info(f"✂️ Бот {CONFIG['BARBER_NAME']} запускается...")
    try:
        await dp.start_polling(bot, skip_updates=True)
    finally:
        await runner.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
