import os
import re
import json
import html
import sqlite3
import datetime as dt
import asyncio
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote as urlquote

import httpx
from dotenv import load_dotenv

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    KeyboardButton,
)
from telegram.constants import ParseMode, ChatAction
from telegram.error import Conflict
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# =============================
# Load env
# =============================
load_dotenv()

# =============================
# ENV
# =============================
TELEGRAM_BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()

APIFOOTBALL_KEY = (os.getenv("APIFOOTBALL_KEY") or "").strip()
APIFOOTBALL_BASE = (os.getenv("APIFOOTBALL_BASE") or "https://v3.football.api-sports.io").strip()
if not APIFOOTBALL_BASE.startswith(("http://", "https://")):
    APIFOOTBALL_BASE = "https://" + APIFOOTBALL_BASE
APIFOOTBALL_BASE = APIFOOTBALL_BASE.rstrip("/")

FEDAPAY_API_KEY = (os.getenv("FEDAPAY_API_KEY") or "").strip()
FEDAPAY_BASE = (os.getenv("FEDAPAY_BASE") or "https://api.fedapay.com/v1").strip()
if not FEDAPAY_BASE.startswith(("http://", "https://")):
    FEDAPAY_BASE = "https://" + FEDAPAY_BASE
FEDAPAY_BASE = FEDAPAY_BASE.rstrip("/")

ADMIN_IDS = {
    int(x.strip())
    for x in (os.getenv("ADMIN_IDS") or "").split(",")
    if x.strip().isdigit()
}

PARIS_TZ_HOURS = int(os.getenv("PARIS_TZ_HOURS", "1"))
DB_PATH = os.getenv("DB_PATH", "bot.db")

SUB_DURATION_DAYS = 30

STANDARD_COUPONS_PER_DAY = 1
VIP_COUPONS_PER_DAY = 2

# Limites d'analyse (vous pouvez ajuster)
FREE_ANALYSES_PER_WEEK = int(os.getenv("FREE_ANALYSES_PER_WEEK", "5"))
STANDARD_ANALYSES_PER_MONTH = int(os.getenv("STANDARD_ANALYSES_PER_MONTH", "15"))
VIP_ANALYSES_PER_MONTH = int(os.getenv("VIP_ANALYSES_PER_MONTH", "60"))

VIP_CHANNEL_LINK = "https://t.me/+fo_0a8c5d_43ZThk"

PAY_LINKS = {
    "STANDARD": os.getenv("PAY_LINK_STANDARD", "https://me.fedapay.com/k8TEq_Ni"),
    "VIP": os.getenv("PAY_LINK_VIP", "https://me.fedapay.com/PZ5cxcPc"),
    "VVIP": os.getenv("PAY_LINK_VVIP", "https://me.fedapay.com/je65fOkF"),
}

MATCHES_PER_LEAGUE_MAX = int(os.getenv("MATCHES_PER_LEAGUE_MAX", "12"))

# Concurrency (API calls)
ANALYSIS_CONCURRENCY = int(os.getenv("ANALYSIS_CONCURRENCY", "5"))

# Broadcast anti-flood
BROADCAST_DELAY_SEC = float(os.getenv("BROADCAST_DELAY_SEC", "0.05"))

BOT_NAME = "BoscoBot"

# =============================
# LEAGUES (Europe + top leagues + D2)
# =============================
FOOT_LEAGUES_FIXED: Dict[str, int] = {
    # EUROPE
    "UEFA Champions League": 2,
    "UEFA Europa League": 3,
    "UEFA Europa Conference League": 848,

    # TOP 5 + D2
    "Angleterre — Premier League": 39,
    "Angleterre — Championship (D2)": 40,

    "Espagne — LaLiga": 140,
    "Espagne — Segunda División (D2)": 141,

    "France — Ligue 1": 61,
    "France — Ligue 2 (D2)": 62,

    "Italie — Serie A": 135,
    "Italie — Serie B (D2)": 136,

    "Allemagne — Bundesliga": 78,
    "Allemagne — Bundesliga 2 (D2)": 79,

    # Autres grosses ligues
    "Portugal — Primeira Liga": 94,
    "Portugal — Liga Portugal 2 (D2)": 95,

    "Turquie — Süper Lig": 203,
    "Turquie — 1. Lig (D2)": 204,

    "Pays-Bas — Eredivisie": 88,
    "Pays-Bas — Eerste Divisie (D2)": 89,
}

# =============================
# TEXTS
# =============================
ONBOARDING_TEXT = (
    f"👋 <b>Bienvenue sur {BOT_NAME}</b> ⚽📊\n\n"
    "🎯 <b>Ce que fait le bot</b>\n"
    "• Affiche les matchs (aujourd’hui / demain / après-demain)\n"
    "• Donne une <b>analyse</b> avec des % (corners, cartons, tirs, fautes…)\n"
    "• Donne des <b>événements</b> en % (OUI/NON)\n"
    "• Affiche le <b>coupon du jour</b>\n"
    "• Vous aide à calculer votre <b>mise</b> (capital)\n\n"
    "💳 <b>Abonnement (important)</b>\n"
    "1) Cliquez sur <b>💳 Abonnement</b>\n"
    "2) Choisissez Standard/VIP/VVIP\n"
    "3) Le bot vous donne un <b>CODE</b>\n"
    "4) Sur FedaPay, collez ce code dans <b>Référence de paiement</b>\n"
    "5) Payez\n"
    "6) Revenez → cliquez <b>✅ J’ai déjà payé</b> → recolle le même code\n\n"
    "🆓 <b>Limites</b>\n"
    f"• FREE : <b>{FREE_ANALYSES_PER_WEEK}</b> analyses / semaine\n"
    "• Standard/VIP : limites plus hautes (menu Statut)\n\n"
    "ℹ️ Les stats sont souvent utilisées sur <b>1xBet</b>, mais elles peuvent servir aussi sur d’autres sites.\n\n"
    "📌 Utilisez le <b>menu en bas</b> 👇"
)

HELP_TEXT = (
    "🧭 <b>Aide</b>\n\n"
    "✅ Utilisez le menu en bas :\n"
    "• 📅 Matchs → choisissez Today/Demain/Après-demain\n"
    "• Sous un match → cliquez <b>🔎 Analyser</b>\n"
    "• 🎟 Coupon du jour\n"
    "• 💳 Abonnement\n"
    "• 💰 Capital\n"
    "• ✅ Statut\n\n"
    "🛑 Si vous êtes bloqué dans un paiement : /cancel"
)

UNKNOWN_TEXT = "🤖 Je n’ai pas compris. Utilisez le menu en bas 👇 (ou /help)."

CAPITAL_RULES_TEXT = (
    "💰 <b>Gestion du capital</b>\n\n"
    "Règle simple (selon le nombre de matchs du coupon) :\n"
    "• Code 2 → miser <b>15%</b>\n"
    "• Code 3 → miser <b>10%</b>\n"
    "• Code 5 → miser <b>8%</b>\n"
    "• Code 7 et + → miser <b>5%</b>\n\n"
    "📌 Cliquez <b>Calculer</b> pour obtenir la mise."
)

CAPITAL_START_TEXT = (
    "💰 <b>Calcul de mise</b>\n\n"
    "1) Envoyez votre <b>capital total</b> (ex: 100000)\n"
    "2) Puis envoyez le <b>code</b> (= nombre de matchs du coupon, ex: 2, 3, 5, 7)\n\n"
    "➡️ Envoyez maintenant votre <b>capital</b>."
)

# =============================
# Helpers
# =============================
def escape(s: str) -> str:
    return html.escape(s or "")

def now_paris() -> dt.datetime:
    return dt.datetime.now(dt.UTC) + dt.timedelta(hours=PARIS_TZ_HOURS)

def season_from_date(d: dt.date) -> int:
    return d.year if d.month >= 8 else d.year - 1

def kickoff_timestamp(fx: Dict[str, Any]) -> int:
    return int(((fx.get("fixture") or {}).get("timestamp")) or 0)

def kickoff_hhmm(fx: Dict[str, Any]) -> str:
    ts = kickoff_timestamp(fx)
    if not ts:
        return "??:??"
    dt_utc = dt.datetime.fromtimestamp(ts, tz=dt.UTC)
    local = dt_utc + dt.timedelta(hours=PARIS_TZ_HOURS)
    return local.strftime("%H:%M")

def chunk_lines(lines: List[str], max_chars: int = 3500) -> List[str]:
    out, buf = [], ""
    for line in lines:
        if len(buf) + len(line) + 1 > max_chars:
            out.append(buf)
            buf = ""
        buf += line + "\n"
    if buf:
        out.append(buf)
    return out

async def send_typing(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    try:
        await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)
    except Exception:
        pass

def is_admin(update: Update) -> bool:
    uid = update.effective_user.id if update.effective_user else 0
    return uid in ADMIN_IDS

def week_key() -> str:
    d = now_paris().date()
    iso_year, iso_week, _ = d.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"

# =============================
# DB
# =============================
def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA journal_mode=WAL;")
    return con

def init_db():
    with db() as con:
        con.execute("""
        CREATE TABLE IF NOT EXISTS users(
            chat_id INTEGER PRIMARY KEY,
            user_id INTEGER,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            created_at TEXT NOT NULL
        )
        """)
        con.execute("""
        CREATE TABLE IF NOT EXISTS subscriptions(
            chat_id INTEGER PRIMARY KEY,
            plan TEXT NOT NULL,
            expires_at TEXT NOT NULL
        )
        """)
        con.execute("""
        CREATE TABLE IF NOT EXISTS coupon_day(
            day TEXT PRIMARY KEY,
            coupon_code TEXT NOT NULL,
            coupon_text TEXT NOT NULL,
            created_by INTEGER NOT NULL,
            created_at TEXT NOT NULL
        )
        """)
        con.execute("""
        CREATE TABLE IF NOT EXISTS usage_day(
            chat_id INTEGER NOT NULL,
            day TEXT NOT NULL,
            coupons_used INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(chat_id, day)
        )
        """)
        con.execute("""
        CREATE TABLE IF NOT EXISTS usage_month(
            chat_id INTEGER NOT NULL,
            month TEXT NOT NULL,
            analyses_used INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(chat_id, month)
        )
        """)
        con.execute("""
        CREATE TABLE IF NOT EXISTS usage_week(
            chat_id INTEGER NOT NULL,
            week TEXT NOT NULL,
            analyses_used INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(chat_id, week)
        )
        """)
        con.execute("""
        CREATE TABLE IF NOT EXISTS subscription_log(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            plan TEXT NOT NULL,
            reference TEXT,
            created_at TEXT NOT NULL
        )
        """)

def upsert_user(update: Update):
    if not update.effective_chat:
        return
    chat_id = update.effective_chat.id
    u = update.effective_user
    with db() as con:
        con.execute("""
        INSERT INTO users(chat_id, user_id, username, first_name, last_name, created_at)
        VALUES(?,?,?,?,?,?)
        ON CONFLICT(chat_id) DO UPDATE SET
            user_id=excluded.user_id,
            username=excluded.username,
            first_name=excluded.first_name,
            last_name=excluded.last_name,
            created_at=excluded.created_at
        """, (
            chat_id,
            (u.id if u else None),
            (u.username if u else None),
            (u.first_name if u else None),
            (u.last_name if u else None),
            now_paris().isoformat(),
        ))

def delete_user(chat_id: int):
    with db() as con:
        con.execute("DELETE FROM users WHERE chat_id=?", (chat_id,))
        con.execute("DELETE FROM subscriptions WHERE chat_id=?", (chat_id,))
        con.execute("DELETE FROM usage_day WHERE chat_id=?", (chat_id,))
        con.execute("DELETE FROM usage_week WHERE chat_id=?", (chat_id,))
        con.execute("DELETE FROM usage_month WHERE chat_id=?", (chat_id,))

def get_all_users_chat_ids() -> List[int]:
    with db() as con:
        rows = con.execute("SELECT chat_id FROM users").fetchall()
    return [int(r[0]) for r in rows]

def get_active_subscribers_chat_ids() -> List[int]:
    now = dt.datetime.now(dt.UTC)
    with db() as con:
        rows = con.execute("SELECT chat_id, expires_at FROM subscriptions").fetchall()
    out = []
    for cid, exp in rows:
        try:
            exp_dt = dt.datetime.fromisoformat(exp)
            if exp_dt >= now:
                out.append(int(cid))
        except Exception:
            continue
    return out

def get_sub(chat_id: int) -> Optional[Dict[str, Any]]:
    with db() as con:
        row = con.execute("SELECT plan, expires_at FROM subscriptions WHERE chat_id=?", (chat_id,)).fetchone()
    if not row:
        return None
    plan, expires_at = row
    exp = dt.datetime.fromisoformat(expires_at)
    if exp < dt.datetime.now(dt.UTC):
        return None
    return {"plan": plan, "expires_at": exp}

def set_sub(chat_id: int, plan: str, reference: Optional[str] = None, days: int = SUB_DURATION_DAYS) -> dt.datetime:
    exp = dt.datetime.now(dt.UTC) + dt.timedelta(days=int(days))
    with db() as con:
        con.execute("""
        INSERT INTO subscriptions(chat_id, plan, expires_at)
        VALUES(?,?,?)
        ON CONFLICT(chat_id) DO UPDATE SET plan=excluded.plan, expires_at=excluded.expires_at
        """, (chat_id, plan, exp.isoformat()))
        con.execute(
            "INSERT INTO subscription_log(chat_id, plan, reference, created_at) VALUES(?,?,?,?)",
            (chat_id, plan, reference, now_paris().isoformat()),
        )
    return exp

def mark_coupon_used(chat_id: int):
    day = now_paris().date().isoformat()
    with db() as con:
        con.execute("""
        INSERT INTO usage_day(chat_id, day, coupons_used) VALUES(?,?,1)
        ON CONFLICT(chat_id, day) DO UPDATE SET coupons_used=coupons_used+1
        """, (chat_id, day))

def coupons_used_today(chat_id: int) -> int:
    day = now_paris().date().isoformat()
    with db() as con:
        row = con.execute("SELECT coupons_used FROM usage_day WHERE chat_id=? AND day=?", (chat_id, day)).fetchone()
    return int(row[0]) if row else 0

def can_get_coupon(chat_id: int, plan: str) -> Tuple[bool, str]:
    if plan in ("VVIP", "ADMIN"):
        return True, ""
    used = coupons_used_today(chat_id)
    limit = STANDARD_COUPONS_PER_DAY if plan == "STANDARD" else VIP_COUPONS_PER_DAY
    if used >= limit:
        return False, f"Limite coupon atteinte ({used}/{limit}) aujourd’hui."
    return True, ""

def mark_analyze_used_month(chat_id: int):
    month = now_paris().strftime("%Y-%m")
    with db() as con:
        con.execute("""
        INSERT INTO usage_month(chat_id, month, analyses_used) VALUES(?,?,1)
        ON CONFLICT(chat_id, month) DO UPDATE SET analyses_used=analyses_used+1
        """, (chat_id, month))

def analyses_used_month(chat_id: int) -> int:
    month = now_paris().strftime("%Y-%m")
    with db() as con:
        row = con.execute("SELECT analyses_used FROM usage_month WHERE chat_id=? AND month=?", (chat_id, month)).fetchone()
    return int(row[0]) if row else 0

def mark_analyze_used_week(chat_id: int):
    wk = week_key()
    with db() as con:
        con.execute("""
        INSERT INTO usage_week(chat_id, week, analyses_used) VALUES(?,?,1)
        ON CONFLICT(chat_id, week) DO UPDATE SET analyses_used=analyses_used+1
        """, (chat_id, wk))

def analyses_used_week(chat_id: int) -> int:
    wk = week_key()
    with db() as con:
        row = con.execute("SELECT analyses_used FROM usage_week WHERE chat_id=? AND week=?", (chat_id, wk)).fetchone()
    return int(row[0]) if row else 0

def can_analyze(chat_id: int, plan: str) -> Tuple[bool, str]:
    if plan in ("VVIP", "ADMIN"):
        return True, ""
    if plan == "FREE":
        used = analyses_used_week(chat_id)
        if used >= FREE_ANALYSES_PER_WEEK:
            return False, f"Limite FREE atteinte ({used}/{FREE_ANALYSES_PER_WEEK}) cette semaine."
        return True, ""
    used_m = analyses_used_month(chat_id)
    limit = VIP_ANALYSES_PER_MONTH if plan == "VIP" else STANDARD_ANALYSES_PER_MONTH
    if used_m >= limit:
        return False, f"Limite analyses atteinte ({used_m}/{limit}) ce mois."
    return True, ""

# =============================
# API calls
# =============================
async def apifootball_get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    if not APIFOOTBALL_KEY:
        raise RuntimeError("APIFOOTBALL_KEY manquante")
    headers = {"x-apisports-key": APIFOOTBALL_KEY}
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(f"{APIFOOTBALL_BASE}{path}", headers=headers, params=params)
        r.raise_for_status()
        return r.json()

async def fedapay_is_paid_by_reference(ref: str) -> Tuple[bool, str]:
    if not FEDAPAY_API_KEY:
        return False, "FEDAPAY_API_KEY manquante"
    reference = (ref or "").strip()
    if not reference:
        return False, "Référence vide"
    url = f"{FEDAPAY_BASE}/transactions/merchant/{urlquote(reference, safe='')}"
    headers = {"Authorization": f"Bearer {FEDAPAY_API_KEY}"}
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(url, headers=headers)
        if r.status_code == 404:
            return False, "Aucune transaction trouvée pour cette référence"
        r.raise_for_status()
        data = r.json()
    tx = data.get("transaction") or data.get("data") or data
    status = str(tx.get("status") or "").lower()
    if status == "approved":
        return True, "Paiement approuvé"
    return False, f"Statut paiement: {status or 'inconnu'}"

# =============================
# MENUS
# =============================
def main_menu_keyboard() -> ReplyKeyboardMarkup:
    kb = [
        [KeyboardButton("📅 Matchs"), KeyboardButton("🔎 Analyse")],
        [KeyboardButton("🎟️ Coupon du jour"), KeyboardButton("💳 Abonnement")],
        [KeyboardButton("💰 Capital"), KeyboardButton("✅ Statut")],
        [KeyboardButton("❓ Aide")],
    ]
    return ReplyKeyboardMarkup(kb, resize_keyboard=True)

def matches_inline_keyboard() -> InlineKeyboardMarkup:
    kb = [
        [
            InlineKeyboardButton("📅 Aujourd’hui", callback_data="M_TODAY"),
            InlineKeyboardButton("📅 Demain", callback_data="M_TOMORROW"),
        ],
        [InlineKeyboardButton("📅 Après-demain", callback_data="M_AFTER")],
        [InlineKeyboardButton("⬅️ Menu", callback_data="BACK_MENU")],
    ]
    return InlineKeyboardMarkup(kb)

def subscription_inline_keyboard() -> InlineKeyboardMarkup:
    kb = [
        [InlineKeyboardButton("✅ Standard (1 mois)", callback_data="SUB_STANDARD")],
        [InlineKeyboardButton("⭐ VIP (1 mois)", callback_data="SUB_VIP")],
        [InlineKeyboardButton("👑 VVIP (1 mois)", callback_data="SUB_VVIP")],
        [InlineKeyboardButton("📘 Comment activer", callback_data="SUB_HELP")],
    ]
    return InlineKeyboardMarkup(kb)

def capital_inline_keyboard() -> InlineKeyboardMarkup:
    kb = [
        [InlineKeyboardButton("🧮 Calculer la mise", callback_data="CAP_CALC")],
        [InlineKeyboardButton("📘 Règles capital", callback_data="CAP_RULES")],
        [InlineKeyboardButton("⬅️ Menu", callback_data="BACK_MENU")],
    ]
    return InlineKeyboardMarkup(kb)

def analyze_button(fixture_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔎 Analyser", callback_data=f"AN_{fixture_id}")]])

# =============================
# CAPITAL
# =============================
def capital_percent_for_code(code: int) -> float:
    if code == 2:
        return 0.15
    if code == 3:
        return 0.10
    if code == 5:
        return 0.08
    if code >= 7:
        return 0.05
    return 0.08

def format_amount(x: float) -> str:
    try:
        return f"{int(round(x)):,}".replace(",", " ")
    except Exception:
        return str(x)

# =============================
# ANALYSE helpers (stats mapping)
# =============================
METRICS = [
    (["Corner Kicks"], "corners"),
    (["Yellow Cards"], "yellow"),
    (["Red Cards"], "red"),
    (["Offsides"], "offsides"),
    (["Total Shots", "Shots Total"], "shots_total"),
    (["Shots on Goal", "Shots on Target"], "shots_on_target"),
    (["Fouls"], "fouls"),
]

def _as_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return int(v)
    if isinstance(v, str):
        m = re.search(r"-?\d+", v.replace("−", "-"))
        if not m:
            return None
        try:
            return int(m.group(0))
        except Exception:
            return None
    return None

async def fixture_stats(fid: int) -> Dict[str, Optional[int]]:
    data = await apifootball_get("/fixtures/statistics", {"fixture": fid})
    resp = data.get("response") or []
    if len(resp) < 2:
        return {}

    type_to_key: Dict[str, str] = {}
    for names, key in METRICS:
        for n in names:
            type_to_key[n.lower()] = key

    totals = {key: None for _names, key in METRICS}
    team_maps: List[Dict[str, Optional[int]]] = []

    for entry in resp[:2]:
        stats = entry.get("statistics") or []
        m = {key: None for _names, key in METRICS}
        for it in stats:
            t = str(it.get("type") or "").lower()
            v = it.get("value")
            k = type_to_key.get(t)
            if k:
                m[k] = _as_int(v)
        team_maps.append(m)

    if len(team_maps) < 2:
        return {}

    for _names, key in METRICS:
        hv = team_maps[0].get(key)
        av = team_maps[1].get(key)
        if hv is None and av is None:
            totals[key] = None
        else:
            totals[key] = (hv or 0) + (av or 0)

    return totals

async def fixture_events(fid: int) -> List[dict]:
    data = await apifootball_get("/fixtures/events", {"fixture": fid})
    return data.get("response") or []

def event_yes_no(events: List[dict]) -> Dict[str, bool]:
    res = {
        "stoppage_ht": False,
        "stoppage_ft": False,
        "red_card": False,
        "streak3": False,
        "goal_88": False,
    }

    for ev in events:
        if (ev.get("type") or "") == "Card":
            detail = (ev.get("detail") or "").lower()
            if ("red" in detail) or ("second yellow" in detail):
                res["red_card"] = True
                break

    goals = [e for e in events if (e.get("type") == "Goal")]
    for g in goals:
        tm = g.get("time") or {}
        elapsed = tm.get("elapsed")
        extra = tm.get("extra")
        if isinstance(elapsed, int) and elapsed >= 88:
            res["goal_88"] = True
        if isinstance(extra, int) and extra >= 1 and elapsed == 45:
            res["stoppage_ht"] = True
        if isinstance(extra, int) and extra >= 1 and elapsed == 90:
            res["stoppage_ft"] = True

    def goal_sort_key(e: dict):
        tm = e.get("time") or {}
        elapsed = tm.get("elapsed") if isinstance(tm.get("elapsed"), int) else 0
        extra = tm.get("extra") if isinstance(tm.get("extra"), int) else 0
        return elapsed * 10 + extra

    goals_sorted = sorted(goals, key=goal_sort_key)
    last_team = None
    streak = 0
    best = 0
    for g in goals_sorted:
        tid = (g.get("team") or {}).get("id")
        if tid is None:
            continue
        if tid == last_team:
            streak += 1
        else:
            streak = 1
            last_team = tid
        best = max(best, streak)
    res["streak3"] = best >= 3

    return res

async def collect_samples(fixtures: List[dict], cap_stats: int = 18, cap_events: int = 12) -> Tuple[Dict[str, List[int]], Dict[str, float], int]:
    fxs_stats = fixtures[:cap_stats]
    fxs_events = fixtures[:cap_events]

    numeric: Dict[str, List[int]] = {key: [] for _names, key in METRICS}
    ev_counts = {"stoppage_ht": 0, "stoppage_ft": 0, "red_card": 0, "streak3": 0, "goal_88": 0}
    ev_n = 0

    sem = asyncio.Semaphore(ANALYSIS_CONCURRENCY)

    async def one_stats(fid: int):
        async with sem:
            try:
                totals = await fixture_stats(fid)
                for _names, key in METRICS:
                    v = totals.get(key)
                    if isinstance(v, int):
                        numeric[key].append(v)
            except Exception:
                pass

    async def one_events(fid: int):
        nonlocal ev_n
        async with sem:
            try:
                evs = await fixture_events(fid)
                flags = event_yes_no(evs)
                for k in ev_counts.keys():
                    if flags.get(k):
                        ev_counts[k] += 1
                ev_n += 1
            except Exception:
                pass

    tasks = []
    for fx in fxs_stats:
        fid = (fx.get("fixture") or {}).get("id")
        if isinstance(fid, int):
            tasks.append(asyncio.create_task(one_stats(fid)))
    for fx in fxs_events:
        fid = (fx.get("fixture") or {}).get("id")
        if isinstance(fid, int):
            tasks.append(asyncio.create_task(one_events(fid)))

    if tasks:
        await asyncio.gather(*tasks)

    ev_freq = {k: (ev_counts[k] / ev_n) if ev_n else 0.0 for k in ev_counts.keys()}
    return numeric, ev_freq, ev_n

def pct(p: float) -> int:
    return int(round(p * 100))

def prob_ge(vals: List[int], th: int) -> float:
    if not vals:
        return 0.0
    return sum(1 for v in vals if v >= th) / len(vals)

def prob_le(vals: List[int], th: int) -> float:
    if not vals:
        return 0.0
    return sum(1 for v in vals if v <= th) / len(vals)

def outcome_probs(fixtures: List[dict]) -> Dict[str, float]:
    w_home = 0
    w_away = 0
    draw = 0
    btts = 0
    n = 0
    for fx in fixtures:
        goals = fx.get("goals") or {}
        gh = _as_int(goals.get("home"))
        ga = _as_int(goals.get("away"))
        if gh is None or ga is None:
            continue
        n += 1
        if gh > ga:
            w_home += 1
        elif ga > gh:
            w_away += 1
        else:
            draw += 1
        if gh > 0 and ga > 0:
            btts += 1
    if n == 0:
        return {"home": 0.0, "draw": 0.0, "away": 0.0, "btts": 0.0}
    return {"home": w_home / n, "draw": draw / n, "away": w_away / n, "btts": btts / n}

# =============================
# STATE mgmt
# =============================
def clear_states(context: ContextTypes.DEFAULT_TYPE):
    for k in [
        "awaiting_ref_plan",
        "expected_pay_code",
        "expected_pay_plan",
        "awaiting_capital_amount",
        "awaiting_capital_code",
        "capital_amount",
    ]:
        context.user_data.pop(k, None)

# =============================
# PAYMENT CODE
# =============================
def make_payment_code(plan: str, chat_id: int) -> str:
    stamp = now_paris().strftime("%Y%m%d")
    return f"{plan}-BOSCO-{chat_id}-{stamp}"

# =============================
# COMMANDS
# =============================
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    clear_states(context)
    await update.message.reply_text(ONBOARDING_TEXT, parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    clear_states(context)
    await update.message.reply_text(HELP_TEXT, parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())

async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    clear_states(context)
    await update.message.reply_text("✅ OK, j’ai annulé l’action en cours. Utilisez le menu 👇", reply_markup=main_menu_keyboard())

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    clear_states(context)
    chat_id = update.effective_chat.id

    if is_admin(update):
        await update.message.reply_text("👑 <b>ADMIN</b> : accès total.", parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())
        return

    sub = get_sub(chat_id)
    if not sub:
        used = analyses_used_week(chat_id)
        await update.message.reply_text(
            "🆓 <b>Statut</b> : FREE\n"
            f"🔎 Analyses semaine : <b>{used}/{FREE_ANALYSES_PER_WEEK}</b>\n\n"
            "➡️ Pour débloquer : <b>💳 Abonnement</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_keyboard()
        )
        return

    plan = sub["plan"]
    exp = sub["expires_at"].strftime("%Y-%m-%d")
    used_m = analyses_used_month(chat_id)
    limit = VIP_ANALYSES_PER_MONTH if plan == "VIP" else STANDARD_ANALYSES_PER_MONTH
    await update.message.reply_text(
        f"✅ Plan : <b>{escape(plan)}</b>\n"
        f"⏳ Expire : <b>{escape(exp)}</b>\n"
        f"🔎 Analyses mois : <b>{used_m}/{limit}</b>\n\n"
        f"🔗 Canal : {escape(VIP_CHANNEL_LINK)}",
        parse_mode=ParseMode.HTML,
        reply_markup=main_menu_keyboard()
    )

async def abonnement_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    clear_states(context)
    await update.message.reply_text(
        "💳 <b>Choisissez votre abonnement</b> :",
        parse_mode=ParseMode.HTML,
        reply_markup=subscription_inline_keyboard(),
    )

async def capital_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    clear_states(context)
    await update.message.reply_text(
        "💰 <b>Capital</b>\n\nChoisissez une option :",
        parse_mode=ParseMode.HTML,
        reply_markup=capital_inline_keyboard(),
    )

async def coupon_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    clear_states(context)
    chat_id = update.effective_chat.id

    plan = "ADMIN" if is_admin(update) else (get_sub(chat_id)["plan"] if get_sub(chat_id) else "FREE")
    if plan == "FREE":
        await update.message.reply_text(
            "🔒 Coupon réservé aux abonnés. Cliquez <b>💳 Abonnement</b>.",
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_keyboard(),
        )
        return

    ok, msg = can_get_coupon(chat_id, plan)
    if not ok:
        await update.message.reply_text(f"⛔ {escape(msg)}", parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())
        return

    day = now_paris().date().isoformat()
    with db() as con:
        row = con.execute("SELECT coupon_code, coupon_text FROM coupon_day WHERE day=?", (day,)).fetchone()

    if not row:
        await update.message.reply_text("📭 Aucun coupon du jour pour le moment.", parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())
        return

    code, text = row
    mark_coupon_used(chat_id)

    await update.message.reply_text(
        f"🎟️ <b>Coupon du jour</b> ({escape(day)})\n\n"
        f"🔢 Code coupon : <b>{escape(code)}</b>\n\n"
        f"{text}\n\n"
        f"🔗 Canal : {escape(VIP_CHANNEL_LINK)}",
        parse_mode=ParseMode.HTML,
        reply_markup=main_menu_keyboard()
    )

async def publier_coupon_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    if not is_admin(update):
        await update.message.reply_text("⛔ Réservé à l’admin.")
        return

    raw = update.message.text or ""
    parts = raw.split(" ", 1)
    if len(parts) < 2 or "|" not in parts[1]:
        await update.message.reply_text(
            "Format:\n/publier_coupon CODE | Texte du coupon\n\nEx:\n/publier_coupon C5 | 1) Match A ...",
        )
        return

    rest = parts[1]
    code_part, text_part = rest.split("|", 1)
    coupon_code = code_part.strip()
    coupon_text = text_part.strip()
    if not coupon_code or not coupon_text:
        await update.message.reply_text("❌ CODE et TEXTE requis.")
        return

    day = now_paris().date().isoformat()
    created_by = update.effective_user.id if update.effective_user else 0

    with db() as con:
        con.execute("""
        INSERT INTO coupon_day(day, coupon_code, coupon_text, created_by, created_at)
        VALUES(?,?,?,?,?)
        ON CONFLICT(day) DO UPDATE SET
            coupon_code=excluded.coupon_code,
            coupon_text=excluded.coupon_text,
            created_by=excluded.created_by,
            created_at=excluded.created_at
        """, (day, coupon_code, coupon_text, created_by, now_paris().isoformat()))

    await update.message.reply_text("✅ Coupon publié. Les abonnés peuvent faire : /coupon", reply_markup=main_menu_keyboard())

async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    if not is_admin(update):
        await update.message.reply_text("⛔ Réservé à l’admin.")
        return

    now = dt.datetime.now(dt.UTC)
    today = now_paris().date().isoformat()
    month = now_paris().strftime("%Y-%m")

    with db() as con:
        total_users = con.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        active_subs = con.execute(
            "SELECT COUNT(*) FROM subscriptions WHERE datetime(expires_at) > datetime(?)",
            (now.isoformat(),)
        ).fetchone()[0]
        coupons_views_today = con.execute(
            "SELECT COALESCE(SUM(coupons_used),0) FROM usage_day WHERE day=?",
            (today,)
        ).fetchone()[0]
        analyses_month = con.execute(
            "SELECT COALESCE(SUM(analyses_used),0) FROM usage_month WHERE month=?",
            (month,)
        ).fetchone()[0]
        last_activations = con.execute(
            "SELECT chat_id, plan, reference, created_at FROM subscription_log ORDER BY id DESC LIMIT 5"
        ).fetchall()

    lines = []
    lines.append("📊 <b>Dashboard Admin</b>")
    lines.append(f"👥 Utilisateurs : <b>{total_users}</b>")
    lines.append(f"💳 Abonnés actifs : <b>{active_subs}</b>")
    lines.append(f"🎟 Coupons consultés aujourd’hui : <b>{coupons_views_today}</b>")
    lines.append(f"🔎 Analyses ce mois : <b>{analyses_month}</b>")
    lines.append("")
    lines.append("🕘 <b>5 dernières activations</b>")
    if last_activations:
        for cid, plan, ref, created_at in last_activations:
            lines.append(f"• <code>{cid}</code> — {escape(plan)} — {escape(ref or '-')}")
    else:
        lines.append("• (aucune)")

    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())

async def grant_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    if not is_admin(update):
        await update.message.reply_text("⛔ Réservé à l’admin.")
        return

    if len(context.args) < 2:
        await update.message.reply_text(
            "Usage:\n"
            "<code>/grant VIP 123456789 30</code>\n"
            "Plans: STANDARD | VIP | VVIP",
            parse_mode=ParseMode.HTML
        )
        return

    plan = context.args[0].strip().upper()
    target = context.args[1].strip()
    days = int(context.args[2]) if len(context.args) >= 3 and context.args[2].isdigit() else SUB_DURATION_DAYS

    if plan not in ("STANDARD", "VIP", "VVIP"):
        await update.message.reply_text("❌ Plan invalide (STANDARD|VIP|VVIP).")
        return

    if not target.isdigit():
        await update.message.reply_text("❌ Donnez un chat_id numérique (ex: /grant VIP 123456789 30).")
        return

    target_chat_id = int(target)
    exp = set_sub(target_chat_id, plan, reference=f"ADMIN-GRANT-{update.effective_user.id}", days=days)
    await update.message.reply_text(
        f"✅ Accès accordé : <b>{escape(plan)}</b> à <code>{target_chat_id}</code>\n"
        f"⏳ Expire : <b>{escape(exp.strftime('%Y-%m-%d'))}</b>",
        parse_mode=ParseMode.HTML
    )

# =============================
# ✅ BROADCAST (ADMIN)
# =============================
async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    if not is_admin(update):
        await update.message.reply_text("⛔ Réservé à l’admin.")
        return

    raw = update.message.text or ""
    parts = raw.split(" ", 1)
    if len(parts) < 2 or not parts[1].strip():
        await update.message.reply_text(
            "Usage:\n"
            "<code>/broadcast Votre message</code>\n\n"
            "Ex:\n"
            "<code>/broadcast 🎟 Coupon du jour dispo ! Faites /coupon</code>",
            parse_mode=ParseMode.HTML
        )
        return

    text = parts[1].strip()
    chat_ids = get_all_users_chat_ids()

    sent = 0
    failed = 0
    removed = 0

    msg0 = await update.message.reply_text(f"📣 Envoi en cours… (0/{len(chat_ids)})")

    for i, cid in enumerate(chat_ids, start=1):
        try:
            await context.bot.send_message(chat_id=cid, text=text, parse_mode=ParseMode.HTML)
            sent += 1
        except Exception:
            failed += 1
            # si l'utilisateur a bloqué le bot, on supprime de la DB
            try:
                delete_user(cid)
                removed += 1
            except Exception:
                pass

        if i % 20 == 0:
            try:
                await msg0.edit_text(f"📣 Envoi en cours… ({i}/{len(chat_ids)})\n✅ {sent} | ❌ {failed} | 🧹 supprimés {removed}")
            except Exception:
                pass

        if BROADCAST_DELAY_SEC > 0:
            await asyncio.sleep(BROADCAST_DELAY_SEC)

    await msg0.edit_text(
        f"✅ Broadcast terminé.\n"
        f"👥 Total: {len(chat_ids)}\n"
        f"✅ Envoyés: {sent}\n"
        f"❌ Échecs: {failed}\n"
        f"🧹 Utilisateurs retirés (bot bloqué): {removed}"
    )

async def broadcast_sub_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    if not is_admin(update):
        await update.message.reply_text("⛔ Réservé à l’admin.")
        return

    raw = update.message.text or ""
    parts = raw.split(" ", 1)
    if len(parts) < 2 or not parts[1].strip():
        await update.message.reply_text(
            "Usage:\n"
            "<code>/broadcast_sub Votre message</code>\n\n"
            "Ex:\n"
            "<code>/broadcast_sub ✅ Coupon VIP dispo dans le bot !</code>",
            parse_mode=ParseMode.HTML
        )
        return

    text = parts[1].strip()
    chat_ids = get_active_subscribers_chat_ids()

    sent = 0
    failed = 0
    removed = 0

    msg0 = await update.message.reply_text(f"📣 Envoi abonnés actifs… (0/{len(chat_ids)})")

    for i, cid in enumerate(chat_ids, start=1):
        try:
            await context.bot.send_message(chat_id=cid, text=text, parse_mode=ParseMode.HTML)
            sent += 1
        except Exception:
            failed += 1
            try:
                delete_user(cid)
                removed += 1
            except Exception:
                pass

        if i % 20 == 0:
            try:
                await msg0.edit_text(f"📣 Envoi abonnés actifs… ({i}/{len(chat_ids)})\n✅ {sent} | ❌ {failed} | 🧹 supprimés {removed}")
            except Exception:
                pass

        if BROADCAST_DELAY_SEC > 0:
            await asyncio.sleep(BROADCAST_DELAY_SEC)

    await msg0.edit_text(
        f"✅ Broadcast abonnés terminé.\n"
        f"👥 Total: {len(chat_ids)}\n"
        f"✅ Envoyés: {sent}\n"
        f"❌ Échecs: {failed}\n"
        f"🧹 Utilisateurs retirés: {removed}"
    )

# =============================
# MATCHES
# =============================
async def send_matches_for_date(chat_id: int, context: ContextTypes.DEFAULT_TYPE, d: dt.date):
    await send_typing(context, chat_id)
    date = d.isoformat()
    season = season_from_date(d)

    ordered = list(FOOT_LEAGUES_FIXED.keys())
    any_found = False

    await context.bot.send_message(
        chat_id=chat_id,
        text=(
            f"📅 <b>Matchs</b> : {escape(date)} (saison {season})\n"
            f"⚽ Compétitions sélectionnées | max {MATCHES_PER_LEAGUE_MAX}/compétition\n\n"
            "💡 Cliquez sur <b>🔎 Analyser</b> sous un match."
        ),
        parse_mode=ParseMode.HTML
    )

    for league_name in ordered:
        league_id = FOOT_LEAGUES_FIXED.get(league_name)
        if not league_id:
            continue

        try:
            data = await apifootball_get("/fixtures", {"date": date, "league": league_id, "season": season})
            fx_list = data.get("response", [])
        except Exception:
            fx_list = []

        if not fx_list:
            continue

        fx_list = sorted(fx_list, key=kickoff_timestamp)[:MATCHES_PER_LEAGUE_MAX]
        any_found = True

        await context.bot.send_message(
            chat_id=chat_id,
            text=f"🏟️ <b>{escape(league_name)}</b>",
            parse_mode=ParseMode.HTML
        )

        for fx in fx_list:
            f = fx.get("fixture", {}) or {}
            teams = fx.get("teams", {}) or {}
            home = str((teams.get("home", {}) or {}).get("name", "?"))
            away = str((teams.get("away", {}) or {}).get("name", "?"))
            fid = f.get("id")
            hhmm = kickoff_hhmm(fx)
            if not isinstance(fid, int):
                continue

            msg = (
                f"• <b>{escape(hhmm)}</b> — {escape(home)} vs {escape(away)}\n"
                f"🆔 <code>{fid}</code>"
            )
            await context.bot.send_message(
                chat_id=chat_id,
                text=msg,
                parse_mode=ParseMode.HTML,
                reply_markup=analyze_button(fid)
            )

    if not any_found:
        await context.bot.send_message(chat_id=chat_id, text="Aucun match trouvé pour ces compétitions à cette date.")

# =============================
# ANALYSE (fixed thresholds)
# =============================
async def run_analysis(chat_id: int, context: ContextTypes.DEFAULT_TYPE, fixture_id: int):
    await send_typing(context, chat_id)
    loading = await context.bot.send_message(chat_id=chat_id, text="⏳ Analyse en cours…")

    try:
        fx = await apifootball_get("/fixtures", {"id": fixture_id})
        resp = fx.get("response") or []
        if not resp:
            await loading.edit_text("❌ Match introuvable.")
            return

        fx0 = resp[0]
        league = (fx0.get("league") or {}).get("name") or "Compétition"
        teams = fx0.get("teams") or {}
        home = teams.get("home") or {}
        away = teams.get("away") or {}
        home_id = home.get("id")
        away_id = away.get("id")
        home_name = home.get("name") or "Home"
        away_name = away.get("name") or "Away"

        if not (isinstance(home_id, int) and isinstance(away_id, int)):
            await loading.edit_text("❌ Données équipes manquantes.")
            return

        h2h = await apifootball_get("/fixtures/headtohead", {"h2h": f"{home_id}-{away_id}", "last": 10})
        h2h_list = (h2h.get("response") or [])[:10]

        recent_home = await apifootball_get("/fixtures", {"team": home_id, "last": 10, "status": "FT"})
        recent_away = await apifootball_get("/fixtures", {"team": away_id, "last": 10, "status": "FT"})
        recent_h = (recent_home.get("response") or [])[:10]
        recent_a = (recent_away.get("response") or [])[:10]

        sample = h2h_list + recent_h + recent_a
        if len(sample) < 6:
            await loading.edit_text("⚠️ Pas assez d’historique pour analyser ce match.")
            return

        numeric, ev_freq, ev_n = await collect_samples(sample, cap_stats=18, cap_events=12)

        corners = numeric.get("corners", [])
        yellow = numeric.get("yellow", [])
        shots_on_target = numeric.get("shots_on_target", [])
        offsides = numeric.get("offsides", [])
        fouls = numeric.get("fouls", [])
        shots_total = numeric.get("shots_total", [])

        lines: List[str] = []
        lines.append(f"⚽ <b>{escape(home_name)} vs {escape(away_name)}</b>")
        lines.append(f"🏆 <b>{escape(league)}</b>")
        lines.append(f"🧪 Base : <b>{len(sample)}</b> matchs (H2H + récents)")
        lines.append("")

        lines.append("📊 <b>Statistiques (pourcentages)</b>")
        if corners:
            lines.append(f"• Corners ≥ 6 : <b>{pct(prob_ge(corners, 6))}%</b> | Corners ≤ 11 : <b>{pct(prob_le(corners, 11))}%</b>")
        else:
            lines.append("• Corners : <i>indisponible</i>")

        if yellow:
            lines.append(f"• Cartons jaunes ≥ 2 : <b>{pct(prob_ge(yellow, 2))}%</b> | ≤ 5 : <b>{pct(prob_le(yellow, 5))}%</b>")
        else:
            lines.append("• Cartons jaunes : <i>indisponible</i>")

        if shots_on_target:
            lines.append(f"• Tirs cadrés ≥ 6 : <b>{pct(prob_ge(shots_on_target, 6))}%</b> | ≤ 11 : <b>{pct(prob_le(shots_on_target, 11))}%</b>")
        else:
            lines.append("• Tirs cadrés : <i>indisponible</i>")

        if offsides:
            lines.append(f"• Hors-jeu ≥ 2 : <b>{pct(prob_ge(offsides, 2))}%</b> | ≤ 5 : <b>{pct(prob_le(offsides, 5))}%</b>")
        else:
            lines.append("• Hors-jeu : <i>indisponible</i>")

        if fouls:
            lines.append(f"• Fautes ≥ 16 : <b>{pct(prob_ge(fouls, 16))}%</b> | ≤ 27 : <b>{pct(prob_le(fouls, 27))}%</b>")
        else:
            lines.append("• Fautes : <i>indisponible</i>")

        if shots_total:
            lines.append(f"• Tirs totaux ≥ 18 : <b>{pct(prob_ge(shots_total, 18))}%</b> | ≤ 26 : <b>{pct(prob_le(shots_total, 26))}%</b>")
        else:
            lines.append("• Tirs totaux : <i>indisponible</i>")

        lines.append("")
        lines.append("🎯 <b>Événements (OUI / NON)</b>")
        lines.append(f"📌 Échantillon événements : <b>{ev_n}</b> match(s)")

        def yn(label: str, p_yes: float) -> str:
            return f"• {escape(label)} : OUI <b>{pct(p_yes)}%</b> | NON <b>{pct(1.0 - p_yes)}%</b>"

        lines.append(yn("But temps additionnel 1ère mi-temps (45+)", ev_freq.get("stoppage_ht", 0.0)))
        lines.append(yn("But temps additionnel 2ème mi-temps (90+)", ev_freq.get("stoppage_ft", 0.0)))
        lines.append(yn("Expulsion / carton rouge", ev_freq.get("red_card", 0.0)))
        lines.append(yn("3 buts d’affilée (même équipe)", ev_freq.get("streak3", 0.0)))
        lines.append(yn("But entre 88’ et fin", ev_freq.get("goal_88", 0.0)))

        lines.append("")
        lines.append("🧠 <b>Prono (basé sur l’historique)</b>")
        probs = outcome_probs(sample)
        opts = [("Victoire domicile", probs["home"]), ("Match nul", probs["draw"]), ("Victoire extérieur", probs["away"])]
        opts.sort(key=lambda x: x[1], reverse=True)
        lines.append(f"• 1X2 le plus probable : <b>{escape(opts[0][0])}</b> — <b>{pct(opts[0][1])}%</b>")
        lines.append(f"• 2e option : <b>{escape(opts[1][0])}</b> — <b>{pct(opts[1][1])}%</b>")
        lines.append(f"• BTTS (les 2 équipes marquent) : <b>{pct(probs['btts'])}%</b>")

        lines.append("")
        lines.append("ℹ️ Stats souvent utilisées sur <b>1xBet</b>, mais valables ailleurs aussi.")
        lines.append(f"🔗 Canal abonnés : {escape(VIP_CHANNEL_LINK)}")

        await loading.edit_text("\n".join(lines), parse_mode=ParseMode.HTML)

    except Exception as e:
        await loading.edit_text(f"⚠️ Erreur analyse : <code>{escape(str(e))}</code>", parse_mode=ParseMode.HTML)

# =============================
# CALLBACKS
# =============================
async def callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    upsert_user(update)
    await q.answer()

    data = q.data or ""
    chat_id = update.effective_chat.id

    if data == "BACK_MENU":
        clear_states(context)
        await context.bot.send_message(chat_id=chat_id, text="📌 Menu :", reply_markup=main_menu_keyboard())
        try:
            await q.delete_message()
        except Exception:
            pass
        return

    if data in ("M_TODAY", "M_TOMORROW", "M_AFTER"):
        clear_states(context)
        base = now_paris().date()
        if data == "M_TODAY":
            d = base
        elif data == "M_TOMORROW":
            d = base + dt.timedelta(days=1)
        else:
            d = base + dt.timedelta(days=2)

        await q.edit_message_text("⏳ Chargement des matchs…", parse_mode=ParseMode.HTML)
        await send_matches_for_date(chat_id, context, d)
        return

    if data.startswith("AN_"):
        clear_states(context)
        fid_str = data.replace("AN_", "").strip()
        if not fid_str.isdigit():
            await context.bot.send_message(chat_id=chat_id, text="⚠️ ID match invalide.")
            return
        fid = int(fid_str)

        if not is_admin(update):
            sub = get_sub(chat_id)
            plan = sub["plan"] if sub else "FREE"
            ok, msg = can_analyze(chat_id, plan)
            if not ok:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"⛔ {escape(msg)}\n\n➡️ Passez à un abonnement : 💳 Abonnement",
                    parse_mode=ParseMode.HTML,
                    reply_markup=main_menu_keyboard()
                )
                return
            if plan == "FREE":
                mark_analyze_used_week(chat_id)
            else:
                mark_analyze_used_month(chat_id)

        await run_analysis(chat_id, context, fid)
        return

    if data == "CAP_RULES":
        clear_states(context)
        await q.edit_message_text(CAPITAL_RULES_TEXT, parse_mode=ParseMode.HTML, reply_markup=capital_inline_keyboard())
        return

    if data == "CAP_CALC":
        clear_states(context)
        context.user_data["awaiting_capital_amount"] = True
        await q.edit_message_text(CAPITAL_START_TEXT, parse_mode=ParseMode.HTML, reply_markup=capital_inline_keyboard())
        return

    if data == "SUB_HELP":
        clear_states(context)
        await q.edit_message_text(
            "📘 <b>Activation abonnement</b>\n\n"
            "• Choisissez un plan\n"
            "• Le bot vous donne un <b>CODE</b>\n"
            "• Sur FedaPay : collez le code dans <b>Référence</b>\n"
            "• Payez\n"
            "• Cliquez <b>✅ J’ai déjà payé</b> puis recolle le code\n\n"
            "🛑 Si vous êtes bloqué : /cancel",
            parse_mode=ParseMode.HTML,
            reply_markup=subscription_inline_keyboard(),
        )
        return

    if data.startswith("SUB_"):
        clear_states(context)
        plan = data.replace("SUB_", "").strip().upper()
        if plan not in ("STANDARD", "VIP", "VVIP"):
            await q.edit_message_text("❌ Plan invalide.")
            return

        pay_url = PAY_LINKS.get(plan)
        pay_code = make_payment_code(plan, chat_id)

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("💳 Payer maintenant", url=pay_url)],
            [InlineKeyboardButton("✅ J’ai déjà payé", callback_data=f"PAID_{plan}")],
            [InlineKeyboardButton("📘 Comment activer", callback_data="SUB_HELP")],
        ])

        context.user_data["expected_pay_code"] = pay_code
        context.user_data["expected_pay_plan"] = plan

        txt = (
            f"💳 <b>Abonnement {escape(plan)}</b>\n\n"
            "✅ <b>Votre code (à mettre dans Référence de paiement)</b> :\n"
            f"<code>{escape(pay_code)}</code>\n\n"
            "1) Cliquez <b>Payer maintenant</b>\n"
            "2) Sur FedaPay : collez le code dans <b>Référence</b>\n"
            "3) Payez\n"
            "4) Revenez → cliquez <b>✅ J’ai déjà payé</b>\n"
            "5) Collez le même code ici\n\n"
            "🛑 Si vous êtes bloqué : /cancel"
        )
        await q.edit_message_text(txt, parse_mode=ParseMode.HTML, reply_markup=kb)
        return

    if data.startswith("PAID_"):
        plan = data.replace("PAID_", "").strip().upper()
        context.user_data["awaiting_ref_plan"] = plan
        expected = context.user_data.get("expected_pay_code")
        txt = "✅ Envoyez maintenant votre <b>Référence de paiement</b> (le code).\n"
        if expected:
            txt += f"\n💡 Code attendu : <code>{escape(str(expected))}</code>"
        txt += "\n\n🛑 Si vous voulez annuler : /cancel"
        await q.edit_message_text(txt, parse_mode=ParseMode.HTML)
        return

# =============================
# TEXT HANDLER
# =============================
async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    chat_id = update.effective_chat.id
    msg = (update.message.text or "").strip()

    if msg in (
        "📅 Matchs", "🔎 Analyse", "🎟️ Coupon du jour", "💳 Abonnement",
        "💰 Capital", "✅ Statut", "❓ Aide"
    ) or msg.lower() == "stat":
        if not context.user_data.get("awaiting_capital_amount") and not context.user_data.get("awaiting_capital_code"):
            clear_states(context)

    if context.user_data.get("awaiting_capital_amount"):
        s = msg.replace(" ", "").replace(",", "").replace("f", "").replace("F", "")
        if not re.fullmatch(r"\d+(\.\d+)?", s):
            await update.message.reply_text("❌ Envoyez un montant valide (ex: 100000).")
            return
        amount = float(s)
        if amount <= 0:
            await update.message.reply_text("❌ Le capital doit être > 0.")
            return
        context.user_data["capital_amount"] = amount
        context.user_data["awaiting_capital_amount"] = False
        context.user_data["awaiting_capital_code"] = True
        await update.message.reply_text("✅ Capital reçu.\n➡️ Envoyez maintenant le <b>code</b> (2, 3, 5, 7...)", parse_mode=ParseMode.HTML)
        return

    if context.user_data.get("awaiting_capital_code"):
        if not msg.isdigit():
            await update.message.reply_text("❌ Envoyez un code valide (ex: 2, 3, 5, 7).")
            return
        code = int(msg)
        amount = float(context.user_data.get("capital_amount", 0.0))
        pctg = capital_percent_for_code(code)
        stake = amount * pctg
        clear_states(context)
        await update.message.reply_text(
            "✅ <b>Résultat</b>\n\n"
            f"💼 Capital : <b>{escape(format_amount(amount))}</b>\n"
            f"🔢 Code : <b>{code}</b>\n"
            f"📊 Pourcentage : <b>{int(pctg*100)}%</b>\n"
            f"🎯 Mise conseillée : <b>{escape(format_amount(stake))}</b>\n\n"
            "📌 Pour recommencer : 💰 Capital",
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_keyboard(),
        )
        return

    plan_wait = context.user_data.get("awaiting_ref_plan")
    if plan_wait:
        ref = msg.strip()
        tmp = await update.message.reply_text("⏳ Vérification du paiement…", parse_mode=ParseMode.HTML)
        try:
            ok, detail = await fedapay_is_paid_by_reference(ref)
            if not ok:
                await tmp.edit_text(
                    "❌ Paiement non confirmé.\n\n"
                    f"📌 Détail : {escape(detail)}\n\n"
                    "✅ Vérifiez la référence et réessayez.\n"
                    "🛑 Pour annuler : /cancel",
                    parse_mode=ParseMode.HTML
                )
                return
            exp = set_sub(chat_id, plan_wait, reference=ref, days=SUB_DURATION_DAYS)
            clear_states(context)
            await tmp.edit_text(
                f"✅ <b>Abonnement {escape(plan_wait)} activé</b>\n"
                f"⏳ Expire le : <b>{escape(exp.strftime('%Y-%m-%d'))}</b>\n\n"
                f"🔗 Canal : {escape(VIP_CHANNEL_LINK)}",
                parse_mode=ParseMode.HTML
            )
            await update.message.reply_text("📌 Menu :", reply_markup=main_menu_keyboard())
            return
        except Exception as e:
            await tmp.edit_text(f"⚠️ Erreur vérification : {escape(str(e))}", parse_mode=ParseMode.HTML)
            return

    low = msg.lower()

    if low == "stat":
        await update.message.reply_text("📌 Menu :", reply_markup=main_menu_keyboard())
        return

    if msg == "📅 Matchs":
        await update.message.reply_text("📅 Choisissez une date :", reply_markup=matches_inline_keyboard())
        return

    if msg == "🔎 Analyse":
        await update.message.reply_text(
            "🔎 <b>Analyse</b>\n\n"
            "1) Cliquez <b>📅 Matchs</b>\n"
            "2) Sous un match, cliquez <b>🔎 Analyser</b>\n\n"
            "🆓 FREE : limite / semaine\n"
            "💳 Abonnés : plus de limites\n",
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_keyboard()
        )
        return

    if msg == "🎟️ Coupon du jour":
        await coupon_cmd(update, context)
        return

    if msg == "💳 Abonnement":
        await abonnement_cmd(update, context)
        return

    if msg == "💰 Capital":
        await capital_cmd(update, context)
        return

    if msg == "✅ Statut":
        await status_cmd(update, context)
        return

    if msg == "❓ Aide":
        await help_cmd(update, context)
        return

    await update.message.reply_text(UNKNOWN_TEXT, reply_markup=main_menu_keyboard())

# =============================
# ERROR HANDLER
# =============================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    try:
        err = context.error
        print("ERROR:", repr(err))
    except Exception:
        pass

# =============================
# MAIN (with conflict backoff)
# =============================
async def post_init(app: Application):
    try:
        await app.bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass

def main():
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN manquant")

    init_db()

    app: Application = (
        ApplicationBuilder()
        .token(TELEGRAM_BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    # commands
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("cancel", cancel_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("abonnement", abonnement_cmd))
    app.add_handler(CommandHandler("coupon", coupon_cmd))
    app.add_handler(CommandHandler("capital", capital_cmd))

    # admin
    app.add_handler(CommandHandler("publier_coupon", publier_coupon_cmd))
    app.add_handler(CommandHandler("admin", admin_cmd))
    app.add_handler(CommandHandler("grant", grant_cmd))
    app.add_handler(CommandHandler("broadcast", broadcast_cmd))
    app.add_handler(CommandHandler("broadcast_sub", broadcast_sub_cmd))

    # callbacks + text
    app.add_handler(CallbackQueryHandler(callbacks))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    app.add_error_handler(error_handler)

    print("Bot lancé. Ctrl+C pour arrêter.")

    backoff = 3
    while True:
        try:
            app.run_polling(drop_pending_updates=True, close_loop=False)
            break
        except Conflict as e:
            print("Conflict (another instance running). Retrying…", e)
            import time
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)

if __name__ == "__main__":
    main()
