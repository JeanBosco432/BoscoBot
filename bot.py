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
# Load env (local). On Railway, vars come from dashboard
# =============================
load_dotenv()

# =============================
# ENV
# =============================
TELEGRAM_BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()

APIFOOTBALL_KEY = (os.getenv("APIFOOTBALL_KEY") or "").strip()
APIFOOTBALL_BASE = (os.getenv("APIFOOTBALL_BASE") or "").strip()
if not APIFOOTBALL_BASE:
    APIFOOTBALL_BASE = "https://v3.football.api-sports.io"
if not APIFOOTBALL_BASE.startswith(("http://", "https://")):
    APIFOOTBALL_BASE = "https://" + APIFOOTBALL_BASE

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

# Paris offset to avoid zoneinfo/tzdata issues
PARIS_TZ_HOURS = int(os.getenv("PARIS_TZ_HOURS", "1"))

# DB
DB_PATH = os.getenv("DB_PATH", "bot.db")

# Subscription duration
SUB_DURATION_DAYS = int(os.getenv("SUB_DURATION_DAYS", "30"))

# Plan limits
STANDARD_COUPONS_PER_DAY = int(os.getenv("STANDARD_COUPONS_PER_DAY", "1"))
VIP_COUPONS_PER_DAY = int(os.getenv("VIP_COUPONS_PER_DAY", "2"))

STANDARD_ANALYSES_PER_MONTH = int(os.getenv("STANDARD_ANALYSES_PER_MONTH", "5"))
VIP_ANALYSES_PER_MONTH = int(os.getenv("VIP_ANALYSES_PER_MONTH", "10"))

# FREE analyses/week
FREE_ANALYSES_PER_WEEK = int(os.getenv("FREE_ANALYSES_PER_WEEK", "5"))

VIP_CHANNEL_LINK = "https://t.me/+fo_0a8c5d_43ZThk"

# Payment links (FedaPay pages)
PAY_LINKS = {
    "STANDARD": "https://me.fedapay.com/k8TEq_Ni",
    "VIP": "https://me.fedapay.com/PZ5cxcPc",
    "VVIP": "https://me.fedapay.com/je65fOkF",
}

# How many matches to show per league
MATCHES_PER_LEAGUE_MAX = int(os.getenv("MATCHES_PER_LEAGUE_MAX", "10"))

# Analyse: fiabilité minimale + nombre de picks recommandés
ANALYSIS_MIN_CONF = float(os.getenv("ANALYSIS_MIN_CONF", "0.85"))  # 85%
ANALYSIS_MAX_PICKS = int(os.getenv("ANALYSIS_MAX_PICKS", "3"))     # 2-3 picks max (par défaut 3)
ANALYSIS_MAX_H2H = int(os.getenv("ANALYSIS_MAX_H2H", "10"))
ANALYSIS_MAX_RECENT = int(os.getenv("ANALYSIS_MAX_RECENT", "10"))
ANALYSIS_MIN_H2H = int(os.getenv("ANALYSIS_MIN_H2H", "3"))
ANALYSIS_MIN_RECENT = int(os.getenv("ANALYSIS_MIN_RECENT", "3"))

# Concurrency limit (API calls)
ANALYSIS_CONCURRENCY = int(os.getenv("ANALYSIS_CONCURRENCY", "5"))

# =============================
# FOOTBALL LEAGUES (IDs via API-FOOTBALL)
# Priorité: UEFA cups + grandes ligues + D2
# =============================
FOOT_LEAGUES_FIXED: Dict[str, int] = {
    # UEFA (priorité)
    "UEFA Champions League": 2,
    "UEFA Europa League": 3,
    "UEFA Europa Conference League": 848,

    # Top 5 + D2
    "Angleterre — Premier League": 39,
    "Angleterre — Championship (D2)": 40,

    "France — Ligue 1": 61,
    "France — Ligue 2 (D2)": 62,

    "Espagne — LaLiga": 140,
    "Espagne — Segunda (D2)": 141,

    "Italie — Serie A": 135,
    "Italie — Serie B (D2)": 136,

    "Allemagne — Bundesliga": 78,
    "Allemagne — Bundesliga 2 (D2)": 79,

    # Portugal + D2
    "Portugal — Primeira Liga": 94,
    "Portugal — Liga Portugal 2 (D2)": 95,

    # Pays-Bas + D2
    "Pays-Bas — Eredivisie": 88,
    "Pays-Bas — Eerste Divisie (D2)": 89,

    # Turquie + D2
    "Turquie — Süper Lig": 203,
    "Turquie — 1. Lig (D2)": 204,
}

LEAGUE_CACHE_FILE = os.getenv("LEAGUE_CACHE_FILE", "league_cache.json")

# =============================
# UI TEXTS
# =============================
BOT_NAME = "BoscoBot"

WELCOME_TEXT = (
    f"👋 <b>Bienvenue sur {BOT_NAME}</b> ⚽📊\n\n"
    "Je vous aide à :\n"
    "✅ Voir les matchs (boutons)\n"
    "✅ Lancer une <b>analyse</b> simple et fiable\n"
    "✅ Consulter le <b>coupon du jour</b>\n"
    "✅ Gérer votre <b>capital</b> (calcul automatique)\n"
    "✅ Accéder aux fonctionnalités selon votre abonnement\n\n"
    "📌 Tapez <b>stat</b> ou utilisez le menu en bas."
)

UNKNOWN_TEXT = (
    "🤖 Je n’ai pas reconnu.\n\n"
    "➡️ Tapez <b>stat</b> pour le menu\n"
    "ou utilisez /help."
)

HELP_TEXT = (
    "🧭 <b>Aide</b>\n\n"
    "📅 <b>Matchs</b>\n"
    "• Utilisez le bouton <b>📅 Matchs</b> (recommandé)\n"
    "• ou /matches demain | /matches 2026-03-01\n\n"
    "🔎 <b>Analyse</b>\n"
    "• Cliquez <b>🔎 Analyser</b> sous un match (recommandé)\n\n"
    "🎟️ <b>Coupon</b>\n"
    "• /coupon ou bouton <b>🎟️ Coupon du jour</b>\n\n"
    "💳 <b>Abonnement</b>\n"
    "• bouton <b>💳 Abonnement</b> → payer → <b>✅ J’ai déjà payé</b>\n"
    "• /status\n\n"
    "💰 <b>Capital</b>\n"
    "• bouton <b>💰 Capital</b>\n\n"
    "📌 Tapez <b>stat</b> pour le menu."
)

HOW_PAY_TEXT = (
    "💳 <b>Comment activer l’abonnement</b>\n\n"
    "1) Cliquez sur un plan (Standard / VIP / VVIP)\n"
    "2) Sur la page FedaPay, collez votre <b>code</b> dans <b>Référence de paiement</b>\n"
    "3) Payez\n"
    "4) Revenez ici → cliquez <b>✅ J’ai déjà payé</b>\n"
    "5) Collez le même code → activation automatique (30 jours)\n\n"
    "⚠️ Le code doit être exactement le même."
)

DESCRIPTION_TEXT = (
    f"ℹ️ <b>{BOT_NAME}</b>\n\n"
    "• Liste des matchs par ligue (boutons)\n"
    "• Analyse grand public : % sur seuils fixes + événements OUI/NON\n"
    "• 2–3 picks recommandés si fiabilité ≥ 85%\n"
    "• Coupon du jour (publié par l’admin)\n"
    "• Gestion du capital (mise conseillée)\n\n"
    "ℹ️ Ces stats sont souvent utilisées sur OneSpot, mais peuvent servir sur d’autres sites aussi."
)

CAPITAL_RULES_TEXT = (
    "💰 <b>Gestion du capital</b>\n\n"
    "Règle simple (selon le nombre de matchs du coupon) :\n"
    "• Code 2 → miser <b>15%</b> du capital\n"
    "• Code 3 → miser <b>10%</b> du capital\n"
    "• Code 5 → miser <b>8%</b> du capital\n"
    "• Code 7 et + → miser <b>5%</b> du capital\n\n"
    "📌 Le “code” = nombre de matchs sur le coupon.\n"
    "✅ Utilisez <b>Calculer</b> pour obtenir la mise exacte."
)

CAPITAL_START_TEXT = (
    "💰 <b>Calcul de mise</b>\n\n"
    "1) Envoyez votre <b>capital total</b> (ex: 100000)\n"
    "2) Puis envoyez le <b>code</b> (= nombre de matchs du coupon : 2, 3, 5, 7)\n\n"
    "➡️ Envoyez maintenant votre <b>capital</b>."
)

# =============================
# Helpers (time/date)
# =============================
def now_paris() -> dt.datetime:
    return dt.datetime.now(dt.UTC) + dt.timedelta(hours=PARIS_TZ_HOURS)

def season_from_date(d: dt.date) -> int:
    return d.year if d.month >= 8 else d.year - 1

def parse_date_text(text: str) -> Optional[dt.date]:
    t = (text or "").strip().lower()
    base = now_paris().date()
    if t in ("", "aujourd'hui", "aujourd’hui"):
        return base
    if t == "demain":
        return base + dt.timedelta(days=1)
    if t in ("apres-demain", "après-demain"):
        return base + dt.timedelta(days=2)
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", t):
        try:
            y, m, d = t.split("-")
            return dt.date(int(y), int(m), int(d))
        except Exception:
            return None
    return None

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

def escape(s: str) -> str:
    return html.escape(s or "")

def week_key_utc() -> str:
    d = dt.datetime.utcnow().date()
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
            week_key TEXT NOT NULL,
            analyses_used INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(chat_id, week_key)
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

def find_chat_id_by_username(username: str) -> Optional[int]:
    u = (username or "").strip()
    if u.startswith("@"):
        u = u[1:]
    if not u:
        return None
    with db() as con:
        row = con.execute("SELECT chat_id FROM users WHERE lower(username)=lower(?)", (u,)).fetchone()
    return int(row[0]) if row else None

# =============================
# Subscription / Access
# =============================
def is_admin(update: Update) -> bool:
    uid = update.effective_user.id if update.effective_user else 0
    return uid in ADMIN_IDS

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

def can_get_coupon(chat_id: int, plan: str) -> Tuple[bool, Optional[str]]:
    if plan in ("VVIP", "ADMIN"):
        return True, None

    today = now_paris().date().isoformat()
    with db() as con:
        row = con.execute("SELECT coupons_used FROM usage_day WHERE chat_id=? AND day=?", (chat_id, today)).fetchone()
        used = row[0] if row else 0

    limit = STANDARD_COUPONS_PER_DAY if plan == "STANDARD" else VIP_COUPONS_PER_DAY
    if used >= limit:
        return False, f"Limite coupon atteinte ({used}/{limit}) aujourd’hui."
    return True, None

def mark_coupon_used(chat_id: int):
    today = now_paris().date().isoformat()
    with db() as con:
        con.execute("""
        INSERT INTO usage_day(chat_id, day, coupons_used) VALUES(?,?,1)
        ON CONFLICT(chat_id, day) DO UPDATE SET coupons_used=coupons_used+1
        """, (chat_id, today))

def can_analyze(chat_id: int, plan: Optional[str]) -> Tuple[bool, Optional[str], str]:
    """
    Retour:
      ok, message, tier
      tier in {"free","standard","vip","vvip","admin"}
    """
    if plan in ("VVIP", "ADMIN"):
        return True, None, "vvip"

    # FREE (pas d'abonnement)
    if not plan:
        wkey = week_key_utc()
        with db() as con:
            row = con.execute(
                "SELECT analyses_used FROM usage_week WHERE chat_id=? AND week_key=?",
                (chat_id, wkey),
            ).fetchone()
            used = int(row[0]) if row else 0
        if used >= FREE_ANALYSES_PER_WEEK:
            return False, f"Limite FREE atteinte ({used}/{FREE_ANALYSES_PER_WEEK}) cette semaine.", "free"
        return True, None, "free"

    # STANDARD / VIP
    month = now_paris().strftime("%Y-%m")
    with db() as con:
        row = con.execute("SELECT analyses_used FROM usage_month WHERE chat_id=? AND month=?", (chat_id, month)).fetchone()
        used = int(row[0]) if row else 0

    limit = VIP_ANALYSES_PER_MONTH if plan == "VIP" else STANDARD_ANALYSES_PER_MONTH
    if used >= limit:
        return False, f"Limite analyses atteinte ({used}/{limit}) pour ce mois.", plan.lower()
    return True, None, plan.lower()

def mark_analyze_used(chat_id: int, tier: str):
    if tier == "free":
        wkey = week_key_utc()
        with db() as con:
            con.execute("""
            INSERT INTO usage_week(chat_id, week_key, analyses_used) VALUES(?,?,1)
            ON CONFLICT(chat_id, week_key) DO UPDATE SET analyses_used=analyses_used+1
            """, (chat_id, wkey))
        return

    month = now_paris().strftime("%Y-%m")
    with db() as con:
        con.execute("""
        INSERT INTO usage_month(chat_id, month, analyses_used) VALUES(?,?,1)
        ON CONFLICT(chat_id, month) DO UPDATE SET analyses_used=analyses_used+1
        """, (chat_id, month))

# =============================
# Telegram UX helpers
# =============================
async def send_typing(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    try:
        await context.bot.send_chat_action(chat_id=chat_id, action=ChatAction.TYPING)
    except Exception:
        pass

def reset_pending_flows(context: ContextTypes.DEFAULT_TYPE):
    # Fix: éviter que le bot reste bloqué dans "paiement" ou "capital"
    context.user_data.pop("awaiting_ref_plan", None)
    context.user_data.pop("expected_pay_code", None)
    context.user_data.pop("expected_pay_plan", None)
    context.user_data.pop("awaiting_capital_amount", None)
    context.user_data.pop("awaiting_capital_code", None)
    context.user_data.pop("capital_amount", None)

# =============================
# API-FOOTBALL wrapper
# =============================
async def apifootball_get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    if not APIFOOTBALL_KEY:
        raise RuntimeError("APIFOOTBALL_KEY manquante")
    headers = {"x-apisports-key": APIFOOTBALL_KEY}
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(f"{APIFOOTBALL_BASE}{path}", headers=headers, params=params)
        r.raise_for_status()
        return r.json()

# =============================
# FedaPay verification
# =============================
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
# Menus (Reply keyboard + inline)
# =============================
def main_menu_keyboard() -> ReplyKeyboardMarkup:
    kb = [
        [KeyboardButton("📅 Matchs"), KeyboardButton("🎟️ Coupon du jour")],
        [KeyboardButton("💳 Abonnement"), KeyboardButton("💰 Capital")],
        [KeyboardButton("✅ Statut"), KeyboardButton("❓ Aide")],
        [KeyboardButton("ℹ️ Description")],
    ]
    return ReplyKeyboardMarkup(kb, resize_keyboard=True)

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
        [InlineKeyboardButton("⬅️ Menu", callback_data="CAP_BACK")],
    ]
    return InlineKeyboardMarkup(kb)

def matches_date_inline_keyboard() -> InlineKeyboardMarkup:
    kb = [
        [InlineKeyboardButton("📅 Aujourd’hui", callback_data="MDAY_TODAY"),
         InlineKeyboardButton("📅 Demain", callback_data="MDAY_TOMORROW")],
        [InlineKeyboardButton("📅 Après-demain", callback_data="MDAY_AFTER")],
    ]
    return InlineKeyboardMarkup(kb)

def analyze_inline_keyboard(fixture_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔎 Analyser", callback_data=f"ANALYZE_{fixture_id}")]
    ])

# =============================
# Capital logic
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
# ANALYSE: métriques (seuils fixes demandés)
# =============================
# Mappings robustes pour éviter "indisponible"
METRICS = [
    (["Corner Kicks"], "corners", "Corners"),
    (["Yellow Cards"], "yellow", "Cartons jaunes"),
    (["Offsides"], "offsides", "Hors-jeu"),
    (["Shots on Goal", "Shots on Target"], "shots_on_target", "Tirs cadrés"),
    (["Total Shots", "Shots Total"], "shots", "Tirs (total)"),
    (["Fouls"], "fouls", "Fautes"),
]

# seuils FIXES (grand public)
RANGES = [
    ("Corners", "corners", 6, 11),
    ("Cartons jaunes", "yellow", 2, 5),
    ("Tirs cadrés", "shots_on_target", 6, 11),
    ("Hors-jeu", "offsides", 2, 5),
    ("Fautes", "fouls", 16, 27),
    ("Tirs (total)", "shots", 18, 26),
]

# événements OUI/NON demandés
EVENTS_LABELS = {
    "goal_stoppage_time": "But en temps additionnel (1ère ou 2ème mi-temps)",
    "expulsion": "Expulsion (rouge / 2e jaune)",
    "streak_3_goals_one_team": "3 buts d’affilée par une seule équipe",
    "goal_88_plus": "But entre 88’ et fin du match",
}

def _as_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return int(v)
    if isinstance(v, str):
        s = v.strip()
        m = re.search(r"-?\d+", s.replace("−", "-"))
        if not m:
            return None
        try:
            return int(m.group(0))
        except Exception:
            return None
    return None

def prob_ge(vals: List[int], th: int) -> Optional[float]:
    if not vals:
        return None
    return sum(1 for v in vals if v >= th) / len(vals)

def prob_le(vals: List[int], th: int) -> Optional[float]:
    if not vals:
        return None
    return sum(1 for v in vals if v <= th) / len(vals)

def _is_red_card_event(ev: dict) -> bool:
    if (ev.get("type") or "") != "Card":
        return False
    detail = (ev.get("detail") or "").lower()
    return ("red" in detail) or ("second yellow" in detail)

def analyze_events_one_match(events: List[dict]) -> Dict[str, bool]:
    res = {
        "expulsion": False,
        "streak_3_goals_one_team": False,
        "goal_stoppage_time": False,
        "goal_88_plus": False,
    }

    # expulsion
    for ev in events:
        if _is_red_card_event(ev):
            res["expulsion"] = True
            break

    goals = [e for e in events if (e.get("type") == "Goal")]

    # But 88+ (88->fin)
    for g in goals:
        minute = (g.get("time") or {}).get("elapsed")
        if isinstance(minute, int) and minute >= 88:
            res["goal_88_plus"] = True
            break

    # But en temps additionnel (mi-temps): elapsed 45/90 et extra >=1
    for g in goals:
        tm = g.get("time") or {}
        elapsed = tm.get("elapsed")
        extra = tm.get("extra")
        if isinstance(extra, int) and extra >= 1 and elapsed in (45, 90):
            res["goal_stoppage_time"] = True
            break

    # 3 buts d'affilée par une équipe
    def goal_sort_key(e: dict):
        tm = e.get("time") or {}
        elapsed = tm.get("elapsed")
        extra = tm.get("extra") or 0
        if not isinstance(elapsed, int):
            elapsed = 0
        if not isinstance(extra, int):
            extra = 0
        return elapsed * 10 + extra

    goals_sorted = sorted(goals, key=goal_sort_key)
    max_streak = 0
    cur = 0
    last_team = None
    for g in goals_sorted:
        team_id = (g.get("team") or {}).get("id")
        if team_id is None:
            continue
        if team_id == last_team:
            cur += 1
        else:
            cur = 1
            last_team = team_id
        max_streak = max(max_streak, cur)
    res["streak_3_goals_one_team"] = (max_streak >= 3)

    return res

async def fixture_events(fid: int) -> List[dict]:
    data = await apifootball_get("/fixtures/events", {"fixture": fid})
    return data.get("response") or []

async def fixture_stats(fid: int) -> Dict[str, Optional[int]]:
    """
    Retourne TOTAL match pour chaque metric_key (corners, yellow, shots, shots_on_target, offsides, fouls).
    """
    data = await apifootball_get("/fixtures/statistics", {"fixture": fid})
    resp = data.get("response") or []
    if len(resp) < 2:
        return {}

    # map type -> key
    type_to_key: Dict[str, str] = {}
    for api_types, key, _label in METRICS:
        for t in api_types:
            type_to_key[t.lower()] = key

    totals: Dict[str, int] = {key: 0 for _api_types, key, _label in METRICS}
    seen_any: Dict[str, bool] = {key: False for _api_types, key, _label in METRICS}

    for entry in resp[:2]:
        stats = entry.get("statistics") or []
        for it in stats:
            t = str(it.get("type") or "").lower()
            v = it.get("value")
            k = type_to_key.get(t)
            if not k:
                continue
            iv = _as_int(v)
            if iv is None:
                continue
            totals[k] = totals.get(k, 0) + iv
            seen_any[k] = True

    # garder seulement les clés réellement présentes
    out: Dict[str, int] = {}
    for k, ok in seen_any.items():
        if ok:
            out[k] = totals.get(k, 0)
    return out

async def collect_numeric_samples(fxs: List[dict]) -> Dict[str, List[int]]:
    out: Dict[str, List[int]] = {key: [] for _api_types, key, _label in METRICS}
    sem = asyncio.Semaphore(ANALYSIS_CONCURRENCY)

    async def one(fid: int):
        async with sem:
            try:
                st = await fixture_stats(fid)
                for _api_types, key, _label in METRICS:
                    if key in st and isinstance(st[key], int):
                        out[key].append(int(st[key]))
            except Exception:
                pass

    tasks = []
    for fx in fxs:
        fid = ((fx.get("fixture") or {}).get("id"))
        if isinstance(fid, int):
            tasks.append(asyncio.create_task(one(fid)))

    if tasks:
        await asyncio.gather(*tasks)
    return out

async def collect_event_frequencies(fxs: List[dict], cap: int = 10) -> Tuple[Dict[str, float], int]:
    keys = list(EVENTS_LABELS.keys())
    counts = {k: 0 for k in keys}
    n = 0
    sem = asyncio.Semaphore(ANALYSIS_CONCURRENCY)

    async def one(fid: int):
        nonlocal n
        async with sem:
            try:
                evs = await fixture_events(fid)
                flags = analyze_events_one_match(evs)
                for k in keys:
                    if flags.get(k):
                        counts[k] += 1
                n += 1
            except Exception:
                pass

    tasks = []
    for fx in fxs[:cap]:
        fid = ((fx.get("fixture") or {}).get("id"))
        if isinstance(fid, int):
            tasks.append(asyncio.create_task(one(fid)))

    if tasks:
        await asyncio.gather(*tasks)

    if n == 0:
        return ({k: 0.0 for k in keys}, 0)
    return ({k: counts[k] / n for k in keys}, n)

def extract_goals_total_from_fixture(fx: dict) -> Optional[int]:
    goals = fx.get("goals") or {}
    h = goals.get("home")
    a = goals.get("away")
    ih = _as_int(h)
    ia = _as_int(a)
    if ih is None or ia is None:
        return None
    return ih + ia

def prono_match_from_sample(sample: List[dict]) -> Optional[Tuple[str, float]]:
    """
    Donne UN prono (sans bookmaker) basé sur fréquences:
    - Over 1.5
    - BTTS Oui
    - Under 3.5
    Choisit celui avec la plus grosse proba.
    """
    totals: List[int] = []
    btts_yes = 0
    n = 0
    for fx in sample:
        goals = fx.get("goals") or {}
        h = _as_int(goals.get("home"))
        a = _as_int(goals.get("away"))
        if h is None or a is None:
            continue
        n += 1
        totals.append(h + a)
        if h > 0 and a > 0:
            btts_yes += 1
    if n == 0:
        return None

    p_over15 = sum(1 for t in totals if t >= 2) / n
    p_under35 = sum(1 for t in totals if t <= 3) / n
    p_btts = btts_yes / n

    candidates = [
        ("Over 1.5 buts (tendance)", p_over15),
        ("Under 3.5 buts (tendance)", p_under35),
        ("BTTS = OUI (tendance)", p_btts),
    ]
    best = max(candidates, key=lambda x: x[1])
    return best

# =============================
# ANALYSE principale (par fixture_id)
# =============================
async def run_analysis_for_fixture(chat_id: int, context: ContextTypes.DEFAULT_TYPE, fixture_id: int, update: Optional[Update] = None):
    await send_typing(context, chat_id)
    loading = await context.bot.send_message(chat_id=chat_id, text="⏳ Analyse en cours…")

    try:
        fx = await apifootball_get("/fixtures", {"id": fixture_id})
        resp = fx.get("response") or []
        if not resp:
            await loading.edit_text("❌ Match introuvable.")
            return

        fx0 = resp[0]
        teams = fx0.get("teams") or {}
        home = teams.get("home") or {}
        away = teams.get("away") or {}
        home_id, away_id = home.get("id"), away.get("id")
        home_name = home.get("name") or "Home"
        away_name = away.get("name") or "Away"
        league = (fx0.get("league") or {}).get("name") or "Compétition"

        if not (isinstance(home_id, int) and isinstance(away_id, int)):
            await loading.edit_text("❌ Données équipes manquantes.")
            return

        # H2H + recents
        h2h = await apifootball_get("/fixtures/headtohead", {"h2h": f"{home_id}-{away_id}", "last": ANALYSIS_MAX_H2H})
        h2h_list = (h2h.get("response") or [])[:ANALYSIS_MAX_H2H]

        recent_home = await apifootball_get("/fixtures", {"team": home_id, "last": ANALYSIS_MAX_RECENT, "status": "FT"})
        recent_away = await apifootball_get("/fixtures", {"team": away_id, "last": ANALYSIS_MAX_RECENT, "status": "FT"})
        recent_h = (recent_home.get("response") or [])[:ANALYSIS_MAX_RECENT]
        recent_a = (recent_away.get("response") or [])[:ANALYSIS_MAX_RECENT]

        if len(h2h_list) < ANALYSIS_MIN_H2H or len(recent_h) < ANALYSIS_MIN_RECENT or len(recent_a) < ANALYSIS_MIN_RECENT:
            await loading.edit_text(
                "⚠️ <b>Analyse impossible</b> (échantillon insuffisant)\n\n"
                f"• H2H : <b>{len(h2h_list)}</b> (min {ANALYSIS_MIN_H2H})\n"
                f"• Récents {escape(home_name)} : <b>{len(recent_h)}</b> (min {ANALYSIS_MIN_RECENT})\n"
                f"• Récents {escape(away_name)} : <b>{len(recent_a)}</b> (min {ANALYSIS_MIN_RECENT})\n",
                parse_mode=ParseMode.HTML
            )
            return

        sample = h2h_list + recent_h + recent_a

        # numeric stats
        numeric = await collect_numeric_samples(sample)

        # probabilities for fixed ranges
        stats_lines: List[str] = []
        candidate_picks: List[Tuple[float, str]] = []

        for label, key, over_th, under_th in RANGES:
            vals = numeric.get(key, [])
            if not vals:
                stats_lines.append(f"• {escape(label)} : <i>indisponible</i>")
                continue

            p_over = prob_ge(vals, over_th) or 0.0
            p_under = prob_le(vals, under_th) or 0.0
            stats_lines.append(
                f"• <b>{escape(label)}</b> : "
                f"+{over_th} = <b>{int(round(p_over*100))}%</b> | "
                f"-{under_th} = <b>{int(round(p_under*100))}%</b>"
            )

            # picks (une “face” peut être recommandée)
            if p_over >= ANALYSIS_MIN_CONF:
                candidate_picks.append((p_over, f"✅ <b>{escape(label)}</b> : <b>+{over_th}</b> — <b>{int(round(p_over*100))}%</b>"))
            if p_under >= ANALYSIS_MIN_CONF:
                candidate_picks.append((p_under, f"✅ <b>{escape(label)}</b> : <b>-{under_th}</b> — <b>{int(round(p_under*100))}%</b>"))

        # events yes/no
        ev_freq, ev_n = await collect_event_frequencies(sample, cap=10)

        # prono match
        pr = prono_match_from_sample(sample)

        # top picks
        candidate_picks.sort(key=lambda x: x[0], reverse=True)
        top_picks = candidate_picks[:ANALYSIS_MAX_PICKS]

        lines: List[str] = []
        lines.append(f"⚽ <b>{escape(home_name)} vs {escape(away_name)}</b>")
        lines.append(f"🏆 <b>{escape(league)}</b>")
        lines.append(
            f"🧪 Base : H2H <b>{len(h2h_list)}</b> | "
            f"Récents <b>{escape(home_name)}</b> <b>{len(recent_h)}</b> | "
            f"Récents <b>{escape(away_name)}</b> <b>{len(recent_a)}</b>"
        )
        lines.append("")

        lines.append("📊 <b>Statistiques clés (pourcentages)</b>")
        lines.extend(stats_lines)
        lines.append("")

        lines.append(f"✅ <b>Picks recommandés</b> (≥ {int(ANALYSIS_MIN_CONF*100)}%)")
        if top_picks:
            for _p, s in top_picks:
                lines.append(f"• {s}")
        else:
            lines.append("• ⚠️ Aucun pick assez fiable (≥ 85%) sur cet échantillon.")
        lines.append("")

        lines.append("🎯 <b>Événements (OUI / NON)</b>")
        lines.append(f"📌 Échantillon événements : <b>{ev_n}</b> match(s) (max 10)")
        for k, lbl in EVENTS_LABELS.items():
            p_yes = float(ev_freq.get(k, 0.0) or 0.0)
            p_no = max(0.0, 1.0 - p_yes)
            lines.append(
                f"• {escape(lbl)} : "
                f"OUI <b>{int(round(p_yes*100))}%</b> | "
                f"NON <b>{int(round(p_no*100))}%</b>"
            )
        lines.append("")

        lines.append("🧠 <b>Prono match (tendance, sans bookmaker)</b>")
        if pr:
            label, p = pr
            lines.append(f"• {escape(label)} : <b>{int(round(p*100))}%</b>")
        else:
            lines.append("• Indisponible (données buts insuffisantes).")
        lines.append("")

        lines.append("ℹ️ Ces stats sont souvent utilisées sur OneSpot, mais peuvent servir sur d’autres sites aussi.")
        lines.append(f"🔗 Canal : {escape(VIP_CHANNEL_LINK)}")

        await loading.edit_text("\n".join(lines), parse_mode=ParseMode.HTML)

    except Exception as e:
        await loading.edit_text(f"⚠️ Erreur analyse : <code>{escape(str(e))}</code>", parse_mode=ParseMode.HTML)

# =============================
# Commands
# =============================
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    reset_pending_flows(context)
    await update.message.reply_text(WELCOME_TEXT, parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())

async def menu_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    reset_pending_flows(context)
    await update.message.reply_text("📌 <b>Menu</b> :", parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    reset_pending_flows(context)
    await update.message.reply_text(HELP_TEXT, parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())

async def howpay_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    reset_pending_flows(context)
    await update.message.reply_text(HOW_PAY_TEXT, parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())

async def description_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    reset_pending_flows(context)
    await update.message.reply_text(DESCRIPTION_TEXT, parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())

async def capital_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    reset_pending_flows(context)
    await update.message.reply_text(
        "💰 <b>Capital</b>\n\nChoisissez une option :",
        parse_mode=ParseMode.HTML,
        reply_markup=capital_inline_keyboard(),
    )

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    reset_pending_flows(context)
    chat_id = update.effective_chat.id
    if is_admin(update):
        await update.message.reply_text("👑 <b>ADMIN</b> : accès total.", parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())
        return

    sub = get_sub(chat_id)
    if not sub:
        await update.message.reply_text(
            f"📦 Statut : <b>FREE</b>\n"
            f"🔎 Analyses FREE : <b>{FREE_ANALYSES_PER_WEEK}</b> / semaine",
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_keyboard(),
        )
        return

    plan = sub["plan"]
    exp = sub["expires_at"].strftime("%Y-%m-%d")
    txt = f"✅ Plan : <b>{escape(plan)}</b>\n⏳ Expire : <b>{escape(exp)}</b>\n\n🔗 Canal : {escape(VIP_CHANNEL_LINK)}"
    await update.message.reply_text(txt, parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())

async def abonnement_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    reset_pending_flows(context)
    await update.message.reply_text(
        "💳 <b>Choisissez votre abonnement</b> :",
        parse_mode=ParseMode.HTML,
        reply_markup=subscription_inline_keyboard(),
    )

async def matches_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Garde la commande /matches, mais on pousse surtout via boutons MDAY_*
    """
    upsert_user(update)
    reset_pending_flows(context)
    chat_id = update.effective_chat.id
    await send_typing(context, chat_id)

    arg = " ".join(context.args).strip() if context.args else ""
    d = parse_date_text(arg)
    if d is None:
        await update.message.reply_text("❌ Date invalide. Ex: /matches demain | /matches 2026-03-01", reply_markup=main_menu_keyboard())
        return
    await send_matches_for_date(update, context, d)

async def send_matches_for_date(update: Update, context: ContextTypes.DEFAULT_TYPE, d: dt.date):
    chat_id = update.effective_chat.id
    date = d.isoformat()
    season = season_from_date(d)

    try:
        leagues_all = dict(FOOT_LEAGUES_FIXED)

        lines_header = [
            f"📅 <b>Matchs</b> : {escape(date)} (saison {season})",
            f"⚽ max {MATCHES_PER_LEAGUE_MAX}/compétition",
            "",
        ]
        await context.bot.send_message(chat_id=chat_id, text="\n".join(lines_header), parse_mode=ParseMode.HTML)

        any_found = False
        # l'ordre dict conserve notre priorité (UEFA d'abord)
        for league_name, league_id in leagues_all.items():
            data = await apifootball_get("/fixtures", {"date": date, "league": league_id, "season": season})
            fx_list = data.get("response", [])
            if not fx_list:
                continue

            fx_list = sorted(fx_list, key=kickoff_timestamp)[:MATCHES_PER_LEAGUE_MAX]
            any_found = True

            await context.bot.send_message(chat_id=chat_id, text=f"🏟️ <b>{escape(league_name)}</b>", parse_mode=ParseMode.HTML)

            for fx in fx_list:
                f = fx.get("fixture", {}) or {}
                teams = fx.get("teams", {}) or {}
                home = str((teams.get("home", {}) or {}).get("name", "?"))
                away = str((teams.get("away", {}) or {}).get("name", "?"))
                fid = f.get("id")
                hhmm = kickoff_hhmm(fx)

                text = f"• <b>{escape(hhmm)}</b> — {escape(home)} vs {escape(away)}"
                if isinstance(fid, int):
                    text += f"\n🆔 <code>{fid}</code>"
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=analyze_inline_keyboard(int(fid)) if isinstance(fid, int) else None
                )

        if not any_found:
            await context.bot.send_message(chat_id=chat_id, text="Aucun match trouvé à cette date.", parse_mode=ParseMode.HTML)

    except Exception as e:
        await context.bot.send_message(chat_id=chat_id, text=f"⚠️ Erreur matches : {escape(str(e))}", parse_mode=ParseMode.HTML)

async def coupon_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    reset_pending_flows(context)
    chat_id = update.effective_chat.id

    sub = get_sub(chat_id)
    plan = "ADMIN" if is_admin(update) else (sub["plan"] if sub else None)
    if not plan:
        await update.message.reply_text("🔒 Coupon réservé aux abonnés. Faites <b>💳 Abonnement</b>.", parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())
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

    coupon_code, coupon_text = row
    mark_coupon_used(chat_id)

    txt = (
        f"🎟️ <b>Coupon du jour</b> ({escape(day)})\n\n"
        f"🔢 Code coupon : <b>{escape(coupon_code)}</b>\n\n"
        f"{coupon_text}\n\n"
        f"🔗 Canal : {escape(VIP_CHANNEL_LINK)}"
    )
    await update.message.reply_text(txt, parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())

# =============================
# Admin: publish coupon
# =============================
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

    with db() as con:
        users = con.execute("SELECT chat_id FROM users").fetchall()

    sent = 0
    for (cid,) in users:
        try:
            await context.bot.send_message(
                chat_id=cid,
                text="📌 <b>Coupon du jour disponible ✅</b>\n➡️ Cliquez sur <b>🎟️ Coupon du jour</b>",
                parse_mode=ParseMode.HTML
            )
            sent += 1
        except Exception:
            pass

    await update.message.reply_text(f"✅ Coupon publié. Notifications envoyées : {sent}")

# =============================
# Admin dashboard + grant
# =============================
async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    if not is_admin(update):
        await update.message.reply_text("⛔ Réservé à l’admin.")
        return

    now = dt.datetime.now(dt.UTC)
    today = now_paris().date().isoformat()
    month = now_paris().strftime("%Y-%m")
    wkey = week_key_utc()

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
        analyses_free_week = con.execute(
            "SELECT COALESCE(SUM(analyses_used),0) FROM usage_week WHERE week_key=?",
            (wkey,)
        ).fetchone()[0]
        last_activations = con.execute(
            "SELECT chat_id, plan, reference, created_at FROM subscription_log ORDER BY id DESC LIMIT 5"
        ).fetchall()

    lines = []
    lines.append("📊 <b>Dashboard Admin</b>")
    lines.append(f"👥 Utilisateurs : <b>{total_users}</b>")
    lines.append(f"💳 Abonnés actifs : <b>{active_subs}</b>")
    lines.append(f"🎟 Coupons consultés aujourd’hui : <b>{coupons_views_today}</b>")
    lines.append(f"🔎 Analyses (abonnés) ce mois : <b>{analyses_month}</b>")
    lines.append(f"🔎 Analyses FREE cette semaine : <b>{analyses_free_week}</b>")
    lines.append("")
    lines.append("🕘 <b>5 dernières activations</b>")
    if last_activations:
        for cid, plan, ref, created_at in last_activations:
            lines.append(f"• {cid} — {escape(plan)} — {escape(ref or '-')}")
    else:
        lines.append("• (aucune)")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)

async def grant_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /grant VIP @username 30
    /grant VIP 123456789 30
    """
    upsert_user(update)
    if not is_admin(update):
        await update.message.reply_text("⛔ Réservé à l’admin.")
        return

    if len(context.args) < 2:
        await update.message.reply_text(
            "Usage:\n"
            "<code>/grant VIP @username 30</code>\n"
            "<code>/grant VIP 123456789 30</code>\n\n"
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

    chat_id = None
    if target.isdigit():
        chat_id = int(target)
    else:
        chat_id = find_chat_id_by_username(target)

    if not chat_id:
        await update.message.reply_text("❌ Utilisateur introuvable. Donnez un chat_id ou un @username existant dans la DB.")
        return

    exp = set_sub(chat_id, plan, reference=f"ADMIN-GRANT-{update.effective_user.id}", days=days)
    await update.message.reply_text(
        f"✅ Accès accordé : <b>{escape(plan)}</b> à <code>{chat_id}</code>\n"
        f"⏳ Expire : <b>{escape(exp.strftime('%Y-%m-%d'))}</b>",
        parse_mode=ParseMode.HTML
    )

# =============================
# Callback handling (subscriptions + capital + matches + analyze)
# =============================
def make_payment_code(plan: str, chat_id: int) -> str:
    stamp = now_paris().strftime("%Y%m%d")
    return f"{plan}-BOSCO-{chat_id}-{stamp}"

async def callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    upsert_user(update)
    await q.answer()

    data = q.data or ""

    # ---- MATCH DATE buttons
    if data in ("MDAY_TODAY", "MDAY_TOMORROW", "MDAY_AFTER"):
        reset_pending_flows(context)
        base = now_paris().date()
        if data == "MDAY_TODAY":
            d = base
        elif data == "MDAY_TOMORROW":
            d = base + dt.timedelta(days=1)
        else:
            d = base + dt.timedelta(days=2)

        try:
            await q.edit_message_text("⏳ Chargement des matchs…", parse_mode=ParseMode.HTML)
        except Exception:
            pass
        # envoie la liste
        dummy_update = update  # même update
        await send_matches_for_date(dummy_update, context, d)
        return

    # ---- ANALYZE button
    if data.startswith("ANALYZE_"):
        reset_pending_flows(context)
        chat_id = update.effective_chat.id
        fid_str = data.replace("ANALYZE_", "").strip()
        if not fid_str.isdigit():
            await context.bot.send_message(chat_id=chat_id, text="❌ ID match invalide.")
            return
        fid = int(fid_str)

        # Access control
        if is_admin(update):
            ok = True
            tier = "admin"
        else:
            sub = get_sub(chat_id)
            plan = sub["plan"] if sub else None
            ok, msg, tier = can_analyze(chat_id, plan)
            if not ok:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"⛔ {escape(msg)}\n\n"
                        "➡️ Pour plus d’analyses : <b>💳 Abonnement</b>"
                    ),
                    parse_mode=ParseMode.HTML,
                    reply_markup=main_menu_keyboard(),
                )
                return

        # consommer quota (FREE ou abonné)
        if not is_admin(update):
            mark_analyze_used(chat_id, tier)

        await run_analysis_for_fixture(chat_id, context, fid, update=update)
        return

    # ---- CAPITAL
    if data == "CAP_BACK":
        reset_pending_flows(context)
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="📌 <b>Menu</b> :",
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_keyboard(),
        )
        try:
            await q.delete_message()
        except Exception:
            pass
        return

    if data == "CAP_RULES":
        reset_pending_flows(context)
        await q.edit_message_text(CAPITAL_RULES_TEXT, parse_mode=ParseMode.HTML, reply_markup=capital_inline_keyboard())
        return

    if data == "CAP_CALC":
        reset_pending_flows(context)
        context.user_data["awaiting_capital_amount"] = True
        await q.edit_message_text(CAPITAL_START_TEXT, parse_mode=ParseMode.HTML, reply_markup=capital_inline_keyboard())
        return

    # ---- SUBSCRIPTIONS
    if data == "SUB_HELP":
        reset_pending_flows(context)
        await q.edit_message_text(HOW_PAY_TEXT, parse_mode=ParseMode.HTML, reply_markup=subscription_inline_keyboard())
        return

    if data.startswith("SUB_"):
        reset_pending_flows(context)
        plan = data.replace("SUB_", "").strip().upper()
        if plan not in ("STANDARD", "VIP", "VVIP"):
            await q.edit_message_text("❌ Plan invalide.")
            return

        chat_id = update.effective_chat.id
        pay_url = PAY_LINKS.get(plan)
        pay_code = make_payment_code(plan, chat_id)

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("💳 Payer maintenant", url=pay_url)],
            [InlineKeyboardButton("✅ J’ai déjà payé", callback_data=f"PAID_{plan}")],
            [InlineKeyboardButton("📘 Comment activer", callback_data="SUB_HELP")],
        ])

        txt = (
            f"💳 <b>Abonnement {escape(plan)}</b>\n\n"
            "✅ <b>Votre code (à mettre dans Référence de paiement)</b> :\n"
            f"<code>{escape(pay_code)}</code>\n\n"
            "1) Cliquez <b>💳 Payer maintenant</b>\n"
            "2) Sur FedaPay, collez le code ci-dessus dans <b>Référence de paiement</b>\n"
            "3) Payez\n"
            "4) Revenez ici → cliquez <b>✅ J’ai déjà payé</b>\n"
            "5) Collez le même code\n"
        )
        context.user_data["expected_pay_code"] = pay_code
        context.user_data["expected_pay_plan"] = plan

        await q.edit_message_text(txt, parse_mode=ParseMode.HTML, reply_markup=kb)
        return

    if data.startswith("PAID_"):
        plan = data.replace("PAID_", "").strip().upper()
        context.user_data["awaiting_ref_plan"] = plan
        expected = context.user_data.get("expected_pay_code")
        txt = "✅ Parfait. Envoyez maintenant votre <b>Référence de paiement</b> (le code).\n"
        if expected:
            txt += f"\n💡 Code attendu : <code>{escape(str(expected))}</code>"
        await q.edit_message_text(txt, parse_mode=ParseMode.HTML)
        return

# =============================
# Text handler (menu buttons + paid flow + stat + capital flow)
# =============================
async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    chat_id = update.effective_chat.id
    msg = (update.message.text or "").strip()

    # =========================
    # CAPITAL FLOW
    # =========================
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

        await update.message.reply_text(
            "✅ Capital reçu.\n➡️ Envoyez maintenant le <b>code</b> (2, 3, 5, 7...)",
            parse_mode=ParseMode.HTML,
        )
        return

    if context.user_data.get("awaiting_capital_code"):
        s = msg.strip()
        if not s.isdigit():
            await update.message.reply_text("❌ Envoyez un code valide (ex: 2, 3, 5, 7).")
            return
        code = int(s)
        if code <= 0:
            await update.message.reply_text("❌ Le code doit être >= 1.")
            return

        amount = float(context.user_data.get("capital_amount", 0.0))
        pct = capital_percent_for_code(code)
        stake = amount * pct

        # reset capital flow
        context.user_data.pop("awaiting_capital_code", None)
        context.user_data.pop("capital_amount", None)

        await update.message.reply_text(
            "✅ <b>Résultat</b>\n\n"
            f"💼 Capital : <b>{escape(format_amount(amount))}</b>\n"
            f"🔢 Code : <b>{code}</b> match(s)\n"
            f"📊 Pourcentage : <b>{int(pct*100)}%</b>\n"
            f"🎯 Mise conseillée : <b>{escape(format_amount(stake))}</b>\n\n"
            "📌 Pour recommencer : cliquez <b>💰 Capital</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_keyboard(),
        )
        return

    # =========================
    # PAID FLOW: expecting ref
    # =========================
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
                    "➡️ Vérifiez la référence et réessayez.\n"
                    "💡 Astuce : revenez sur <b>💳 Abonnement</b> pour récupérer le code.",
                    parse_mode=ParseMode.HTML
                )
                return

            exp = set_sub(chat_id, plan_wait, reference=ref, days=SUB_DURATION_DAYS)
            context.user_data.pop("awaiting_ref_plan", None)

            txt = (
                f"✅ <b>Abonnement {escape(plan_wait)} activé</b>\n"
                f"⏳ Expire le : <b>{escape(exp.strftime('%Y-%m-%d'))}</b>\n\n"
                f"🔗 Canal : {escape(VIP_CHANNEL_LINK)}"
            )
            await tmp.edit_text(txt, parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())
            return

        except Exception as e:
            await tmp.edit_text(f"⚠️ Erreur vérification : {escape(str(e))}", parse_mode=ParseMode.HTML)
            return

    # =========================
    # Menu shortcuts
    # =========================
    low = msg.lower()

    if low == "stat":
        reset_pending_flows(context)
        await update.message.reply_text("📌 <b>Menu</b> :", parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())
        return

    if msg == "📅 Matchs":
        reset_pending_flows(context)
        await update.message.reply_text(
            "📅 <b>Choisissez une date</b> :",
            parse_mode=ParseMode.HTML,
            reply_markup=matches_date_inline_keyboard(),
        )
        return

    if msg == "🎟️ Coupon du jour":
        reset_pending_flows(context)
        await coupon_cmd(update, context)
        return

    if msg == "💳 Abonnement":
        reset_pending_flows(context)
        await abonnement_cmd(update, context)
        return

    if msg == "💰 Capital":
        reset_pending_flows(context)
        await capital_cmd(update, context)
        return

    if msg == "✅ Statut":
        reset_pending_flows(context)
        await status_cmd(update, context)
        return

    if msg == "❓ Aide":
        reset_pending_flows(context)
        await help_cmd(update, context)
        return

    if msg == "ℹ️ Description":
        reset_pending_flows(context)
        await description_cmd(update, context)
        return

    await update.message.reply_text(UNKNOWN_TEXT, parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())

# =============================
# Main
# =============================
def main():
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN manquant")

    init_db()

    app: Application = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    # commands
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("menu", menu_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("howpay", howpay_cmd))
    app.add_handler(CommandHandler("abonnement", abonnement_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("capital", capital_cmd))
    app.add_handler(CommandHandler("description", description_cmd))
    app.add_handler(CommandHandler("matches", matches_cmd))
    app.add_handler(CommandHandler("coupon", coupon_cmd))

    # admin
    app.add_handler(CommandHandler("publier_coupon", publier_coupon_cmd))
    app.add_handler(CommandHandler("admin", admin_cmd))
    app.add_handler(CommandHandler("grant", grant_cmd))

    # callbacks
    app.add_handler(CallbackQueryHandler(callbacks))

    # text
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    print("Bot lancé. Ctrl+C pour arrêter.")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
