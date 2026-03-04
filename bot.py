import os
import re
import json
import html
import sqlite3
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

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
SUB_DURATION_DAYS = 30

# Plan limits
STANDARD_COUPONS_PER_DAY = 1
VIP_COUPONS_PER_DAY = 2

STANDARD_ANALYSES_PER_MONTH = int(os.getenv("STANDARD_ANALYSES_PER_MONTH", "5"))
VIP_ANALYSES_PER_MONTH = 10

VIP_CHANNEL_LINK = "https://t.me/+fo_0a8c5d_43ZThk"

# Payment links (FedaPay pages)
PAY_LINKS = {
    "STANDARD": "https://me.fedapay.com/k8TEq_Ni",
    "VIP": "https://me.fedapay.com/PZ5cxcPc",
    "VVIP": "https://me.fedapay.com/je65fOkF",
}

# How many matches to show per league
MATCHES_PER_LEAGUE_MAX = int(os.getenv("MATCHES_PER_LEAGUE_MAX", "10"))

# =============================
# FOOTBALL LEAGUES (IDs via API-FOOTBALL)
# Keep all requested (5 major + UEFA cups + others you added)
# =============================
FOOT_LEAGUES_FIXED: Dict[str, int] = {
    "Premier League (Angleterre)": 39,
    "LaLiga (Espagne)": 140,
    "Ligue 1 (France)": 61,
    "Serie A (Italie)": 135,
    "Bundesliga (Allemagne)": 78,
    "UEFA Champions League": 2,
    "UEFA Europa League": 3,
    "UEFA Europa Conference League": 848,
}

FOOT_LEAGUES_DYNAMIC: List[Dict[str, str]] = [
    {"label": "Premier League (Russie)", "country": "Russia", "name": "Premier League"},
    {"label": "Eredivisie (Pays-Bas)", "country": "Netherlands", "name": "Eredivisie"},
    {"label": "Primeira Liga (Portugal)", "country": "Portugal", "name": "Primeira Liga"},
    {"label": "Süper Lig (Turquie)", "country": "Turkey", "name": "Süper Lig"},
    {"label": "Saudi Pro League (Arabie Saoudite)", "country": "Saudi-Arabia", "name": "Pro League"},
]

LEAGUE_CACHE_FILE = os.getenv("LEAGUE_CACHE_FILE", "league_cache.json")

# =============================
# UI TEXTS
# =============================
BOT_NAME = "BoscoBot"

WELCOME_TEXT = (
    f"👋 <b>Bienvenue sur {BOT_NAME}</b> ⚽📊\n\n"
    "Je vous aide à :\n"
    "✅ Voir les matchs (par date)\n"
    "✅ Consulter le <b>coupon du jour</b>\n"
    "✅ Gérer votre <b>capital</b> (règles simples)\n"
    "✅ Accéder aux fonctionnalités selon votre abonnement\n\n"
    "📌 Tapez <b>stat</b> ou cliquez sur <b>Menu</b>."
)

HELP_TEXT = (
    "🧭 <b>Aide</b>\n\n"
    "📅 <b>Matchs</b>\n"
    "• /matches\n"
    "• /matches demain\n"
    "• /matches apres-demain\n"
    "• /matches AAAA-MM-JJ\n\n"
    "🎟️ <b>Coupon</b>\n"
    "• /coupon\n\n"
    "💳 <b>Abonnement</b>\n"
    "• /abonnement → choisir un plan → payer → <b>J’ai déjà payé</b>\n"
    "• /status\n\n"
    "💰 <b>Capital</b>\n"
    "• /capital\n\n"
    "📌 Tapez <b>stat</b> pour le menu."
)

HOW_PAY_TEXT = (
    "💳 <b>Comment activer l’abonnement</b>\n\n"
    "1) Cliquez sur un plan (Standard / VIP / VVIP)\n"
    "2) Sur la page FedaPay, mettez votre code dans <b>Référence de paiement</b>\n"
    "   (ex: <code>VIP-BOSCO-1234</code>)\n"
    "3) Payez\n"
    "4) Revenez ici → cliquez <b>J’ai déjà payé</b>\n"
    "5) Envoyez le même code → activation automatique (30 jours)\n\n"
    "⚠️ Le code doit être <b>exactement</b> celui utilisé dans la référence."
)

UNKNOWN_TEXT = (
    "🤖 Je n’ai pas reconnu.\n\n"
    "➡️ Tapez <b>stat</b> pour le menu\n"
    "ou utilisez /help."
)

CAPITAL_TEXT = (
    "💰 <b>Gestion du capital</b>\n\n"
    "Règle simple (selon le nombre de matchs du coupon) :\n"
    "• Code 2 → miser <b>15%</b> du capital\n"
    "• Code 3 → miser <b>10%</b> du capital\n"
    "• Code 5 → miser <b>8%</b> du capital\n"
    "• Code 7 et + → miser <b>5%</b> du capital\n\n"
    "📌 Le “code” = nombre de matchs sur le coupon."
)

# =============================
# Helpers (time/date)
# =============================
def now_paris() -> dt.datetime:
    return dt.datetime.now(dt.UTC) + dt.timedelta(hours=PARIS_TZ_HOURS)

def season_from_date(d: dt.date) -> int:
    # Football seasons typically start around Aug
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
        CREATE TABLE IF NOT EXISTS coupon_day(
            day TEXT PRIMARY KEY,
            coupon_code TEXT NOT NULL,
            coupon_text TEXT NOT NULL,
            created_by INTEGER NOT NULL,
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
            last_name=excluded.last_name
        """, (
            chat_id,
            (u.id if u else None),
            (u.username if u else None),
            (u.first_name if u else None),
            (u.last_name if u else None),
            now_paris().isoformat(),
        ))

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

def set_sub(chat_id: int, plan: str) -> dt.datetime:
    exp = dt.datetime.now(dt.UTC) + dt.timedelta(days=SUB_DURATION_DAYS)
    with db() as con:
        con.execute("""
        INSERT INTO subscriptions(chat_id, plan, expires_at)
        VALUES(?,?,?)
        ON CONFLICT(chat_id) DO UPDATE SET plan=excluded.plan, expires_at=excluded.expires_at
        """, (chat_id, plan, exp.isoformat()))
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

def can_analyze(chat_id: int, plan: str) -> Tuple[bool, Optional[str]]:
    if plan in ("VVIP", "ADMIN"):
        return True, None

    month = now_paris().strftime("%Y-%m")
    with db() as con:
        row = con.execute("SELECT analyses_used FROM usage_month WHERE chat_id=? AND month=?", (chat_id, month)).fetchone()
        used = row[0] if row else 0

    limit = VIP_ANALYSES_PER_MONTH if plan == "VIP" else STANDARD_ANALYSES_PER_MONTH
    if used >= limit:
        return False, f"Limite analyses atteinte ({used}/{limit}) pour ce mois."
    return True, None

def mark_analyze_used(chat_id: int):
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
# League cache for dynamic leagues
# =============================
def load_league_cache() -> Dict[str, int]:
    try:
        if os.path.exists(LEAGUE_CACHE_FILE):
            with open(LEAGUE_CACHE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return {k: int(v) for k, v in data.items()}
    except Exception:
        pass
    return {}

def save_league_cache(cache: Dict[str, int]) -> None:
    try:
        with open(LEAGUE_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

async def resolve_league_id_dynamic(label: str, country: str, name: str, cache: Dict[str, int]) -> Optional[int]:
    if label in cache:
        return cache[label]
    data = await apifootball_get("/leagues", {"country": country, "name": name})
    resp = data.get("response", [])
    for item in resp:
        league = item.get("league") or {}
        lid = league.get("id")
        if isinstance(lid, int):
            cache[label] = lid
            save_league_cache(cache)
            return lid
    return None

# =============================
# FedaPay verification
# =============================
async def fedapay_is_paid_by_reference(ref: str) -> Tuple[bool, str]:
    """
    Verify payment using merchant_reference.
    Endpoint documented: /transactions/merchant/{reference}
    Success status usually 'approved'. :contentReference[oaicite:1]{index=1}
    """
    if not FEDAPAY_API_KEY:
        return False, "FEDAPAY_API_KEY manquante"

    reference = (ref or "").strip()
    if not reference:
        return False, "Référence vide"

    url = f"{FEDAPAY_BASE}/transactions/merchant/{httpx.utils.quote(reference, safe='')}"
    headers = {"Authorization": f"Bearer {FEDAPAY_API_KEY}"}

    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(url, headers=headers)
        if r.status_code == 404:
            return False, "Aucune transaction trouvée pour cette référence"
        r.raise_for_status()
        data = r.json()

    # Depending on API shape, transaction may be at top-level or under key
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
        [KeyboardButton("❓ Aide"), KeyboardButton("ℹ️ Description")],
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

# =============================
# Commands
# =============================
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    await update.message.reply_text(WELCOME_TEXT, parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())

async def menu_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    await update.message.reply_text("📌 <b>Menu</b> :", parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    await update.message.reply_text(HELP_TEXT, parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())

async def description_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    txt = (
        f"ℹ️ <b>{BOT_NAME}</b>\n\n"
        "• Liste des matchs par championnat et par date\n"
        "• Coupon du jour (publié par l’admin)\n"
        "• Gestion du capital (règles simples)\n"
        "• Abonnements Standard / VIP / VVIP\n\n"
        "📌 Tapez <b>stat</b> pour le menu."
    )
    await update.message.reply_text(txt, parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())

async def capital_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    await update.message.reply_text(CAPITAL_TEXT, parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    chat_id = update.effective_chat.id
    if is_admin(update):
        await update.message.reply_text("👑 <b>ADMIN</b> : accès total.", parse_mode=ParseMode.HTML)
        return

    sub = get_sub(chat_id)
    if not sub:
        await update.message.reply_text("❌ Aucun abonnement actif. Faites /abonnement.", parse_mode=ParseMode.HTML)
        return

    plan = sub["plan"]
    exp = sub["expires_at"].strftime("%Y-%m-%d")
    txt = f"✅ Plan : <b>{html.escape(plan)}</b>\n⏳ Expire : <b>{html.escape(exp)}</b>"

    if plan == "VIP":
        txt += f"\n🔗 Canal VIP : {VIP_CHANNEL_LINK}"

    await update.message.reply_text(txt, parse_mode=ParseMode.HTML)

async def abonnement_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    await update.message.reply_text(
        "💳 <b>Choisissez votre abonnement</b> :",
        parse_mode=ParseMode.HTML,
        reply_markup=subscription_inline_keyboard(),
    )

async def matches_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    chat_id = update.effective_chat.id
    await send_typing(context, chat_id)

    arg = " ".join(context.args).strip() if context.args else ""
    d = parse_date_text(arg)
    if d is None:
        await update.message.reply_text("❌ Date invalide. Ex: /matches demain | /matches 2026-03-01")
        return

    date = d.isoformat()
    season = season_from_date(d)

    try:
        cache = load_league_cache()
        leagues_all: Dict[str, int] = dict(FOOT_LEAGUES_FIXED)
        for it in FOOT_LEAGUES_DYNAMIC:
            lid = await resolve_league_id_dynamic(it["label"], it["country"], it["name"], cache)
            if isinstance(lid, int):
                leagues_all[it["label"]] = lid

        lines = [
            f"📅 <b>Matchs</b> : {html.escape(date)} (saison {season})",
            f"⚽ <b>Football</b> | max {MATCHES_PER_LEAGUE_MAX}/compétition",
            "",
        ]

        ordered_labels = list(FOOT_LEAGUES_FIXED.keys()) + [x["label"] for x in FOOT_LEAGUES_DYNAMIC]

        any_found = False
        for league_name in ordered_labels:
            league_id = leagues_all.get(league_name)
            if not league_id:
                continue

            data = await apifootball_get("/fixtures", {"date": date, "league": league_id, "season": season})
            fx_list = data.get("response", [])
            if not fx_list:
                continue

            fx_list = sorted(fx_list, key=kickoff_timestamp)[:MATCHES_PER_LEAGUE_MAX]
            any_found = True

            lines.append(f"🏟️ <b>{html.escape(league_name)}</b>")
            for fx in fx_list:
                f = fx.get("fixture", {}) or {}
                teams = fx.get("teams", {}) or {}
                home = str((teams.get("home", {}) or {}).get("name", "?"))
                away = str((teams.get("away", {}) or {}).get("name", "?"))
                fid = str(f.get("id", "?"))
                hhmm = kickoff_hhmm(fx)
                lines.append(f"• <b>{hhmm}</b> — {html.escape(home)} vs {html.escape(away)} — <code>{html.escape(fid)}</code>")
            lines.append("")

        if not any_found:
            lines.append("Aucun match trouvé pour ces compétitions à cette date.")

        for part in chunk_lines(lines):
            await update.message.reply_text(part, parse_mode=ParseMode.HTML)

    except Exception as e:
        await update.message.reply_text(f"⚠️ Erreur matches : {html.escape(str(e))}")

async def coupon_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    chat_id = update.effective_chat.id

    plan = "ADMIN" if is_admin(update) else (get_sub(chat_id)["plan"] if get_sub(chat_id) else None)
    if not plan:
        await update.message.reply_text("🔒 Coupon réservé aux abonnés. Faites /abonnement.", parse_mode=ParseMode.HTML)
        return

    ok, msg = can_get_coupon(chat_id, plan)
    if not ok:
        await update.message.reply_text(f"⛔ {html.escape(msg)}", parse_mode=ParseMode.HTML)
        return

    day = now_paris().date().isoformat()
    with db() as con:
        row = con.execute("SELECT coupon_code, coupon_text FROM coupon_day WHERE day=?", (day,)).fetchone()

    if not row:
        await update.message.reply_text("📭 Aucun coupon du jour pour le moment.", parse_mode=ParseMode.HTML)
        return

    coupon_code, coupon_text = row
    mark_coupon_used(chat_id)

    txt = (
        f"🎟️ <b>Coupon du jour</b> ({html.escape(day)})\n\n"
        f"🔢 Code coupon : <b>{html.escape(coupon_code)}</b>\n\n"
        f"{coupon_text}"
    )
    await update.message.reply_text(txt, parse_mode=ParseMode.HTML)

# =============================
# Admin: publish coupon
# =============================
async def publier_coupon_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    if not is_admin(update):
        await update.message.reply_text("⛔ Réservé à l’admin.")
        return

    # Usage: /publier_coupon CODE | Texte...
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

    # store
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

    # notify users
    with db() as con:
        users = con.execute("SELECT chat_id FROM users").fetchall()

    sent = 0
    for (cid,) in users:
        try:
            await context.bot.send_message(
                chat_id=cid,
                text=f"📌 <b>Coupon du jour disponible ✅</b>\n➡️ Faites /coupon",
                parse_mode=ParseMode.HTML
            )
            sent += 1
        except Exception:
            pass

    await update.message.reply_text(f"✅ Coupon publié. Notifications envoyées : {sent}")

# =============================
# Callback handling (subscriptions)
# =============================
async def callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    upsert_user(update)
    await q.answer()

    data = q.data or ""

    if data == "SUB_HELP":
        await q.edit_message_text(HOW_PAY_TEXT, parse_mode=ParseMode.HTML, reply_markup=subscription_inline_keyboard())
        return

    if data.startswith("SUB_"):
        plan = data.replace("SUB_", "").strip().upper()
        if plan not in ("STANDARD", "VIP", "VVIP"):
            await q.edit_message_text("❌ Plan invalide.")
            return

        pay_url = PAY_LINKS.get(plan)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("💳 Payer maintenant", url=pay_url)],
            [InlineKeyboardButton("✅ J’ai déjà payé", callback_data=f"PAID_{plan}")],
            [InlineKeyboardButton("📘 Comment activer", callback_data="SUB_HELP")],
        ])

        txt = (
            f"💳 <b>Abonnement {html.escape(plan)}</b>\n\n"
            "1) Cliquez <b>💳 Payer maintenant</b>\n"
            "2) Sur FedaPay, mettez votre code dans <b>Référence de paiement</b>\n"
            "3) Payez\n"
            "4) Revenez ici → cliquez <b>✅ J’ai déjà payé</b>\n"
        )
        await q.edit_message_text(txt, parse_mode=ParseMode.HTML, reply_markup=kb)
        return

    if data.startswith("PAID_"):
        plan = data.replace("PAID_", "").strip().upper()
        context.user_data["awaiting_ref_plan"] = plan
        await q.edit_message_text(
            "✅ Parfait. Envoyez maintenant votre <b>Référence de paiement</b> (le code que vous avez saisi).",
            parse_mode=ParseMode.HTML
        )
        return

# =============================
# Text handler (menu buttons + paid flow + "stat")
# =============================
async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update)
    chat_id = update.effective_chat.id
    msg = (update.message.text or "").strip()

    # Paid flow: expecting ref
    plan_wait = context.user_data.get("awaiting_ref_plan")
    if plan_wait:
        ref = msg.strip()
        tmp = await update.message.reply_text("⏳ Vérification du paiement…", parse_mode=ParseMode.HTML)
        try:
            ok, detail = await fedapay_is_paid_by_reference(ref)
            if not ok:
                await tmp.edit_text(
                    "❌ Paiement non confirmé.\n\n"
                    f"📌 Détail : {html.escape(detail)}\n\n"
                    "Vérifiez la référence et réessayez.",
                    parse_mode=ParseMode.HTML
                )
                return

            exp = set_sub(chat_id, plan_wait)
            context.user_data.pop("awaiting_ref_plan", None)

            txt = (
                f"✅ <b>Abonnement {html.escape(plan_wait)} activé</b>\n"
                f"⏳ Expire le : <b>{html.escape(exp.strftime('%Y-%m-%d'))}</b>\n"
            )
            if plan_wait == "VIP":
                txt += f"\n🔗 Canal VIP : {VIP_CHANNEL_LINK}"

            await tmp.edit_text(txt, parse_mode=ParseMode.HTML)
            return

        except Exception as e:
            await tmp.edit_text(f"⚠️ Erreur vérification : {html.escape(str(e))}", parse_mode=ParseMode.HTML)
            return

    # Menu shortcuts
    low = msg.lower()

    if low == "stat":
        await update.message.reply_text("📌 <b>Menu</b> :", parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())
        return

    if msg == "📅 Matchs":
        await update.message.reply_text("📅 Choisissez : /matches | /matches demain | /matches AAAA-MM-JJ")
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

    if msg == "❓ Aide":
        await help_cmd(update, context)
        return

    if msg == "ℹ️ Description":
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
    app.add_handler(CommandHandler("howpay", lambda u, c: u.message.reply_text(HOW_PAY_TEXT, parse_mode=ParseMode.HTML)))
    app.add_handler(CommandHandler("abonnement", abonnement_cmd))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("capital", capital_cmd))
    app.add_handler(CommandHandler("description", description_cmd))
    app.add_handler(CommandHandler("matches", matches_cmd))
    app.add_handler(CommandHandler("coupon", coupon_cmd))

    # admin
    app.add_handler(CommandHandler("publier_coupon", publier_coupon_cmd))

    # callbacks
    app.add_handler(CallbackQueryHandler(callbacks))

    # text
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    print("Bot lancé. Ctrl+C pour arrêter.")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()