import os
import re
import json
import hmac
import base64
import hashlib
import secrets
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List, Tuple

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# =========================
# CONFIG
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "")
APIFOOTBALL_KEY = os.getenv("APIFOOTBALL_KEY")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL")  # ex: https://xxx.up.railway.app

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN manquant.")
if not APIFOOTBALL_KEY:
    raise RuntimeError("APIFOOTBALL_KEY manquant.")
if not PUBLIC_BASE_URL or not PUBLIC_BASE_URL.startswith("http"):
    raise RuntimeError("PUBLIC_BASE_URL manquant ou invalide (doit commencer par http/https).")

ADMIN_IDS = set()
for x in re.split(r"[,\s]+", ADMIN_IDS_RAW.strip()):
    if x.strip().isdigit():
        ADMIN_IDS.add(int(x.strip()))

UTC = timezone.utc

DATA_DIR = os.getenv("DATA_DIR", "data")
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "boscobot.db")

API_FOOTBALL_BASE = "https://v3.football.api-sports.io"

TIERS = ["FREE", "STANDARD", "VIP1", "VVIP"]

# Prix (info seulement, pas de paiement automatique ici)
PRICES_FCFA = {
    "STANDARD": 3599,
    "VIP1": 6999,
    "VVIP": 12999,
}

# Telegram group link (VVIP)
VIP_GROUP_LINK = "https://t.me/+fo_0a8c5d_43ZThk"

# =========================
# DB
# =========================
def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con

def init_db():
    con = db()
    cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        first_name TEXT,
        created_at TEXT NOT NULL
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS subscriptions (
        user_id INTEGER PRIMARY KEY,
        tier TEXT NOT NULL,
        expires_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS codes (
        code TEXT PRIMARY KEY,
        tier TEXT NOT NULL,
        duration_days INTEGER NOT NULL,
        max_uses INTEGER NOT NULL,
        uses INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL,
        note TEXT
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS coupons (
        day TEXT NOT NULL,               -- YYYY-MM-DD
        tier TEXT NOT NULL,              -- STANDARD/VIP1/VVIP
        content TEXT NOT NULL,
        PRIMARY KEY (day, tier)
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS capital (
        user_id INTEGER PRIMARY KEY,
        amount REAL NOT NULL,
        updated_at TEXT NOT NULL
    )
    """)
    con.commit()
    con.close()

def now_iso() -> str:
    return datetime.now(UTC).isoformat()

def today_str() -> str:
    return datetime.now(UTC).date().isoformat()

def upsert_user(u):
    con = db()
    cur = con.cursor()
    cur.execute("""
        INSERT INTO users(user_id, username, first_name, created_at)
        VALUES(?,?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET
            username=excluded.username,
            first_name=excluded.first_name
    """, (u.id, u.username or "", u.first_name or "", now_iso()))
    con.commit()
    con.close()

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def get_subscription(user_id: int) -> Tuple[str, Optional[datetime]]:
    """Return (tier, expires_dt) where tier is FREE if none/expired."""
    if is_admin(user_id):
        return ("VVIP", datetime(2099, 1, 1, tzinfo=UTC))  # admin has all

    con = db()
    cur = con.cursor()
    cur.execute("SELECT tier, expires_at FROM subscriptions WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    con.close()
    if not row:
        return ("FREE", None)

    tier = row["tier"]
    expires_at = datetime.fromisoformat(row["expires_at"])
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=UTC)

    if expires_at <= datetime.now(UTC):
        return ("FREE", None)
    return (tier, expires_at)

def set_subscription(user_id: int, tier: str, days: int):
    expires_at = datetime.now(UTC) + timedelta(days=days)
    con = db()
    cur = con.cursor()
    cur.execute("""
        INSERT INTO subscriptions(user_id, tier, expires_at, updated_at)
        VALUES(?,?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET
            tier=excluded.tier,
            expires_at=excluded.expires_at,
            updated_at=excluded.updated_at
    """, (user_id, tier, expires_at.isoformat(), now_iso()))
    con.commit()
    con.close()

def cleanup_expired():
    con = db()
    cur = con.cursor()
    cur.execute("DELETE FROM subscriptions WHERE expires_at <= ?", (datetime.now(UTC).isoformat(),))
    con.commit()
    con.close()

def create_code(tier: str, duration_days: int, max_uses: int, note: str = "") -> str:
    code = secrets.token_urlsafe(10).replace("-", "").replace("_", "")
    # rendre plus lisible (lettres+chiffres)
    code = code[:12].upper()

    con = db()
    cur = con.cursor()
    cur.execute("""
        INSERT INTO codes(code, tier, duration_days, max_uses, uses, created_at, note)
        VALUES(?,?,?,?,?,?,?)
    """, (code, tier, duration_days, max_uses, 0, now_iso(), note))
    con.commit()
    con.close()
    return code

def redeem_code(user_id: int, code: str) -> Tuple[bool, str]:
    code = code.strip().upper()
    con = db()
    cur = con.cursor()
    cur.execute("SELECT code, tier, duration_days, max_uses, uses FROM codes WHERE code=?", (code,))
    row = cur.fetchone()
    if not row:
        con.close()
        return (False, "Code invalide.")

    if row["uses"] >= row["max_uses"]:
        con.close()
        return (False, "Ce code a déjà été utilisé (limite atteinte).")

    tier = row["tier"]
    days = int(row["duration_days"])

    # increment uses
    cur.execute("UPDATE codes SET uses = uses + 1 WHERE code=?", (code,))
    con.commit()
    con.close()

    set_subscription(user_id, tier, days)
    return (True, f"✅ Abonnement activé : {tier} pour {days} jours.")

# =========================
# API-FOOTBALL
# =========================
async def af_get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    headers = {"x-apisports-key": APIFOOTBALL_KEY}
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(f"{API_FOOTBALL_BASE}{path}", headers=headers, params=params)
        r.raise_for_status()
        return r.json()

# League IDs list (vous pourrez enrichir)
# Objectif: couvrir "top" + D2 + coupes demandées.
LEAGUE_IDS = [
    # England
    39,   # Premier League
    40,   # Championship (D2)
    # Spain
    140,  # La Liga
    141,  # Segunda División (D2)
    143,  # Copa del Rey
    # Italy
    135,  # Serie A
    136,  # Serie B (D2)
    # Germany
    78,   # Bundesliga
    79,   # 2. Bundesliga (D2)
    # France
    61,   # Ligue 1
    62,   # Ligue 2
    # Europe
    2,    # UCL
    3,    # UEL
    848,  # UECL (souvent 848)
]

async def get_fixtures_by_date(date_yyyy_mm_dd: str) -> List[Dict[str, Any]]:
    # On récupère "tous" les fixtures du jour, puis on filtre sur LEAGUE_IDS
    data = await af_get("/fixtures", {"date": date_yyyy_mm_dd})
    items = data.get("response", [])
    filtered = []
    for fx in items:
        league_id = fx.get("league", {}).get("id")
        if league_id in LEAGUE_IDS:
            filtered.append(fx)
    return filtered

async def get_fixture_stats(fixture_id: int) -> Dict[str, Any]:
    stats = await af_get("/fixtures/statistics", {"fixture": fixture_id})
    preds = None
    try:
        preds = await af_get("/predictions", {"fixture": fixture_id})
    except Exception:
        preds = None
    return {"statistics": stats, "predictions": preds}

def fmt_fixture_line(fx: Dict[str, Any]) -> str:
    league = fx["league"]["name"]
    country = fx["league"].get("country", "")
    home = fx["teams"]["home"]["name"]
    away = fx["teams"]["away"]["name"]
    dt = fx["fixture"]["date"]
    # dt is ISO
    try:
        d = datetime.fromisoformat(dt.replace("Z", "+00:00")).astimezone(UTC)
        hhmm = d.strftime("%H:%M UTC")
    except Exception:
        hhmm = "heure ?"
    return f"{hhmm} — {home} vs {away} ({country} • {league})"

def extract_stat(team_stats: List[Dict[str, Any]], name: str) -> Optional[str]:
    for s in team_stats:
        if s.get("type", "").lower() == name.lower():
            v = s.get("value")
            return str(v) if v is not None else None
    return None

def pick_team_stats(statistics_payload: Dict[str, Any]) -> Tuple[str, List[Dict[str, Any]], str, List[Dict[str, Any]]]:
    resp = statistics_payload.get("response", [])
    if len(resp) < 2:
        return ("", [], "", [])
    t1 = resp[0].get("team", {}).get("name", "Home")
    s1 = resp[0].get("statistics", [])
    t2 = resp[1].get("team", {}).get("name", "Away")
    s2 = resp[1].get("statistics", [])
    return (t1, s1, t2, s2)

# =========================
# UI / MENUS
# =========================
def main_menu_kb(user_id: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("📊 Analyses (matchs)", callback_data="menu:analysis")],
        [InlineKeyboardButton("🎟 Coupon du jour", callback_data="menu:coupon")],
        [InlineKeyboardButton("💰 Gestion du capital", callback_data="menu:capital")],
        [InlineKeyboardButton("⭐ Abonnements", callback_data="menu:sub")],
        [InlineKeyboardButton("ℹ️ Aide", callback_data="menu:help")],
    ]
    if is_admin(user_id):
        rows.append([InlineKeyboardButton("🛠 Admin", callback_data="menu:admin")])
    return InlineKeyboardMarkup(rows)

def subs_menu_kb(user_id: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("🔑 Entrer un code d’abonnement", callback_data="sub:redeem")],
        [InlineKeyboardButton("📌 Voir mon statut", callback_data="sub:status")],
        [InlineKeyboardButton("⬅️ Retour", callback_data="menu:home")],
    ]
    return InlineKeyboardMarkup(rows)

def admin_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Générer code STANDARD (30j)", callback_data="admin:gen:STANDARD")],
        [InlineKeyboardButton("➕ Générer code VIP1 (30j)", callback_data="admin:gen:VIP1")],
        [InlineKeyboardButton("➕ Générer code VVIP (30j)", callback_data="admin:gen:VVIP")],
        [InlineKeyboardButton("🎟 Définir coupon du jour", callback_data="admin:setcoupon")],
        [InlineKeyboardButton("📣 Broadcast message", callback_data="admin:broadcast")],
        [InlineKeyboardButton("⬅️ Retour", callback_data="menu:home")],
    ])

def capital_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✍️ Définir mon capital", callback_data="cap:set")],
        [InlineKeyboardButton("🧮 Calculer une mise", callback_data="cap:calc")],
        [InlineKeyboardButton("⬅️ Retour", callback_data="menu:home")],
    ])

def help_text() -> str:
    return (
        "📌 <b>BOSCOBOT — Guide rapide</b>\n\n"
        "✅ <b>Analyses</b>\n"
        "• Cliquez <i>Analyses (matchs)</i> → vous voyez la liste des matchs du jour.\n"
        "• Cliquez sur un match → vous obtenez les stats + infos utiles.\n\n"
        "✅ <b>Coupon du jour</b>\n"
        "• Standard : 1 coupon (cote 2) / jour\n"
        "• VIP1 : 1–2 coupons / jour\n"
        "• VVIP : tous les coupons + accès communauté Telegram\n\n"
        "✅ <b>Abonnement par code</b>\n"
        "• Après paiement (hors bot), le client reçoit un code.\n"
        "• Il clique <i>Entrer un code</i> et le bot active l’accès pendant 30 jours.\n"
        "• L’accès est retiré automatiquement à expiration.\n\n"
        "✅ <b>Gestion du capital</b>\n"
        "• Définissez votre capital puis calculez vos mises.\n\n"
        "Besoin d’aide ? Cliquez ici : <i>Aide</i>."
    )

# =========================
# STATE (simple)
# =========================
WAITING_CODE: Dict[int, str] = {}       # user_id -> "redeem"
WAITING_CAPITAL: Dict[int, str] = {}    # user_id -> "set"
WAITING_ADMIN_COUPON: Dict[int, str] = {}   # admin_id -> tier waiting
WAITING_ADMIN_BROADCAST: Dict[int, str] = {}

# =========================
# ACCESS CONTROL
# =========================
def tier_rank(tier: str) -> int:
    return {"FREE": 0, "STANDARD": 1, "VIP1": 2, "VVIP": 3}.get(tier, 0)

def require_tier(user_id: int, needed: str) -> bool:
    if is_admin(user_id):
        return True
    t, exp = get_subscription(user_id)
    return tier_rank(t) >= tier_rank(needed)

def format_status(user_id: int) -> str:
    if is_admin(user_id):
        return "👑 <b>ADMIN</b> — accès total (illimité)."
    tier, exp = get_subscription(user_id)
    if tier == "FREE":
        return "🔒 <b>FREE</b> — aucun abonnement actif.\n➡️ Activez un code pour accéder aux options payantes."
    exp_str = exp.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC") if exp else "?"
    return f"✅ <b>{tier}</b> — actif jusqu’au <b>{exp_str}</b>"

# =========================
# COUPONS
# =========================
def set_coupon(day: str, tier: str, content: str):
    con = db()
    cur = con.cursor()
    cur.execute("""
        INSERT INTO coupons(day, tier, content)
        VALUES(?,?,?)
        ON CONFLICT(day, tier) DO UPDATE SET content=excluded.content
    """, (day, tier, content))
    con.commit()
    con.close()

def get_coupon(day: str, tier: str) -> Optional[str]:
    con = db()
    cur = con.cursor()
    cur.execute("SELECT content FROM coupons WHERE day=? AND tier=?", (day, tier))
    row = cur.fetchone()
    con.close()
    return row["content"] if row else None

def coupon_for_user(user_id: int) -> str:
    day = today_str()
    user_tier, _ = get_subscription(user_id)
    if is_admin(user_id):
        user_tier = "VVIP"

    if user_tier == "FREE":
        return (
            "🔒 <b>Coupon du jour</b>\n\n"
            "Vous n’avez pas d’abonnement actif.\n"
            "➡️ Allez dans <b>Abonnements</b> puis <b>Entrer un code</b>."
        )

    lines = [f"🎟 <b>Coupon du jour</b> — {day}\n"]
    if user_tier == "STANDARD":
        c = get_coupon(day, "STANDARD")
        lines.append("⭐ <b>STANDARD</b>\n" + (c or "Aucun coupon défini aujourd’hui."))
    elif user_tier == "VIP1":
        c1 = get_coupon(day, "STANDARD")
        c2 = get_coupon(day, "VIP1")
        lines.append("⭐ <b>VIP1</b>\n")
        lines.append("• Coupon 1:\n" + (c1 or "Non défini."))
        lines.append("\n• Coupon 2:\n" + (c2 or "Non défini."))
    else:  # VVIP
        c1 = get_coupon(day, "STANDARD")
        c2 = get_coupon(day, "VIP1")
        c3 = get_coupon(day, "VVIP")
        lines.append("👑 <b>VVIP</b>\n")
        lines.append("• Coupon STANDARD:\n" + (c1 or "Non défini."))
        lines.append("\n• Coupon VIP1:\n" + (c2 or "Non défini."))
        lines.append("\n• Coupon VVIP:\n" + (c3 or "Non défini."))
        lines.append(f"\n\n🔗 <b>Communauté Telegram</b> : {VIP_GROUP_LINK}")

    return "\n".join(lines)

# =========================
# CAPITAL
# =========================
def set_capital(user_id: int, amount: float):
    con = db()
    cur = con.cursor()
    cur.execute("""
        INSERT INTO capital(user_id, amount, updated_at)
        VALUES(?,?,?)
        ON CONFLICT(user_id) DO UPDATE SET amount=excluded.amount, updated_at=excluded.updated_at
    """, (user_id, float(amount), now_iso()))
    con.commit()
    con.close()

def get_capital(user_id: int) -> Optional[float]:
    con = db()
    cur = con.cursor()
    cur.execute("SELECT amount FROM capital WHERE user_id=?", (user_id,))
    row = cur.fetchone()
    con.close()
    return float(row["amount"]) if row else None

def calc_stake(cap: float, mode: str) -> str:
    # Simple et sûr (vous pouvez ajuster)
    presets = {
        "safe": 0.05,     # 5%
        "normal": 0.10,   # 10%
        "aggressive": 0.15 # 15%
    }
    p = presets.get(mode, 0.10)
    stake = cap * p
    return f"Capital: <b>{cap:.0f}</b> FCFA\nMode: <b>{mode}</b> ({int(p*100)}%)\nMise conseillée: <b>{stake:.0f}</b> FCFA"

# =========================
# HANDLERS
# =========================
WELCOME = (
    "👋 <b>Bienvenue sur BOSCOBOT</b>\n\n"
    "Un bot pro pour :\n"
    "✅ Analyses de matchs (liste cliquable)\n"
    "✅ Coupons du jour\n"
    "✅ Gestion du capital\n"
    "✅ Abonnement par code (activation instant)\n\n"
    "Choisissez une option 👇"
)

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user:
        upsert_user(update.effective_user)
    cleanup_expired()
    await update.message.reply_text(WELCOME, parse_mode=ParseMode.HTML, reply_markup=main_menu_kb(update.effective_user.id))

async def cmd_myid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    await update.message.reply_text(f"🆔 Votre ID Telegram : <b>{uid}</b>", parse_mode=ParseMode.HTML)

async def render_home(query_or_msg, user_id: int):
    cleanup_expired()
    txt = WELCOME + "\n\n" + "📌 " + format_status(user_id)
    if hasattr(query_or_msg, "edit_message_text"):
        await query_or_msg.edit_message_text(txt, parse_mode=ParseMode.HTML, reply_markup=main_menu_kb(user_id))
    else:
        await query_or_msg.reply_text(txt, parse_mode=ParseMode.HTML, reply_markup=main_menu_kb(user_id))

# ---------- ANALYSIS (fixtures list + click) ----------
async def show_fixtures_page(query, user_id: int, date_str: str, page: int):
    if not require_tier(user_id, "STANDARD"):
        await query.edit_message_text(
            "🔒 <b>Analyses</b>\n\nCette option est réservée aux abonnés.\n➡️ Allez dans <b>Abonnements</b> → <b>Entrer un code</b>.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⭐ Abonnements", callback_data="menu:sub")],
                                               [InlineKeyboardButton("⬅️ Retour", callback_data="menu:home")]])
        )
        return

    fixtures = await get_fixtures_by_date(date_str)
    fixtures.sort(key=lambda x: x.get("fixture", {}).get("timestamp", 0))

    page_size = 10
    start = page * page_size
    end = start + page_size
    chunk = fixtures[start:end]

    title = f"📊 <b>Matchs du jour</b> — {date_str}\n"
    if not fixtures:
        await query.edit_message_text(
            title + "\nAucun match trouvé (dans la liste des championnats configurés).",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Retour", callback_data="menu:home")]])
        )
        return

    buttons = []
    for fx in chunk:
        fid = fx["fixture"]["id"]
        home = fx["teams"]["home"]["name"]
        away = fx["teams"]["away"]["name"]
        league = fx["league"]["name"]
        label = f"{home} vs {away} ({league})"
        buttons.append([InlineKeyboardButton(label, callback_data=f"fx:{fid}")])

    nav = []
    if start > 0:
        nav.append(InlineKeyboardButton("⬅️ Précédent", callback_data=f"fxpage:{date_str}:{page-1}"))
    if end < len(fixtures):
        nav.append(InlineKeyboardButton("Suivant ➡️", callback_data=f"fxpage:{date_str}:{page+1}"))
    if nav:
        buttons.append(nav)

    buttons.append([InlineKeyboardButton("⬅️ Menu", callback_data="menu:home")])

    await query.edit_message_text(
        title + "\nCliquez sur un match pour voir l’analyse 👇",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(buttons)
    )

async def show_fixture_analysis(query, fixture_id: int, user_id: int):
    if not require_tier(user_id, "VIP1"):
        await query.edit_message_text(
            "🔒 <b>Analyse avancée</b>\n\nRéservé à <b>VIP1</b> et <b>VVIP</b>.\n➡️ Activez un code VIP1/VVIP.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⭐ Abonnements", callback_data="menu:sub")],
                                               [InlineKeyboardButton("⬅️ Retour", callback_data="menu:analysis")]])
        )
        return

    await query.edit_message_text("⏳ Récupération des statistiques...", parse_mode=ParseMode.HTML)
    try:
        pack = await get_fixture_stats(fixture_id)
        stats_payload = pack["statistics"]
        preds_payload = pack["predictions"]

        t1, s1, t2, s2 = pick_team_stats(stats_payload)

        def stat_line(name: str) -> str:
            a = extract_stat(s1, name) or "?"
            b = extract_stat(s2, name) or "?"
            return f"• {name}: <b>{a}</b> — <b>{b}</b>"

        lines = [
            f"📈 <b>Analyse match</b> (fixture #{fixture_id})\n",
            f"<b>{t1}</b> vs <b>{t2}</b>\n",
            "<b>Stats clés</b>",
            stat_line("Shots on Goal"),
            stat_line("Shots off Goal"),
            stat_line("Total Shots"),
            stat_line("Ball Possession"),
            stat_line("Fouls"),
            stat_line("Corners"),
            stat_line("Yellow Cards"),
            stat_line("Red Cards"),
        ]

        if preds_payload and preds_payload.get("response"):
            pr = preds_payload["response"][0]
            advice = pr.get("predictions", {}).get("advice")
            winner = pr.get("predictions", {}).get("winner", {}).get("name")
            percent = pr.get("predictions", {}).get("percent", {})
            lines.append("\n<b>Prédiction (API)</b>")
            if winner:
                lines.append(f"• Favori: <b>{winner}</b>")
            if advice:
                lines.append(f"• Conseil: <b>{advice}</b>")
            if percent:
                lines.append(f"• Chances: {json.dumps(percent, ensure_ascii=False)}")

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Retour aux matchs", callback_data=f"fxpage:{today_str()}:0")],
            [InlineKeyboardButton("🏠 Menu", callback_data="menu:home")],
        ])

        await query.edit_message_text("\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=kb)

    except Exception as e:
        await query.edit_message_text(
            f"⚠️ Erreur pendant l’analyse: <code>{str(e)}</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Retour", callback_data="menu:analysis")]])
        )

# ---------- CALLBACK ROUTER ----------
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    upsert_user(query.from_user)
    cleanup_expired()

    data = query.data or ""

    if data == "menu:home":
        await render_home(query, user_id)
        return

    if data == "menu:help":
        await query.edit_message_text(help_text(), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Retour", callback_data="menu:home")]
        ]))
        return

    if data == "menu:analysis":
        await show_fixtures_page(query, user_id, today_str(), 0)
        return

    if data.startswith("fxpage:"):
        _, date_str, page_str = data.split(":")
        await show_fixtures_page(query, user_id, date_str, int(page_str))
        return

    if data.startswith("fx:"):
        fixture_id = int(data.split(":")[1])
        await show_fixture_analysis(query, fixture_id, user_id)
        return

    if data == "menu:coupon":
        await query.edit_message_text(
            coupon_for_user(user_id),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Retour", callback_data="menu:home")]])
        )
        return

    if data == "menu:capital":
        cap = get_capital(user_id)
        txt = "💰 <b>Gestion du capital</b>\n\n"
        txt += f"Capital actuel : <b>{cap:.0f}</b> FCFA\n\n" if cap is not None else "Capital actuel : <b>non défini</b>\n\n"
        txt += "Choisissez une action 👇"
        await query.edit_message_text(txt, parse_mode=ParseMode.HTML, reply_markup=capital_menu_kb())
        return

    if data == "cap:set":
        WAITING_CAPITAL[user_id] = "set"
        await query.edit_message_text(
            "✍️ Envoyez votre capital en FCFA (ex: 50000).",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Annuler", callback_data="menu:capital")]])
        )
        return

    if data == "cap:calc":
        cap = get_capital(user_id)
        if cap is None:
            await query.edit_message_text(
                "⚠️ Capital non défini.\n➡️ Cliquez <b>Définir mon capital</b> d’abord.",
                parse_mode=ParseMode.HTML,
                reply_markup=capital_menu_kb()
            )
            return

        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🟢 Safe (5%)", callback_data="capmode:safe")],
            [InlineKeyboardButton("🟡 Normal (10%)", callback_data="capmode:normal")],
            [InlineKeyboardButton("🔴 Aggressive (15%)", callback_data="capmode:aggressive")],
            [InlineKeyboardButton("⬅️ Retour", callback_data="menu:capital")],
        ])
        await query.edit_message_text("🧮 Choisissez un mode de mise :", parse_mode=ParseMode.HTML, reply_markup=kb)
        return

    if data.startswith("capmode:"):
        mode = data.split(":")[1]
        cap = get_capital(user_id) or 0.0
        await query.edit_message_text(
            "🧮 <b>Calcul de mise</b>\n\n" + calc_stake(cap, mode),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Retour", callback_data="menu:capital")]])
        )
        return

    if data == "menu:sub":
        txt = "⭐ <b>Abonnements</b>\n\n"
        txt += format_status(user_id) + "\n\n"
        txt += (
            "💳 <b>Tarifs (30 jours)</b>\n"
            f"• STANDARD : {PRICES_FCFA['STANDARD']} FCFA\n"
            f"• VIP1 : {PRICES_FCFA['VIP1']} FCFA\n"
            f"• VVIP : {PRICES_FCFA['VVIP']} FCFA\n\n"
            "✅ Après paiement (hors bot), le client reçoit un <b>CODE</b>.\n"
            "Il l’entre ici et l’accès s’active automatiquement."
        )
        await query.edit_message_text(txt, parse_mode=ParseMode.HTML, reply_markup=subs_menu_kb(user_id))
        return

    if data == "sub:status":
        await query.edit_message_text(format_status(user_id), parse_mode=ParseMode.HTML,
                                     reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Retour", callback_data="menu:sub")]]))
        return

    if data == "sub:redeem":
        WAITING_CODE[user_id] = "redeem"
        await query.edit_message_text(
            "🔑 Envoyez votre <b>code</b> d’abonnement (lettres + chiffres).\n\nEx: <code>AB12CD34EF56</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Annuler", callback_data="menu:sub")]])
        )
        return

    if data == "menu:admin":
        if not is_admin(user_id):
            await query.edit_message_text("⛔ Accès admin refusé.", parse_mode=ParseMode.HTML,
                                          reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Retour", callback_data="menu:home")]]))
            return
        await query.edit_message_text("🛠 <b>Admin</b>\nChoisissez :", parse_mode=ParseMode.HTML, reply_markup=admin_menu_kb())
        return

    if data.startswith("admin:gen:"):
        if not is_admin(user_id):
            return
        tier = data.split(":")[2]
        code = create_code(tier=tier, duration_days=30, max_uses=1, note="Auto-generated")
        await query.edit_message_text(
            f"✅ Code généré ({tier}, 30 jours, 1 utilisation)\n\n"
            f"<code>{code}</code>\n\n"
            "➡️ Copiez-collez ce code au client.",
            parse_mode=ParseMode.HTML,
            reply_markup=admin_menu_kb()
        )
        return

    if data == "admin:setcoupon":
        if not is_admin(user_id):
            return
        WAITING_ADMIN_COUPON[user_id] = "choose_tier"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("STANDARD (1 coupon)", callback_data="admin:coupon:STANDARD")],
            [InlineKeyboardButton("VIP1 (2e coupon)", callback_data="admin:coupon:VIP1")],
            [InlineKeyboardButton("VVIP (bonus)", callback_data="admin:coupon:VVIP")],
            [InlineKeyboardButton("⬅️ Retour", callback_data="menu:admin")],
        ])
        await query.edit_message_text("🎟 Choisissez quel coupon vous définissez :", parse_mode=ParseMode.HTML, reply_markup=kb)
        return

    if data.startswith("admin:coupon:"):
        if not is_admin(user_id):
            return
        tier = data.split(":")[2]
        WAITING_ADMIN_COUPON[user_id] = tier
        await query.edit_message_text(
            f"✍️ Envoyez le texte du coupon pour <b>{tier}</b> (date: {today_str()}).",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Annuler", callback_data="menu:admin")]])
        )
        return

    if data == "admin:broadcast":
        if not is_admin(user_id):
            return
        WAITING_ADMIN_BROADCAST[user_id] = "waiting"
        await query.edit_message_text(
            "📣 Envoyez le message à diffuser à tous les utilisateurs :",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Annuler", callback_data="menu:admin")]])
        )
        return

    # fallback
    await query.edit_message_text("⚠️ Action inconnue.", parse_mode=ParseMode.HTML, reply_markup=main_menu_kb(user_id))

# ---------- TEXT INPUTS ----------
async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    upsert_user(update.effective_user)
    cleanup_expired()

    text = (update.message.text or "").strip()

    # Redeem code
    if WAITING_CODE.get(user_id) == "redeem":
        WAITING_CODE.pop(user_id, None)
        ok, msg = redeem_code(user_id, text)
        await update.message.reply_text(
            msg + "\n\n" + format_status(user_id),
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_kb(user_id)
        )
        return

    # Set capital
    if WAITING_CAPITAL.get(user_id) == "set":
        WAITING_CAPITAL.pop(user_id, None)
        try:
            amt = float(text.replace(" ", "").replace(",", "."))
            if amt <= 0:
                raise ValueError()
            set_capital(user_id, amt)
            await update.message.reply_text(
                f"✅ Capital défini : <b>{amt:.0f}</b> FCFA",
                parse_mode=ParseMode.HTML,
                reply_markup=main_menu_kb(user_id)
            )
        except Exception:
            await update.message.reply_text(
                "⚠️ Format invalide. Exemple attendu: 50000",
                parse_mode=ParseMode.HTML,
                reply_markup=main_menu_kb(user_id)
            )
        return

    # Admin coupon text
    if is_admin(user_id) and WAITING_ADMIN_COUPON.get(user_id) in ["STANDARD", "VIP1", "VVIP"]:
        tier = WAITING_ADMIN_COUPON.pop(user_id)
        set_coupon(today_str(), tier, text)
        await update.message.reply_text(
            f"✅ Coupon {tier} défini pour {today_str()}",
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_kb(user_id)
        )
        return

    # Admin broadcast
    if is_admin(user_id) and WAITING_ADMIN_BROADCAST.get(user_id) == "waiting":
        WAITING_ADMIN_BROADCAST.pop(user_id, None)
        # send to all users
        con = db()
        cur = con.cursor()
        cur.execute("SELECT user_id FROM users")
        rows = cur.fetchall()
        con.close()

        sent = 0
        for r in rows:
            uid = int(r["user_id"])
            try:
                await context.bot.send_message(chat_id=uid, text=text)
                sent += 1
            except Exception:
                pass

        await update.message.reply_text(
            f"📣 Diffusion terminée. Messages envoyés: {sent}/{len(rows)}",
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_kb(user_id)
        )
        return

    # default
    await update.message.reply_text(
        "ℹ️ Utilisez le menu ci-dessous 👇",
        parse_mode=ParseMode.HTML,
        reply_markup=main_menu_kb(user_id)
    )

# =========================
# FASTAPI + TELEGRAM WEBHOOK
# =========================
app = FastAPI()
tg_app: Optional[Application] = None

@app.on_event("startup")
async def on_startup():
    global tg_app
    init_db()

    tg_app = Application.builder().token(BOT_TOKEN).build()

    tg_app.add_handler(CommandHandler("start", cmd_start))
    tg_app.add_handler(CommandHandler("myid", cmd_myid))
    tg_app.add_handler(CallbackQueryHandler(on_callback))
    tg_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    await tg_app.initialize()
    await tg_app.start()

    # set webhook to FastAPI endpoint
    webhook_url = PUBLIC_BASE_URL.rstrip("/") + "/tg/webhook"
    await tg_app.bot.set_webhook(url=webhook_url)

@app.on_event("shutdown")
async def on_shutdown():
    global tg_app
    if tg_app:
        await tg_app.stop()
        await tg_app.shutdown()
        tg_app = None

@app.get("/")
async def root():
    return {"ok": True, "service": "Boscobot", "time": now_iso()}

@app.post("/tg/webhook")
async def telegram_webhook(request: Request):
    global tg_app
    if tg_app is None:
        return JSONResponse({"ok": False, "error": "bot not ready"}, status_code=503)

    data = await request.json()
    update = Update.de_json(data, tg_app.bot)
    await tg_app.process_update(update)
    return {"ok": True}