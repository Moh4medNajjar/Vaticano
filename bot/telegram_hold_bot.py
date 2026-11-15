#!/usr/bin/env python3
"""
Local Telegram control bot for the Vatican hold flow.

Runs on your laptop in polling mode (no public URL / webhook needed). Listens
only in the configured TELEGRAM_CHAT_ID; every other chat is ignored silently.

Commands:
    /start                      — show help + current config
    /status                     — show whether a hold is in flight + last result
    /dry   <YYYY-MM-DD> [num]   — run api_hold.py (dry-run; no captcha spend)
    /hold  <YYYY-MM-DD> [num]   — run api_hold.py --live (spends 1 captcha, catches a real ~15 min hold)
    /cancel                     — kill the currently running /hold or /dry
    /logs                       — tail the live log of the running run
    /last                       — path + highlights of the last reports/hold_*.json

Reads config from ../.env (TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID,
YESCAPTCHA_CLIENT_KEY). Kept local to the laptop — no deploy required.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import logging
import os
import pathlib
import re
import shlex
import signal
import sys
import time
from dataclasses import dataclass, field
from typing import Optional

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import (Application, CommandHandler, CallbackQueryHandler,
                          ContextTypes, MessageHandler, filters)

# --------------------------------------------------------------------- paths
BOT_DIR     = pathlib.Path(__file__).resolve().parent
PROJECT_DIR = BOT_DIR.parent
ENV_FILE    = PROJECT_DIR / ".env"
REPORTS     = PROJECT_DIR / "reports"
LOGS_DIR    = PROJECT_DIR / "scratch_rea"
LOGS_DIR.mkdir(exist_ok=True)

# --------------------------------------------------------------------- env load
def _load_env(p: pathlib.Path) -> None:
    if not p.exists(): return
    for ln in p.read_text(encoding="utf-8").splitlines():
        ln = ln.strip()
        if not ln or ln.startswith("#"): continue
        k, _, v = ln.partition("=")
        if k and v and k.strip() not in os.environ:
            os.environ[k.strip()] = v.strip()

_load_env(ENV_FILE)

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID   = int(os.environ.get("TELEGRAM_CHAT_ID", "0") or "0")
YESCAPTCHA_KEY     = os.environ.get("YESCAPTCHA_CLIENT_KEY", "").strip()

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    print("ERROR: set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env", file=sys.stderr)
    sys.exit(2)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s | %(message)s",
)
log = logging.getLogger("tg_hold")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)


# --------------------------------------------------------------------- state
# Multiple holds can run in parallel. Each is a single `api_hold.py`
# subprocess with its own log and stream task. Keyed by `hold_id`
# (short slug like "h1794432186").
@dataclass
class RunState:
    hold_id:      str = ""         # "h<unix_ts>"
    kind:         str = ""         # "dry" | "live" | "longhold" (perpetual recap)
    proc:         Optional[asyncio.subprocess.Process] = None
    started_at:   Optional[dt.datetime] = None
    args_summary: str = ""         # human label for /status
    log_path:     Optional[pathlib.Path] = None
    log_handle:   object = None
    last_lines:   list[str] = field(default_factory=list)   # tail buffer
    stream_task:  Optional[asyncio.Task] = None
    # Slot config captured at spawn time so /held and /pay can read it back
    # without having to parse JSON snapshots.
    slot_meta:    dict = field(default_factory=dict)        # date/time/product/full/reduced/audio/names
    # Points at the live snapshot file api_hold.py rewrites each cycle,
    # so /held can read the latest reservationId without touching the process.
    snapshot_glob: str = ""                                 # e.g. "hold_*_recap_live.json"
    # Back-compat: "mode" used to be dry|live. Keep it as an alias of kind.
    @property
    def mode(self) -> str: return self.kind

HOLDS: dict[str, RunState] = {}
_HELD_FILE = PROJECT_DIR / ".held_slots.json"


def _save_held_registry() -> None:
    """Persist what's needed so bot-restart can rebuild /held cards."""
    try:
        d = {}
        for hid, rs in HOLDS.items():
            pid = rs.proc.pid if rs.proc else None
            alive = bool(rs.proc and rs.proc.returncode is None)
            d[hid] = {
                "hold_id":       hid,
                "kind":          rs.kind,
                "pid":           pid,
                "alive":         alive,
                "started_utc":   (rs.started_at.isoformat() + "Z"
                                  if rs.started_at else None),
                "args_summary":  rs.args_summary,
                "slot":          rs.slot_meta,
                "log_path":      str(rs.log_path) if rs.log_path else None,
                "snapshot_glob": rs.snapshot_glob,
            }
        _HELD_FILE.write_text(json.dumps(d, indent=2), encoding="utf-8")
    except Exception as exc:
        log.warning("held registry save failed: %s", exc)


def _load_held_registry_shells() -> None:
    """On startup, reload the registry as 'shell' entries. We cannot reattach
    to the old subprocesses' stdout (that stream is gone with the previous
    bot instance), so each entry comes back without a live stream. /held
    still works because it reads the live snapshot JSON on disk."""
    if not _HELD_FILE.exists(): return
    try:
        d = json.loads(_HELD_FILE.read_text(encoding="utf-8"))
    except Exception: return
    for hid, info in d.items():
        rs = RunState(
            hold_id=hid, kind=info.get("kind","longhold"),
            args_summary=info.get("args_summary",""),
            log_path=(pathlib.Path(info["log_path"]) if info.get("log_path") else None),
            slot_meta=info.get("slot") or {},
            snapshot_glob=info.get("snapshot_glob") or "",
        )
        try:
            if info.get("started_utc"):
                rs.started_at = dt.datetime.fromisoformat(info["started_utc"].rstrip("Z"))
        except Exception: pass
        # Reattach the pid if the old process is still alive — Python can't
        # read its stdout any more, but we can send it SIGTERM on /release.
        pid = info.get("pid")
        if pid and _pid_alive(pid):
            try:
                rs.proc = _AttachedProc(pid=pid)
            except Exception: pass
        HOLDS[hid] = rs


def _pid_alive(pid: int) -> bool:
    try:
        import os, signal
        if sys.platform == "win32":
            # Windows has no POSIX signal 0. Use OpenProcess via ctypes.
            import ctypes
            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            h = ctypes.windll.kernel32.OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION, False, int(pid))
            if not h: return False
            exit_code = ctypes.c_ulong(0)
            got = ctypes.windll.kernel32.GetExitCodeProcess(h, ctypes.byref(exit_code))
            ctypes.windll.kernel32.CloseHandle(h)
            # STILL_ACTIVE = 259
            return bool(got) and exit_code.value == 259
        os.kill(int(pid), 0)
        return True
    except Exception:
        return False


class _AttachedProc:
    """Minimal stand-in for asyncio.subprocess.Process after a bot restart.
    We can only check liveness and terminate it — no stdout streaming."""
    def __init__(self, pid: int):
        self.pid = pid
        self.returncode = None  # unknown; refreshed by _pid_alive checks
    def terminate(self):
        import os, signal
        try:
            if sys.platform == "win32":
                import ctypes
                PROCESS_TERMINATE = 0x0001
                h = ctypes.windll.kernel32.OpenProcess(PROCESS_TERMINATE, False, int(self.pid))
                if h:
                    ctypes.windll.kernel32.TerminateProcess(h, 1)
                    ctypes.windll.kernel32.CloseHandle(h)
            else:
                os.kill(int(self.pid), signal.SIGTERM)
        except Exception: pass
        self.returncode = -15
    def kill(self): self.terminate()
    async def wait(self):
        import asyncio as _a
        while _pid_alive(self.pid):
            await _a.sleep(0.5)
        self.returncode = self.returncode or 0
        return self.returncode


def _active_hold() -> Optional[RunState]:
    """Return the single most-recently-started non-perpetual run, or any
    running one, for back-compat with /cancel /logs /status that didn't take
    a hold_id argument."""
    cand = [rs for rs in HOLDS.values()
            if rs.proc and rs.proc.returncode is None]
    if not cand: return None
    cand.sort(key=lambda r: r.started_at or dt.datetime.min, reverse=True)
    return cand[0]


# Back-compat shim. Old command bodies read "STATE.proc", "STATE.started_at" etc.
# Those still apply to the most-recently-active run. Writes go through
# _active_hold() or fresh HOLDS entries.
class _StateShim:
    def __getattr__(self, name):
        rs = _active_hold()
        if rs is None:
            # Empty state so old code's `STATE.proc` is None, `STATE.last_lines` is []
            fallback = RunState()
            return getattr(fallback, name)
        return getattr(rs, name)
STATE = _StateShim()

# Per-chat menu state. Holds the current selection until the user hits Catch.
# Persists across bot restarts via the JSON file below so your mate doesn't
# have to retype buyer info every time.
@dataclass
class MenuSel:
    date:   str = ""           # YYYY-MM-DD
    time:   str = ""           # "" = any, else HH or HH:MM
    product: str = "admission" # admission | guided | gardens | bus
    full:   int = 0            # count of Full Price tickets
    reduced:int = 1            # count of Reduced tickets
    audio:  int = 0            # count of Audioguides
    mode:   str = "hold"       # "hold" (no captcha, keep for days) | "live" (pay now) | "dry"
    keepalive: bool = True     # hold past the 5-min countdown (live mode only)
    keepalive_min: int = 20    # minutes of keepalive (live mode only)
    rerecap_min:  int = 2      # /recap refresh interval in minutes (hold mode)
    rerecap_sess: int = 3      # /recap cycles per proxy before rotating (hold mode)
    # Buyer — ALL empty by default. Your mate fills them in via the /buyer form
    # before tapping 💳 Pay. If any required field is empty when payment is
    # attempted, the bot refuses and sends them to the form.
    first:  str = ""
    last:   str = ""
    email:  str = ""
    phone:  str = ""
    country:str = ""
    city:   str = ""
    gender: str = ""   # "M" | "F" | ""
    birth:  str = ""   # YYYY-MM-DD | ""
    # Participant names (per visitor), as "First Last, First Last…"
    names:  str = ""

    @property
    def num(self) -> int: return max(1, self.full + self.reduced)

    @property
    def estimate_eur(self) -> float:
        return 20 * self.full + 10 * self.reduced + 5 * self.num + 8 * self.audio


MENU: dict[int, MenuSel] = {}
_MENU_STATE_FILE = PROJECT_DIR / ".menu_state.json"


def _load_menu_state() -> None:
    try:
        d = json.loads(_MENU_STATE_FILE.read_text(encoding="utf-8"))
        for cid, fields in d.items():
            m = MenuSel()
            for k, v in fields.items():
                if hasattr(m, k): setattr(m, k, v)
            MENU[int(cid)] = m
    except Exception: pass


def _save_menu_state() -> None:
    try:
        d = {str(cid): {k: getattr(m, k) for k in
                       ("date","time","product","full","reduced","audio","mode",
                        "keepalive","keepalive_min","rerecap_min","rerecap_sess",
                        "first","last","email","phone","country","city","gender",
                        "birth","names")}
             for cid, m in MENU.items()}
        _MENU_STATE_FILE.write_text(json.dumps(d, indent=2), encoding="utf-8")
    except Exception as exc:
        log.warning("menu state save failed: %s", exc)


_load_menu_state()


# One-shot: wipe the legacy "Mohamed Najjar" default values that older
# bot versions may have persisted into .menu_state.json. If an existing
# entry still carries those exact strings, clear them so your mate starts
# with a blank buyer form.
_LEGACY_DEFAULTS = {"first": "Mohamed", "last": "Najjar",
                    "email": "najjarmohamed443@gmail.com", "phone": "20124532",
                    "country": "Tunisia", "city": "Tunis",
                    "gender": "M", "birth": "1990-01-01"}
for _m in MENU.values():
    if all(getattr(_m, k, "") == v for k, v in _LEGACY_DEFAULTS.items()):
        for k in _LEGACY_DEFAULTS: setattr(_m, k, "")
_save_menu_state()


# When your mate taps a buyer-form field, we store {chat_id: {"field":"first",
# "return_to":"pay:h123" | "menu"}} so the next plain-text message is captured
# as the new value. See `on_plain_text`.
AWAITING: dict[int, dict] = {}


# Required buyer fields before /pay is allowed. `names` (per-visitor names)
# stays optional — the buyer name is used as fallback for all participants.
_REQUIRED_BUYER_FIELDS = ("first", "last", "email", "phone",
                          "country", "city", "gender", "birth")


def _try_parse_date(t: str) -> tuple[bool, str]:
    try:
        dt.datetime.strptime(t, "%Y-%m-%d")
        return True, ""
    except Exception:
        return False, "format must be YYYY-MM-DD"


# Human labels + prompts + validators for the step-by-step form.
_BUYER_FIELD_SPEC: dict[str, dict] = {
    "first":   {"label": "👤 First name",    "prompt": "Type the FIRST NAME of the visitor and send it.",
                "validate": lambda t: (bool(t.strip()), "cannot be empty")},
    "last":    {"label": "👤 Last name",     "prompt": "Type the LAST NAME of the visitor and send it.",
                "validate": lambda t: (bool(t.strip()), "cannot be empty")},
    "email":   {"label": "📧 Email",         "prompt": "Type the EMAIL (Vatican sends the booking PDF here).",
                "validate": lambda t: ("@" in t and "." in t, "that doesn't look like an email")},
    "phone":   {"label": "📞 Phone",         "prompt": "Type the PHONE number (digits only, e.g. 20124532).",
                "validate": lambda t: (sum(c.isdigit() for c in t) >= 6, "need at least 6 digits")},
    "country": {"label": "🌍 Country",       "prompt": "Type the COUNTRY (e.g. Tunisia, France, Italy).",
                "validate": lambda t: (bool(t.strip()), "cannot be empty")},
    "city":    {"label": "🏙️ City",          "prompt": "Type the CITY of residence.",
                "validate": lambda t: (bool(t.strip()), "cannot be empty")},
    "birth":   {"label": "🎂 Birth date",    "prompt": "Type the BIRTH DATE as YYYY-MM-DD (e.g. 1994-05-31).",
                "validate": lambda t: _try_parse_date(t.strip())},
    "names":   {"label": "📝 Participant names", "prompt":
                "Type the per-visitor names as:\n"
                "<code>First1 Last1, First2 Last2, …</code>\n\n"
                "Or send a single dash <code>-</code> to leave it empty "
                "(buyer name is reused for all participants).",
                "validate": lambda t: (True, "")},
}


def _buyer_form_text(sel: MenuSel) -> str:
    """Form body — shows each field's current value or (not set)."""
    def row(label, val):
        return f"{label}: <code>{val or '(not set)'}</code>"
    missing = [k for k in _REQUIRED_BUYER_FIELDS if not getattr(sel, k, "")]
    status = ("<b>✅ Buyer complete — ready to pay.</b>"
              if not missing else
              f"<b>⚠️ Still missing:</b> {', '.join(missing)}")
    return (
        "<b>📝 Set the visitor (buyer)</b>\n"
        "Tap each field, then send the value as a message.\n\n"
        f"{row('👤 First name', sel.first)}\n"
        f"{row('👤 Last name',  sel.last)}\n"
        f"{row('📧 Email',      sel.email)}\n"
        f"{row('📞 Phone',      sel.phone)}\n"
        f"{row('🌍 Country',    sel.country)}\n"
        f"{row('🏙️ City',       sel.city)}\n"
        f"{row('⚧ Gender',      sel.gender)}\n"
        f"{row('🎂 Birth date', sel.birth)}\n"
        f"{row('📝 Participants', sel.names)}\n\n"
        f"{status}"
    )


def _buyer_form_render(sel: MenuSel, return_to: str) -> InlineKeyboardMarkup:
    """return_to is "pay:<hid>" (back to pay confirm) or "menu" (back to menu)
    or "none" (standalone /buyer command)."""
    def fld(key: str) -> list:
        spec  = _BUYER_FIELD_SPEC[key]
        value = getattr(sel, key, "") or "(not set)"
        return [InlineKeyboardButton(f"{spec['label']}: {value}",
                                     callback_data=f"b:set:{key}:{return_to}")]
    tag = lambda v: "●" if v else "○"
    rows = [
        fld("first"), fld("last"), fld("email"), fld("phone"),
        fld("country"), fld("city"),
        [InlineKeyboardButton(f"{tag(sel.gender=='M')} ⚧ M (male)",
                              callback_data=f"b:gender:M:{return_to}"),
         InlineKeyboardButton(f"{tag(sel.gender=='F')} ⚧ F (female)",
                              callback_data=f"b:gender:F:{return_to}")],
        fld("birth"),
        [InlineKeyboardButton(f"📝 Participant names: "
                              + (sel.names[:30] + "…" if len(sel.names) > 30
                                 else (sel.names or "(auto)")),
                              callback_data=f"b:set:names:{return_to}")],
        [InlineKeyboardButton("🧹 Clear ALL",  callback_data=f"b:clear:{return_to}")],
    ]
    if return_to.startswith("pay:"):
        rows.append([InlineKeyboardButton("✅ Done — back to pay confirm",
                                          callback_data=f"h:pay:{return_to[4:]}")])
    else:
        rows.append([InlineKeyboardButton("✅ Done", callback_data="b:close")])
    return InlineKeyboardMarkup(rows)


# --------------------------------------------------------------------- helpers
AUTHORIZED_CHAT = TELEGRAM_CHAT_ID

def only_our_chat(func):
    """Decorator: silently ignore commands from any chat other than ours."""
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id if update.effective_chat else None
        if chat_id != AUTHORIZED_CHAT:
            log.warning("ignoring command from chat_id=%s (expected %s)",
                        chat_id, AUTHORIZED_CHAT)
            return
        return await func(update, ctx)
    wrapper.__name__ = func.__name__
    return wrapper


def _parse_date_num(args: list[str]) -> tuple[str, int, list[str]]:
    """Accept `YYYY-MM-DD [num]`. Returns (date, num, remaining)."""
    if not args:
        raise ValueError("usage: /hold YYYY-MM-DD [num]")
    date = args[0]
    try:
        dt.datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        raise ValueError(f"bad date '{date}' — use YYYY-MM-DD")
    num = 2
    rest = args[1:]
    if rest and rest[0].isdigit():
        num = int(rest[0])
        rest = rest[1:]
    if num < 1 or num > 6:
        raise ValueError(f"num must be 1..6 (got {num})")
    return date, num, rest


async def _send(ctx: ContextTypes.DEFAULT_TYPE, text: str,
                parse: Optional[str] = None) -> None:
    await ctx.bot.send_message(chat_id=AUTHORIZED_CHAT, text=text,
                               parse_mode=parse,
                               disable_web_page_preview=True)


# --------------------------------------------------------------------- subprocess runner
async def _spawn_api_hold(ctx: ContextTypes.DEFAULT_TYPE, mode: str,
                          date: str, num: int, days_ahead: int = 30) -> None:
    """Classic /dry /hold entry point — delegates to the custom spawn with
    the proven default ticket type."""
    await _spawn_api_hold_custom(ctx, mode, date, num,
                                 days_ahead=days_ahead, ticket="reduced")


async def _spawn_api_hold_custom(ctx: ContextTypes.DEFAULT_TYPE, mode: str,
                                 date: str, num: int,
                                 days_ahead: int = 7,
                                 ticket: str = "reduced") -> None:
    """Legacy entry point used by /hold and /dry (no menu, no perpetual hold).
    Only one of these runs at a time — if a non-perpetual run is in flight,
    ask the user to cancel it first."""
    # Only block on non-longhold runs — multiple longholds are fine in parallel.
    cur = _active_hold()
    if cur and cur.kind != "longhold":
        await _send(ctx, "⚠️ Another /hold run is already in flight. /cancel it first.")
        return

    cmd = [sys.executable, "api_hold.py",
           "--date", date, "--num", str(num),
           "--days-ahead", str(days_ahead),
           # The catch pattern that first landed ref 2L2NFJYZ19XTF21OL1 on 2026-11-11.
           "--headless",
           "--ticket", ticket,
           "--session-aging-sec", "75",
           "--rotate-ip",
           "--ip-retries", "15"]
    if mode == "live":
        cmd += ["--live", "--keepalive", "--keepalive-minutes", "20"]

    await _spawn_api_hold_raw(
        ctx,
        kind=mode,
        cmd=cmd,
        args_summary=f"date={date} num={num} ticket={ticket}",
        slot_meta={"date": date, "num": num, "ticket": ticket},
    )


async def _spawn_api_hold_raw(ctx: ContextTypes.DEFAULT_TYPE, *,
                              kind: str,
                              cmd: list[str],
                              args_summary: str,
                              slot_meta: dict,
                              snapshot_glob: str = "") -> RunState:
    """Shared launcher: spawns api_hold.py, registers in HOLDS, streams stdout."""
    hold_id = f"h{int(time.time())}"
    while hold_id in HOLDS:
        hold_id = f"h{int(time.time())}{len(HOLDS)}"
    log_file = LOGS_DIR / f"tg_{kind}_{hold_id}.log"
    fh = open(log_file, "w", encoding="utf-8")

    proc = await asyncio.create_subprocess_exec(
        *cmd, cwd=str(BOT_DIR),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        env={**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"},
    )
    rs = RunState(
        hold_id=hold_id,
        kind=kind,
        proc=proc,
        started_at=dt.datetime.utcnow(),
        args_summary=args_summary,
        log_path=log_file,
        log_handle=fh,
        last_lines=[],
        slot_meta=slot_meta,
        snapshot_glob=snapshot_glob,
    )
    HOLDS[hold_id] = rs
    _save_held_registry()

    await _send(ctx, f"🚀 <b>{kind.upper()}</b> [{hold_id}] started · {args_summary}\n"
                     f"pid={proc.pid}  log={log_file.name}",
                parse=ParseMode.HTML)

    rs.stream_task = asyncio.create_task(_stream(ctx, rs))
    return rs


# Patterns in api_hold.py stdout we want to surface as Telegram messages
INTERESTING = [
    (re.compile(r"=== session attempt (\d+)/(\d+)\s+proxy=([^ ]+)"),
                                                               lambda m: f"🔁 attempt {m.group(1)}/{m.group(2)} · IP {m.group(3)}"),
    (re.compile(r"canary .* F5-rejected — rotating IP"),       "🔄 cold IP, rotating…"),
    (re.compile(r"=== 0/5|--- F5 TSPD challenge ---"),         "⚙️ F5 TSPD challenge…"),
    (re.compile(r"F5 challenge passed"),                       "✅ F5 passed"),
    (re.compile(r"Angular did not load"),                      "❌ F5 did NOT redirect (fingerprint low score)"),
    (re.compile(r"=== 1/5|--- slot discovery ---"),            "🔎 slot discovery…"),
    (re.compile(r"\[(\d{2}/\d{2}/\d{4})\] visit: id=(\d+).*avail=(\w+)"),
                                                               lambda m: f"🎟️ {m.group(1)} visit matched (avail={m.group(3)})"),
    (re.compile(r"slot picked: Slot\([^)]*visit_time='([^']+)'[^)]*availability='([^']+)'"),
                                                               lambda m: f"🎯 slot: {m.group(1)} ({m.group(2)})"),
    (re.compile(r"=== 3/5|--- POST /api/visit/recap"),         "📨 POST /recap (creating hold)…"),
    (re.compile(r"/recap OK\s+recapId=(\S+)\s+reservationId=(\S+)\s+total=(\S+)"),
                                                               lambda m: f"✅ HOLD at /recap\nreservationId=<code>{m.group(2)}</code>\nrecapId={m.group(1)}\ntotal=€{m.group(3)}"),
    (re.compile(r"=== 4/5"),                                   "🫧 browser warmup (9 GETs)…"),
    (re.compile(r"=== 5/5  solving Turnstile"),                "🧩 solving Turnstile…"),
    (re.compile(r"Turnstile solved"),                          "✅ Turnstile OK"),
    (re.compile(r"POST /api/visit/reservation.*HTTP (\d+).*attempt (\d+)/3"),
                                                               lambda m: f"⚠️ /reservation HTTP {m.group(1)} (attempt {m.group(2)}/3) — retrying"),
    (re.compile(r"HELD  ref=(\S+)\s+pay=(\S+)\s+total=(\S+)"),
                                                               lambda m: f"🏆 <b>LOCKED</b>\nref=<code>{m.group(1)}</code>\npay={m.group(2)}\ntotal=€{m.group(3)}"),
    # Keepalive / rerecap progress lines are intentionally NOT forwarded to
    # Telegram — your mate asked to see only the catch + problems, not every
    # heartbeat. Problem signals below still fire.
    (re.compile(r"💔 heartbeat #(\d+): Vatican says the hold is gone"),
                                                               lambda m: f"💔 hold expired at heartbeat #{m.group(1)}"),
    (re.compile(r"FAILED: (.+)"),                              lambda m: f"❌ FAILED: {m.group(1)[:300]}"),
    (re.compile(r"Request Rejected"),                          "🚫 F5 rejected (fingerprint/reputation)"),
    (re.compile(r"ERR_TIMED_OUT"),                             "💀 proxy IP dead (will NOT auto-switch this run — retry)"),
]


def _match_line(line: str) -> Optional[str]:
    for pat, msg in INTERESTING:
        m = pat.search(line)
        if m:
            return msg if isinstance(msg, str) else msg(m)
    return None


async def _stream(ctx: ContextTypes.DEFAULT_TYPE, rs: RunState) -> None:
    """Pull one subprocess's stdout; copy to log file; forward interesting lines."""
    proc = rs.proc
    assert proc and proc.stdout
    tag = f"[{rs.hold_id}] " if rs.hold_id else ""
    try:
        while True:
            raw = await proc.stdout.readline()
            if not raw:
                break
            line = raw.decode("utf-8", errors="replace").rstrip()
            rs.log_handle.write(line + "\n"); rs.log_handle.flush()
            rs.last_lines.append(line)
            if len(rs.last_lines) > 500:
                rs.last_lines = rs.last_lines[-500:]
            msg = _match_line(line)
            if msg:
                try:
                    await _send(ctx, f"{tag}{msg}", parse=ParseMode.HTML)
                except Exception as exc:
                    log.warning("tg send failed: %s", exc)

        rc = await proc.wait()
        elapsed = ((dt.datetime.utcnow() - rs.started_at).total_seconds()
                   if rs.started_at else 0)
        await _send(ctx,
                    f"🏁 {tag}finished · rc={rc} · {elapsed:.1f}s\n"
                    f"log: {rs.log_path.name if rs.log_path else '?'}\n"
                    f"/held to see the collection, /last for details")
    finally:
        try:    rs.log_handle and rs.log_handle.close()
        except: pass
        _save_held_registry()


# --------------------------------------------------------------------- menu


def _menu_default_date() -> str:
    """Default start-date for the menu date picker: today + 30 days (far enough
    forward that most slots are AVAILABLE)."""
    return (dt.date.today() + dt.timedelta(days=30)).isoformat()


PRODUCTS = [("admission", "🎟️ Admission Ticket"),
            ("guided",    "🧑‍🏫 Guided Tour — Museums"),
            ("gardens",   "🌳 Guided Tour — Gardens+Museums"),
            ("bus",       "🚌 Open Bus — Gardens")]
TIMES    = [("",      "any"),
            ("09",    "morning (09:xx)"),
            ("12",    "lunchtime (12:xx)"),
            ("14",    "afternoon (14:xx)"),
            ("16",    "late (16:xx)"),
            ("17",    "evening (17:xx)")]


def _menu_render(sel: MenuSel) -> InlineKeyboardMarkup:
    """Full Vatican-style config. Every Vatican booking knob is here.

    In 📦 HOLD mode the buyer row is intentionally hidden — /recap doesn't send
    buyer info to Vatican, so there's nothing to set until /pay time. Your mate
    can lock a slot a week in advance, then /setbuyer the day before paying.
    """
    tag = lambda v: "●" if v else "○"
    is_hold = (sel.mode == "hold")

    time_label = next((lbl for v, lbl in TIMES if v == sel.time), "any")
    product_label = next((lbl for v, lbl in PRODUCTS if v == sel.product),
                         sel.product)
    rows = [
        # Date
        [InlineKeyboardButton(f"📅 Date:  {sel.date or '(pick one)'}",
                              callback_data="m:date:open")],
        # Time
        [InlineKeyboardButton(f"🕐 Time:  {time_label}",
                              callback_data="m:time:open")],
        # Product type
        [InlineKeyboardButton(f"🎯 Product:  {product_label}",
                              callback_data="m:product:open")],
        # Tickets — Full price
        [InlineKeyboardButton(f"🎫 Full €20:  {sel.full}", callback_data="noop"),
         InlineKeyboardButton("➖", callback_data="m:full:dec"),
         InlineKeyboardButton("➕", callback_data="m:full:inc")],
        # Tickets — Reduced
        [InlineKeyboardButton(f"🎫 Reduced €10:  {sel.reduced}", callback_data="noop"),
         InlineKeyboardButton("➖", callback_data="m:red:dec"),
         InlineKeyboardButton("➕", callback_data="m:red:inc")],
        # Audioguides
        [InlineKeyboardButton(f"🎧 Audioguide €8:  {sel.audio}", callback_data="noop"),
         InlineKeyboardButton("➖", callback_data="m:audio:dec"),
         InlineKeyboardButton("➕", callback_data="m:audio:inc")],
        # Mode (3-way): HOLD (no captcha, keep for days) · LIVE (pay now) · DRY
        [InlineKeyboardButton(f"{tag(sel.mode=='hold')} 📦 HOLD",
                              callback_data="m:mode:hold"),
         InlineKeyboardButton(f"{tag(sel.mode=='live')} 💳 PAY NOW",
                              callback_data="m:mode:live"),
         InlineKeyboardButton(f"{tag(sel.mode=='dry')} 🧪 DRY",
                              callback_data="m:mode:dry")],
    ]
    # LIVE mode needs keepalive + buyer. HOLD mode hides both (not relevant yet).
    if not is_hold:
        rows.append(
            [InlineKeyboardButton(f"💚 Keepalive: {'ON' if sel.keepalive else 'off'} "
                                  f"({sel.keepalive_min} min)",
                                  callback_data="m:ka:toggle"),
             InlineKeyboardButton("➖", callback_data="m:ka:dec"),
             InlineKeyboardButton("➕", callback_data="m:ka:inc")])
        rows.append(
            [InlineKeyboardButton(f"👤 Buyer: {sel.first} {sel.last} ✏️",
                                  callback_data="m:buyer:show")])
    # Fire / Back
    rows.append(
        [InlineKeyboardButton("✅ CATCH!", callback_data="m:fire"),
         InlineKeyboardButton("↩ Back",   callback_data="top:back")])
    return InlineKeyboardMarkup(rows)


def _menu_text(sel: MenuSel) -> str:
    time_label = next((lbl for v, lbl in TIMES if v == sel.time), "any")
    product_label = next((lbl for v, lbl in PRODUCTS if v == sel.product),
                         sel.product)
    is_hold = (sel.mode == "hold")
    if is_hold:
        mode_tag = (f"HOLD (no captcha · refreshes every {sel.rerecap_min} min · "
                    "keep in your hand for days · pay via /pay &lt;id&gt; later)")
    elif sel.mode == "live":
        mode_tag = "PAY NOW (spends ≈$0.005 captcha · creates a real reservation)"
    else:
        mode_tag = "DRY (no captcha · no reservation · discovery only)"
    txt = (
        f"<b>🎟️ Vatican Catch — configure reservation</b>\n"
        f"Tap each field. When ready, tap <b>✅ CATCH!</b>\n\n"
        f"<b>Slot</b>\n"
        f"📅 Date: <code>{sel.date or '(pick one)'}</code>\n"
        f"🕐 Time: <code>{time_label}</code>\n"
        f"🎯 Product: <code>{product_label}</code>\n\n"
        f"<b>Tickets</b>\n"
        f"🎫 Full price (€20): <b>{sel.full}</b>\n"
        f"🎫 Reduced (€10):    <b>{sel.reduced}</b>\n"
        f"🎧 Audioguide (€8):  <b>{sel.audio}</b>\n"
        f"👥 Visitors total:   <b>{sel.num}</b>\n"
        f"💶 Reservation Fees: €{5*sel.num}\n"
        f"💶 <b>Estimated: €{sel.estimate_eur:.2f}</b>\n\n"
    )
    if not is_hold:
        names_disp = sel.names or f"{sel.first} {sel.last} (× {sel.num})"
        txt += (
            f"<b>Buyer</b>\n"
            f"👤 {sel.first} {sel.last} · {sel.email}\n"
            f"📞 {sel.phone} · 🌍 {sel.country} / {sel.city}\n"
            f"🎂 {sel.birth} · ⚧ {sel.gender}\n"
            f"📝 Participants: <code>{names_disp}</code>\n"
            f"   <i>edit buyer: /setbuyer first=… last=… email=…</i>\n"
            f"   <i>edit names: /setnames First1 Last1, First2 Last2</i>\n\n"
        )
    else:
        txt += (
            "<b>Buyer</b>\n"
            "   <i>not needed yet — HOLD doesn't send buyer to Vatican.</i>\n"
            "   <i>Set the real visitor with /setbuyer before tapping 💳 Pay.</i>\n\n"
        )
    txt += f"🔧 Mode: <b>{mode_tag}</b>"
    if not is_hold:
        txt += "\n💚 Keepalive: <b>" + (
            f"ON — holds past the 5-min countdown for {sel.keepalive_min} min"
            if sel.keepalive else "off — exits right after catching"
        ) + "</b>"
    return txt + "​"  # trailing zero-width space to force Telegram redraw


def _time_picker() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(lbl, callback_data=f"m:time:set:{v or 'any'}")]
            for v, lbl in TIMES]
    rows.append([InlineKeyboardButton("↩ back", callback_data="m:back")])
    return InlineKeyboardMarkup(rows)


def _product_picker() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(lbl, callback_data=f"m:product:set:{v}")]
            for v, lbl in PRODUCTS]
    rows.append([InlineKeyboardButton("↩ back", callback_data="m:back")])
    return InlineKeyboardMarkup(rows)


def _date_picker(center: str) -> InlineKeyboardMarkup:
    """Date-picker sub-menu — 7 days around `center` + prev/next buttons."""
    base = dt.date.fromisoformat(center)
    # Show 8 days (2 rows of 4) starting 3 days before center for context
    dates = [(base + dt.timedelta(days=i)) for i in range(-3, 5)]
    rows = []
    # 2 rows of 4 dates each
    for r in (dates[:4], dates[4:]):
        rows.append([InlineKeyboardButton(
            d.strftime("%a %d/%m"),
            callback_data=f"m:date:set:{d.isoformat()}"
        ) for d in r])
    # Navigation
    prev7 = (base - dt.timedelta(days=7)).isoformat()
    next7 = (base + dt.timedelta(days=7)).isoformat()
    rows.append([
        InlineKeyboardButton("⏪ -7d", callback_data=f"m:date:nav:{prev7}"),
        InlineKeyboardButton("⏩ +7d", callback_data=f"m:date:nav:{next7}"),
        InlineKeyboardButton("↩ back", callback_data="m:back"),
    ])
    return InlineKeyboardMarkup(rows)


def kbd(rows: list[list[tuple[str, str]]]) -> InlineKeyboardMarkup:
    """Shorthand for building keyboards from [(label, data), …] rows.
    Mirrors botmanager/botmanager.py so the look is consistent across bots."""
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(label, callback_data=data) for label, data in row]
         for row in rows])


# Mirrors omniavaticanrome/sniper.js:mainMenuText + mainMenuKb — same header
# shape, same 2x2 grid, same callback_data names ('cards','jobs','cons','site')
# so an operator onboarding from the Carcer Tullianum + Colosseo bot reads
# the exact same screen.
def _hunting_jobs() -> list[RunState]:
    """Jobs currently trying to grab the slot (process alive AND no snapshot yet)."""
    out = []
    for rs in HOLDS.values():
        if not (rs.proc and rs.proc.returncode is None): continue
        snap = _latest_snapshot_for(rs)
        if snap is None: out.append(rs)
    return out


def _held_jobs() -> list[RunState]:
    """Jobs with an active server-side hold (snapshot exists + process alive)."""
    out = []
    for rs in HOLDS.values():
        if not (rs.proc and rs.proc.returncode is None): continue
        snap = _latest_snapshot_for(rs)
        if snap is not None: out.append(rs)
    return out


def _live_jobs() -> list[RunState]:
    """Any hold with a live subprocess."""
    return [rs for rs in HOLDS.values()
            if rs.proc and rs.proc.returncode is None]


def _launcher_text() -> str:
    """Matches sniper.js:mainMenuText style. Adds a Buyer line because Vatican
    booking carries a per-visit `representativeUser` that sniper's Carcer/
    Colosseo API does not — so the mate needs to see buyer readiness at a
    glance (it's required right before /pay, not at hunt time)."""
    hunting = len(_hunting_jobs())
    held    = len(_held_jobs())
    sel     = MENU.get(AUTHORIZED_CHAT) or MenuSel()
    missing = _buyer_missing(sel)
    buyer_badge = (f"👤 Buyer: ✅ {sel.first} {sel.last}" if not missing
                   else "👤 Buyer: ⚠️ not set (needed before /pay)")
    return (f"🎯 <b>Vaticano Sniper</b>\n"
            f"🎯 {hunting} hunting · ✅ {held} held\n"
            f"{buyer_badge}")


def _launcher_kbd() -> InlineKeyboardMarkup:
    """3x2 grid — sniper.js's 2x2 extended with a Buyer row (Vatican needs it)."""
    jobs_n = len(_live_jobs())
    sel    = MENU.get(AUTHORIZED_CHAT) or MenuSel()
    buyer_tag = "✅" if not _buyer_missing(sel) else "⚠️"
    return kbd([
        [("🎫 New hunt",    "cards"), (f"🎯 Jobs ({jobs_n})", "jobs")],
        [("📊 Consumption", "cons"),  ("🌐 Site status",      "site")],
        [(f"{buyer_tag} Buyer", "buyer_open"), ("❓ Help", "help_open")],
    ])


# Track the current menu message id per chat so cmd_menu can tgEdit instead
# of spamming a new message — matches sniper.js's S.menuMsgs[chatId] pattern.
_MENU_MSG_ID: dict[int, int] = {}


def _jobs_screen() -> tuple[str, InlineKeyboardMarkup]:
    """Mirrors sniper.js jobsCardsText + jobsCardsKb. One card per live job.
    Since Telegram can't show N independent cards in a single edit, we put
    a concise summary in the text and per-job buttons below."""
    jobs = sorted(_live_jobs(),
                  key=lambda r: r.started_at or dt.datetime.min, reverse=True)
    if not jobs:
        return ("🎯 <b>Jobs</b>\n\nNothing hunting or held right now.\n"
                "Tap 🎫 New hunt to catch a slot.",
                kbd([[("↩ Back", "main")]]))
    lines = ["🎯 <b>Jobs</b>", ""]
    rows = []
    for rs in jobs:
        s    = rs.slot_meta or {}
        age  = ((dt.datetime.utcnow() - rs.started_at).total_seconds()
                if rs.started_at else 0)
        age_tag = f"{int(age/60)}m{int(age%60):02d}s"
        snap = _latest_snapshot_for(rs)
        emoji = "✅" if snap else "🎯"
        tix  = (f"{s.get('full',0)}f+{s.get('reduced',0)}r"
                + (f"+{s.get('audio',0)}a" if s.get('audio',0) else ""))
        slot_label = f"{s.get('date','?')} {s.get('time') or 'any'}"
        lines.append(
            f"{emoji} <b>{slot_label}</b> · {tix} · age {age_tag} "
            f"<i>({rs.hold_id})</i>"
        )
        # Human-readable button labels: date + time, not hold-id
        if rs.kind == "longhold":
            rows.append([(f"💳 Pay {slot_label}",  f"h:pay:{rs.hold_id}"),
                         (f"🛑 Stop {slot_label}", f"h:rel:{rs.hold_id}")])
        elif rs.kind == "live":
            rows.append([(f"💳 Pay form {slot_label}", f"h:paynow:{rs.hold_id}"),
                         (f"🛑 Stop {slot_label}",     f"h:rel:{rs.hold_id}")])
        else:
            rows.append([(f"🛑 Stop {slot_label}", f"h:rel:{rs.hold_id}")])
    rows.append([("🔄 Refresh", "jobs"), ("↩ Back", "main")])
    return ("\n".join(lines), kbd(rows))


def _format_mb(b: float) -> str:
    if b >= 1024*1024*1024: return f"{b/(1024**3):.2f} GB"
    if b >= 1024*1024:      return f"{b/(1024**2):.1f} MB"
    if b >= 1024:           return f"{b/1024:.1f} KB"
    return f"{b:.0f} B"


def _consumption_screen() -> tuple[str, InlineKeyboardMarkup]:
    """Mirrors sniper.js:consumptionText. No Evomi API, so this is estimated
    from our subprocess activity (≈1 MB per F5 solve + ~5 KB per /recap POST
    + ~3 KB per GET keepalive). Still useful to see per-hold burn and the
    hunting/held split.
    """
    hunting = len(_hunting_jobs())
    held    = len(_held_jobs())
    jobs    = _live_jobs()

    # Rough internal byte meter: scan each job's subprocess log for cycles
    # and multiply by the measured per-cycle averages.
    AVG_F5_SOLVE_BYTES   = 1_000_000   # ~1 MB per fresh F5 TSPD solve
    AVG_RECAP_POST_BYTES =     5_000   # POST /recap + response
    AVG_GET_BYTES        =     3_000   # cheap /api/config/isAgency etc.

    per_job: list[tuple[str, int]] = []
    total_bytes = 0
    for rs in jobs:
        f5_n = recap_n = get_n = 0
        try:
            if rs.log_path and rs.log_path.exists():
                txt = rs.log_path.read_text(encoding="utf-8", errors="replace")
                f5_n    = txt.count("F5 solve finished")
                recap_n = txt.count("/recap OK")
                get_n   = sum(1 for line in txt.splitlines()
                              if "GET /api/" in line and "→ HTTP 200" in line)
        except Exception: pass
        b = f5_n*AVG_F5_SOLVE_BYTES + recap_n*AVG_RECAP_POST_BYTES + get_n*AVG_GET_BYTES
        total_bytes += b
        if b > 0: per_job.append((rs.hold_id, b))

    # Rate: estimate across live hunting jobs — 1 /recap cycle every ~2 min
    # per rotating-rerecap config = ~5 KB/min = ~300 KB/hr per slot, plus
    # occasional F5 solve adds ~1 MB every ~6 min per slot.
    rate_per_slot_mb_hr = 10.0 if hunting > 0 else 0.0   # rough: 10 MB/hr
    total_rate = rate_per_slot_mb_hr * (hunting + held)

    # Rough cost estimate — Evomi residential IT is roughly $3–5/GB.
    USD_PER_GB_LOW  = 3.0
    USD_PER_GB_HIGH = 5.0
    gb_used = total_bytes / (1024**3)
    usd_low  = gb_used * USD_PER_GB_LOW
    usd_high = gb_used * USD_PER_GB_HIGH

    body = ["📊 <b>Proxy consumption</b>", ""]
    body.append(f"🎯 Hunting (active catches): <b>{hunting}</b>")
    body.append(f"✅ Held (locked slots):     <b>{held}</b>")
    body.append("")
    body.append(f"📦 Data used this session: <b>{_format_mb(total_bytes)}</b>")
    body.append(f"💰 Approx cost so far: <b>${usd_low:.2f}–${usd_high:.2f}</b> "
                "<i>(at $3–5/GB)</i>")
    if total_rate > 0:
        body.append(f"⚡ Rate right now: <b>~{total_rate:.1f} MB/hr</b> "
                    f"(≈ ${(total_rate/1024*USD_PER_GB_HIGH):.2f}/hr)")
    else:
        body.append("⚡ Rate right now: <b>~0 MB/hr</b> (nothing hunting)")
    if per_job:
        body.append("")
        body.append("<b>Per hold</b>")
        per_job.sort(key=lambda x: -x[1])
        for hid, b in per_job[:10]:
            body.append(f"  <code>{hid}</code>: {_format_mb(b)}")
    body.append("")
    body.append("<i>Estimate only — no Evomi API key configured. Holding is "
                "cheap; most data goes to the initial catches.</i>")
    return "\n".join(body), kbd([[("🔄 Refresh", "cons"), ("↩ Back", "main")]])


def _site_status_screen() -> tuple[str, InlineKeyboardMarkup]:
    """Mirrors sniper.js:siteStatusText. For Vatican we don't have an API-
    layer health probe — we show the last successful catch time, last F5
    rejection time, and whether we're currently rate-limited (any job with
    a recent F5 reject in its log)."""
    now = dt.datetime.utcnow()
    last_recap: Optional[dt.datetime] = None
    last_reject: Optional[dt.datetime] = None
    for rs in HOLDS.values():
        if not rs.log_path or not rs.log_path.exists(): continue
        try:
            txt = rs.log_path.read_text(encoding="utf-8", errors="replace")
        except Exception: continue
        for line in txt.splitlines()[-500:]:
            if "/recap OK" in line and "[INFO]" in line:
                try:
                    t = dt.datetime.strptime(line[:19], "%Y-%m-%d %H:%M:%S")
                    if last_recap is None or t > last_recap: last_recap = t
                except Exception: pass
            if ("Request Rejected" in line or "F5 REJECTED" in line or
                "406" in line and "Bot" in line):
                try:
                    t = dt.datetime.strptime(line[:19], "%Y-%m-%d %H:%M:%S")
                    if last_reject is None or t > last_reject: last_reject = t
                except Exception: pass

    body = ["🌐 <b>Site status</b>  (tickets.museivaticani.va)", ""]
    if last_recap:
        age = int((now - last_recap).total_seconds())
        body.append(f"✅ Last /recap OK: <b>{int(age/60)}m{age%60:02d}s ago</b>")
    else:
        body.append("✅ Last /recap OK: — no catch yet")
    if last_reject:
        age = int((now - last_reject).total_seconds())
        symbol = "⚠️" if age < 300 else "·"
        body.append(f"{symbol} Last F5 reject: <b>{int(age/60)}m{age%60:02d}s ago</b>")
    else:
        body.append("· Last F5 reject: — none observed")
    body.append("")
    body.append(f"🎯 Hunting: {len(_hunting_jobs())}")
    body.append(f"✅ Held:    {len(_held_jobs())}")
    body.append("")
    body.append("<i>F5 TSPD + Cloudflare Turnstile are the two gates. "
                "Reject in last 5 min means the site is velocity-blocking "
                "our IP pool — expect slower rotations.</i>")
    return "\n".join(body), kbd([[("🔄 Refresh", "site"), ("↩ Back", "main")]])


async def _render_main_menu(ctx: ContextTypes.DEFAULT_TYPE,
                            edit_message: Optional[int] = None) -> None:
    """Send or edit the main-menu message. Mirrors tgEdit(chatId, text, kb)."""
    txt = _launcher_text()
    kb  = _launcher_kbd()
    if edit_message:
        try:
            await ctx.bot.edit_message_text(
                chat_id=AUTHORIZED_CHAT, message_id=edit_message,
                text=txt, reply_markup=kb, parse_mode=ParseMode.HTML,
                disable_web_page_preview=True)
            return
        except Exception as exc:
            log.debug("menu edit failed, falling back to send: %s", exc)
    msg = await ctx.bot.send_message(
        chat_id=AUTHORIZED_CHAT, text=txt, reply_markup=kb,
        parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    _MENU_MSG_ID[AUTHORIZED_CHAT] = msg.message_id


@only_our_chat
async def cmd_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Top-level launcher — identical to sniper.js /menu."""
    # Sniper's pattern: /menu drops the menu msg_id so a fresh card is sent,
    # then subsequent sub-screen taps edit THAT card.
    _MENU_MSG_ID.pop(AUTHORIZED_CHAT, None)
    await _render_main_menu(ctx)


# ── 4-step hunt wizard (matches sniper.js's step-by-step card flow) ────
# Each step edits the SAME message (tgEdit pattern). The MenuSel state
# collects date/time/tickets/mode across steps.

def _hunt_date_kbd(center_iso: str) -> InlineKeyboardMarkup:
    """Date picker: 8 days around `center` (2 rows × 4) + prev/next/back."""
    base = dt.date.fromisoformat(center_iso)
    days = [base + dt.timedelta(days=i) for i in range(-1, 7)]
    r1 = [(d.strftime("%a %d/%m"), f"hunt:date:set:{d.isoformat()}") for d in days[:4]]
    r2 = [(d.strftime("%a %d/%m"), f"hunt:date:set:{d.isoformat()}") for d in days[4:]]
    prev7 = (base - dt.timedelta(days=7)).isoformat()
    next7 = (base + dt.timedelta(days=7)).isoformat()
    return kbd([
        r1, r2,
        [("⏪ -7d", f"hunt:date:nav:{prev7}"),
         ("⏩ +7d", f"hunt:date:nav:{next7}")],
        [("↩ Back", "main")],
    ])


def _hunt_step1_date(sel: MenuSel) -> tuple[str, InlineKeyboardMarkup]:
    center = sel.date or _menu_default_date()
    txt = ("🎫 <b>New hunt · Step 1 of 4: Date</b>\n\n"
           f"Showing days around <code>{center}</code>.")
    return txt, _hunt_date_kbd(center)


_TIMES_PICKER = [
    ("any time",       ""),
    ("morning (09)",   "09"),
    ("mid-day (12)",   "12"),
    ("afternoon (14)", "14"),
    ("late (16)",      "16"),
    ("evening (17)",   "17"),
]


def _hunt_step2_time(sel: MenuSel) -> tuple[str, InlineKeyboardMarkup]:
    rows = []
    for label, val in _TIMES_PICKER:
        tag = "● " if sel.time == val else "○ "
        rows.append([(tag + label, f"hunt:time:set:{val or 'any'}")])
    rows.append([("↩ Back (date)", "hunt:back:1")])
    txt = ("🎫 <b>New hunt · Step 2 of 4: Time</b>\n\n"
           f"📅 Date: <code>{sel.date}</code>\n"
           "Pick a time (bot still falls back to any available slot in that window):")
    return txt, kbd(rows)


# Preset ticket mixes keyed to common Vatican admission combos.
_TIX_PRESETS = [
    ("1 reduced",         (0, 1, 0)),
    ("1 full",            (1, 0, 0)),
    ("2 full",            (2, 0, 0)),
    ("2 full + 1 reduced",(2, 1, 0)),
    ("2 full + 1 red + 1 audio", (2, 1, 1)),
    ("Custom (± buttons)", None),
]


def _hunt_step3_tix(sel: MenuSel, custom: bool = False) -> tuple[str, InlineKeyboardMarkup]:
    time_label = next((lbl for lbl, v in _TIMES_PICKER if v == sel.time), "any")
    header = (f"🎫 <b>New hunt · Step 3 of 4: Tickets</b>\n\n"
              f"📅 Date: <code>{sel.date}</code>\n"
              f"🕐 Time: <code>{time_label}</code>\n\n")
    if not custom:
        rows = []
        for label, triple in _TIX_PRESETS:
            if triple is None:
                rows.append([(label, "hunt:tix:custom")])
            else:
                tag = "● " if (sel.full, sel.reduced, sel.audio) == triple else "○ "
                rows.append([(tag + label, f"hunt:tix:preset:{triple[0]}:{triple[1]}:{triple[2]}")])
        rows.append([("↩ Back (time)", "hunt:back:2")])
        return header + "Pick a preset or Custom:", kbd(rows)
    # Custom: +/- buttons
    rows = [
        [("🎫 Full €20",     "noop"),
         ("➖",               "hunt:cust:full:dec"),
         (str(sel.full),     "noop"),
         ("➕",               "hunt:cust:full:inc")],
        [("🎫 Reduced €10",  "noop"),
         ("➖",               "hunt:cust:red:dec"),
         (str(sel.reduced),  "noop"),
         ("➕",               "hunt:cust:red:inc")],
        [("🎧 Audioguide €8", "noop"),
         ("➖",                "hunt:cust:audio:dec"),
         (str(sel.audio),     "noop"),
         ("➕",                "hunt:cust:audio:inc")],
        [("✅ Done — next (mode)", "hunt:tix:done")],
        [("↩ Back (presets)",    "hunt:back:3preset")],
    ]
    total = 20*sel.full + 10*sel.reduced + 5*(sel.full+sel.reduced) + 8*sel.audio
    body = header + (f"Custom — tap ± to adjust.\n\n"
                     f"👥 Visitors: <b>{sel.num}</b>\n"
                     f"💶 Estimated: <b>€{total:.2f}</b>")
    return body, kbd(rows)


def _hunt_step4_mode(sel: MenuSel) -> tuple[str, InlineKeyboardMarkup]:
    time_label = next((lbl for lbl, v in _TIMES_PICKER if v == sel.time), "any")
    total = 20*sel.full + 10*sel.reduced + 5*sel.num + 8*sel.audio
    txt = ("🎫 <b>New hunt · Step 4 of 4: Mode</b>\n\n"
           f"📅 {sel.date}  🕐 {time_label}\n"
           f"🎫 {sel.full}f + {sel.reduced}r"
           + (f" + {sel.audio}a" if sel.audio else "")
           + f"  ·  <b>€{total:.2f}</b>\n\n"
           "Pick the mode:")
    rows = [
        [("📦 HOLD — no captcha, keep for days", "hunt:mode:hold")],
        [("💳 PAY NOW — 1 captcha, instant",     "hunt:mode:live")],
        [("↩ Back (tickets)",                     "hunt:back:3")],
    ]
    return txt, kbd(rows)


def _hunt_confirm(sel: MenuSel) -> tuple[str, InlineKeyboardMarkup]:
    time_label = next((lbl for lbl, v in _TIMES_PICKER if v == sel.time), "any")
    total = 20*sel.full + 10*sel.reduced + 5*sel.num + 8*sel.audio
    mode_tag = {"hold": "📦 HOLD (no captcha)",
                "live": "💳 PAY NOW (1 captcha)",
                "dry":  "🧪 DRY"}.get(sel.mode, sel.mode)
    missing = _buyer_missing(sel) if sel.mode == "live" else []
    body = ("🎫 <b>Confirm hunt</b>\n\n"
            f"📅 {sel.date}  🕐 {time_label}\n"
            f"🎯 Admission Ticket\n"
            f"🎫 {sel.full}f + {sel.reduced}r"
            + (f" + {sel.audio}a" if sel.audio else "")
            + f"  ·  👥 {sel.num}  ·  <b>€{total:.2f}</b>\n"
            f"🔧 {mode_tag}")
    if missing:
        body += ("\n\n⚠️ <b>Buyer incomplete</b> — PAY NOW will refuse until "
                 f"you set: {', '.join(missing)}. Tap below to open the form.")
        rows = [
            [("👤 Fix buyer first", "buyer_open")],
            [("↩ Back (mode)",    "hunt:back:4")],
        ]
    else:
        rows = [
            [("✅ CATCH!",          "hunt:fire"),
             ("↩ Back (mode)",    "hunt:back:4")],
        ]
    return body, kbd(rows)


async def _open_catch_config(q, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Entry into the 4-step New-hunt wizard."""
    sel = MENU.setdefault(AUTHORIZED_CHAT, MenuSel())
    if not sel.date:
        sel.date = _menu_default_date()
    txt, markup = _hunt_step1_date(sel)
    try:
        await q.message.edit_text(txt, parse_mode=ParseMode.HTML, reply_markup=markup)
    except Exception as exc:
        log.debug("open_catch_config failed: %s", exc)


@only_our_chat
async def cb_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle every click on the inline menu."""
    q = update.callback_query
    if not q or not q.data:
        return
    await q.answer()   # dismiss the "loading…" spinner on the button

    sel = MENU.setdefault(AUTHORIZED_CHAT, MenuSel())
    if not sel.date:
        sel.date = _menu_default_date()

    data = q.data
    parts = data.split(":")

    if data == "noop":
        return

    # ---------- carcere-style Refresh/Verbose on /status ----------
    if parts[0] == "refreshstatus":
        verbose = len(parts) > 1 and parts[1] == "verbose"
        try:
            await q.message.edit_text(
                _build_status_text(verbose=verbose),
                parse_mode=ParseMode.HTML,
                reply_markup=kbd([[("Refresh", "refreshstatus"),
                                    ("Verbose", "refreshstatus:verbose")]]))
        except Exception as exc:
            log.debug("refreshstatus edit failed: %s", exc)
        return

    # ---------- sniper.js-style top-level ----------
    if data == "cards":
        # 🎫 New hunt — open the catch-config screen (existing flow)
        await _open_catch_config(q, ctx); return

    if data == "jobs":
        txt, markup = _jobs_screen()
        try:
            await q.message.edit_text(txt, parse_mode=ParseMode.HTML,
                                      reply_markup=markup)
        except Exception as exc:
            log.debug("jobs render failed: %s", exc)
        return

    if data == "cons":
        txt, markup = _consumption_screen()
        try:
            await q.message.edit_text(txt, parse_mode=ParseMode.HTML,
                                      reply_markup=markup)
        except Exception as exc:
            log.debug("cons render failed: %s", exc)
        return

    if data == "site":
        txt, markup = _site_status_screen()
        try:
            await q.message.edit_text(txt, parse_mode=ParseMode.HTML,
                                      reply_markup=markup)
        except Exception as exc:
            log.debug("site render failed: %s", exc)
        return

    if data == "main":
        # Back to main menu (edit in place)
        try:
            await q.message.edit_text(_launcher_text(), parse_mode=ParseMode.HTML,
                                      reply_markup=_launcher_kbd(),
                                      disable_web_page_preview=True)
        except Exception as exc:
            log.debug("main edit failed: %s", exc)
        return

    if data == "buyer_open":
        b = MENU.setdefault(AUTHORIZED_CHAT, MenuSel())
        try:
            await q.message.edit_text(
                _buyer_form_text(b), parse_mode=ParseMode.HTML,
                reply_markup=_buyer_form_render(b, "main"))
        except Exception as exc:
            log.debug("buyer_open failed: %s", exc)
        return

    # ---------- 4-step New-hunt wizard ----------
    if parts[0] == "hunt":
        if not sel.date:
            sel.date = _menu_default_date()
        action = parts[1] if len(parts) > 1 else ""

        async def render(builder, *args):
            txt, markup = builder(sel, *args)
            try:
                await q.message.edit_text(txt, parse_mode=ParseMode.HTML,
                                          reply_markup=markup)
            except Exception as exc:
                log.debug("hunt render failed: %s", exc)

        # ── Back ────────────────────────────────────────────────────────
        if action == "back":
            where = parts[2] if len(parts) > 2 else "1"
            if where == "1":       await render(_hunt_step1_date); return
            if where == "2":       await render(_hunt_step2_time); return
            if where == "3":       await render(_hunt_step3_tix);  return
            if where == "3preset": await render(_hunt_step3_tix, False); return
            if where == "4":       await render(_hunt_step4_mode); return
            return

        # ── Step 1: date ────────────────────────────────────────────────
        if action == "date":
            op = parts[2]
            if op == "nav":
                center = parts[3]
                try:
                    await q.message.edit_text(
                        f"🎫 <b>New hunt · Step 1 of 4: Date</b>\n\n"
                        f"Showing days around <code>{center}</code>.",
                        parse_mode=ParseMode.HTML,
                        reply_markup=_hunt_date_kbd(center))
                except Exception: pass
                return
            if op == "set":
                sel.date = parts[3]
                _save_menu_state()
                await render(_hunt_step2_time); return

        # ── Step 2: time ────────────────────────────────────────────────
        if action == "time":
            if parts[2] == "set":
                val = parts[3]
                sel.time = "" if val == "any" else val
                _save_menu_state()
                await render(_hunt_step3_tix); return

        # ── Step 3: tickets ─────────────────────────────────────────────
        if action == "tix":
            op = parts[2]
            if op == "preset":
                full_, red_, audio_ = int(parts[3]), int(parts[4]), int(parts[5])
                sel.full, sel.reduced, sel.audio = full_, red_, audio_
                _save_menu_state()
                await render(_hunt_step4_mode); return
            if op == "custom":
                await render(_hunt_step3_tix, True); return
            if op == "done":
                if sel.full + sel.reduced < 1:
                    try: await q.answer("Pick at least one ticket!", show_alert=True)
                    except Exception: pass
                    return
                if sel.audio > sel.num:
                    sel.audio = sel.num
                _save_menu_state()
                await render(_hunt_step4_mode); return

        if action == "cust":
            # Custom +/- controls (tix step)
            kind, op = parts[2], parts[3]
            if kind == "full":
                if op == "inc" and sel.full + sel.reduced < 6: sel.full += 1
                elif op == "dec" and sel.full > 0: sel.full -= 1
            elif kind == "red":
                if op == "inc" and sel.full + sel.reduced < 6: sel.reduced += 1
                elif op == "dec" and sel.reduced > 0: sel.reduced -= 1
                if sel.full == 0 and sel.reduced == 0: sel.reduced = 1
            elif kind == "audio":
                if op == "inc" and sel.audio < sel.num: sel.audio += 1
                elif op == "dec" and sel.audio > 0: sel.audio -= 1
            _save_menu_state()
            await render(_hunt_step3_tix, True); return

        # ── Step 4: mode ────────────────────────────────────────────────
        if action == "mode":
            sel.mode = parts[2]
            _save_menu_state()
            await render(_hunt_confirm); return

        # ── Fire ────────────────────────────────────────────────────────
        if action == "fire":
            try:
                await q.message.edit_text(
                    f"🚀 Firing <b>{sel.mode.upper()}</b>\n"
                    f"📅 {sel.date}  🕐 {sel.time or 'any'}\n"
                    f"🎫 {sel.full}f + {sel.reduced}r"
                    + (f" + {sel.audio}a" if sel.audio else ""),
                    parse_mode=ParseMode.HTML)
            except Exception: pass
            _save_menu_state()
            await _spawn_api_hold_from_menu(ctx, sel)
            return

        return

    if data == "help_open":
        try:
            await q.message.edit_text(
                HELP_TEXT, parse_mode=ParseMode.HTML,
                reply_markup=kbd([[("↩ Back", "main")]]),
                disable_web_page_preview=True)
        except Exception as exc:
            log.debug("help_open failed: %s", exc)
        return

    # ---------- legacy top:* (kept so earlier messages still work) ----------
    if parts[0] == "top":
        action = parts[1] if len(parts) > 1 else ""

        if action == "close":
            try: await q.message.edit_text("✖ closed. /menu to reopen.")
            except Exception: pass
            return

        if action == "back":
            # Back to the launcher
            try:
                await q.message.edit_text(_launcher_text(), parse_mode=ParseMode.HTML,
                                          reply_markup=_launcher_kbd())
            except Exception: pass
            return

        if action == "catch":
            await _open_catch_config(q, ctx); return

        if action == "held":
            # Replace current message with the collection summary, then
            # cards post as separate messages from cmd_held's send loop.
            if not HOLDS:
                try: await q.message.edit_text(
                    "📦 nothing held. Tap 🎟️ New catch → 📦 HOLD mode.",
                    reply_markup=kbd([[("↩ Back", "top:back")]]))
                except Exception: pass
                return
            # Reuse cmd_held's rendering: edit the launcher to a header, then
            # fire the cards as new messages below.
            try:
                rows = sorted(HOLDS.values(),
                              key=lambda r: r.started_at or dt.datetime.min, reverse=True)
                await q.message.edit_text(
                    f"📦 <b>Your collection</b> · {len(rows)} slot(s)",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kbd([[("↩ Back", "top:back")]]))
                for rs in rows:
                    await ctx.bot.send_message(
                        chat_id=AUTHORIZED_CHAT, text=_hold_card(rs),
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True,
                        reply_markup=_hold_buttons(rs.hold_id, rs.kind))
            except Exception as exc:
                log.debug("top:held failed: %s", exc)
            return

        if action == "buyer":
            b = MENU.setdefault(AUTHORIZED_CHAT, MenuSel())
            try:
                await q.message.edit_text(
                    _buyer_form_text(b), parse_mode=ParseMode.HTML,
                    reply_markup=_buyer_form_render(b, "top"))
            except Exception as exc:
                log.debug("top:buyer failed: %s", exc)
            return

        if action == "status":
            try:
                await q.message.edit_text(
                    _build_status_text(verbose=False),
                    parse_mode=ParseMode.HTML,
                    reply_markup=kbd([[("Refresh", "refreshstatus"),
                                        ("Verbose", "refreshstatus:verbose")],
                                       [("↩ Back", "top:back")]]))
            except Exception: pass
            return

        if action == "pay":
            # Show each held longhold as a pickable row — tap to open pay confirm.
            longholds = [rs for rs in HOLDS.values() if rs.kind == "longhold"
                         and rs.proc and rs.proc.returncode is None]
            if not longholds:
                try: await q.message.edit_text(
                    "📦 nothing to pay. Catch a HOLD first, then come back.",
                    reply_markup=kbd([[("↩ Back", "top:back")]]))
                except Exception: pass
                return
            rows = []
            for rs in longholds:
                s = rs.slot_meta or {}
                label = f"{rs.hold_id}  {s.get('date','?')} {s.get('time') or 'any'}"
                rows.append([(label, f"h:pay:{rs.hold_id}")])
            rows.append([("↩ Back", "top:back")])
            try:
                await q.message.edit_text(
                    "<b>💳 Which hold do you want to pay?</b>\n"
                    "(this spends 1 captcha + opens the Catholica form)",
                    parse_mode=ParseMode.HTML, reply_markup=kbd(rows))
            except Exception: pass
            return

        if action == "logs":
            running = [rs for rs in HOLDS.values()
                       if rs.proc and rs.proc.returncode is None]
            if not running:
                try: await q.message.edit_text(
                    "no runs — nothing to show.",
                    reply_markup=kbd([[("↩ Back", "top:back")]]))
                except Exception: pass
                return
            rows = [[(rs.hold_id, f"top:logs1:{rs.hold_id}")] for rs in running]
            rows.append([("↩ Back", "top:back")])
            try:
                await q.message.edit_text(
                    "Pick a run to view the last 30 log lines:",
                    reply_markup=kbd(rows))
            except Exception: pass
            return

        if action == "logs1":
            hid = parts[2] if len(parts) > 2 else ""
            rs = HOLDS.get(hid)
            if rs is None or not rs.last_lines:
                try: await q.message.edit_text(
                    f"no lines yet for <code>{hid}</code>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kbd([[("↩ Back", "top:logs")]]))
                except Exception: pass
                return
            body = "\n".join(rs.last_lines[-30:])[-3500:]
            try:
                await q.message.edit_text(
                    f"<b>{hid}</b>\n<pre>{_html_escape(body)}</pre>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kbd([[("↩ Back", "top:logs")]]))
            except Exception: pass
            return

        if action == "live":
            try: await q.message.edit_text(
                "📡 <b>Live stream</b>\n\n"
                "Use <code>/live &lt;hold_id&gt;</code> — streams any new "
                "interesting line for 5 min.\n\n"
                "See /held for hold_ids.",
                parse_mode=ParseMode.HTML,
                reply_markup=kbd([[("↩ Back", "top:back")]]))
            except Exception: pass
            return

        if action == "slots":
            try: await q.message.edit_text(
                "🔎 <b>Slot probe</b>\n\n"
                "Use <code>/slots YYYY-MM-DD</code> to list Admission times "
                "Vatican currently offers on that date. No captcha.",
                parse_mode=ParseMode.HTML,
                reply_markup=kbd([[("↩ Back", "top:back")]]))
            except Exception: pass
            return

        if action == "more":
            try: await q.message.edit_text(
                HELP_TEXT, parse_mode=ParseMode.HTML,
                reply_markup=kbd([[("↩ Back", "top:back")]]))
            except Exception: pass
            return

        if action == "help":
            try: await q.message.edit_text(
                HELP_TEXT, parse_mode=ParseMode.HTML,
                reply_markup=kbd([[("↩ Back", "top:back")]]))
            except Exception: pass
            return

        return

    # ---------- /buyer form buttons ----------
    if parts[0] == "b":
        action = parts[1] if len(parts) > 1 else ""

        if action == "close":
            # "✅ Done" from the launcher-opened form — go back to top-level
            # menu so the mate stays in the drill-down flow instead of
            # landing on a dead screen.
            try:
                await q.message.edit_text(_launcher_text(),
                                          parse_mode=ParseMode.HTML,
                                          reply_markup=_launcher_kbd())
            except Exception:
                try: await q.message.edit_text("✅ buyer saved.")
                except Exception: pass
            return

        if action == "set":
            # "b:set:<field>:<return_to>"  — where <return_to> may itself be
            # "pay:<hid>" so parts length is 4+
            field = parts[2] if len(parts) > 2 else ""
            return_to = ":".join(parts[3:]) if len(parts) > 3 else "none"
            if field not in _BUYER_FIELD_SPEC:
                return
            AWAITING[AUTHORIZED_CHAT] = {"field": field, "return_to": return_to}
            spec = _BUYER_FIELD_SPEC[field]
            prompt_msg = (f"<b>{spec['label']}</b>\n\n{spec['prompt']}\n\n"
                          f"<i>Send the value as your next message, "
                          f"or tap ✖ to cancel.</i>")
            try:
                await q.message.edit_text(
                    prompt_msg, parse_mode=ParseMode.HTML,
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("✖ cancel",
                                             callback_data=f"b:cancel:{return_to}")
                    ]]),
                )
            except Exception as exc:
                log.debug("buyer field prompt render failed: %s", exc)
            return

        if action == "gender":
            val = parts[2] if len(parts) > 2 else ""
            return_to = ":".join(parts[3:]) if len(parts) > 3 else "none"
            sel.gender = val if val in ("M", "F") else sel.gender
            _save_menu_state()
            try:
                await q.message.edit_text(
                    _buyer_form_text(sel), parse_mode=ParseMode.HTML,
                    reply_markup=_buyer_form_render(sel, return_to))
            except Exception: pass
            return

        if action == "clear":
            return_to = ":".join(parts[2:]) if len(parts) > 2 else "none"
            for k in list(_BUYER_FIELD_SPEC.keys()):
                setattr(sel, k, "")
            sel.gender = ""
            _save_menu_state()
            try:
                await q.message.edit_text(
                    _buyer_form_text(sel), parse_mode=ParseMode.HTML,
                    reply_markup=_buyer_form_render(sel, return_to))
            except Exception: pass
            return

        if action == "cancel":
            return_to = ":".join(parts[2:]) if len(parts) > 2 else "none"
            AWAITING.pop(AUTHORIZED_CHAT, None)
            try:
                await q.message.edit_text(
                    _buyer_form_text(sel), parse_mode=ParseMode.HTML,
                    reply_markup=_buyer_form_render(sel, return_to))
            except Exception: pass
            return

        return

    # ---------- /held card buttons ----------
    if parts[0] == "h":
        action = parts[1] if len(parts) > 1 else ""
        hid    = parts[2] if len(parts) > 2 else ""
        rs = HOLDS.get(hid)

        if action == "ref":
            if rs is None:
                try: await q.message.edit_text(f"❌ hold {hid} is gone (released or died)")
                except Exception: pass
                return
            try:
                await q.message.edit_text(_hold_card(rs),
                                          parse_mode=ParseMode.HTML,
                                          reply_markup=_hold_buttons(rs.hold_id, rs.kind))
            except Exception as exc:
                log.debug("held refresh failed: %s", exc)
            return

        if action == "rel":
            msg = await _do_release(ctx, hid)
            try:
                await q.message.edit_text(msg, parse_mode=ParseMode.HTML)
            except Exception:
                await _send(ctx, msg)
            return

        if action == "pay":
            if rs is None:
                try: await q.message.edit_text(f"❌ hold {hid} is gone")
                except Exception: pass
                return
            if rs.kind != "longhold":
                try: await q.answer(f"{hid} is a {rs.kind}, not a perpetual HOLD.",
                                    show_alert=True)
                except Exception: pass
                return
            est = ""
            s = rs.slot_meta or {}
            try:
                total_eur = 20*s.get("full",0) + 10*s.get("reduced",0) + 5*s.get("num",0) + 8*s.get("audio",0)
                est = f" · estimated €{total_eur}"
            except Exception: pass
            # Pull the CURRENT /setbuyer state — this is what will get written
            # on the PDF, not what was active at catch time.
            b = MENU.get(AUTHORIZED_CHAT) or MenuSel()
            missing = _buyer_missing(b)
            # If buyer is incomplete, open the form directly — no confirm card
            # yet, because the YES button would just error out.
            if missing:
                txt = (f"<b>📝 Buyer incomplete — fill it in before paying #{hid}</b>\n\n"
                       f"Missing: <b>{', '.join(missing)}</b>\n\n"
                       + _buyer_form_text(b))
                try:
                    await q.message.edit_text(txt, parse_mode=ParseMode.HTML,
                                              reply_markup=_buyer_form_render(b, f"pay:{hid}"))
                except Exception as exc:
                    log.debug("buyer form render failed: %s", exc)
                return
            names_disp = s.get("names") or b.names or f"{b.first} {b.last} (× {s.get('num',1)})"
            txt = (f"<b>💳 Confirm payment</b>\n\n{_hold_card(rs)}\n"
                   f"<b>👤 Buyer (will go on the PDF)</b>\n"
                   f"👤 <b>{b.first} {b.last}</b> · {b.email}\n"
                   f"📞 {b.phone} · 🌍 {b.country}/{b.city}\n"
                   f"🎂 {b.birth} · ⚧ {b.gender}\n"
                   f"📝 Participants: <code>{names_disp}</code>\n\n"
                   f"⚠️ This will spend <b>1 Turnstile solve (≈$0.005)</b>"
                   f"{est}\nand open the Catholica payment form in a browser.")
            try:
                await q.message.edit_text(txt, parse_mode=ParseMode.HTML,
                                          reply_markup=_pay_confirm_buttons(hid))
            except Exception as exc:
                log.debug("pay confirm render failed: %s", exc)
            return

        if action == "paybuy":
            # Open the step-by-step buyer form, pre-wired to return to the
            # pay-confirm card for this specific hold.
            b = MENU.setdefault(AUTHORIZED_CHAT, MenuSel())
            try:
                await q.message.edit_text(
                    _buyer_form_text(b), parse_mode=ParseMode.HTML,
                    reply_markup=_buyer_form_render(b, f"pay:{hid}"))
            except Exception as exc:
                log.debug("buyer form render failed: %s", exc)
            return

        if action == "back":
            if rs is None:
                try: await q.message.edit_text(f"❌ hold {hid} is gone")
                except Exception: pass
                return
            try:
                await q.message.edit_text(_hold_card(rs),
                                          parse_mode=ParseMode.HTML,
                                          reply_markup=_hold_buttons(rs.hold_id, rs.kind))
            except Exception: pass
            return

        if action == "paygo":
            # Confirmed — fire the pay-catch.
            ok, msg = await _do_pay(ctx, hid)
            try:
                await q.message.edit_text(
                    msg + ("\n\nCatch starting — you'll see milestones here."
                           if ok else ""),
                    parse_mode=ParseMode.HTML)
            except Exception:
                await _send(ctx, msg)
            return

        if action == "paynow":
            # A LIVE hold already has reservation.referenceOrder + epay.mac,
            # so we can skip the catch entirely and just reopen pay.py on
            # the existing snapshot. No captcha, no re-catch.
            if rs is None:
                try: await q.message.edit_text(f"❌ hold {hid} is gone")
                except Exception: pass
                return
            snap = _latest_snapshot_for(rs)
            if snap is None:
                try: await q.message.edit_text(
                    f"❌ no snapshot found for {hid} yet — still catching?")
                except Exception: pass
                return
            # Preflight: confirm the snapshot actually carries a reservation
            try:
                d = json.loads(snap.read_text(encoding="utf-8"))
                res = d.get("reservation") or {}
                ref = res.get("referenceOrder")
                total = res.get("total")
                urlms = (res.get("epay") or {}).get("urlMs", "")
            except Exception as exc:
                try: await q.message.edit_text(
                    f"❌ snapshot read failed: {exc}")
                except Exception: pass
                return
            if not ref:
                try: await q.message.edit_text(
                    f"❌ snapshot {snap.name} has no referenceOrder yet — "
                    "catch may still be in-flight. Try refreshing the card.")
                except Exception: pass
                return
            # Launch pay.py in the background — it writes pay_<ref>.html and
            # opens it in the default browser on the laptop running this bot.
            try:
                import subprocess as _sp
                _sp.Popen(
                    [sys.executable, "pay.py", "--file", str(snap)],
                    cwd=str(BOT_DIR),
                    stdout=_sp.DEVNULL, stderr=_sp.DEVNULL,
                    creationflags=(0x00000008 if sys.platform == "win32" else 0),
                )
                opened_msg = (f"💳 Opening Catholica form on your laptop…\n\n"
                              f"ref: <code>{ref}</code>\n"
                              f"total: €{total}\n"
                              f"urlMs: <code>{urlms[:60]}…</code>")
                try:
                    await q.message.edit_text(opened_msg, parse_mode=ParseMode.HTML)
                except Exception:
                    await _send(ctx, opened_msg, parse=ParseMode.HTML)
            except Exception as exc:
                try: await q.message.edit_text(f"❌ pay.py launch failed: {exc}")
                except Exception: pass
            return

        # Unknown h:* action — ignore
        return
    if data == "m:back":
        await q.message.edit_text(_menu_text(sel), reply_markup=_menu_render(sel),
                                  parse_mode=ParseMode.HTML)
        return
    if data == "m:cancel":
        await q.message.edit_text("❌ Menu cancelled.")
        MENU.pop(AUTHORIZED_CHAT, None)
        return

    async def refresh():
        """Edit the message to show the current menu state."""
        try:
            await q.message.edit_text(_menu_text(sel), reply_markup=_menu_render(sel),
                                      parse_mode=ParseMode.HTML)
        except Exception as exc:
            log.debug("menu refresh skipped: %s", exc)
        _save_menu_state()

    # ---------- Date ----------
    if parts[1] == "date":
        action = parts[2]
        if action == "open":
            await q.message.edit_text(
                f"📅 Pick a visit date (showing days around <b>{sel.date}</b>):",
                reply_markup=_date_picker(sel.date),
                parse_mode=ParseMode.HTML,
            ); return
        if action == "nav":
            await q.message.edit_text(
                f"📅 Pick a visit date (around <b>{parts[3]}</b>):",
                reply_markup=_date_picker(parts[3]),
                parse_mode=ParseMode.HTML,
            ); return
        if action == "set":
            sel.date = parts[3]; await refresh(); return

    # ---------- Time ----------
    if parts[1] == "time":
        if parts[2] == "open":
            await q.message.edit_text(
                "🕐 Pick a time preference:",
                reply_markup=_time_picker(),
            ); return
        if parts[2] == "set":
            sel.time = "" if parts[3] == "any" else parts[3]
            await refresh(); return

    # ---------- Product ----------
    if parts[1] == "product":
        if parts[2] == "open":
            await q.message.edit_text(
                "🎯 Pick a product type:",
                reply_markup=_product_picker(),
            ); return
        if parts[2] == "set":
            sel.product = parts[3]; await refresh(); return

    # ---------- Ticket counts ----------
    if parts[1] == "full":
        if parts[2] == "inc" and (sel.full + sel.reduced) < 6: sel.full += 1
        elif parts[2] == "dec" and sel.full > 0:               sel.full -= 1
        await refresh(); return
    if parts[1] == "red":
        if parts[2] == "inc" and (sel.full + sel.reduced) < 6: sel.reduced += 1
        elif parts[2] == "dec" and sel.reduced > 0:            sel.reduced -= 1
        # Keep at least 1 visitor total
        if sel.full == 0 and sel.reduced == 0: sel.reduced = 1
        await refresh(); return
    if parts[1] == "audio":
        if parts[2] == "inc" and sel.audio < sel.num: sel.audio += 1
        elif parts[2] == "dec" and sel.audio > 0:     sel.audio -= 1
        await refresh(); return

    # ---------- Mode ----------
    if parts[1] == "mode":
        sel.mode = parts[2]; await refresh(); return

    # ---------- Keepalive ----------
    if parts[1] == "ka":
        if parts[2] == "toggle":
            sel.keepalive = not sel.keepalive
        elif parts[2] == "inc":
            sel.keepalive_min = min(30, sel.keepalive_min + 5)
        elif parts[2] == "dec":
            sel.keepalive_min = max(5,  sel.keepalive_min - 5)
        await refresh(); return

    # ---------- Buyer info display ----------
    if parts[1] == "buyer":
        if parts[2] == "show":
            txt = (f"<b>👤 Current buyer</b>\n"
                   f"First:  <code>{sel.first}</code>\n"
                   f"Last:   <code>{sel.last}</code>\n"
                   f"Email:  <code>{sel.email}</code>\n"
                   f"Phone:  <code>{sel.phone}</code>\n"
                   f"Country:<code>{sel.country}</code>\n"
                   f"City:   <code>{sel.city}</code>\n"
                   f"Gender: <code>{sel.gender}</code>\n"
                   f"Birth:  <code>{sel.birth}</code>\n"
                   f"Names:  <code>{sel.names or '(auto — uses buyer name)'}</code>\n\n"
                   f"To edit, send:\n"
                   f"<code>/setbuyer first=Amin last=MOZ email=you@x.com "
                   f"phone=50038590 country=Tunisia city=ariana gender=M "
                   f"birth=1994-05-31</code>\n\n"
                   f"To set per-visitor names:\n"
                   f"<code>/setnames Amin MOZ, Mohamed Najjar</code>")
            await q.message.edit_text(
                txt, parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("↩ back to menu", callback_data="m:back")
                ]])); return

    # ---------- Fire ----------
    if data == "m:fire":
        if not sel.date:
            await q.message.reply_text("❗ Pick a date first.")
            return
        if sel.full + sel.reduced < 1:
            await q.message.reply_text("❗ Pick at least one ticket.")
            return
        await q.message.edit_text(
            f"🚀 Firing <b>{sel.mode.upper()}</b>\n"
            f"date={sel.date}  time={sel.time or 'any'}  product={sel.product}\n"
            f"tickets: {sel.full} full + {sel.reduced} reduced  audio={sel.audio}",
            parse_mode=ParseMode.HTML,
        )
        _save_menu_state()
        await _spawn_api_hold_from_menu(ctx, sel)
        return


async def _spawn_api_hold_from_menu(ctx: ContextTypes.DEFAULT_TYPE,
                                    sel: MenuSel) -> None:
    """Fire api_hold.py from the menu selection. Routes to HOLD (perpetual,
    no captcha) or LIVE (pay-now) depending on sel.mode."""
    mode = sel.mode  # "hold" | "live" | "dry"

    # Multiple HOLDs can run in parallel; only one LIVE/DRY at a time.
    cur = _active_hold()
    if mode in ("live", "dry") and cur and cur.kind in ("live", "dry"):
        await _send(ctx, "⚠️ Another non-hold run is in flight. /cancel first.")
        return

    # LIVE mode sends buyer fields to Vatican — refuse if any required field
    # is blank (otherwise argparse would accept the placeholder fallbacks below
    # and a fake "Guest Visitor" would end up on the PDF).
    if mode == "live":
        missing = _buyer_missing(sel)
        if missing:
            await _send(ctx,
                        f"❌ Buyer is incomplete — missing: <b>{', '.join(missing)}</b>.\n"
                        "Open /buyer to fill them in before firing a PAY NOW catch.",
                        parse=ParseMode.HTML)
            return

    cmd = [sys.executable, "api_hold.py",
           "--date",             sel.date,
           "--num",              str(sel.num),
           "--days-ahead",       "7",
           "--headless",
           "--session-aging-sec","75",
           "--rotate-ip",
           "--ip-retries",       "15",
           "--product",          sel.product,
           "--full",             str(sel.full),
           "--reduced",          str(sel.reduced),
           "--audio",            str(sel.audio),
           # Buyer args — api_hold.py's argparse validates --gender ∈ {M,F} and
           # the catch build_reservation_config fills representativeUser from
           # these. For HOLD mode nothing is sent to Vatican (only /recap body,
           # which has no buyer), so empty/default placeholders are harmless.
           # For LIVE mode the catch IS sent — _do_pay already blocks that path
           # when the buyer form is incomplete, so these fallbacks never ship
           # fake data to Vatican.
           "--first",            sel.first   or "Guest",
           "--last",             sel.last    or "Visitor",
           "--email",            sel.email   or "guest@example.com",
           "--phone",            sel.phone   or "0000000000",
           "--country",          sel.country or "Italy",
           "--city",             sel.city    or "Roma",
           "--gender",           sel.gender  or "M",
           "--birth",            sel.birth   or "1990-01-01"]
    if sel.time:
        cmd += ["--time", sel.time]
    if sel.names:
        cmd += ["--participants", sel.names]

    if mode == "hold":
        # LOW-COST perpetual hold: just /recap, no Turnstile, no money.
        cmd += ["--hold-only",
                "--rerecap-min",         str(sel.rerecap_min),
                "--rerecap-sess-cycles", str(sel.rerecap_sess),
                "--rerecap-cycles",      "0"]   # 0 = forever
        kind = "longhold"
        # Two filenames are written: the recap-only snapshot at t=0 (`*_recap.json`)
        # and the live-updated one each cycle (`*_recap_live.json`). Match both.
        snapshot_glob = "hold_*_recap*.json"
    elif mode == "live":
        cmd.append("--live")
        if sel.keepalive:
            cmd += ["--keepalive", "--keepalive-minutes", str(sel.keepalive_min)]
        kind = "live"
        snapshot_glob = "hold_*_live.json"
    else:
        kind = "dry"
        snapshot_glob = "hold_*_dry.json"

    args_summary = (f"{sel.date} {sel.time or 'any'} {sel.product} · "
                    f"full={sel.full} red={sel.reduced} audio={sel.audio}")
    slot_meta = {"date": sel.date, "time": sel.time, "product": sel.product,
                 "full": sel.full, "reduced": sel.reduced, "audio": sel.audio,
                 "num": sel.num, "names": sel.names,
                 "first": sel.first, "last": sel.last, "email": sel.email,
                 "phone": sel.phone, "country": sel.country, "city": sel.city,
                 "gender": sel.gender, "birth": sel.birth}

    await _spawn_api_hold_raw(ctx, kind=kind, cmd=cmd,
                              args_summary=args_summary,
                              slot_meta=slot_meta,
                              snapshot_glob=snapshot_glob)


# --------------------------------------------------------------------- commands
HELP_TEXT = """🎯 <b>Vaticano Sniper — commands</b>

<b>Main</b>
/menu          — main menu (🎫 New hunt · 🎯 Jobs · 📊 Consumption · 🌐 Site)
/jobs (/holds) — list every hunting + held slot, with Pay/Release buttons
/consumption (/usage) — Evomi/proxy burn estimate per hold
/site          — Vatican API health (last /recap OK, last F5 reject)
/refresh       — refresh menu counts

<b>Hunt / Hold / Pay</b>
/catch &lt;date&gt; [time] [tickets] [mode]
   e.g. /catch 2026-12-25 17:00 2full+1reduced+1audio hold
   Modes: hold (auto-refresh, no captcha) · live (1 captcha, instant pay)
/pay &lt;hold_id&gt;      — send a held slot to Catholica (1 captcha)
/stop &lt;hold_id&gt;     — stop holding (alias: /release)
/status [v]         — compact table (v = verbose)
/logs &lt;hold_id&gt;     — last 30 subprocess log lines
/live &lt;hold_id&gt;     — 5-min streaming log tail
/slots [date]       — probe Vatican availability (no captcha)

<b>Buyer</b>
/buyer              — step-by-step form (tap each field, send value)
/setbuyer first=… last=… email=… phone=… country=… city=… gender=M|F birth=YYYY-MM-DD
/setnames First1 Last1, First2 Last2 — per-visitor names
/resetbuyer         — wipe buyer data

Legacy aliases preserved: /dry /hold /cancel /last /release
"""


@only_our_chat
async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    await _send(ctx, HELP_TEXT, parse=ParseMode.HTML)


# ── sniper.js-style commands ─────────────────────────────────────────────
@only_our_chat
async def cmd_jobs(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Same as tapping 🎯 Jobs in the menu. Alias: /holds."""
    txt, markup = _jobs_screen()
    await ctx.bot.send_message(chat_id=AUTHORIZED_CHAT, text=txt,
                               reply_markup=markup, parse_mode=ParseMode.HTML,
                               disable_web_page_preview=True)


@only_our_chat
async def cmd_consumption(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Same as tapping 📊 Consumption. Aliases: /usage."""
    txt, markup = _consumption_screen()
    await ctx.bot.send_message(chat_id=AUTHORIZED_CHAT, text=txt,
                               reply_markup=markup, parse_mode=ParseMode.HTML,
                               disable_web_page_preview=True)


@only_our_chat
async def cmd_site(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Same as tapping 🌐 Site status."""
    txt, markup = _site_status_screen()
    await ctx.bot.send_message(chat_id=AUTHORIZED_CHAT, text=txt,
                               reply_markup=markup, parse_mode=ParseMode.HTML,
                               disable_web_page_preview=True)


@only_our_chat
async def cmd_refresh(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """/refresh — same name as sniper. Shows the menu with fresh counts."""
    await _render_main_menu(ctx)


@only_our_chat
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Show the main menu directly — matches sniper.js's /start behaviour.
    For the full command reference, the mate taps ❓ Help or sends /help."""
    _MENU_MSG_ID.pop(AUTHORIZED_CHAT, None)
    await _render_main_menu(ctx)


def _build_status_text(verbose: bool = False) -> str:
    """Carcere-style compact table: one line per hold."""
    running = [rs for rs in HOLDS.values()
               if rs.proc and rs.proc.returncode is None]
    dead    = [rs for rs in HOLDS.values()
               if not (rs.proc and rs.proc.returncode is None)]
    if not running and not dead:
        return "💤 no holds. /catch or /menu to start."
    lines = [f"<b>Holds:</b> {len(running)} running · {len(dead)} dead"]
    for rs in sorted(running, key=lambda r: r.started_at or dt.datetime.min):
        s = rs.slot_meta or {}
        age_s = (dt.datetime.utcnow() - rs.started_at).total_seconds() if rs.started_at else 0
        age = f"{int(age_s/60)}m{int(age_s%60):02d}s"
        lines.append(
            f"🟢 <code>{rs.hold_id}</code> {rs.kind} age={age} "
            f"{s.get('date','?')} {s.get('time') or 'any'} "
            f"full={s.get('full',0)} red={s.get('reduced',0)} audio={s.get('audio',0)}"
        )
        if verbose and rs.last_lines:
            lines.append(f"  <i>{_html_escape(rs.last_lines[-1][:200])}</i>")
    for rs in dead:
        lines.append(f"🔴 <code>{rs.hold_id}</code> {rs.kind} (dead)")
    return "\n".join(lines)


@only_our_chat
async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Carcere-style /status. Supports an optional hold_id arg and adds the
    Refresh/Verbose buttons the mate already knows from the other bot."""
    verbose = False
    if ctx.args:
        a = ctx.args[0].strip().lower()
        if a in ("v", "verbose", "-v"): verbose = True
    await ctx.bot.send_message(
        chat_id=AUTHORIZED_CHAT,
        text=_build_status_text(verbose=verbose),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
        reply_markup=kbd([[("Refresh", "refreshstatus"),
                            ("Verbose", "refreshstatus:verbose")]]),
    )


@only_our_chat
async def cmd_dry(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        date, num, _ = _parse_date_num(ctx.args or [])
    except ValueError as exc:
        await _send(ctx, f"❌ {exc}"); return
    await _spawn_api_hold(ctx, "dry", date, num)


@only_our_chat
async def cmd_hold(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        date, num, _ = _parse_date_num(ctx.args or [])
    except ValueError as exc:
        await _send(ctx, f"❌ {exc}"); return
    if not YESCAPTCHA_KEY:
        await _send(ctx, "❌ YESCAPTCHA_CLIENT_KEY is not set in .env"); return
    await _spawn_api_hold(ctx, "live", date, num)


async def _kill_hold(rs: RunState) -> None:
    try:
        rs.proc.terminate()
        try: await asyncio.wait_for(rs.proc.wait(), timeout=5)
        except asyncio.TimeoutError: rs.proc.kill()
    except Exception as exc:
        log.warning("kill %s failed: %s", rs.hold_id, exc)


@only_our_chat
async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """/cancel [hold_id]. No id = cancels the most-recent non-hold run (back-compat)."""
    args = ctx.args or []
    if args:
        hid = args[0].strip().lstrip("#")
        rs = HOLDS.get(hid)
        if rs is None or rs.proc is None or rs.proc.returncode is not None:
            await _send(ctx, f"❌ no live hold with id <code>{hid}</code>",
                        parse=ParseMode.HTML); return
        await _kill_hold(rs)
        await _send(ctx, f"🛑 cancelled {hid}")
        _save_held_registry()
        return
    rs = _active_hold()
    if rs is None:
        await _send(ctx, "💤 no run in flight. Use /held to list perpetual holds."); return
    await _kill_hold(rs)
    await _send(ctx, f"🛑 cancelled {rs.hold_id}")
    _save_held_registry()


# ----------------------------------------------------- /held /release /pay
def _latest_snapshot_for(rs: RunState) -> Optional[pathlib.Path]:
    """Find the most recent live snapshot matching this hold's glob."""
    if not rs.snapshot_glob: return None
    files = sorted(REPORTS.glob(rs.snapshot_glob), key=lambda p: p.stat().st_mtime)
    # Only count snapshots written AFTER this hold started
    started_ts = rs.started_at.timestamp() if rs.started_at else 0
    for f in reversed(files):
        if f.stat().st_mtime >= started_ts - 5:
            return f
    return files[-1] if files else None


def _hold_card(rs: RunState) -> str:
    alive_pid = rs.proc and _pid_alive(getattr(rs.proc, "pid", 0) or 0)
    alive_rc  = rs.proc is None or rs.proc.returncode is None
    alive     = bool(alive_pid and alive_rc)
    status    = "🟢 live" if alive else "🔴 dead"
    kind_tag  = {"longhold":"📦 HOLD","live":"💳 LIVE","dry":"🧪 DRY"}.get(rs.kind, rs.kind)
    s = rs.slot_meta or {}
    tickets = (f"{s.get('full',0)}×full + {s.get('reduced',0)}×reduced"
               + (f" + {s.get('audio',0)} audio" if s.get('audio',0) else ""))
    header  = (f"<b>#{rs.hold_id}</b>  {kind_tag}  {status}\n"
               f"📅 {s.get('date','?')} ⏰ {s.get('time') or 'any'} · 🎯 {s.get('product','?')}\n"
               f"🎫 {tickets}  👥 {s.get('num','?')}\n"
               f"👤 {s.get('first','?')} {s.get('last','?')}  ({s.get('email','?')})\n")
    snap = _latest_snapshot_for(rs)
    if snap:
        try:
            d = json.loads(snap.read_text(encoding="utf-8"))
            rr   = d.get("rotating_rerecap") or {}
            cyc  = rr.get("cycles") or []
            last = cyc[-1] if cyc else None
            res_id = (rr.get("last_reservation_id")
                      or (last or {}).get("reservation_id")
                      or (d.get("recap") or {}).get("reservationId")
                      or (d.get("reservation") or {}).get("referenceOrder"))
            total  = ((d.get("recap") or {}).get("total")
                      or (d.get("reservation") or {}).get("total")
                      or (last or {}).get("total"))
            parts = [f"🎟️ latest ref: <code>{res_id}</code>"] if res_id else []
            if total is not None:
                parts.append(f"💶 €{total}")
            if cyc:
                parts.append(f"🔁 {len(cyc)} refresh cycles")
            if last and last.get("when"):
                parts.append(f"⏱ last cycle {last['when'][11:19]} UTC")
            if parts:
                header += "  " + " · ".join(parts) + "\n"
        except Exception as exc:
            header += f"  <i>snapshot read failed: {exc}</i>\n"
    else:
        header += "  <i>no snapshot yet (catching…)</i>\n"
    return header


def _hold_buttons(hid: str, kind: str) -> InlineKeyboardMarkup:
    """Inline buttons for a /held card.

    - 📦 HOLD (longhold): 💳 Pay triggers a fresh LIVE catch on the same slot
      (one captcha solve, then Catholica).
    - 💳 LIVE: the reservation already exists with a real referenceOrder, so
      💳 Pay here just re-opens the Catholica payment form via pay.py — no
      captcha, no re-catch.
    """
    rows = []
    if kind == "longhold":
        rows.append([
            InlineKeyboardButton("💳 Send to checkout (1 captcha)",
                                 callback_data=f"h:pay:{hid}"),
            InlineKeyboardButton("🛑 Release",
                                 callback_data=f"h:rel:{hid}"),
        ])
    elif kind == "live":
        rows.append([
            InlineKeyboardButton("💳 Open Catholica form (no captcha)",
                                 callback_data=f"h:paynow:{hid}"),
            InlineKeyboardButton("🛑 Release",
                                 callback_data=f"h:rel:{hid}"),
        ])
    else:
        rows.append([
            InlineKeyboardButton("🛑 Release", callback_data=f"h:rel:{hid}"),
        ])
    rows.append([InlineKeyboardButton("🔄 Refresh card",
                                      callback_data=f"h:ref:{hid}")])
    return InlineKeyboardMarkup(rows)


def _pay_confirm_buttons(hid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📝 Change buyer first",
                              callback_data=f"h:paybuy:{hid}")],
        [InlineKeyboardButton("✅ YES, pay now (1 captcha)",
                              callback_data=f"h:paygo:{hid}"),
         InlineKeyboardButton("↩ cancel",
                              callback_data=f"h:back:{hid}")],
    ])


@only_our_chat
async def cmd_held(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Show every slot we're keeping in our hand right now.

    Posts one message per hold with inline 💳 Pay and 🛑 Release buttons,
    so your mate never has to type a hold id."""
    if not HOLDS:
        await _send(ctx, "📦 nothing held. Use /menu or /hold to catch one."); return
    rows = sorted(HOLDS.values(),
                  key=lambda r: r.started_at or dt.datetime.min, reverse=True)
    await _send(ctx, f"📦 <b>Your collection</b> · {len(rows)} slot(s)",
                parse=ParseMode.HTML)
    for rs in rows:
        try:
            await ctx.bot.send_message(
                chat_id=AUTHORIZED_CHAT,
                text=_hold_card(rs),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=_hold_buttons(rs.hold_id, rs.kind),
            )
        except Exception as exc:
            log.warning("held card send failed for %s: %s", rs.hold_id, exc)


async def _do_release(ctx: ContextTypes.DEFAULT_TYPE, hid: str) -> str:
    rs = HOLDS.get(hid)
    if rs is None:
        return f"❌ no hold with id {hid}"
    if rs.proc and rs.proc.returncode is None:
        await _kill_hold(rs)
    HOLDS.pop(hid, None)
    _save_held_registry()
    return f"🛑 released {hid}. Slot unlocks within ~15 min."


async def _do_pay(ctx: ContextTypes.DEFAULT_TYPE, hid: str) -> tuple[bool, str]:
    """Trigger the pay-catch for a held slot.

    Uses the CURRENT /setbuyer state (from MenuSel), not the buyer that was
    active at catch time. That's the point: hold a slot a week in advance
    without any buyer data, then /setbuyer the real visitor right before
    paying."""
    src = HOLDS.get(hid)
    if src is None:
        return False, f"❌ no hold with id {hid}"
    if src.kind != "longhold":
        return False, f"⚠️ {hid} is a {src.kind}, not a perpetual HOLD."
    if not YESCAPTCHA_KEY:
        return False, "❌ YESCAPTCHA_CLIENT_KEY not set in .env — can't /pay"
    cur = _active_hold()
    if cur and cur.kind == "live":
        return False, "⚠️ Another LIVE catch is already running. Wait for it."

    s = src.slot_meta or {}               # slot info only (date/time/tickets/names)
    b = MENU.get(AUTHORIZED_CHAT) or MenuSel()   # buyer from current /setbuyer
    missing = _buyer_missing(b)
    if missing:
        return False, (f"❌ buyer is incomplete — missing: <b>{', '.join(missing)}</b>.\n"
                       f"Open /buyer to fill them in, then try /pay again.")

    # Stop re-recapping so the final /reservation lands on OUR hold, not a
    # race between two parallel sessions on the same slot.
    if src.proc and src.proc.returncode is None:
        await _kill_hold(src)
    HOLDS.pop(hid, None)
    _save_held_registry()

    cmd = [sys.executable, "api_hold.py",
           "--date",         s.get("date", ""),
           "--num",          str(s.get("num", 1)),
           "--days-ahead",   "3",
           "--headless",
           "--session-aging-sec","75",
           "--rotate-ip",
           "--ip-retries",   "15",
           "--product",      s.get("product", "admission"),
           "--full",         str(s.get("full", 0)),
           "--reduced",      str(s.get("reduced", 1)),
           "--audio",        str(s.get("audio", 0)),
           # Buyer from CURRENT MenuSel (/setbuyer at pay time), not catch time
           "--first",        b.first,
           "--last",         b.last,
           "--email",        b.email,
           "--phone",        b.phone,
           "--country",      b.country,
           "--city",         b.city,
           "--gender",       b.gender,
           "--birth",        b.birth,
           "--live",
           "--keepalive", "--keepalive-minutes", "20"]
    if s.get("time"):  cmd += ["--time",         s["time"]]
    # Participant names: prefer slot-time names if your mate set them per-slot,
    # else current MenuSel.names, else buyer name is used for all participants.
    names = s.get("names") or b.names
    if names:          cmd += ["--participants", names]

    await _spawn_api_hold_raw(ctx, kind="live", cmd=cmd,
                              args_summary=f"pay for {hid} · buyer={b.first} {b.last}",
                              slot_meta={**s, "pay_buyer": {
                                  "first": b.first, "last": b.last,
                                  "email": b.email}},
                              snapshot_glob="hold_*_live.json")
    return True, (f"💳 pay-catch started for {hid}\n"
                  f"  slot: {s.get('date','?')} {s.get('time') or 'any'} "
                  f"{s.get('product','?')}\n"
                  f"  buyer: {b.first} {b.last} · {b.email}")


@only_our_chat
async def cmd_release(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """/release <hold_id> — stop refreshing a specific hold. Slot unlocks ~15 min."""
    if not ctx.args:
        await _send(ctx, "usage: <code>/release &lt;hold_id&gt;</code>  "
                         "(or tap 🛑 Release in /held)", parse=ParseMode.HTML); return
    hid = ctx.args[0].strip().lstrip("#")
    await _send(ctx, await _do_release(ctx, hid))


@only_our_chat
async def cmd_buyer(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Open the step-by-step buyer form — tap each field and send its value."""
    sel = MENU.setdefault(AUTHORIZED_CHAT, MenuSel())
    await ctx.bot.send_message(
        chat_id=AUTHORIZED_CHAT,
        text=_buyer_form_text(sel),
        parse_mode=ParseMode.HTML,
        reply_markup=_buyer_form_render(sel, "none"),
        disable_web_page_preview=True,
    )


@only_our_chat
async def cmd_resetbuyer(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Wipe the buyer fields to blank."""
    sel = MENU.setdefault(AUTHORIZED_CHAT, MenuSel())
    for k in _BUYER_FIELD_SPEC: setattr(sel, k, "")
    sel.gender = ""
    _save_menu_state()
    await _send(ctx, "🧹 buyer cleared. Open /buyer to set a new one.")


@only_our_chat
async def on_plain_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Capture the reply to the 📝 buyer-form prompts.

    Does nothing if no field is waiting — plain chit-chat in the group is
    ignored. Validates and stores the value, then re-renders the form so
    your mate can keep editing or tap Done."""
    waiting = AWAITING.get(AUTHORIZED_CHAT)
    if not waiting:
        return
    text = (update.message.text or "").strip()
    field = waiting.get("field", "")
    return_to = waiting.get("return_to", "none")
    spec = _BUYER_FIELD_SPEC.get(field)
    if not spec:
        AWAITING.pop(AUTHORIZED_CHAT, None); return
    # Special: dash-only for names clears it
    if field == "names" and text == "-":
        text = ""
    # Validate (names/validator always returns True)
    try:
        ok, reason = spec["validate"](text)
    except Exception as exc:
        ok, reason = False, str(exc)
    if not ok:
        await _send(ctx,
                    f"❌ invalid {field}: {reason}\n"
                    f"Try again — send the value, or tap ✖ to cancel.")
        return
    sel = MENU.setdefault(AUTHORIZED_CHAT, MenuSel())
    setattr(sel, field, text)
    AWAITING.pop(AUTHORIZED_CHAT, None)
    _save_menu_state()
    # Re-send the buyer form with the updated value
    try:
        await ctx.bot.send_message(
            chat_id=AUTHORIZED_CHAT,
            text=f"✅ <b>{spec['label']}</b> → <code>{text or '(cleared)'}</code>\n\n"
                 + _buyer_form_text(sel),
            parse_mode=ParseMode.HTML,
            reply_markup=_buyer_form_render(sel, return_to),
            disable_web_page_preview=True,
        )
    except Exception as exc:
        log.warning("form re-render failed: %s", exc)


def _buyer_missing(sel: MenuSel) -> list[str]:
    """Which required buyer fields are still empty?"""
    return [k for k in _REQUIRED_BUYER_FIELDS if not getattr(sel, k, "")]


_TICKET_RE = re.compile(r"(\d+)\s*(full|reduced|audio)", re.IGNORECASE)


def _parse_tickets_spec(spec: str) -> dict:
    """Parse '2full+1reduced+1audio' → {'full':2,'reduced':1,'audio':1}.
    Missing keys default to 0. Returns {} on unparseable input."""
    out = {"full": 0, "reduced": 0, "audio": 0}
    found = False
    for m in _TICKET_RE.finditer(spec or ""):
        out[m.group(2).lower()] = int(m.group(1)); found = True
    return out if found else {}


@only_our_chat
async def cmd_catch(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """/catch <date> [time] [tickets] [mode]

    Carcere-style one-shot catch. Uses the current /setbuyer when mode=live.
    Examples:
      /catch 2026-12-25 17:00 1reduced hold
      /catch 2026-12-25 any 2full+1reduced+1audio live
      /catch 2026-12-25               (defaults: any time, 1 reduced, hold)
    """
    args = ctx.args or []
    if not args:
        await _send(ctx, "usage: <code>/catch &lt;date&gt; [time] [tickets] [mode]</code>\n"
                         "e.g. <code>/catch 2026-12-25 17:00 2full+1reduced+1audio hold</code>",
                    parse=ParseMode.HTML); return
    date = args[0]
    try: dt.datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        await _send(ctx, f"❌ bad date '{date}' — use YYYY-MM-DD"); return

    time_arg    = ""
    tickets_arg = ""
    mode_arg    = "hold"
    for a in args[1:]:
        al = a.lower()
        if al in ("hold","live","dry"):           mode_arg = al
        elif _TICKET_RE.search(a):                tickets_arg = a
        elif al == "any":                         time_arg = ""
        elif re.fullmatch(r"\d{1,2}(:\d{2})?", a):time_arg = a
        else:
            await _send(ctx, f"❓ ignored unknown token: <code>{a}</code>",
                        parse=ParseMode.HTML)

    tickets = _parse_tickets_spec(tickets_arg) or {"full": 0, "reduced": 1, "audio": 0}
    if tickets["full"] + tickets["reduced"] < 1:
        tickets["reduced"] = 1

    sel = MENU.setdefault(AUTHORIZED_CHAT, MenuSel())
    sel.date = date
    sel.time = time_arg
    sel.full, sel.reduced, sel.audio = tickets["full"], tickets["reduced"], tickets["audio"]
    sel.mode = mode_arg
    _save_menu_state()
    await _send(ctx,
        f"🎯 catch <b>{mode_arg.upper()}</b> "
        f"{date} {time_arg or 'any'} · "
        f"full={tickets['full']} red={tickets['reduced']} audio={tickets['audio']}",
        parse=ParseMode.HTML)
    await _spawn_api_hold_from_menu(ctx, sel)


@only_our_chat
async def cmd_stop(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Alias of /release (matches carcere's /stop)."""
    await cmd_release(update, ctx)


@only_our_chat
async def cmd_live(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """/live <hold_id> — stream that hold's new log lines for 5 min.
    /live — picks the most-recent live hold."""
    rs: Optional[RunState] = None
    if ctx.args:
        rs = HOLDS.get(ctx.args[0].strip().lstrip("#"))
    else:
        rs = _active_hold()
    if rs is None:
        await _send(ctx, "💤 no live runs — /menu → 🎫 New hunt first."); return
    hid = rs.hold_id
    if rs.log_path is None or not rs.log_path.exists():
        await _send(ctx, f"❌ log file not found for {hid}"); return

    await _send(ctx, f"📡 live stream {hid} for 5 min")
    deadline = time.monotonic() + 5 * 60
    last_size = 0
    try:
        last_size = rs.log_path.stat().st_size
    except Exception: pass
    sent_any = False
    while time.monotonic() < deadline:
        await asyncio.sleep(5)
        try:
            with rs.log_path.open("r", encoding="utf-8", errors="replace") as f:
                f.seek(last_size)
                chunk = f.read()
                last_size = f.tell()
        except Exception as exc:
            await _send(ctx, f"stream err: {exc}"); break
        if not chunk.strip():
            continue
        sent_any = True
        # Forward each interesting line; mimic the bot's streaming filter so
        # we don't spam the whole Chrome debug output.
        for line in chunk.splitlines():
            msg = _match_line(line)
            if msg:
                try: await _send(ctx, f"[{hid}] {msg}", parse=ParseMode.HTML)
                except Exception: pass
    if not sent_any:
        await _send(ctx, f"📡 stream {hid} done · no new events")
    else:
        await _send(ctx, f"📡 stream {hid} ended")


@only_our_chat
async def cmd_slots(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """/slots [date] — tell the mate what Admission times exist for a date,
    without catching. Pure read of Vatican's public API, no captcha.
    Mirrors carcere's /slots. Default date = today+30 days."""
    args = ctx.args or []
    target = args[0] if args else (dt.date.today() + dt.timedelta(days=30)).isoformat()
    try: dt.datetime.strptime(target, "%Y-%m-%d")
    except ValueError:
        await _send(ctx, f"❌ bad date '{target}' — use YYYY-MM-DD"); return
    # Use the probe_slots path via a short-lived api_hold.py --hold-only dry
    # run — spawn in background, pipe the "visit:" + "slot picked" lines.
    cmd = [sys.executable, "api_hold.py",
           "--date", target, "--days-ahead", "0", "--num", "1",
           "--headless", "--session-aging-sec", "0",
           "--rotate-ip", "--ip-retries", "3"]
    await _send(ctx, f"🔎 probing Vatican for {target}…")
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, cwd=str(BOT_DIR),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env={**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"},
        )
        lines = []
        try:
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=90)
            text = (stdout or b"").decode("utf-8", errors="replace")
        except asyncio.TimeoutError:
            proc.kill(); text = ""
        for line in text.splitlines():
            if "visit:" in line or "slot picked" in line or "timetable" in line:
                lines.append(line.split("|",1)[-1].strip())
        body = "\n".join(lines[:30]) or "no slots returned"
        await _send(ctx, f"<pre>{_html_escape(body[:3500])}</pre>",
                    parse=ParseMode.HTML)
    except Exception as exc:
        await _send(ctx, f"/slots failed: {exc}")


@only_our_chat
async def cmd_pay(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """/pay <hold_id> — pay a specific hold.
    /pay (no args) — shows a picker of live HOLDs to tap."""
    if not ctx.args:
        longholds = [rs for rs in HOLDS.values()
                     if rs.kind == "longhold"
                     and rs.proc and rs.proc.returncode is None]
        if not longholds:
            await _send(ctx,
                "💤 nothing to pay. Catch a HOLD first (🎫 New hunt in /menu)."
            ); return
        rows = []
        for rs in longholds:
            s = rs.slot_meta or {}
            label = (f"💳 {s.get('date','?')} {s.get('time') or 'any'} "
                     f"· {s.get('full',0)}f+{s.get('reduced',0)}r")
            rows.append([(label, f"h:pay:{rs.hold_id}")])
        rows.append([("↩ Back", "main")])
        await ctx.bot.send_message(
            chat_id=AUTHORIZED_CHAT,
            text="<b>💳 Which hold do you want to pay?</b>",
            reply_markup=kbd(rows), parse_mode=ParseMode.HTML)
        return
    hid = ctx.args[0].strip().lstrip("#")
    ok, msg = await _do_pay(ctx, hid)
    await _send(ctx, msg)


@only_our_chat
async def cmd_logs(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """/logs <hold_id> — tail that hold's log. /logs — picks the most recent."""
    rs: Optional[RunState] = None
    if ctx.args:
        rs = HOLDS.get(ctx.args[0].strip().lstrip("#"))
        if rs is None:
            await _send(ctx, f"❌ no hold <code>{ctx.args[0]}</code>",
                        parse=ParseMode.HTML); return
    else:
        rs = _active_hold()
        if rs is None:
            await _send(ctx, "💤 no runs — /menu → 🎫 New hunt first."); return
    lines = rs.last_lines[-30:]
    if not lines:
        # Pull directly from the log file if the in-memory buffer is empty
        # (e.g. after a bot restart with a reloaded shell entry).
        try:
            if rs.log_path and rs.log_path.exists():
                with rs.log_path.open("r", encoding="utf-8", errors="replace") as f:
                    lines = f.read().splitlines()[-30:]
        except Exception: pass
    if not lines:
        await _send(ctx, f"no lines yet for <code>{rs.hold_id}</code>",
                    parse=ParseMode.HTML); return
    body = "\n".join(lines)[-3500:]
    await _send(ctx, f"<b>{rs.hold_id}</b>\n<pre>{_html_escape(body)}</pre>",
                parse=ParseMode.HTML)


def _html_escape(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


_BUYER_FIELDS = {"first", "last", "email", "phone", "country", "city",
                 "gender", "birth"}


@only_our_chat
async def cmd_setbuyer(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """`/setbuyer first=Amin last=MOZ email=you@x.com phone=50038590
    country=Tunisia city=ariana gender=M birth=1994-05-31`

    Any subset is accepted. Values with spaces must use quotes: last='Van Damme'.
    """
    sel = MENU.setdefault(AUTHORIZED_CHAT, MenuSel())
    if not ctx.args:
        await _send(ctx,
            "usage: <code>/setbuyer first=… last=… email=… phone=… "
            "country=… city=… gender=M|F birth=YYYY-MM-DD</code>",
            parse=ParseMode.HTML); return
    changed = {}
    raw = " ".join(ctx.args)
    try:
        tokens = shlex.split(raw)
    except ValueError:
        tokens = raw.split()
    for tok in tokens:
        if "=" not in tok: continue
        k, _, v = tok.partition("=")
        k = k.strip().lower(); v = v.strip()
        if k in _BUYER_FIELDS and v:
            if k == "gender" and v.upper() not in ("M", "F"): continue
            if k == "birth":
                try: dt.datetime.strptime(v, "%Y-%m-%d")
                except ValueError: continue
            setattr(sel, k, v.upper() if k == "gender" else v)
            changed[k] = v
    if not changed:
        await _send(ctx, "⚠️ No valid key=value pairs found. "
                         "Allowed keys: " + ", ".join(sorted(_BUYER_FIELDS)))
        return
    _save_menu_state()
    await _send(ctx,
        "✅ Buyer updated:\n"
        + "\n".join(f"  <b>{k}</b> = <code>{v}</code>" for k, v in changed.items())
        + f"\n\n👤 <b>{sel.first} {sel.last}</b> · {sel.email}\n"
        f"📞 {sel.phone} · 🌍 {sel.country}/{sel.city}\n"
        f"🎂 {sel.birth} · ⚧ {sel.gender}",
        parse=ParseMode.HTML)


@only_our_chat
async def cmd_setnames(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """`/setnames First1 Last1, First2 Last2, …` — per-visitor names."""
    sel = MENU.setdefault(AUTHORIZED_CHAT, MenuSel())
    text = (update.message.text or "").partition(" ")[2].strip()
    if not text:
        # Clear = fall back to buyer name for all
        sel.names = ""
        _save_menu_state()
        await _send(ctx,
            "🧹 Participant names cleared — will use buyer "
            f"<b>{sel.first} {sel.last}</b> for all {sel.num} visitors.",
            parse=ParseMode.HTML); return
    sel.names = text
    _save_menu_state()
    lst = [p.strip() for p in text.split(",") if p.strip()]
    await _send(ctx,
        "✅ Participants set:\n"
        + "\n".join(f"  {i+1}. <code>{n}</code>" for i, n in enumerate(lst)),
        parse=ParseMode.HTML)


@only_our_chat
async def cmd_last(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    files = sorted(REPORTS.glob("hold_*.json"), key=lambda p: p.stat().st_mtime)
    if not files:
        await _send(ctx, "no hold_*.json yet")
        return
    latest = files[-1]
    try:
        d = json.loads(latest.read_text(encoding="utf-8"))
    except Exception as exc:
        await _send(ctx, f"could not read {latest.name}: {exc}"); return
    slot   = d.get("slot") or {}
    recap  = d.get("recap") or {}
    res    = d.get("reservation") or {}
    epay   = (res.get("epay") or {}) if isinstance(res, dict) else {}
    parts = [f"<b>{latest.name}</b>",
             f"result: {d.get('result') or d.get('error','?')}"]
    if slot:
        parts.append(f"slot: {slot.get('visit_date')} {slot.get('visit_time')} ({slot.get('availability')})")
    if recap:
        parts.append(f"recap: reservationId=<code>{recap.get('reservationId')}</code>  total=€{recap.get('total')}")
    if res:
        parts.append(f"reservation: ref=<code>{res.get('referenceOrder')}</code>  pay={epay.get('urlMs')}")
    await _send(ctx, "\n".join(parts), parse=ParseMode.HTML)


# --------------------------------------------------------------------- entry
def build_app() -> Application:
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start",  cmd_start))
    app.add_handler(CommandHandler("help",   cmd_help))
    app.add_handler(CommandHandler("menu",   cmd_menu))
    # Sniper.js-style verbs (Carcer Tullianum + Colosseo bot's exact surface)
    app.add_handler(CommandHandler("jobs",        cmd_jobs))
    app.add_handler(CommandHandler("holds",       cmd_jobs))
    app.add_handler(CommandHandler("consumption", cmd_consumption))
    app.add_handler(CommandHandler("usage",       cmd_consumption))
    app.add_handler(CommandHandler("site",        cmd_site))
    app.add_handler(CommandHandler("refresh",     cmd_refresh))
    # Catch + hold lifecycle (carcere-style verbs)
    app.add_handler(CommandHandler("catch",    cmd_catch))
    app.add_handler(CommandHandler("held",     cmd_held))
    app.add_handler(CommandHandler("pay",      cmd_pay))
    app.add_handler(CommandHandler("stop",     cmd_stop))        # alias of release
    app.add_handler(CommandHandler("release",  cmd_release))
    app.add_handler(CommandHandler("status",   cmd_status))
    app.add_handler(CommandHandler("logs",     cmd_logs))
    app.add_handler(CommandHandler("live",     cmd_live))
    app.add_handler(CommandHandler("slots",    cmd_slots))
    # Legacy /hold /dry /cancel still accepted
    app.add_handler(CommandHandler("dry",      cmd_dry))
    app.add_handler(CommandHandler("hold",     cmd_hold))
    app.add_handler(CommandHandler("cancel",   cmd_cancel))
    app.add_handler(CommandHandler("last",     cmd_last))
    # Buyer form + shortcuts
    app.add_handler(CommandHandler("buyer",      cmd_buyer))
    app.add_handler(CommandHandler("setbuyer",   cmd_setbuyer))
    app.add_handler(CommandHandler("setnames",   cmd_setnames))
    app.add_handler(CommandHandler("resetbuyer", cmd_resetbuyer))
    # NB: must include "callback_query" so inline-menu button taps reach us.
    app.add_handler(CallbackQueryHandler(cb_menu))
    # Plain text (non-command) listener — captures buyer-form field replies.
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_plain_text))
    return app


def main() -> None:
    log.info("starting Vatican hold bot  chat_id=%s  captcha=%s",
             AUTHORIZED_CHAT, "set" if YESCAPTCHA_KEY else "MISSING")
    # Reload the hold registry so /held shows what was running before a restart
    _load_held_registry_shells()
    if HOLDS:
        log.info("reloaded %d previously-registered holds from .held_slots.json",
                 len(HOLDS))
    app = build_app()
    app.run_polling(allowed_updates=["message", "callback_query"],
                    drop_pending_updates=True)


if __name__ == "__main__":
    main()
