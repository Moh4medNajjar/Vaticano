#!/usr/bin/env python3
"""
Vatican Museums — HOLD a ticket via the proven /recap → /reservation flow.

Request flow copied verbatim from ../../vaticano-inspire/vaticani-monitor/booker
(reserve.sh + watcher.py:book_in_python). All HTTP goes through F5Session's
Playwright browser context so F5 TSPD is cleared — tested 2026-11-09, pure
httpx gets 244-byte "Request Rejected" on every /api/search/* call now
regardless of HTTP/2 + JSESSIONID.

Flow:
  (F5Session.solve() — loads /home, clears TSPD challenge, harvests cookies)
  GET  /api/config/initValues                 bootstrap-only (noop after F5 solve)
  GET  /api/search/filter?areaId=1
  GET  /api/search/calendar?areaId=1          open dates list
  GET  /api/search/result?area=1&who=1&...    visits for date → pick by name
  GET  /api/visit?visitTypeId=…               ticket types + mandatory services
  GET  /api/visit/timeavail?visitTypeId=…     timetable → visitId (slot id)
  GET  /api/visit/services?visitId=…          optional add-ons
  POST /api/visit/recap                       → recapId + reservationId  (the HOLD)
  9× GET warmup (gdpr/country/isAgency/startup/purchaserules)
  solve Turnstile                             page_url = /home/checkout
  POST /api/visit/reservation                 → referenceOrder + epay.urlMs

Three facts that bit us until we matched them:
  1. Visit IDs are session-bound to the backing JSESSIONID — one session for the whole flow.
  2. /recap issues reservationId + recapId (don't generate client-side).
  3. /reservation needs Accept-Encoding: gzip — without it, Spring 500s.

Usage:
    python api_hold.py --date 2026-12-20 --num 2
    python api_hold.py --date 2026-12-20 --num 2 --live          (really POSTs /reservation)

Writes ../reports/hold_<ts>.json. Dry-run stops after /recap (the hold is created
by /recap already — /reservation just locks it + returns the pay URL).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import pathlib
import random
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import Optional
from urllib.parse import urlencode

from config import BASE_URL, Config
from session import F5Session
from captcha import CaptchaSolver
from proxy import ProxyManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s | %(message)s",
)
log = logging.getLogger("hold")


# ----------------------------------------------------------------------------
# Constants (from the working reference)
# ----------------------------------------------------------------------------
API_BASE = f"{BASE_URL}/api"
PAGE_URL = f"{BASE_URL}/home/checkout"

# Matched against what the real Vatican SPA sends. Observed 2026-11-09
# via spa_investigator.py: the SPA uses the "resultPerTag" endpoint with
# tag=MV-Biglietti, area=2, who="" (empty) and lang=it.
DEFAULT_AREA = 2
DEFAULT_WHO  = ""
# User's successful manual checkout on 2026-11-11 16:41 UTC had LINGUA=en
# in the Catholica form POST. Matching that so our Vatican /reservation
# doesn't diverge from the working flow.
DEFAULT_LANG = "en"

# Product tiers — VISIT_NAME_REQUIRED is overridden by --product on the CLI.
# Names in English (lang=en) since DEFAULT_LANG switched. Admission ticket's
# English name is "Admission Ticket" — the SPA capture earlier showed
# "Vatican Museums - Admission Ticket" under lang=en.
PRODUCT_NAME_MAP = {
    "admission": ["Vatican Museums", "Admission Ticket"],
    "guided":    ["Guided Tours",    "Museums"],
    "gardens":   ["Guided Tours",    "Gardens"],
    "bus":       ["Vatican City Tours", "Open Bus"],
}
VISIT_NAME_REQUIRED = PRODUCT_NAME_MAP["admission"]   # default

# Ticket + service names in English (match what Vatican's /api/visit returns
# under lang=en — confirmed by live /api/visit call on 2026-11-11).
# The SPA uses the exact English strings in /recap / /reservation body too.
TICKET_FULL_PRICE  = {"id": 60, "name": "Full Price Ticket", "price": 20}
TICKET_REDUCED     = {"id": 61, "name": "Reduced Ticket",    "price": 10}
SERVICE_RES_FEES   = {"id": 58, "name": "Reservation Fees",  "price": 5}
SERVICE_AUDIOGUIDE = {"id": 62, "name": "Audioguide",        "price": 8}


# ----------------------------------------------------------------------------
# Proxy loading — reuse Haroun's Evomi pool integrated at ../../openyasemsem
# ----------------------------------------------------------------------------
# Vaticano/bot/api_hold.py → parents are bot, Vaticano, vatican, workspace.
# openyasemsem lives directly under workspace, so we need .parents[3].
_WORKSPACE = pathlib.Path(__file__).resolve().parents[3]
PROXY_SOURCES_IN_ORDER = [
    # Local override (project root)
    pathlib.Path(__file__).resolve().parent.parent / "proxies.txt",
    # Haroun's Evomi pool at the sibling openyasemsem repo — 10 files of
    # 2000 sticky-session proxies each (country=US,IT on core-residential.evomi.com:1000).
    *(_WORKSPACE / "openyasemsem" / f"proxies_{i}.txt" for i in range(1, 11)),
]

def load_proxy_pool() -> list[str]:
    for p in PROXY_SOURCES_IN_ORDER:
        if p.exists():
            lines = [ln.strip() for ln in p.read_text(encoding="utf-8").splitlines()
                     if ln.strip() and not ln.startswith("#")]
            if lines:
                log.info("proxy pool: %d entries from %s", len(lines), p)
                return lines
    log.warning("no proxy pool found — F5 will see the local IP (prone to cooldown)")
    return []


# ----------------------------------------------------------------------------
# Exceptions
# ----------------------------------------------------------------------------
class F5Rejected(Exception):
    """F5 returned a 'Request Rejected' page instead of JSON. Rotate IP."""


# ----------------------------------------------------------------------------
# F5Session wrappers that match the httpx api we want
# ----------------------------------------------------------------------------
async def api_get_json(f5: F5Session, path: str, params: dict | None = None,
                       retries: int = 3) -> dict:
    """GET via Playwright page.evaluate(XMLHttpRequest) → JSON, with 5xx retry.
    Raises F5Rejected on the 'Request Rejected' HTML body so the caller can
    rotate the Evomi IP on the next attempt."""
    for attempt in range(1, retries + 1):
        r = await f5.api_get(path, params=params or {})
        body = r.get("body", "") or ""
        status = r.get("status", 0)
        if status == 200 and body.strip() and not body.strip().startswith("<"):
            try:
                return json.loads(body)
            except Exception as exc:
                raise RuntimeError(f"GET {path}: JSON parse failed ({exc}): {body[:200]}")
        # F5 soft-reject (200 with HTML "Request Rejected" body) — bubble up
        # so the caller can rotate IPs instead of pointlessly retrying.
        if "Request Rejected" in body or (status == 200 and body.strip().startswith("<")):
            raise F5Rejected(f"GET {path}: F5 reject page")
        if status < 500 and status != 200:
            raise RuntimeError(f"GET {path} → HTTP {status}: {body[:200]}")
        if attempt == retries:
            raise RuntimeError(f"GET {path} kept failing (status={status}): {body[:200]}")
        log.warning("GET %s → HTTP %s (attempt %d/%d), retrying", path, status, attempt, retries)
        await asyncio.sleep(3 * attempt)


async def api_post_json(f5: F5Session, path: str, body: dict,
                        retries: int = 3,
                        extra_headers: dict | None = None,
                        referrer: Optional[str] = None) -> dict:
    """
    POST via Playwright page.evaluate(fetch) with Accept-Encoding pinned.
    Retries on 5xx with exponential backoff (/recap + /reservation flake 5xx).

    `referrer` lets us pretend the fetch originated from a specific SPA URL
    (e.g. /home/checkout for /reservation) — F5 and Vatican both validate it.
    """
    import json as _json
    for attempt in range(1, retries + 1):
        page = f5._page
        payload = _json.dumps(body)
        # The reference code pins accept-encoding to force the server's gzip
        # branch — without it /reservation 500s. The browser sends this by
        # default but passing it explicitly matches the HAR.
        result = await page.evaluate(
            """
            async (args) => {
                const h = {
                    'content-type': 'application/json',
                    'accept': 'application/json, text/plain, */*',
                    'accept-encoding': 'gzip, deflate, br, zstd',
                    'x-requested-with': 'XMLHttpRequest',
                };
                for (const [k, v] of Object.entries(args.extra || {})) h[k] = v;
                const fetchOpts = {
                    method: 'POST', credentials: 'include',
                    headers: h, body: args.body,
                };
                if (args.referrer) {
                    fetchOpts.referrer = args.referrer;
                    fetchOpts.referrerPolicy = 'strict-origin-when-cross-origin';
                }
                const r = await fetch(args.url, fetchOpts);
                return {status: r.status, body: await r.text()};
            }
            """,
            {"url": path, "body": payload, "extra": extra_headers or {},
             "referrer": referrer},
        )
        status = result["status"]; rb = result["body"]
        if status in (200, 201) and rb.strip() and not rb.strip().startswith("<"):
            try:
                return json.loads(rb)
            except Exception as exc:
                raise RuntimeError(f"POST {path}: JSON parse failed ({exc}): {rb[:200]}")
        if status < 500 and status not in (200, 201):
            raise RuntimeError(f"POST {path} → HTTP {status}: {rb[:400]}")
        if attempt == retries:
            raise RuntimeError(f"POST {path} kept failing (status={status}): {rb[:400]}")
        log.warning("POST %s → HTTP %s (attempt %d/%d), retrying", path, status, attempt, retries)
        await asyncio.sleep(5 * attempt)


# ----------------------------------------------------------------------------
# Slot discovery (matches reserve.sh + watcher.py find_slot)
# ----------------------------------------------------------------------------
@dataclass
class Slot:
    visit_type_id:   int
    visit_type_name: str
    visit_id:        str
    visit_date:      str   # DD/MM/YYYY
    visit_time:      str   # HH:MM
    availability:    str


def to_dmy(iso_date: str) -> str:
    return datetime.strptime(iso_date, "%Y-%m-%d").strftime("%d/%m/%Y")


async def find_slot(f5: F5Session, start_iso: str, days_ahead: int,
                    visitors: int, prefer_time_hhmm: Optional[str] = None) -> Slot:
    """Walk filter → calendar → resultPerTag → timeavail. Mirrors the real SPA.

    Switched from /api/search/result?area=1&who=1&lang=en to
    /api/search/resultPerTag?area=2&who=&lang=it&tag=MV-Biglietti —
    the latter is what Vatican's own frontend uses and returns a different
    (bookable) visitTypeId. The /search/result endpoint we used before
    returns a different product tier whose /reservation always 500s.
    """
    # filter (sanity + warm-up) with the SPA's exact params
    try:
        await api_get_json(f5, "/api/search/filter", {
            "lang": DEFAULT_LANG, "tag": "MV-Biglietti", "areaId": DEFAULT_AREA})
    except Exception as exc:
        log.warning("filter non-fatal: %s", exc)

    cal = await api_get_json(f5, "/api/search/calendar",
                             {"lang": DEFAULT_LANG, "areaId": DEFAULT_AREA})
    open_dates = {e["date"] for e in cal.get("calendar", []) if e.get("state") == 1}
    log.info("calendar: %d open dates in window", len(open_dates))

    start = datetime.strptime(start_iso, "%Y-%m-%d").date()
    for offset in range(days_ahead + 1):
        date_iso = (start + timedelta(days=offset)).isoformat()
        date_dmy = to_dmy(date_iso)
        if date_dmy not in open_dates:
            continue
        res = await api_get_json(f5, "/api/search/resultPerTag", {
            "lang": DEFAULT_LANG, "visitorNum": visitors, "visitDate": date_dmy,
            "area": DEFAULT_AREA, "who": DEFAULT_WHO, "page": 0,
            "tag": "MV-Biglietti",
        })
        matches = [v for v in res.get("visits", [])
                   if all(k.lower() in (v.get("name","") or "").lower()
                          for k in VISIT_NAME_REQUIRED)
                   and v.get("availability") in ("AVAILABLE", "LOW_AVAILABILITY")]
        if not matches:
            names = [(v.get("name",""), v.get("availability","?"))
                     for v in res.get("visits", [])][:3]
            log.info("[%s] no matching available visit. sample: %s", date_dmy, names)
            continue
        visit = matches[0]
        vtid  = int(visit["id"])
        vname = visit.get("name", "")
        log.info("[%s] visit: id=%s  %s  avail=%s",
                 date_dmy, vtid, vname[:50], visit.get("availability"))

        times = await api_get_json(f5, "/api/visit/timeavail", {
            "lang": DEFAULT_LANG, "visitLang": "", "visitTypeId": vtid,
            "visitorNum": visitors, "visitDate": date_dmy,
        })
        timetable = times.get("timetable", [])
        ranked: list[tuple[int, dict]] = []
        for t in timetable:
            av = (t.get("availability") or "").upper()
            if av not in ("AVAILABLE", "LOW_AVAILABILITY"):
                continue
            rank = 0 if av == "AVAILABLE" else 1
            if prefer_time_hhmm and t.get("time", "").startswith(prefer_time_hhmm):
                rank -= 10
            ranked.append((rank, t))
        ranked.sort(key=lambda x: x[0])
        if not ranked:
            sample = [(t.get("time"), t.get("availability")) for t in timetable[:10]]
            log.info("[%s] no bookable slots. sample: %s", date_dmy, sample)
            continue
        best = ranked[0][1]
        return Slot(
            visit_type_id=vtid, visit_type_name=vname,
            visit_id=best["id"], visit_date=date_dmy,
            visit_time=best.get("time", ""),
            availability=best.get("availability"),
        )
    raise RuntimeError("No bookable slot found in the window.")


# ----------------------------------------------------------------------------
# Reservation config
# ----------------------------------------------------------------------------
def build_reservation_config(slot: Slot, args) -> dict:
    birth_iso_z = f"{args.birth}T23:00:00.000Z"
    name_parts = [p.strip() for p in args.participants.split(",")] if args.participants else []
    def nm(i: int) -> tuple[str, str]:
        if i < len(name_parts) and name_parts[i]:
            p = name_parts[i].split(maxsplit=1)
            return (p[0], p[1] if len(p) > 1 else args.last)
        return (args.first, args.last)
    # Ticket mix. Modern flags: --full N --reduced M (counts combine).
    # If both are 0 we fall back to --ticket (legacy) + --num.
    full_n    = max(0, int(getattr(args, "full", 0) or 0))
    reduced_n = max(0, int(getattr(args, "reduced", 0) or 0))
    audio_n   = max(0, int(getattr(args, "audio", 0) or 0))
    if full_n == 0 and reduced_n == 0:
        if args.ticket == "reduced":
            reduced_n = args.num
        else:
            full_n = args.num
    total_n = full_n + reduced_n
    # Keep --num in sync so downstream code (and services quantity) is right.
    args.num = total_n
    audio_n = min(audio_n, total_n)  # can't audioguide more than visitors

    tickets = [
        {**TICKET_FULL_PRICE, "quantity": full_n},
        {**TICKET_REDUCED,    "quantity": reduced_n},
    ]
    # Reservation fee is mandatory (id=58). Audioguide (id=62) optional.
    services = [{**SERVICE_RES_FEES, "quantity": total_n}]
    if audio_n > 0:
        services.append({**SERVICE_AUDIOGUIDE, "quantity": audio_n})
    # Build participants: first `full_n` are intero (id=60), remainder ridotto (61).
    participants = []
    for i in range(total_n):
        f, l = nm(i)
        if i < full_n:
            pid, ptype = 60, "intero"
        else:
            pid, ptype = 61, "ridotto"
        svc_ids = [58]
        if i < audio_n:
            svc_ids.append(62)
        participants.append({"surname": l, "name": f, "id": pid,
                             "ticketType": ptype, "services": svc_ids})
    return {
        "visitId":     slot.visit_id,
        "visitTypeId": slot.visit_type_id,
        "visitorNum":  args.num,
        "lang":        DEFAULT_LANG,       # "en" — matches user's working POST
        # NOTE: visitLang REMOVED. The successful manual checkout (2026-11-11 16:41
        # UTC) did NOT send visitLang in its /reservation body. Sending it was one
        # of the possible 500 triggers.
        "tickets":     tickets,
        "services":    services,
        "representativeUser": {
            "surname":        args.last,
            "name":           args.first,
            "gender":         args.gender,
            "country":        args.country,
            "city":           args.city,
            "birthDate":      birth_iso_z,
            "email":          args.email,
            "confirmEmail":   args.email,
            "telephoneNumber":args.phone,
            # The successful user POST set representative language to "it"
            # while top-level lang was "en" — matching that.
            "language":       "it",
        },
        "participantUser": participants,
        # User's working body had all THREE GDPR entries accepted. Earlier
        # /visit/gdpr endpoint returns which are active for the current
        # product; accepting all three has been seen to succeed.
        "gdpr": [{"id": 1, "check": True},
                 {"id": 2, "check": True},
                 {"id": 3, "check": True}],
    }


async def post_recap(f5: F5Session, cfg: dict) -> dict:
    # Matches the SPA's /recap body — no visitLang, no extra keys.
    body = {
        "visitId":     cfg["visitId"],
        "visitTypeId": cfg["visitTypeId"],
        "visitorNum":  cfg.get("visitorNum", 1),
        "lang":        cfg.get("lang", DEFAULT_LANG),
        "tickets":     cfg["tickets"],
        "additionalCosts": {f"service-{i}": svc
                            for i, svc in enumerate(cfg.get("services") or [])},
        "services":    cfg.get("services") or [],
    }
    # The SPA posts /recap from /home/fromtag/.../MV-Biglietti/<num>. We
    # don't have the exact timestamp arg, but /home/fromtag/2/0/MV-Biglietti/2
    # with the standard checkout referer works for F5's referer check.
    j = await api_post_json(f5, "/api/visit/recap", body,
                            referrer=f"{BASE_URL}/home/fromtag/2/0/MV-Biglietti/{cfg.get('visitorNum',2)}")
    log.info("/recap OK  recapId=%s  reservationId=%s  total=%s  visit=%s",
             j.get("recapId"), j.get("reservationId"), j.get("total"),
             j.get("visitDateTime"))
    return j


async def _rotating_rerecap(args, out: dict) -> dict:
    """Perpetual low-cost hold: re-POST /recap every `--rerecap-min` minutes on a
    FRESH F5Session (new Evomi IP + new Chrome profile) every N cycles.

    Why rotate: the earlier `_rerecap_loop` (which reused one session) found
    that F5 downgrades a single session after ~4 /recap POSTs and starts
    returning "Request Rejected" HTML. A clean session per burst of cycles
    sidesteps that.

    Cost: zero Turnstile solves. Each cycle spends ~30 s on F5 solve +
    one new Evomi IP (you have 3000). The slot's server-side TTL is ~15 min
    from the last successful /recap, so interval_min MUST stay < 15.

    Writes every cycle's result back to the on-disk snapshot so pay.py can
    always find the most recent reservationId.
    """
    import asyncio as _asyncio
    import random as _random
    import time as _time

    interval_min    = int(args.rerecap_min)
    max_cycles      = int(getattr(args, "rerecap_cycles", 0) or 0)
    sess_lifetime   = max(1, int(getattr(args, "rerecap_sess_cycles", 3) or 3))
    sleep_s         = max(60, interval_min * 60)

    slot_info = out.get("slot") or {}
    if not slot_info:
        log.error("🔁 rotating rerecap: no slot in out — cannot rebuild cfg")
        return {"error": "missing slot"}
    slot = Slot(
        visit_type_id=slot_info["visit_type_id"],
        visit_type_name=slot_info["visit_type_name"],
        visit_id=slot_info["visit_id"],
        visit_date=slot_info["visit_date"],
        visit_time=slot_info["visit_time"],
        availability=slot_info.get("availability", "AVAILABLE"),
    )
    cfg = build_reservation_config(slot, args)

    pool_all = [] if args.no_proxy else load_proxy_pool()
    # Seed used_proxies with the first-catch proxy (pinned_proxy.txt). The
    # Chrome profile dir is keyed by SHA1(proxy+username), so picking the same
    # proxy line reuses the SAME profile — which still carries the first
    # session's F5-downgraded cookies. That poisons the fresh session: Vatican
    # returns "Request Rejected" on the first GET. Excluding this proxy from
    # the rotating pool guarantees a brand-new profile per rotation.
    used_proxies: list[str] = []
    pinned_file = pathlib.Path(__file__).resolve().parent.parent / ".pw_profiles" / "pinned_proxy.txt"
    if pinned_file.exists():
        try:
            pinned = pinned_file.read_text(encoding="utf-8").strip()
            if pinned: used_proxies.append(pinned)
        except Exception: pass
    cfg_session = Config(target_date=args.date, num_tickets=args.num,
                         headless=args.headless, yescaptcha_key=args.yescaptcha_key)

    # Where the live snapshot lives (so pay.py sees the latest ref)
    reports_dir = pathlib.Path(__file__).resolve().parent.parent / "reports"
    reports_dir.mkdir(exist_ok=True)
    snapshot_path = reports_dir / f"hold_{int(time.time())}_recap_live.json"

    result = {"mode": "rotating_rerecap",
              "started_utc": datetime.utcnow().isoformat() + "Z",
              "interval_min": interval_min,
              "cycles_per_session": sess_lifetime,
              "max_cycles": max_cycles,
              "slot": slot_info,
              "cycles": []}

    def _save_snapshot():
        try:
            snap = dict(out); snap["rotating_rerecap"] = result
            snapshot_path.write_text(json.dumps(snap, indent=2, default=str),
                                     encoding="utf-8")
        except Exception as exc:
            log.warning("snapshot write failed: %s", exc)

    def _pick_proxy() -> list[str]:
        if args.proxy: return [args.proxy]
        if not pool_all: return []
        remaining = [p for p in pool_all if p not in used_proxies]
        pick = _random.choice(remaining or pool_all)
        used_proxies.append(pick)
        return [pick]

    log.info("🔁 rotating rerecap: every %d min  fresh_session_each=%d cycles  "
             "max_cycles=%s  snapshot=%s",
             interval_min, sess_lifetime,
             max_cycles or "∞", snapshot_path.name)
    _save_snapshot()

    cycle = 0
    while True:
        if max_cycles and cycle >= max_cycles:
            log.info("🔁 reached max_cycles=%d — stopping", max_cycles)
            result["stopped_reason"] = "max_cycles"
            _save_snapshot(); return result

        try:
            await _asyncio.sleep(sleep_s + _random.uniform(-20, 20))
        except _asyncio.CancelledError:
            result["stopped_reason"] = "cancelled"
            _save_snapshot(); return result

        # One FRESH session covers up to `sess_lifetime` cycles, then rotates.
        chosen = _pick_proxy()
        proxy_mgr = ProxyManager(chosen, 1)
        sanitized = (chosen[0].split("@", 1)[-1] if chosen and "@" in chosen[0]
                     else (chosen[0] if chosen else "direct"))

        try:
            async with F5Session(cfg_session, proxy_mgr) as f5:
                log.info("🔁 session boot  proxy=%s", sanitized)
                try:
                    await f5.solve()
                except Exception as exc:
                    log.warning("🔁 F5 solve failed on %s: %s — rotating", sanitized, exc)
                    continue
                if not getattr(f5, "challenge_passed", False):
                    log.warning("🔁 F5 cold on %s — rotating", sanitized)
                    continue
                # Light SPA presence before /recap so Vatican sees a real browser
                try:
                    await api_get_json(f5, "/api/search/filter",
                                       {"lang": "en", "areaId": DEFAULT_AREA},
                                       retries=1)
                except Exception: pass

                sub_cycles = 0
                while sub_cycles < sess_lifetime:
                    if max_cycles and cycle >= max_cycles:
                        break
                    cycle += 1; sub_cycles += 1
                    rec = {"cycle": cycle, "sub": sub_cycles,
                           "proxy": sanitized,
                           "when": datetime.utcnow().isoformat() + "Z"}
                    try:
                        r = await post_recap(f5, cfg)
                        rec["reservation_id"] = r.get("reservationId")
                        rec["recap_id"]       = r.get("recapId")
                        rec["total"]          = r.get("total")
                        rec["ok"] = True
                        result["last_reservation_id"] = r.get("reservationId")
                        result["last_recap_id"]      = r.get("recapId")
                        log.info("🔁 cycle=%d sub=%d  res=%s  recap=%s  proxy=%s",
                                 cycle, sub_cycles, r.get("reservationId"),
                                 r.get("recapId"), sanitized)
                    except F5Rejected:
                        rec["ok"] = False
                        rec["error"] = "f5_rejected"
                        log.warning("🔁 cycle=%d F5-rejected — ending session, "
                                    "will rotate on next tick", cycle)
                        result["cycles"].append(rec); _save_snapshot()
                        break
                    except Exception as exc:
                        rec["ok"] = False
                        rec["error"] = str(exc)[:200]
                        log.warning("🔁 cycle=%d error: %s", cycle, exc)
                    result["cycles"].append(rec); _save_snapshot()
                    if sub_cycles < sess_lifetime:
                        await _asyncio.sleep(sleep_s + _random.uniform(-20, 20))
        except Exception as exc:
            log.warning("🔁 session %s bootstrap failed: %s — rotating", sanitized, exc)


async def _rerecap_loop(f5: F5Session, cfg: dict, first_recap: dict,
                        interval_min: int, max_cycles: int) -> dict:
    """LOW-COST perpetual hold: re-POST /visit/recap every `interval_min` minutes.

    Idea: a /recap POST locks a slot for ~15 min server-side without spending a
    Turnstile. Re-POSTing with the SAME body before the TTL expires should keep
    the slot held (either by extending the existing reservationId's TTL or by
    atomically re-grabbing it before anyone else can). Costs: 0 captcha per
    cycle, 1 Vatican /recap POST, some bytes.

    UNKNOWN until we test:
      - Does Vatican return the SAME reservationId each cycle (= extension)
        or a NEW one (= we're releasing + re-grabbing each cycle)?
      - Does Vatican velocity-track /recap from a single IP+fingerprint over
        hours/days? If yes, we need proxy rotation or longer intervals.

    The loop records every cycle's response so we can inspect the TTL pattern.
    """
    import asyncio as _asyncio
    import random as _random
    import time as _time

    sleep_s = max(60, interval_min * 60)
    started = _time.monotonic()
    page = f5._page
    first_res_id = (first_recap or {}).get("reservationId")

    out = {
        "mode": "hold_only_rerecap",
        "started_utc": datetime.utcnow().isoformat() + "Z",
        "interval_min": interval_min,
        "max_cycles": max_cycles,
        "first_reservation_id": first_res_id,
        "first_recap_id":       (first_recap or {}).get("recapId"),
        "cycles":  [],
        "stopped_reason": "unknown",
    }

    log.info("🔁 re-recap loop: every %d min  (first res=%s)  cycles=%s",
             interval_min, first_res_id, max_cycles or "∞")

    # Light SPA presence: stay on /home, scroll periodically. No navigation.
    try:
        if "/home" not in page.url:
            await page.goto(f"{BASE_URL}/home", wait_until="domcontentloaded",
                            timeout=15_000)
    except Exception: pass

    cycle = 0
    while True:
        # Sleep first — we already have a fresh recap from the initial catch.
        try:
            await _asyncio.sleep(sleep_s + _random.uniform(-30, 30))
        except _asyncio.CancelledError:
            out["stopped_reason"] = "cancelled"
            return out

        cycle += 1
        rec = {"cycle": cycle, "when": datetime.utcnow().isoformat() + "Z",
               "elapsed_s": int(_time.monotonic() - started)}
        try:
            # Scroll a bit before the POST so the session has human-looking activity.
            try:
                await page.evaluate(
                    f"window.scrollBy(0, {_random.randint(-200, 220)});"
                )
            except Exception: pass

            r = await post_recap(f5, cfg)
            new_res_id = r.get("reservationId")
            rec["reservation_id"] = new_res_id
            rec["recap_id"]       = r.get("recapId")
            rec["total"]          = r.get("total")
            rec["same_as_first"]  = (new_res_id == first_res_id)
            log.info("🔁 re-recap cycle=%d  res=%s  same=%s  elapsed=%ds",
                     cycle, new_res_id, rec["same_as_first"], rec["elapsed_s"])
        except F5Rejected as exc:
            rec["error"] = f"f5_rejected: {exc}"
            log.warning("🔁 re-recap cycle=%d F5-rejected — hold likely still valid, "
                        "but session degraded", cycle)
        except Exception as exc:
            rec["error"] = str(exc)[:240]
            log.warning("🔁 re-recap cycle=%d error: %s", cycle, exc)

        out["cycles"].append(rec)

        if max_cycles and cycle >= max_cycles:
            out["stopped_reason"] = "max_cycles"
            log.info("🔁 re-recap loop reached max_cycles=%d — stopping", max_cycles)
            return out


async def _keepalive_loop(f5: F5Session,
                          reservation_id: str,
                          recap_id: Optional[str],
                          reference_order: Optional[str],
                          minutes: int) -> dict:
    """Hold the Vatican reservation ALIVE past its 5-min client-side countdown.

    The SPA shows "Remaining time: 04:59" and then zeros out — but server-side
    the slot is held ~15 min from /recap time. The 5-min widget is just the
    *click-ACQUISTA* window; once the Vatican SPA considers it expired it
    deletes the reservation via /visit/cancellation from the page's beforeunload.

    This loop prevents that by:
      1. Keeping the Playwright page alive on /home/checkout (no navigation,
         so no beforeunload cancel is triggered).
      2. Every 30 s pinging /api/visit/recap (POST with the recap body
         resubmits the same recapId — Vatican returns 200 with the recap
         echo and resets its idle timer). GET /api/visit/recap is also tried
         as a cheaper heartbeat first.
      3. Scrolling + mouse hover to vary the fingerprint so F5 doesn't
         downgrade the session score.

    Stops when:
      - `minutes` elapses (hard cap, default 30 min)
      - A heartbeat returns a server rejection indicating the hold is gone
      - The parent process is killed (SIGINT/SIGTERM) — finally unwinds the
        F5Session which closes Chrome, dropping JSESSIONID.
    """
    import asyncio as _asyncio
    import random as _random
    import time as _time
    deadline = _time.monotonic() + minutes * 60
    started  = _time.monotonic()
    page = f5._page
    ka = {"started_utc": datetime.utcnow().isoformat() + "Z",
          "reservation_id": reservation_id,
          "reference_order": reference_order,
          "beats": [], "stopped_reason": "unknown"}

    log.info("💚 keepalive loop started — holding %s for up to %d min",
             reference_order or reservation_id, minutes)

    # Make sure we're parked on the checkout page so the SPA believes we're
    # still staring at the countdown. Only navigate once, and only if we're not
    # already on the right path — any navigation drops the countdown state.
    try:
        url_now = page.url
        if "/home/checkout" not in url_now:
            await page.goto(f"{BASE_URL}/home/checkout", wait_until="domcontentloaded",
                            timeout=15_000)
    except Exception as exc:
        log.warning("keepalive goto /home/checkout failed: %s — continuing anyway", exc)

    # Neutralise the client-side timer so the SPA doesn't try to auto-cancel
    # the hold when its local countdown hits zero. This JS removes any
    # beforeunload handlers Vatican added and freezes the window.sessionStorage
    # timer if present. Safe to no-op if the DOM shape is different.
    try:
        await page.evaluate("""
        (() => {
            try {
                window.onbeforeunload = null;
                window.addEventListener('beforeunload', e => { e.stopImmediatePropagation(); }, true);
                // Any interval that counts toward expiration — leave it
                // ticking for the UI, but neutralise any auto-cancel call.
                const _oldFetch = window.fetch;
                window.fetch = function(u, o) {
                    try {
                        const url = (typeof u === 'string') ? u : (u && u.url);
                        if (url && /\\/visit\\/cancellation/i.test(url)) {
                            console.log('[keepalive] blocked /visit/cancellation');
                            return Promise.resolve(new Response('{"blocked":true}',
                                {status: 200, headers:{'content-type':'application/json'}}));
                        }
                    } catch (e) {}
                    return _oldFetch.apply(this, arguments);
                };
            } catch (e) { console.log('keepalive patch err', e); }
        })();
        """)
        log.info("💚 cancellation blocker installed on /home/checkout")
    except Exception as exc:
        log.warning("keepalive page patch failed: %s", exc)

    cycle = 0
    while _time.monotonic() < deadline:
        cycle += 1
        elapsed = int(_time.monotonic() - started)
        beat = {"cycle": cycle, "elapsed_s": elapsed, "when": _time.strftime("%H:%M:%S")}

        # 1) GET /api/visit/recap — the SPA's own heartbeat. Pass recapId when
        # known. Vatican returns the full recap object on success.
        params = {"lang": DEFAULT_LANG}
        if recap_id:       params["recapId"]       = recap_id
        if reservation_id: params["reservationId"] = reservation_id
        try:
            j = await api_get_json(f5, "/api/visit/recap", params, retries=1)
            beat["get_recap"]   = "ok"
            beat["recap_total"] = j.get("total")
        except F5Rejected:
            beat["get_recap"] = "f5_rejected"
            log.warning("💚 heartbeat #%d: /recap F5-rejected — session likely downgraded", cycle)
        except Exception as exc:
            beat["get_recap"]  = f"err:{str(exc)[:120]}"
            if any(s in str(exc).lower() for s in ("404", "410", "expired", "not found")):
                ka["stopped_reason"] = "hold_expired"
                log.error("💔 heartbeat #%d: Vatican says the hold is gone: %s", cycle, exc)
                ka["beats"].append(beat)
                return ka

        # 2) Light UI activity — scroll a bit so the fingerprint stays active.
        try:
            await page.evaluate(
                f"window.scrollBy(0, {_random.randint(-120, 180)});"
            )
        except Exception: pass

        # 3) Every 2nd cycle, hover a visible element so mousemove telemetry fires.
        if cycle % 2 == 0:
            try:
                await page.evaluate("""
                (() => {
                    const els = Array.from(document.querySelectorAll('button, a'))
                        .filter(el => el.offsetParent !== null);
                    if (!els.length) return;
                    const pick = els[Math.floor(Math.random()*Math.min(5, els.length))];
                    const rect = pick.getBoundingClientRect();
                    pick.dispatchEvent(new MouseEvent('mouseover', {
                        bubbles: true, clientX: rect.left + 3, clientY: rect.top + 3
                    }));
                })()
                """)
            except Exception: pass

        ka["beats"].append(beat)

        # One summary line per minute (otherwise log noise).
        if cycle % 2 == 0:
            log.info("💚 keepalive  cycle=%d  elapsed=%ds  get_recap=%s",
                     cycle, elapsed, beat.get("get_recap"))

        try:
            await _asyncio.sleep(30 + _random.uniform(-3, 3))
        except _asyncio.CancelledError:
            ka["stopped_reason"] = "cancelled"
            log.info("💚 keepalive cancelled by parent — total %ds", elapsed)
            return ka

    ka["stopped_reason"] = "deadline"
    log.info("💚 keepalive deadline reached (%d min) — stopping", minutes)
    return ka


async def _session_aging(f5: F5Session, duration_sec: int) -> None:
    """Fake a few minutes of realistic user browsing AFTER F5 solve and
    BEFORE /recap. Stays on /home — no navigation (navigating to /home/calendar
    can trigger a SPA router redirect to /404 or an F5 re-challenge that
    kills the page handle). Pure scroll + hover + wait.

    No captcha spend. No /recap calls. Pure UI simulation."""
    import asyncio as _asyncio
    import random as _random
    logger = __import__("logging").getLogger("hold")
    page = f5._page
    deadline = _asyncio.get_event_loop().time() + duration_sec

    async def _safe_eval(js: str):
        try: await page.evaluate(js)
        except Exception as exc:
            logger.debug("session-aging eval failed: %s", exc)

    async def _safe_sleep(min_ms: int, max_ms: int):
        await _asyncio.sleep(_random.uniform(min_ms, max_ms) / 1000.0)

    async def _scroll_and_wait():
        await _safe_eval("window.scrollBy(0, Math.floor(Math.random()*400)+150)")
        await _safe_sleep(2000, 4500)

    # Loop: scroll + random pause until deadline. Occasionally hover a visible
    # element to vary the fingerprint signal.
    cycle = 0
    while _asyncio.get_event_loop().time() < deadline:
        cycle += 1
        await _scroll_and_wait()
        # Every 3rd cycle, try a light hover on a visible element
        if cycle % 3 == 0:
            try:
                await _safe_eval("""
                    (() => {
                        const els = Array.from(document.querySelectorAll('button, a, div[role=\"button\"]'))
                            .filter(el => el.offsetParent !== null);
                        if (els.length === 0) return;
                        const pick = els[Math.floor(Math.random() * Math.min(10, els.length))];
                        const rect = pick.getBoundingClientRect();
                        pick.dispatchEvent(new MouseEvent('mouseover', {
                            bubbles: true, clientX: rect.left + 5, clientY: rect.top + 5,
                        }));
                    })()
                """)
            except Exception: pass
        # Occasionally scroll back up
        if cycle % 5 == 0:
            await _safe_eval("window.scrollTo(0, 0)")
            await _safe_sleep(1500, 3000)
    logger.info("session-aging done (%d cycles)", cycle)


async def warmup_gets(f5: F5Session) -> None:
    """9 pure GETs mirroring the browser between /recap and /reservation.
    Lang MUST match /recap + /reservation — mixing en and it in the same
    session confuses Vatican's backend state and 500s /reservation."""
    L = DEFAULT_LANG
    paths = [
        "/api/config/isAgency",
        f"/api/visit/gdpr?lang={L}",
        f"/api/visit/country?lang={L}",
        f"/api/visit/representetivelanguage?lang={L}",
        f"/api/visit/gdpr?lang={L}",
        f"/api/config/startup?lang={L}",
        "/api/config/isAgency",
        f"/api/purchaserules?lang={L}",
        f"/api/visit/gdpr?lang={L}",
    ]
    for p in paths:
        try:
            await f5.api_get(p)
        except Exception:
            pass
    log.info("warmup: %d GETs sent", len(paths))


async def solve_turnstile_in_browser(f5: F5Session,
                                     site_key: str = "0x4AAAAAAB2Edz1zEK7o5Rj1",
                                     timeout_sec: int = 90) -> Optional[str]:
    """Render a Cloudflare Turnstile widget INSIDE our Playwright page and
    wait for its token. That token is cookie-bound to our session (same
    domain, same cookies, same fingerprint) — so Vatican's server-side
    Turnstile verify step sees the token as "this user, right here".

    Unlike YesCaptcha which solves in a remote browser (making the token
    look session-mismatched to the backend's cross-check), this stays
    entirely in our own session.

    Returns the token string or None on timeout.
    """
    import asyncio as _asyncio
    page = f5._page
    logger = __import__("logging").getLogger("hold")
    logger.info("rendering Turnstile widget in-session…")

    # Load Turnstile's JS API, then render the widget invisibly
    render_js = r"""
    async (sitekey) => {
      // Inject Turnstile API if not already on page
      if (!window.turnstile) {
        await new Promise((resolve) => {
          const s = document.createElement('script');
          s.src = 'https://challenges.cloudflare.com/turnstile/v0/api.js?onload=__cfReady';
          s.async = true; s.defer = true;
          window.__cfReady = () => resolve();
          document.head.appendChild(s);
          setTimeout(resolve, 10000);  // fallback
        });
      }
      // Clear any previous widget
      const old = document.getElementById('__ts_host');
      if (old) old.remove();
      const host = document.createElement('div');
      host.id = '__ts_host';
      host.style.position = 'fixed';
      host.style.right = '-9999px';
      document.body.appendChild(host);
      window.__TS_TOKEN = null;
      window.__TS_ERROR = null;
      if (!window.turnstile) {
        window.__TS_ERROR = 'turnstile API never loaded';
        return;
      }
      try {
        window.turnstile.render(host, {
          sitekey: sitekey,
          action: 'reservation',
          callback: (tok) => { window.__TS_TOKEN = tok; },
          'error-callback': (e) => { window.__TS_ERROR = String(e); },
        });
      } catch (e) {
        window.__TS_ERROR = 'render exception: ' + String(e);
      }
    }
    """
    try:
        await page.evaluate(render_js, site_key)
    except Exception as exc:
        logger.warning("turnstile render JS failed: %s", exc)
        return None

    # Poll for token
    deadline = _asyncio.get_event_loop().time() + timeout_sec
    while _asyncio.get_event_loop().time() < deadline:
        try:
            state = await page.evaluate(
                "() => ({tok: window.__TS_TOKEN, err: window.__TS_ERROR})")
        except Exception: state = {}
        if state.get("tok"):
            logger.info("in-browser Turnstile solved in %.1fs  tok=%s…",
                        timeout_sec - (deadline - _asyncio.get_event_loop().time()),
                        state["tok"][:36])
            return state["tok"]
        if state.get("err"):
            logger.warning("turnstile error: %s", state["err"]); return None
        await _asyncio.sleep(1)
    logger.warning("in-browser Turnstile timed out after %ds", timeout_sec)
    return None


async def post_reservation(f5: F5Session, cfg: dict, recap: dict,
                           turnstile_token: str) -> dict:
    # EXACT order + fields from the user's successful manual checkout on
    # 2026-11-11 16:41 UTC. No visitLang — the real body has none.
    body = {
        "recaptcha":          turnstile_token,
        "reservationId":      recap["reservationId"],
        "lang":               cfg.get("lang", DEFAULT_LANG),
        "recapId":            recap["recapId"],
        "visitorNum":         cfg.get("visitorNum", 1),
        "visitId":            cfg["visitId"],
        "visitTypeId":        cfg["visitTypeId"],
        "tickets":            cfg["tickets"],
        "services":           cfg.get("services") or [],
        "representativeUser": cfg["representativeUser"],
        "participantUser":    cfg["participantUser"],
        "gdpr":               cfg["gdpr"],
    }
    return await api_post_json(f5, "/api/visit/reservation", body,
                               referrer=f"{BASE_URL}/home/checkout")


# ----------------------------------------------------------------------------
# Orchestration
# ----------------------------------------------------------------------------
async def main_async(args) -> dict:
    out: dict = {
        "timestamp_utc": datetime.utcnow().isoformat() + "Z",
        "args": {k: v for k, v in vars(args).items() if k != "yescaptcha_key"},
        "live": bool(args.live),
    }

    cfg_session = Config(
        target_date=args.date,
        num_tickets=args.num,
        headless=args.headless,
        yescaptcha_key=args.yescaptcha_key,
    )
    # Pinned-proxy mode. Inspired by Colosseo bot's cf_clearance.db which
    # caches cookies per proxy — we do the same at the Playwright profile
    # level. Once a proxy is "paired" to a profile, we reuse that pair
    # across runs so Vatican sees the same (IP, fingerprint, cookies) tuple
    # accumulating trust instead of 20 fresh sessions an hour.
    #
    # The pairing lives in ../.pw_profiles/pinned_proxy.txt. If present,
    # we always use that proxy. Only on an explicit --rotate-ip flag or when
    # the file is missing do we pick a new proxy.
    pool_all = [] if args.no_proxy else load_proxy_pool()
    used_proxies: list[str] = []
    out["ip_attempts"] = []

    pinned_file = pathlib.Path(__file__).resolve().parent.parent / ".pw_profiles" / "pinned_proxy.txt"
    pinned_file.parent.mkdir(exist_ok=True)

    def _pick_proxy() -> list[str]:
        if args.proxy:
            return [args.proxy]
        if not pool_all:
            return []
        # Honour a pinned proxy from an earlier run
        if not getattr(args, "rotate_ip", False) and pinned_file.exists():
            pinned = pinned_file.read_text(encoding="utf-8").strip()
            if pinned in pool_all and pinned not in used_proxies:
                used_proxies.append(pinned)
                return [pinned]
        # Otherwise pick a fresh one and pin it
        remaining = [p for p in pool_all if p not in used_proxies]
        pick = random.choice(remaining or pool_all)
        used_proxies.append(pick)
        try: pinned_file.write_text(pick, encoding="utf-8")
        except Exception: pass
        return [pick]

    max_sessions = max(1, args.ip_retries)
    last_err: Optional[Exception] = None

    for sess_i in range(1, max_sessions + 1):
        chosen_proxy = _pick_proxy()
        proxy_mgr = ProxyManager(chosen_proxy, 1)
        sanitized = (chosen_proxy[0].split("@", 1)[-1]
                     if chosen_proxy and "@" in chosen_proxy[0]
                     else (chosen_proxy[0] if chosen_proxy else "direct"))
        out["ip_attempts"].append({"attempt": sess_i, "proxy": sanitized})
        log.info("=== session attempt %d/%d  proxy=%s ===",
                 sess_i, max_sessions, sanitized)

        try:
            async with F5Session(cfg_session, proxy_mgr) as f5:
                log.info("--- F5 TSPD challenge ---")
                await f5.solve()
                out["cookies_count"] = len(f5._cookies)

                # Fast fail: if Angular never loaded, this IP is cold.
                # Skip the canary (don't waste another ~5s) and rotate immediately.
                if not getattr(f5, "challenge_passed", False):
                    log.warning("F5 challenge did not fully resolve on this IP — rotating")
                    continue

                # Canary probe: F5 passed the challenge, now does it accept /api?
                # Some IPs pass the challenge but still score /api/* below threshold.
                try:
                    await api_get_json(f5, "/api/search/filter",
                                       {"lang": "en", "areaId": DEFAULT_AREA},
                                       retries=1)
                except F5Rejected:
                    log.warning("canary /search/filter F5-rejected — rotating IP")
                    continue   # next session attempt

                # Session aging — only on live. Vatican's /reservation seems
                # to check minimum elapsed time between F5 solve and commit.
                # Real humans spend minutes browsing; our bot does it in 15 s
                # and gets 500. Spending 60 s doing realistic SPA page visits
                # between F5 solve and /recap makes the session look human.
                if args.live and args.session_aging_sec > 0:
                    log.info("--- session aging (%d s of realistic browsing) ---",
                             args.session_aging_sec)
                    await _session_aging(f5, args.session_aging_sec)

                log.info("--- slot discovery ---")
                slot = await find_slot(f5, args.date, args.days_ahead, args.num,
                                       prefer_time_hhmm=args.time)
                out["slot"] = asdict(slot)
                log.info("slot picked: %s", slot)

                # Supporting endpoints — same lang as /recap and /reservation.
                log.info("--- pre-reservation probes ---")
                for probe_path, probe_params in [
                    ("/api/visit", {"lang": DEFAULT_LANG, "visitTypeId": slot.visit_type_id,
                                    "visitDate": slot.visit_date, "visitorNum": args.num}),
                    ("/api/visit/services", {"lang": DEFAULT_LANG, "visitId": slot.visit_id,
                                             "visitTypeId": slot.visit_type_id,
                                             "visitorNum": args.num}),
                ]:
                    try:
                        await api_get_json(f5, probe_path, probe_params, retries=1)
                    except F5Rejected:
                        log.warning("probe %s F5-rejected mid-session — rotating IP",
                                    probe_path); raise
                    except Exception as exc:
                        log.warning("probe %s non-fatal: %s", probe_path, exc)

                # Dry mode: STOP HERE. Do NOT call /recap.
                # Vatican's app-level bot check counts /recap POSTs per IP+fingerprint
                # — we observed 3 /recap calls in 45s trigger a 406 "BOT Detected"
                # response on all subsequent /recap attempts. Dry mode now only
                # confirms discovery (free GETs) and reports the slot it would book.
                #
                # EXCEPTION: --hold-only. That mode wants /recap (to lock the slot)
                # but NOT /reservation (which would spend a Turnstile). So the
                # dry-run early-exit only triggers when neither --live nor
                # --hold-only was asked for.
                if not args.live and not getattr(args, "hold_only", False):
                    out["result"] = "discovery_ok_dry_run"
                    out["recap"] = None
                    log.info("--- dry-run stops here (no /recap to save bot-check budget) ---")
                    return out

                # Build config + /recap — LIVE ONLY now
                cfg = build_reservation_config(slot, args)
                out["reservation_cfg_keys"] = sorted(cfg.keys())
                log.info("--- POST /api/visit/recap (creates the HOLD) ---")
                try:
                    recap = await post_recap(f5, cfg)
                except F5Rejected:
                    log.warning("/recap F5-rejected mid-session — rotating IP"); continue
                out["recap"] = recap

                # ── LOW-COST HOLD-ONLY MODE ─────────────────────────────────
                # /recap already locks the slot server-side for ~15 min. If
                # the user only wants a cheap hold (no payment path yet),
                # STOP here — no Turnstile, no captcha. Optionally re-POST
                # /recap in a loop to extend the TTL indefinitely.
                if getattr(args, "hold_only", False):
                    out["result"] = "held_recap_only"
                    log.info("🟢 hold-only mode: /recap lock acquired, SKIPPING "
                             "/reservation (no captcha spend)")
                    # Snapshot to disk immediately so a kill doesn't lose the ref.
                    try:
                        reports = pathlib.Path(__file__).resolve().parent.parent / "reports"
                        reports.mkdir(exist_ok=True)
                        early = reports / f"hold_{int(time.time())}_recap.json"
                        early.write_text(json.dumps(out, indent=2, default=str),
                                         encoding="utf-8")
                        log.info("💾 recap snapshot: %s", early.name)
                    except Exception as exc:
                        log.warning("snapshot write failed: %s", exc)
                    # The rotating re-recap loop is intentionally run OUTSIDE
                    # this F5Session context (main() launches it after we return)
                    # so it builds its own fresh F5Session per cycle batch —
                    # the single-session design dies after ~4 /recap POSTs.
                    return out

                # Warmup
                log.info("--- 9-step browser warmup ---")
                await warmup_gets(f5)

                log.info("--- solving Turnstile ---")
                token: Optional[str] = None
                # Prefer in-browser Turnstile (token cookie-bound to our session).
                # Fall back to YesCaptcha if the in-page widget can't solve.
                if not args.no_browser_turnstile:
                    token = await solve_turnstile_in_browser(f5)
                if not token:
                    if not args.yescaptcha_key:
                        raise RuntimeError("no YesCaptcha key and in-browser solve failed")
                    log.info("falling back to YesCaptcha remote solve")
                    solver = CaptchaSolver(args.yescaptcha_key)
                    token = await solver.solve_turnstile(page_url=PAGE_URL)
                    out["turnstile_source"] = "yescaptcha"
                else:
                    out["turnstile_source"] = "in_browser"
                out["turnstile_len"] = len(token)
                log.info("--- POST /api/visit/reservation ---")
                try:
                    res = await post_reservation(f5, cfg, recap, token)
                except F5Rejected:
                    # Note: captcha already spent. The token is single-use, so
                    # rotating the IP for a new attempt would need a fresh solve.
                    # We stop here to avoid burning multiple captchas back-to-back.
                    log.error("/reservation F5-rejected after Turnstile spend — stopping"); raise
                out["reservation"] = res
                out["result"] = "held"
                epay = res.get("epay") or {}
                log.info("HELD  ref=%s  pay=%s  total=%s",
                         res.get("referenceOrder"), epay.get("urlMs"), res.get("total"))

                # ── OPTIONAL KEEPALIVE ──────────────────────────────────────
                # Keep the Playwright session (and the Vatican JSESSIONID /
                # TSPD cookies) alive past the 5-min client-side countdown.
                # Writes the hold JSON to disk first so pay.py can already
                # pick it up even if the keepalive is killed mid-flight.
                if getattr(args, "keepalive", False):
                    try:
                        reports = pathlib.Path(__file__).resolve().parent.parent / "reports"
                        reports.mkdir(exist_ok=True)
                        early_fname = f"hold_{int(time.time())}_live.json"
                        (reports / early_fname).write_text(
                            json.dumps(out, indent=2, default=str), encoding="utf-8")
                        log.info("💾 pre-keepalive hold snapshot: %s", early_fname)
                    except Exception as exc:
                        log.warning("snapshot write failed: %s", exc)
                    try:
                        ka = await _keepalive_loop(
                            f5,
                            reservation_id=(recap or {}).get("reservationId")
                                           or res.get("reservationId") or "",
                            recap_id=(recap or {}).get("recapId"),
                            reference_order=res.get("referenceOrder"),
                            minutes=int(getattr(args, "keepalive_minutes", 30) or 30),
                        )
                        out["keepalive"] = ka
                    except Exception as exc:
                        log.warning("keepalive crashed: %s", exc)
                        out["keepalive"] = {"error": str(exc)}
                return out
        except F5Rejected as exc:
            last_err = exc
            log.info("session attempt %d ended with F5 rejection — trying another IP",
                     sess_i)
            continue
        except Exception as exc:
            # Non-F5 error — bubble up (likely Vatican backend 500, not an IP issue)
            raise

    raise RuntimeError(f"All {max_sessions} session attempts F5-rejected "
                       f"(last: {last_err})")


def parse_args():
    p = argparse.ArgumentParser(description="Vatican hold-ticket simulator")
    default_date = (datetime.today() + timedelta(days=30)).strftime("%Y-%m-%d")
    p.add_argument("--date",  default=default_date, help="Start date YYYY-MM-DD")
    p.add_argument("--days-ahead", type=int, default=45,
                   help="Max days forward to search if start date has nothing")
    p.add_argument("--time",  default=None, help="Preferred HH:MM (loose match)")
    p.add_argument("--num",   type=int, default=2, help="Number of visitors")
    p.add_argument("--first", default="Mohamed")
    p.add_argument("--last",  default="Najjar")
    p.add_argument("--email", default="najjarmohamed443@gmail.com")
    p.add_argument("--phone", default="20124532")
    p.add_argument("--country", default="Tunisia")
    p.add_argument("--city",  default="Tunis")
    p.add_argument("--gender", default="M", choices=["M", "F"])
    p.add_argument("--birth", default="1990-01-01", help="Buyer birth date YYYY-MM-DD")
    p.add_argument("--participants", default="",
                   help="Optional comma-separated 'First Last, First Last…'")
    p.add_argument("--headless", action="store_true", help="Run Playwright headless")
    p.add_argument("--live", action="store_true",
                   help="POST /reservation for real (default: dry-run after /recap)")
    p.add_argument("--proxy", default=None,
                   help="Force a specific proxy URL (overrides the Evomi pool)")
    p.add_argument("--no-proxy", action="store_true",
                   help="Disable proxies entirely — hit the Vatican site from the local IP")
    p.add_argument("--ip-retries", type=int, default=5,
                   help="Max Evomi IPs to try before giving up when F5 rejects "
                        "(default 5; dry-runs may use higher values)")
    p.add_argument("--product", choices=list(PRODUCT_NAME_MAP.keys()),
                   default="admission",
                   help="Product tier: admission (default), guided, gardens, bus")
    p.add_argument("--rotate-ip", action="store_true",
                   help="Ignore the pinned proxy from .pw_profiles/pinned_proxy.txt "
                        "and pick a fresh Evomi IP (and new Chrome profile with it). "
                        "Use when Vatican bans the current pinned pair.")
    p.add_argument("--no-browser-turnstile", action="store_true",
                   help="Skip the in-browser Turnstile render and go straight "
                        "to YesCaptcha. Only useful for debugging.")
    p.add_argument("--session-aging-sec", type=int, default=75,
                   help="Seconds of realistic SPA browsing between F5 solve and "
                        "/recap on a LIVE run, to fake a human session age. "
                        "Vatican's /reservation may reject sessions <60 s old. "
                        "0 = disable aging (faster but risky).")
    p.add_argument("--ticket", choices=["full", "reduced"], default="reduced",
                   help="Legacy all-or-nothing ticket type. Prefer --full/--reduced "
                        "which can be mixed. Only used when both --full and --reduced "
                        "are 0 (default behaviour: build --num reduced tickets).")
    p.add_argument("--full",    type=int, default=0,
                   help="Count of Full Price tickets (€20, id=60, 'intero'). "
                        "Combines with --reduced; the sum overrides --num.")
    p.add_argument("--reduced", type=int, default=0,
                   help="Count of Reduced tickets (€10, id=61, 'ridotto').")
    p.add_argument("--audio",   type=int, default=0,
                   help="Count of audioguides (€8, id=62) added as a service.")
    p.add_argument("--keepalive", action="store_true",
                   help="After /reservation success, keep the Playwright session alive "
                        "and heartbeat the hold past the 5-min client-side countdown. "
                        "Required for post-countdown payments.")
    p.add_argument("--keepalive-minutes", type=int, default=20,
                   help="Max minutes to run the keepalive loop (default 20). "
                        "Vatican's server-side slot hold is ~15 min from /recap, "
                        "so values above 15 mostly just buy forgiveness.")
    p.add_argument("--hold-only", action="store_true",
                   help="LOW-COST mode: stop after POST /recap (no Turnstile, no "
                        "captcha spend, no /reservation). The slot is still locked "
                        "by /recap for ~15 min. Combine with --rerecap-min to "
                        "re-call /recap in a loop and try to extend the hold.")
    p.add_argument("--rerecap-min", type=int, default=0,
                   help="With --hold-only, re-POST /recap every N minutes to try "
                        "to refresh Vatican's server-side slot TTL. 0 disables. "
                        "Typical: 13 (just before the ~15-min TTL expires). Zero "
                        "captcha spend per re-recap.")
    p.add_argument("--rerecap-cycles", type=int, default=0,
                   help="With --rerecap-min, max cycles before exit. 0 = unlimited "
                        "(runs until killed). Useful for finite tests.")
    p.add_argument("--rerecap-sess-cycles", type=int, default=3,
                   help="How many /recap cycles one F5Session handles before "
                        "rotating to a new Evomi IP + fresh Chrome profile. "
                        "F5 downgrades a single session after ~4 /recap POSTs, "
                        "so 3 is the safe default.")
    p.add_argument("--yescaptcha-key",
                   default=os.environ.get("YESCAPTCHA_CLIENT_KEY")
                           or os.environ.get("YESCAPTCHA_API_KEY")
                           or "5dba7283ad8625948f2ed8babf24c1882eb1cef571662")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    # Switch which visit the catcher will pick
    VISIT_NAME_REQUIRED = PRODUCT_NAME_MAP[args.product]
    globals()["VISIT_NAME_REQUIRED"] = VISIT_NAME_REQUIRED
    log.info("product=%s  match=%s", args.product, VISIT_NAME_REQUIRED)
    try:
        out = asyncio.run(main_async(args))
    except Exception as exc:
        log.error("FAILED: %s", exc)
        out = {"error": str(exc), "timestamp_utc": datetime.utcnow().isoformat() + "Z"}

    reports = pathlib.Path(__file__).resolve().parent.parent / "reports"
    reports.mkdir(exist_ok=True)
    fname = f"hold_{int(time.time())}{'_live' if args.live else '_dry'}.json"
    fp = reports / fname
    fp.write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
    log.info("wrote %s", fp)

    summary = {k: v for k, v in out.items() if k not in ("recap", "reservation")}
    print(json.dumps(summary, indent=2, default=str))
    if "reservation" in out:
        print("\n  referenceOrder:", out["reservation"].get("referenceOrder"))
        print("  pay URL:       ", (out["reservation"].get("epay") or {}).get("urlMs"))
    elif out.get("recap"):
        print("\n  recapId:        ", out["recap"].get("recapId"))
        print("  reservationId:  ", out["recap"].get("reservationId"))
        print("  total (cents):  ", out["recap"].get("total"))

    # ── Rotating re-recap: runs AFTER the first session closes.
    # Separate because each cycle needs a fresh Evomi IP + fresh Chrome profile
    # (F5 downgrades a session after ~4 POSTs). No captcha spent here.
    if (getattr(args, "hold_only", False)
            and int(getattr(args, "rerecap_min", 0) or 0) > 0
            and out.get("result") == "held_recap_only"):
        log.info("🚀 handing off to rotating rerecap loop (fresh IP each burst)")
        try:
            asyncio.run(_rotating_rerecap(args, out))
        except KeyboardInterrupt:
            log.info("🛑 rotating rerecap stopped by Ctrl-C")
