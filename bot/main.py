"""
Vatican Museums ticket bot — orchestrator.

Flow:
  1. Solve F5 TSPD challenge (Playwright) → harvest TS cookies
  2. Solve reCAPTCHA v2 (YesCaptcha) → get g-recaptcha-response token
  3. Poll availability (httpx) until target slot opens
  4. Walk booking funnel (httpx API calls) using cookies + token
  5. Stop before payment if dry_run=True

Usage:
  python main.py

Configure the Config object at the bottom of this file before running.
Run interceptor.py first to populate endpoint constants in booking.py and
monitor.py with the real API paths.
"""

import asyncio
import logging

from config import Config
from session import F5Session
from captcha import CaptchaSolver
from monitor import AvailabilityMonitor
from booking import BookingSession
from proxy import ProxyManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


async def run(cfg: Config):
    proxy_mgr = ProxyManager(cfg.proxy_list, cfg.rotate_every)

    async with F5Session(cfg, proxy_mgr) as f5:
        # ── Step 1: F5 challenge ──────────────────────────────────────────────
        logger.info("=== Step 1/4: Solving F5 TSPD challenge ===")
        await f5.solve()

        # ── Step 2: Cloudflare Turnstile token ───────────────────────────────
        logger.info("=== Step 2/4: Solving Cloudflare Turnstile via YesCaptcha ===")
        solver = CaptchaSolver(cfg.yescaptcha_key)
        captcha_token = await solver.solve_turnstile(
            site_key=f5.turnstile_site_key,
            page_url="https://tickets.museivaticani.va/home",
        )

        # ── Step 3: Monitor availability ─────────────────────────────────────
        logger.info("=== Step 3/4: Monitoring slot availability ===")
        monitor = AvailabilityMonitor(cfg, f5)
        slot = await monitor.wait_for_slot()

        # ── Step 4: Book ──────────────────────────────────────────────────────
        logger.info("=== Step 4/4: Executing booking funnel ===")
        booking = BookingSession(cfg, f5, captcha_token)
        result = await booking.run(slot)

    if result.success:
        if cfg.dry_run:
            logger.info("✓ DRY RUN complete. Reservation ID: %s", result.reservation_id)
        else:
            logger.info("✓ Booking complete. Reservation ID: %s", result.reservation_id)
    else:
        logger.error("✗ Booking failed: %s", result.error)

    return result


# ── CONFIGURATION ─────────────────────────────────────────────────────────────
# Edit this block before running.

if __name__ == "__main__":
    config = Config(
        # Target visit date
        target_date="2026-11-20",

        # Ticket type: "standard" or "early_entry"
        ticket_type="standard",
        num_tickets=2,

        # Leave None to take the first available time slot
        time_slot=None,

        # Buyer (the "Manager data" form)
        buyer_first="Mohamed",
        buyer_last="Najjar",
        buyer_email="najjarmohamed443@gmail.com",
        buyer_dob="1990-01-01",

        # One entry per ticket — names must match ID at entry gate
        participants=[
            {"first": "Mohamed", "last": "Najjar"},
            {"first": "Guest",   "last": "Two"},
        ],

        yescaptcha_key="5dba7283ad8625948f2ed8babf24c1882eb1cef571662",

        # No proxy — test local IP (may have recovered from rate limit after ~1hr)
        proxy_list=[],
        rotate_every=1,

        # Safety: True = stop before the payment step
        dry_run=True,
        headless=False,
        poll_interval=30.0,    # 30s between attempts — lets F5 session score recover
        max_poll_attempts=5,   # give more chances for session to warm up
    )

    asyncio.run(run(config))
