"""
Availability monitor — polls the slot availability endpoint.

Uses lightweight httpx (no browser) with the TS cookies from a solved F5
session.  Runs continuously until a slot matching the target date/type opens,
then returns the slot data for the booking module to act on.

The exact endpoint path is discovered by interceptor.py.  This module uses
a configurable endpoint; update AVAILABILITY_ENDPOINT after running the
interceptor.
"""

import asyncio
import logging
from datetime import datetime
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from session import F5Session

from config import BASE_URL, Config
from proxy import ProxyManager

logger = logging.getLogger(__name__)

AVAILABILITY_ENDPOINT = f"{BASE_URL}/api/search/calendar"
TIMEAVAIL_ENDPOINT    = f"{BASE_URL}/api/visit/timeavail"




class AvailabilityMonitor:

    def __init__(self, config: Config, f5_session: "F5Session"):
        self.config = config
        self.session = f5_session

    async def wait_for_slot(self) -> dict:
        """
        Poll until a slot matching config.target_date and config.ticket_type
        is available.  Returns the slot dict from the API response.
        """
        logger.info(
            "Monitoring availability for %s (%s)…",
            self.config.target_date,
            self.config.ticket_type,
        )
        attempt = 0
        max_attempts = getattr(self.config, "max_poll_attempts", 0)  # 0 = infinite
        while True:
            attempt += 1
            try:
                slot = await self._check()
                if slot:
                    logger.info("Slot found on attempt %d: %s", attempt, slot)
                    return slot
                logger.info("[%d] No slot yet for %s.", attempt, self.config.target_date)
            except Exception as exc:
                logger.warning("HTTP error during availability check: %s", exc)
            if max_attempts and attempt >= max_attempts:
                raise RuntimeError(f"No slot found after {attempt} attempts — stopping dry-run poll")
            await asyncio.sleep(self.config.poll_interval)

    async def _check(self) -> Optional[dict]:
        """
        Fetch calendar availability via page.evaluate(fetch(...)) — runs inside
        Chrome's authenticated session, reusing the same TLS fingerprint that
        passed F5. This is the same technique that successfully fetched initValues.
        """
        import json

        page = self.session._page

        # If the page already has calendar cells rendered (Angular loaded cleanly),
        # skip the XHR check and go straight to the UI flow.
        pre_cells = await page.evaluate(
            "() => document.querySelectorAll('button.mat-calendar-body-cell').length"
        )
        cal_data = None
        if pre_cells > 0:
            logger.info("Calendar already rendered (%d cells) — skipping XHR calendar check.",
                        pre_cells)
        else:
            # First: verify via XHR that the date has availability before doing any UI work.
            r = await self.session.api_get(
                "/api/search/calendar",
                params={"tag": "MV-Biglietti", "lang": "it", "num": "2"},
            )
            if r["status"] not in (200, 201):
                logger.warning("Calendar API returned HTTP %d: %r", r["status"], r["body"][:200])
                return None
            if r["body"].strip().startswith("<"):
                logger.info("Calendar API F5-blocked — continuing with SPA navigation anyway")
                cal_data = None
            else:
                try:
                    cal_data = json.loads(r["body"])
                except Exception as exc:
                    logger.warning("Calendar JSON parse failed: %s  body=%r", exc, r["body"][:200])
                    cal_data = None
                if cal_data and not self._date_available(cal_data):
                    return None

            # Note: Angular components never render (inputs=0, buttons=0) — the app
            # requires a successful initValues API call which F5 often blocks.
            # We rely entirely on XHR-based access; fall through to timeavail XHR below.

        # Step 2: try to click a calendar date if Angular rendered the calendar UI
        try:
            mat_cells = await page.evaluate(
                "() => document.querySelectorAll('button.mat-calendar-body-cell').length"
            )
        except Exception:
            mat_cells = 0
        if mat_cells > 0:
            logger.info("Calendar UI rendered (%d cells) — navigating via SPA for %s…", mat_cells, self.config.target_date)
            target_day = str(int(self.config.target_date.split("-")[2]))  # "10" from "2026-11-10"
            try:
                # Log all buttons to help diagnose which one to click
                btn_texts = await page.evaluate("""
                    () => Array.from(document.querySelectorAll('button')).map(b => ({
                        text: b.textContent.trim().slice(0,40), cls: b.className.slice(0,50),
                        disabled: b.disabled, visible: b.offsetParent !== null
                    }))
                """)
                logger.info("Page buttons: %s", [(b["text"], b["disabled"]) for b in btn_texts if b["visible"]])

                # Wrap ALL interactions inside expect_response so we catch timeavail
                # regardless of whether it fires on date click or on CERCA click
                # Capture ALL /api/ responses during this flow to understand what Angular calls
                captured_api = []
                async def _on_api_resp(r):
                    if "/api/" in r.url:
                        try:
                            body = await r.body()
                            txt = body.decode("utf-8", errors="replace")
                            captured_api.append({"url": r.url, "status": r.status,
                                                 "html": txt.strip().startswith("<"), "body": txt})
                        except Exception:
                            pass
                page.on("response", _on_api_resp)

                # 1. Fill visitor count
                try:
                    await page.fill('#numberInput', str(self.config.num_tickets))
                    logger.info("Visitor count filled: %d", self.config.num_tickets)
                except Exception as e:
                    logger.warning("fill #numberInput failed: %s", e)
                await asyncio.sleep(0.5)

                # 2. Click target date
                clicked = await page.evaluate("""
                    (day) => {
                        for (const btn of document.querySelectorAll('button.mat-calendar-body-cell')) {
                            const span = btn.querySelector('.mat-calendar-body-cell-content');
                            if (span && span.textContent.trim() === day && !btn.disabled &&
                                !btn.classList.contains('mat-calendar-body-disabled')) {
                                btn.click();
                                return true;
                            }
                        }
                        return false;
                    }
                """, target_day)
                logger.info("Date click day %s: %s", target_day, clicked)
                await asyncio.sleep(1.0)

                # 3. Click CONFERMA (not CERCA — the calendar page uses CONFERMA)
                conferma = page.get_by_role("button", name="CONFERMA", exact=True)
                n_conf = await conferma.count()
                logger.info("CONFERMA buttons: %d", n_conf)
                if n_conf > 0:
                    await conferma.first.click()
                    logger.info("Clicked CONFERMA — waiting for Angular timeslot page…")
                else:
                    # fallback: btn-primary (skip reset button)
                    bp = page.locator('button.btn-primary')
                    nbp = await bp.count()
                    if nbp > 0:
                        txt_bp = await page.evaluate(
                            "() => Array.from(document.querySelectorAll('button.btn-primary')).map(b=>b.textContent.trim())")
                        logger.info("btn-primary texts: %s", txt_bp)
                        await bp.first.click()
                        logger.info("Clicked btn-primary fallback")
                    else:
                        await page.keyboard.press("Enter")
                        logger.info("No CONFERMA or btn-primary — pressed Enter")

                # 4. Wait for resultPerTag (Angular navigates to fromtag page)
                await asyncio.sleep(10)

                # Extract visit id from resultPerTag response
                rpt = next((c for c in captured_api
                            if "/api/search/resultPerTag" in c["url"]
                            and not c["html"] and c["status"] == 200), None)

                visit_id = None
                if rpt:
                    try:
                        rpt_data = json.loads(rpt["body"])
                        visits = rpt_data.get("visits", [])
                        if visits:
                            visit_id = visits[0].get("id")
                            logger.info("Visit ID from resultPerTag: %s  name=%s",
                                        visit_id, visits[0].get("name","")[:60])
                    except Exception as e:
                        logger.warning("resultPerTag parse error: %s", e)

                # 5. On the fromtag page, click the visit card to get to timeslots
                # Buttons on this page: cards with visit names or PRENOTA buttons
                fromtag_buttons = await page.evaluate("""
                    () => Array.from(document.querySelectorAll('button, a[class*="btn"], .card, [class*="prenota"]')).slice(0,20).map(el => ({
                        tag: el.tagName, text: el.textContent.trim().slice(0,60),
                        cls: el.className.slice(0,60)
                    }))
                """)
                logger.info("fromtag page elements: %s", [(e["tag"], e["text"], e["cls"][:30]) for e in fromtag_buttons[:8]])

                # Click first visit card / PRENOTA button
                prenota = page.locator('button:has-text("PRENOTA"), button:has-text("Prenota"), a:has-text("PRENOTA"), .card button, button.btn-primary')
                n_prenota = await prenota.count()
                logger.info("PRENOTA/btn-primary count: %d", n_prenota)

                if n_prenota > 0:
                    # Start new capture for timeavail
                    captured_api2 = []
                    async def _on_api_resp2(r):
                        if "/api/" in r.url:
                            try:
                                body = await r.body()
                                txt = body.decode("utf-8", errors="replace")
                                captured_api2.append({"url": r.url, "status": r.status,
                                                      "html": txt.strip().startswith("<"), "body": txt})
                            except Exception:
                                pass
                    page.on("response", _on_api_resp2)

                    await prenota.first.click()
                    logger.info("Clicked PRENOTA/btn-primary — waiting for timeavail…")
                    await asyncio.sleep(15)
                    page.remove_listener("response", _on_api_resp2)

                    logger.info("APIs after PRENOTA (%d total):", len(captured_api2))
                    for c in captured_api2:
                        logger.info("  HTTP %d html=%-5s  %s", c["status"], c["html"], c["url"][:100])
                        if not c["html"] and c["status"] == 200:
                            logger.info("    JSON: %s", c["body"][:400])

                    ta_data = next((c for c in captured_api2
                                    if "/api/visit/timeavail" in c["url"]
                                    and not c["html"] and c["status"] == 200), None)
                    if ta_data:
                        logger.info("timeavail captured after PRENOTA! body=%r", ta_data["body"][:300])
                        page.remove_listener("response", _on_api_resp)
                        # Extract visitTypeId from the timeavail URL
                        import re as _re
                        m = _re.search(r"visitTypeId=(\d+)", ta_data["url"])
                        visit_type_id = m.group(1) if m else None
                        slot = self._find_slot(json.loads(ta_data["body"]))
                        if slot and visit_type_id:
                            slot["visitTypeId"] = visit_type_id

                        # Capture page DOM state for booking.py to use the SPA
                        dom_state = await page.evaluate("""
                            () => ({
                                url: window.location.href,
                                bodyLen: document.body?.innerHTML?.length || 0,
                                btnCount: document.querySelectorAll('button').length,
                                btnTexts: Array.from(document.querySelectorAll('button')).slice(0,15).map(b=>b.textContent.trim().slice(0,40)),
                                inputs: document.querySelectorAll('input').length,
                            })
                        """)
                        logger.info("Page state after timeavail: %s", dom_state)
                        if slot:
                            slot["_page_url"] = dom_state.get("url")
                        return slot

                page.remove_listener("response", _on_api_resp)

                # Check DOM for time slot elements
                dom_slots = await page.evaluate("""
                    () => {
                        const timeTexts = [];
                        document.querySelectorAll('*').forEach(el => {
                            if (el.children.length === 0) {
                                const t = el.textContent.trim();
                                if (/^\\d{2}:\\d{2}/.test(t)) timeTexts.push(t);
                            }
                        });
                        return {url: window.location.href,
                                bodyLen: document.body?.innerHTML?.length || 0,
                                btnCount: document.querySelectorAll('button').length,
                                timeTexts: timeTexts.slice(0,20)};
                    }
                """)
                logger.info("DOM after full SPA flow: url=%s bodyLen=%d buttons=%d times=%s",
                            dom_slots.get("url",""), dom_slots.get("bodyLen",0),
                            dom_slots.get("btnCount",0), dom_slots.get("timeTexts",[]))
                logger.warning("SPA flow: no timeavail captured")
            except Exception as exc:
                logger.warning("SPA timeavail flow failed: %s", exc)

        # Convert date to DD/MM/YYYY (the format the API expects, matching calendar response)
        from datetime import datetime as _dt
        visit_date_dmy = _dt.strptime(self.config.target_date, "%Y-%m-%d").strftime("%d/%m/%Y")
        from urllib.parse import urlencode

        # Step A: Discover visitTypeId via httpx (1 request, not a full browser nav).
        # Fall back to page.goto() only if httpx is WAF-blocked.
        visit_type_id = None
        rpt_params = {
            "tag": "MV-Biglietti",
            "visitDate": visit_date_dmy,
            "visitorNum": str(self.config.num_tickets),
            "lang": "it",
        }
        logger.info("Navigating browser to resultPerTag (discover visitTypeId)…")
        try:
            r_rpt = await self.session.api_get("/api/search/resultPerTag", params=rpt_params)
            rpt_body = r_rpt["body"]
            if r_rpt["status"] == 200 and rpt_body.strip().startswith("{"):
                rpt_data = json.loads(rpt_body)
                visits = rpt_data.get("visits", [])
                if visits:
                    visit_type_id = str(visits[0].get("id", ""))
                    logger.info("visitTypeId discovered: %s  visit=%s",
                                visit_type_id, str(visits[0])[:120])
            else:
                # httpx blocked — fall back to page.goto() (browser navigation)
                logger.info("httpx resultPerTag blocked (%d) — trying browser nav", r_rpt["status"])
                rpt_url = f"{BASE_URL}/api/search/resultPerTag?{urlencode(rpt_params)}"
                await page.goto(rpt_url, wait_until="load", timeout=35_000)
                for _ in range(10):
                    rpt_body = await page.evaluate("() => document.body.textContent || ''")
                    if rpt_body.strip().startswith("{"):
                        break
                    await asyncio.sleep(1)
                rpt_final = page.url
                logger.info("resultPerTag landed at: %s  bodyLen=%d  preview=%r",
                            rpt_final, len(rpt_body), rpt_body[:300])
                if rpt_body.strip().startswith("{"):
                    rpt_data = json.loads(rpt_body.strip())
                    visits = rpt_data.get("visits", [])
                    if visits:
                        visit_type_id = str(visits[0].get("id", ""))
                        logger.info("visitTypeId discovered via browser: %s", visit_type_id)
        except Exception as exc_rpt:
            logger.warning("resultPerTag failed: %s", exc_rpt)

        if not visit_type_id:
            logger.warning("visitTypeId not discovered — trying without it")

        ta_params_dict = {
            "tag": "MV-Biglietti",
            "visitDate": visit_date_dmy,
            "visitorNum": str(self.config.num_tickets),
            "lang": "it",
        }
        if visit_type_id:
            ta_params_dict["visitTypeId"] = visit_type_id
        ta_full_url = f"{BASE_URL}/api/visit/timeavail?{urlencode(ta_params_dict)}"
        logger.info("timeavail URL: %s", ta_full_url)

        # Step B: Get timeslots via httpx (1 request). Fall back to browser nav if blocked.
        logger.info("Navigating browser to timeavail URL (lets F5 APM challenge complete)…")
        ta_body = ""
        try:
            r_ta = await self.session.api_get("/api/visit/timeavail", params=ta_params_dict)
            ta_body = r_ta["body"]
            if r_ta["status"] != 200 or ta_body.strip().startswith("<"):
                logger.info("httpx timeavail blocked (%d) — trying browser nav", r_ta["status"])
                ta_body = ""
                try:
                    await page.goto(ta_full_url, wait_until="load", timeout=35_000)
                except Exception as exc_ta_nav:
                    logger.warning("timeavail goto/load failed: %s", exc_ta_nav)
                for _wait in range(12):
                    try:
                        ta_body = await page.evaluate("() => document.body.textContent || ''")
                    except Exception:
                        ta_body = ""
                    stripped = ta_body.strip()
                    if stripped and "canvas {display:none;}" not in stripped and len(stripped) > 50:
                        break
                    await asyncio.sleep(1)
        except Exception as exc_ta:
            logger.warning("timeavail failed: %s", exc_ta)

        final_url = page.url
        logger.info("timeavail nav landed at: %s  bodyLen=%d", final_url, len(ta_body))
        logger.info("timeavail page body preview: %r", ta_body[:400])

        if not ta_body.strip() or ta_body.strip().startswith("<"):
            logger.warning("timeavail returned HTML block page or empty")
            return None

        try:
            slot = self._find_slot(json.loads(ta_body.strip()))
            if slot:
                if visit_type_id:
                    slot["visitTypeId"] = visit_type_id
                return slot
            logger.info("timeavail parsed but no available slot found")
            return None
        except Exception as exc_parse:
            logger.warning("timeavail JSON parse failed: %s  body=%r", exc_parse, ta_body[:200])
            return None

    def _date_available(self, cal_data) -> bool:
        """
        Checks the calendar response to see if target_date has availability.
        Real API format: {"calendar":[{"date":"DD/MM/YYYY","state":1},…]}
        state=1 means available, state=0 means closed/sold-out.
        """
        target = self.config.target_date  # "YYYY-MM-DD"
        # Convert "YYYY-MM-DD" → "DD/MM/YYYY" to match API format
        try:
            from datetime import datetime
            t = datetime.strptime(target, "%Y-%m-%d")
            target_dmy = t.strftime("%d/%m/%Y")
        except Exception:
            target_dmy = target

        entries = []
        if isinstance(cal_data, dict):
            entries = cal_data.get("calendar") or cal_data.get("dates") or []
        elif isinstance(cal_data, list):
            entries = cal_data

        for entry in entries:
            d = entry.get("date", "")
            state = entry.get("state", entry.get("available", 0))
            if d == target_dmy:
                logger.info("Date %s found in calendar with state=%s", d, state)
                return bool(state)
        logger.info("Date %s not found in calendar (%d entries)", target_dmy, len(entries))
        return False

    def _find_slot(self, data) -> Optional[dict]:
        """
        Parse the /api/visit/timeavail response and return the best available slot.
        Real format: {"timetable": [{"id": "2026*11250", "time": "09:00",
                                      "availability": "SOLD_OUT"|"AVAILABLE",
                                      "residual": 0, ...}]}
        """
        slots = []
        if isinstance(data, list):
            slots = data
        elif isinstance(data, dict):
            slots = (data.get("timetable") or data.get("slots") or
                     data.get("timeSlots") or data.get("data") or [])

        logger.info("Slot check: %d slots from timeavail", len(slots))
        for slot in slots:
            avail = slot.get("availability") or slot.get("available")
            residual = slot.get("residual", 0) or 0

            if isinstance(avail, str):
                # AVAILABLE = normal, LOW_AVAILABILITY = few seats (Vatican hides exact count),
                # SOLD_OUT = full. Both AVAILABLE and LOW_AVAILABILITY are bookable.
                BOOKABLE = {"AVAILABLE", "LOW_AVAILABILITY", "POSTI_DISPONIBILI",
                            "POCHI_POSTI", "LOW", "PARTIALLY_AVAILABLE"}
                if avail.upper() not in BOOKABLE:
                    continue
                # Only enforce residual check when it's provided and > 0
                if residual > 0 and residual < self.config.num_tickets:
                    continue
            elif isinstance(avail, (int, float)):
                if int(avail) < self.config.num_tickets:
                    continue
            else:
                continue

            time_val = slot.get("time") or slot.get("startTime") or slot.get("ora", "")
            if self.config.time_slot and self.config.time_slot not in time_val:
                continue

            logger.info("Found available slot: %s residual=%s", slot, residual)
            return slot

        # Log availability summary
        avail_summary = [(s.get("time","?"), s.get("availability","?"), s.get("residual",0))
                         for s in slots[:8]]
        logger.info("No available slot. Sample: %s", avail_summary)
        return None

    async def snapshot_60_day_window(self) -> dict:
        """
        Pull availability for every date in the 60-day window at once.
        Useful for mapping exactly when new dates go live (release-time recon).
        """
        from datetime import timedelta
        today = datetime.today().date()
        results = {}
        async with httpx.AsyncClient(
            proxy=self.proxy_mgr.httpx_proxy(),
            cookies=self.cookies,
            headers=HEADERS,
            timeout=self.config.request_timeout,
            follow_redirects=True,
            verify=False,
        ) as client:
            for offset in range(60):
                d = (today + timedelta(days=offset)).isoformat()
                r = await client.get(
                    AVAILABILITY_ENDPOINT,
                    params={"date": d, "type": self.config.ticket_type, "qty": 1},
                )
                if r.status_code == 200:
                    results[d] = r.json()
                await asyncio.sleep(0.3)   # gentle pacing
        return results
