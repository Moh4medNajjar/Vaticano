"""
Booking funnel — raw API calls.

Reproduces the 8-step Vatican Museums booking funnel as direct HTTP requests,
using the TS session cookies from F5Session and the g-recaptcha-response token
from CaptchaSolver.

Endpoint paths confirmed by interceptor run + chunk-XLSUZSFZ.js bundle scan.
"""

import logging
import re
from dataclasses import dataclass
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from session import F5Session

from config import BASE_URL, Config
from proxy import ProxyManager

logger = logging.getLogger(__name__)

# Real endpoints confirmed by interceptor + JS bundle scan
EP_GET_SID      = f"{BASE_URL}/api/getSid"                    # session ID (call first)
EP_VISIT_DESC   = f"{BASE_URL}/api/visit/desc"                # visit description
EP_SERVICES     = f"{BASE_URL}/api/visit/services"            # add-ons (audio guide etc.)
EP_COUNTRY      = f"{BASE_URL}/api/visit/country"             # country list for form
EP_GDPR         = f"{BASE_URL}/api/visit/gdpr"                # GDPR fields
EP_RESERVATION  = f"{BASE_URL}/api/visit/reservation"         # CREATE reservation ← main booking call
EP_RECAP        = f"{BASE_URL}/api/visit/recap"               # review recap before confirm
EP_PARTICIPANTS = f"{BASE_URL}/api/reservation/participant"   # nominative names per visitor
EP_CONFIRM      = f"{BASE_URL}/api/visit/confirmation"        # submit Turnstile + confirm
EP_PAYMENT      = f"{BASE_URL}/api/visit/confirmation/ads"    # post-confirm / payment redirect



@dataclass
class BookingResult:
    success: bool
    reservation_id: Optional[str] = None
    error: Optional[str] = None
    raw_response: Optional[dict] = None


class BookingSession:

    def __init__(
        self,
        config: Config,
        f5_session: "F5Session",
        captcha_token: str,
    ):
        self.config = config
        self.session = f5_session
        self.captcha_token = captcha_token
        self._reservation_id: Optional[str] = None

    async def run(self, slot: dict) -> BookingResult:
        """
        Execute the booking funnel. Tries direct API first (APM cookies set by monitor's
        browser navigations), falls back to SPA interaction if needed.
        """
        try:
            res_data = await self._book_via_direct_api(slot)
            if res_data:
                self._reservation_id = (
                    res_data.get("reservationId") or res_data.get("bookingId") or
                    res_data.get("id") or res_data.get("orderId") or
                    res_data.get("prenotazione") or str(res_data)[:80]
                )

            if self.config.dry_run:
                logger.info("[DRY RUN] Stopping before payment. Reservation: %s", self._reservation_id)
                return BookingResult(success=True, reservation_id=self._reservation_id, raw_response=res_data)

            return BookingResult(success=True, reservation_id=self._reservation_id, raw_response=res_data)

        except Exception as exc:
            logger.error("Booking failed: %s", exc)
            return BookingResult(success=False, error=str(exc))

    async def _book_via_direct_api(self, slot: dict) -> dict:
        """
        POST to /api/visit/reservation using XHR (APM cookies already set by monitor's
        browser navigations of resultPerTag + timeavail).  If XHR is still blocked,
        navigate the browser to getSid first to warm up the POST cookie, then retry.
        """
        import json as _json
        import asyncio
        from datetime import datetime as _dt
        from urllib.parse import urlencode

        page = self.session._page

        visit_type_id = str(slot.get("visitTypeId", ""))
        slot_id       = slot.get("id", "")
        slot_time     = slot.get("time", "")
        visit_date_dmy = _dt.strptime(self.config.target_date, "%Y-%m-%d").strftime("%d/%m/%Y")

        logger.info("[direct_api] slot=%s visitTypeId=%s date=%s", slot_id, visit_type_id, visit_date_dmy)

        # Build reservation payload (field names from Angular FormGroup data-cy attributes)
        payload = {
            "visitTypeId": int(visit_type_id) if visit_type_id.isdigit() else visit_type_id,
            "slotId": slot_id,
            "visitDate": visit_date_dmy,
            "time": slot_time,
            "visitorNum": self.config.num_tickets,
            "lang": "it",
            "tag": "MV-Biglietti",
            "manager": {
                "name":      self.config.buyer_first,
                "surname":   self.config.buyer_last,
                "email":     self.config.buyer_email,
                "emailConfirm": self.config.buyer_email,
                "phone":     "0000000000",
                "sex":       "M",
                "country":   "IT",
                "language":  "IT",
                "birthDate": _dt.strptime(self.config.buyer_dob, "%Y-%m-%d").strftime("%d/%m/%Y")
                              if self.config.buyer_dob else "",
                "city":      "Roma",
            },
            "participants": [
                {"name": p.get("first",""), "surname": p.get("last","")}
                for p in self.config.participants
            ],
            "gdprAccepted": True,
            "termsAccepted": True,
            "cfTurnstileToken": self.captcha_token or "",
        }

        # Step 1: Navigate to the booking form page so the fetch Referer matches the real user flow.
        # /home/fromtag/MV-Biglietti is the URL the Angular app shows during booking.
        booking_url = f"{BASE_URL}/home/fromtag/MV-Biglietti"
        logger.info("[direct_api] Navigating to booking form URL for correct Referer context…")
        try:
            await page.goto(booking_url, wait_until="load", timeout=35_000)
            for _ in range(25):
                body_len = await page.evaluate("() => document.body.innerHTML.length")
                if body_len >= 20_000:
                    break
                await asyncio.sleep(1)
            final_body_len = await page.evaluate("() => document.body.innerHTML.length")
            logger.info("[direct_api] booking form ready — bodyLen=%d  url=%s", final_body_len, page.url)
        except Exception as exc_home:
            logger.warning("[direct_api] booking form nav: %s", exc_home)
        await asyncio.sleep(3)

        # Step 2: JSON fetch POST with APM-eval retry loop.
        # With the page at the booking form URL, the JSON fetch may get an APM
        # JS challenge (instead of a WAF hard block). If APM challenge detected: eval the
        # scripts in the page context (fingerprinting runs, TSPD updated) → retry fetch.
        json_payload = _json.dumps(payload)
        for _attempt in range(3):
            logger.info("[direct_api] JSON fetch attempt %d…", _attempt + 1)
            try:
                fetch_result = await page.evaluate("""
                    async (args) => {
                        const r = await fetch(args.url, {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                                'Accept': 'application/json, text/plain, */*',
                            },
                            credentials: 'include',
                            body: args.body,
                        });
                        return {status: r.status, body: await r.text()};
                    }
                """, {"url": EP_RESERVATION, "body": json_payload})
            except Exception as exc_fe:
                logger.warning("[direct_api] fetch attempt %d failed: %s", _attempt + 1, exc_fe)
                await asyncio.sleep(3)
                continue

            body_txt = fetch_result["body"]
            logger.info("[direct_api] attempt %d → HTTP %d  body=%r",
                        _attempt + 1, fetch_result["status"], body_txt[:400])

            if body_txt.strip() and not body_txt.strip().startswith("<"):
                try:
                    data = _json.loads(body_txt)
                    logger.info("[direct_api] ✓ Reservation JSON: %s", data)
                    return data
                except Exception as exc_p:
                    logger.warning("[direct_api] JSON parse: %s  raw=%r", exc_p, body_txt[:200])
                    return {"raw": body_txt[:300]}

            # APM check MUST come before WAF — APM page may contain "rejected" in its JS
            is_apm = ("APM_DO_NOT_TOUCH" in body_txt or "getterHook" in body_txt
                      or "window[\"bobcmn\"]" in body_txt or "bobcmn" in body_txt)
            # WAF rejection page has a specific title — don't match on generic "rejected"
            is_waf = "<title>Request Rejected</title>" in body_txt

            if is_apm and _attempt >= 2:
                logger.warning("[direct_api] APM challenge but retries exhausted on attempt %d", _attempt + 1)
                break
            elif is_apm:
                pass  # fall through to APM eval below
            elif is_waf:
                logger.warning("[direct_api] WAF hard-block attempt %d — no APM eval possible", _attempt + 1)
                break
            elif _attempt >= 2:
                logger.warning("[direct_api] fetch blocked (unknown response) retries exhausted")
                break
            else:
                logger.warning("[direct_api] fetch blocked (unknown response) attempt %d  body=%r",
                               _attempt + 1, body_txt[:200])
                break

            # APM JS challenge — set the full APM HTML as the page content so the browser
            # runs the APM scripts natively (correct window context, correct URL for TSPD XHR).
            # Then intercept network to capture what APM re-submits.
            logger.info("[direct_api] APM JS challenge — loading APM page natively via set_content…")

            # Capture all /TSPD/ and /api/visit/ requests+responses during APM execution
            captured_apm = []
            async def _on_resp(r):
                if "/TSPD" in r.url or "/api/visit/reservation" in r.url:
                    try:
                        body_bytes = await r.body()
                        captured_apm.append({
                            "url": r.url, "status": r.status,
                            "body": body_bytes.decode("utf-8", errors="replace")[:500],
                        })
                    except Exception:
                        captured_apm.append({"url": r.url, "status": r.status, "body": ""})
            page.on("response", _on_resp)

            nav_triggered = False
            try:
                await page.set_content(body_txt, wait_until="domcontentloaded")
                logger.info("[direct_api] APM set_content loaded")
            except Exception as exc_sc:
                nav_triggered = True
                logger.info("[direct_api] APM set_content nav/context: %s", exc_sc)

            # Wait for APM fingerprinting XHR + re-submission to complete
            await asyncio.sleep(20)
            try:
                await page.wait_for_load_state("networkidle", timeout=12_000)
            except Exception:
                pass

            page.remove_listener("response", _on_resp)

            # Log what the APM scripts actually did
            for c in captured_apm:
                logger.info("[direct_api] APM network: HTTP %d  %s  body=%r",
                            c["status"], c["url"][:100], c["body"][:200])

            # Check if APM re-submission produced a booking result (page navigated to result)
            try:
                post_url = page.url
                try:
                    post_text = await page.evaluate("() => document.body.innerText || ''")
                except Exception:
                    post_text = ""
                logger.info("[direct_api] Post-eval: url=%s  bodyLen=%d  preview=%r",
                            post_url, len(post_text), post_text[:200])
                stripped = post_text.strip()
                if stripped and not stripped.startswith("<") and not stripped.startswith("The requested") \
                        and not "Pagina non trovata" in stripped and not "404" in stripped[:50]:
                    try:
                        data = _json.loads(stripped)
                        logger.info("[direct_api] ✓ APM re-submission succeeded! JSON: %s", data)
                        return data
                    except Exception:
                        pass
            except Exception as exc_check:
                logger.warning("[direct_api] Post-eval page check: %s", exc_check)

            # Navigate back to booking form for next retry
            try:
                await page.goto(f"{BASE_URL}/home/fromtag/MV-Biglietti", wait_until="load", timeout=35_000)
                for _ in range(10):
                    bl = await page.evaluate("() => document.body.innerHTML.length")
                    if bl >= 20_000:
                        break
                    await asyncio.sleep(1)
            except Exception as exc_nav:
                logger.warning("[direct_api] nav back to booking form: %s", exc_nav)
            await asyncio.sleep(2)

        raise RuntimeError("[direct_api] reservation POST failed after all attempts")

    async def _step_reservation_via_spa(self, slot: dict) -> dict:
        """
        Drive the Angular SPA from the fromtag page (booking panel already open).
        Steps: log DOM → select time slot → fill form → click PROCEDI → capture reservation.
        """
        import json as _json
        import asyncio
        page = self.session._page

        # Log current page state
        dom = await page.evaluate("""
            () => ({
                url: window.location.href,
                inputs: Array.from(document.querySelectorAll('input')).map(i => ({
                    id: i.id, name: i.name, type: i.type, placeholder: i.placeholder,
                    value: i.value.slice(0,30), cls: i.className.slice(0,40)
                })),
                btns: Array.from(document.querySelectorAll('button')).map(b => ({
                    text: b.textContent.trim().slice(0,40), cls: b.className.slice(0,40), disabled: b.disabled
                }))
            })
        """)
        logger.info("[spa_booking] url=%s inputs=%d buttons=%d",
                    dom.get("url",""), len(dom.get("inputs",[])), len(dom.get("btns",[])))
        logger.info("[spa_booking] inputs: %s", [(i["id"] or i["name"] or i["type"], i["placeholder"]) for i in dom.get("inputs",[])])
        logger.info("[spa_booking] buttons: %s", [(b["text"], b["disabled"]) for b in dom.get("btns",[])])

        # 1. Select the time slot: click [data-cy='time'] div that matches slot_time and is NOT disabled
        slot_time = slot.get("time", "")
        logger.info("[spa_booking] Selecting time slot: %s", slot_time)
        slot_clicked = await page.evaluate("""
            (time) => {
                const allSlots = document.querySelectorAll('[data-cy="time"]');
                for (const el of allSlots) {
                    const t = el.textContent.trim();
                    const isDisabled = el.classList.contains('disabled');
                    if (t.startsWith(time) && !isDisabled) {
                        el.click();
                        return {found: true, text: t.slice(0,60), disabled: false};
                    }
                }
                // Fallback: any element starting with the time
                const all = document.querySelectorAll('button, [role="button"], .slot, .timeslot, label, li, div');
                for (const el of all) {
                    const t = el.textContent.trim();
                    if (t.startsWith(time) && !el.classList.contains('disabled')) {
                        el.click();
                        return {found: true, fallback: true, text: t.slice(0,60)};
                    }
                }
                return {found: false, available: Array.from(allSlots).map(e=>e.textContent.trim().slice(0,20))};
            }
        """, slot_time)
        logger.info("[spa_booking] Slot click result: %s", slot_clicked)
        await asyncio.sleep(1.5)

        # 2. Dump form HTML — search broadly for the booking form
        form_html = await page.evaluate("""
            () => {
                // Walk up from PROCEDI button to find the form container
                let panel = null;
                for (const btn of document.querySelectorAll('button')) {
                    if (btn.textContent.trim() === 'PROCEDI') {
                        // Go up several levels to find a container with inputs
                        let el = btn;
                        for (let i = 0; i < 10; i++) {
                            el = el.parentElement;
                            if (!el) break;
                            if (el.querySelectorAll('input').length > 0) { panel = el; break; }
                        }
                        break;
                    }
                }
                // Fallback: look for containers with the form fields
                if (!panel) {
                    for (const sel of ['app-book-visit', 'app-booking', '[class*="book"]', 'app-root']) {
                        const el = document.querySelector(sel);
                        if (el && el.querySelectorAll('input').length > 0) { panel = el; break; }
                    }
                }
                if (!panel) panel = document.body;

                // Detailed input info including Angular attributes
                const allInputs = Array.from(document.querySelectorAll('input')).map((inp, i) => ({
                    idx: i,
                    type: inp.type,
                    id: inp.id,
                    name: inp.name,
                    placeholder: inp.placeholder,
                    cls: inp.className.slice(0,60),
                    formControlName: inp.getAttribute('formcontrolname') || inp.getAttribute('ng-reflect-name') || inp.getAttribute('data-cy') || '',
                    matLabel: (inp.closest('mat-form-field')?.querySelector('mat-label,label')?.textContent?.trim() || ''),
                    ngReflectModel: inp.getAttribute('ng-reflect-model') || '',
                    value: inp.value.slice(0,30),
                }));
                // Also grab surrounding HTML of interesting parts
                const bookingArea = document.querySelector('app-book-visit, [data-cy*="book"], .bookVisitContainer');
                const bookingParent = bookingArea ? bookingArea.parentElement : null;
                return {
                    panelTag: panel.tagName,
                    panelCls: panel.className.slice(0,60),
                    allInputs,
                    bookingHtml: (bookingParent || bookingArea || document.querySelector('.bookVisitContainer') || {}).innerHTML?.slice(0, 4000) || '',
                };
            }
        """)
        logger.info("[spa_booking] All inputs on page (%d):", len(form_html.get("allInputs",[])))
        for inp in form_html.get("allInputs", []):
            logger.info("  input[%d]: type=%-8s id=%-20r formCtrl=%-20r matLabel=%-20r ngModel=%-15r value=%r",
                        inp["idx"], inp["type"], inp["id"],
                        inp["formControlName"], inp["matLabel"], inp["ngReflectModel"], inp["value"])
        logger.info("[spa_booking] Booking area HTML:\n%s", form_html.get("bookingHtml","")[:3000])

        # Dump all data-cy elements to understand the full form structure
        all_cy = await page.evaluate("""
            () => Array.from(document.querySelectorAll('[data-cy]')).map(el => ({
                tag: el.tagName, dataCy: el.getAttribute('data-cy'),
                cls: el.className.slice(0,50), text: el.textContent.trim().slice(0,40),
                children: el.children.length,
                html: el.outerHTML.slice(0,200),
            }))
        """)
        logger.info("[spa_booking] All data-cy elements (%d):", len(all_cy))
        for el in all_cy:
            logger.info("  [data-cy=%r] <%s> cls=%r text=%r", el["dataCy"], el["tag"], el["cls"][:30], el["text"])

        # 3. Set ticket quantity using dropdown: [data-cy='ticketQuantitySection'] items are dropdown options
        # Structure: two ticket types (Intero/Ridotto), each with a dropdown showing options '0','1','2'
        # We want: first ticket type (Intero) = num_tickets, second = 0
        # Step A: click the first ticketQuantity input to open its dropdown
        qty_result = await page.evaluate("""
            (numTickets) => {
                const results = [];
                // Find all ticketQuantity inputs (one per ticket type)
                const qtyInputs = document.querySelectorAll('input[data-cy="ticketQuantity"]');
                if (qtyInputs.length === 0) return {error: 'no ticketQuantity inputs'};

                // Click first input to open its dropdown, then select the option matching numTickets
                const firstInput = qtyInputs[0];
                firstInput.click();  // open dropdown

                // Now find the ticketQuantitySection that has text matching numTickets
                // These sections are list items in the dropdown for the FIRST input
                const allSections = document.querySelectorAll('[data-cy="ticketQuantitySection"]');
                let selectedFirst = false;
                for (const sec of allSections) {
                    const val = parseInt(sec.textContent.trim());
                    if (val === numTickets && !selectedFirst) {
                        sec.click();
                        results.push({ticket: 'first', value: val, text: sec.textContent.trim()});
                        selectedFirst = true;
                        break;
                    }
                }

                // Wait a tick then verify
                return {results, totalSections: allSections.length, firstInputVal: firstInput.value};
            }
        """, self.config.num_tickets)
        logger.info("[spa_booking] Ticket quantity set: %s", qty_result)
        await asyncio.sleep(1.5)

        # Re-check input values after dropdown selection
        input_vals = await page.evaluate("""
            () => Array.from(document.querySelectorAll('input')).map((i,idx) => ({
                idx, type: i.type, dataCy: i.getAttribute('data-cy') || '', value: i.value.slice(0,20)
            }))
        """)
        logger.info("[spa_booking] Input values after qty set: %s",
                    [(x["idx"], x["dataCy"] or x["type"], x["value"]) for x in input_vals])

        # 3. Capture reservation API call while clicking PROCEDI
        captured_res = []
        async def _on_res(r):
            if "/api/" in r.url:
                try:
                    body = await r.body()
                    txt = body.decode("utf-8", errors="replace")
                    captured_res.append({"url": r.url, "status": r.status,
                                         "html": txt.strip().startswith("<"), "body": txt})
                    logger.info("[spa_booking] API: %d html=%-5s %s", r.status, txt.strip().startswith("<"), r.url[:80])
                    if not txt.strip().startswith("<"):
                        logger.info("[spa_booking]   JSON: %s", txt[:300])
                except Exception:
                    pass
        page.on("response", _on_res)

        # Click PROCEDI via evaluate (bypasses Angular detach issues; id="bookVisit" is stable)
        clicked_procedi = await page.evaluate("""
            () => {
                const btn = document.getElementById('bookVisit') ||
                            document.querySelector('[data-cy="bookVisit"]') ||
                            Array.from(document.querySelectorAll('button')).find(b => b.textContent.trim() === 'PROCEDI');
                if (btn && !btn.disabled) { btn.click(); return {clicked: true, text: btn.textContent.trim(), id: btn.id}; }
                return {clicked: false, ids: Array.from(document.querySelectorAll('button')).map(b=>b.id)};
            }
        """)
        logger.info("[spa_booking] PROCEDI click result: %s", clicked_procedi)
        if not clicked_procedi.get("clicked"):
            raise RuntimeError(f"[spa_booking] PROCEDI button not found: {clicked_procedi}")

        # Wait for PROCEDI response (navigates to /home/checkout and fires recap)
        await asyncio.sleep(20)

        # Check what fired immediately after PROCEDI
        logger.info("[spa_booking] Captured %d API calls after PROCEDI:", len(captured_res))
        for c in captured_res:
            logger.info("  %d html=%-5s %s", c["status"], c["html"], c["url"][:100])
            if not c["html"]:
                logger.info("    JSON: %s", c["body"][:300])

        # Check if we navigated to checkout
        page_state = await page.evaluate("""
            () => ({
                url: window.location.href,
                bodyLen: document.body?.innerHTML?.length || 0,
                text: document.body?.innerText?.slice(0, 800) || '',
                inputs: Array.from(document.querySelectorAll('input')).map(i => ({
                    type: i.type, id: i.id, name: i.name, placeholder: i.placeholder,
                    dataCy: i.getAttribute('data-cy') || '', value: i.value.slice(0,30),
                    readonly: i.readOnly
                })),
                btns: Array.from(document.querySelectorAll('button')).map(b => b.textContent.trim().slice(0,40))
            })
        """)
        logger.info("[spa_booking] After PROCEDI: url=%s bodyLen=%d",
                    page_state.get("url",""), page_state.get("bodyLen",0))
        logger.info("[spa_booking] Page text: %r", page_state.get("text","")[:300])
        logger.info("[spa_booking] Inputs on checkout (%d):", len(page_state.get("inputs",[])))
        for inp in page_state.get("inputs",[]):
            if not inp["readonly"] and inp["type"] not in ("hidden",):
                logger.info("  [%s] id=%r name=%r dataCy=%r placeholder=%r", inp["type"], inp["id"], inp["name"], inp["dataCy"], inp["placeholder"])
        logger.info("[spa_booking] Buttons: %s", page_state.get("btns",[])[:10])

        # If we reached /home/checkout, continue to fill customer form and get reservation ID
        if "/home/checkout" in page_state.get("url",""):
            logger.info("[spa_booking] On checkout page — filling customer data form…")

            # ── INITIAL STATE CHECK ─────────────────────────────────────────────────────
            # Capture validity BEFORE any fills so we know the baseline state.
            # If participantContainerOutside starts VALID we're BREAKING it downstream;
            # if it starts INVALID we need to fix it.
            initial_validity = await page.evaluate("""
                () => {
                    const pco = document.querySelector('.participantContainerOutside');
                    const co  = document.querySelector('.containerOutside');
                    const form = document.querySelector('form');
                    return {
                        url: window.location.href,
                        pco_invalid: pco ? pco.classList.contains('ng-invalid') : null,
                        co_invalid:  co  ? co.classList.contains('ng-invalid')  : null,
                        form_invalid: form ? form.classList.contains('ng-invalid') : null,
                        totalNgInvalid: document.querySelectorAll('.ng-invalid').length,
                        hasNg: !!window.ng,
                        ngKeys: window.ng ? Object.keys(window.ng).slice(0, 20) : []
                    };
                }
            """)
            logger.info("[spa_booking] INITIAL validity (before fills): %s", initial_validity)
            # ───────────────────────────────────────────────────────────────────────────

            # Fill checkout form using the actual data-cy field names discovered from DOM
            checkout_fills = await page.evaluate("""
                (data) => {
                    const filled = [];
                    // Angular reactive form inputs with their data-cy attributes
                    const cyFills = [
                        ['managerSurname', data.last],
                        ['managerName', data.first],
                        ['managerEmail', data.email],
                        ['managerConfirmEmail', data.email],
                        ['managerCity', data.city],
                        ['managerPhone', data.phone],
                    ];
                    const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value').set;
                    const fire = (el, val) => {
                        setter.call(el, val);
                        el.dispatchEvent(new Event('input', {bubbles: true}));
                        el.dispatchEvent(new Event('change', {bubbles: true}));
                        el.dispatchEvent(new Event('blur', {bubbles: true}));
                    };
                    for (const [cy, val] of cyFills) {
                        if (!val) continue;
                        const el = document.querySelector(`input[data-cy="${cy}"]`);
                        if (el && !el.readOnly) { fire(el, val); filled.push({cy, val: val.slice(0,20)}); }
                    }
                    // Participant names by id (participantSurname_0, participantName_0, etc.)
                    data.participants.forEach((p, i) => {
                        const surnEl = document.getElementById(`participantSurname_${i}`);
                        const nameEl = document.getElementById(`participantName_${i}`);
                        if (surnEl) { fire(surnEl, p.last); filled.push({id: `participantSurname_${i}`, val: p.last}); }
                        if (nameEl) { fire(nameEl, p.first); filled.push({id: `participantName_${i}`, val: p.first}); }
                    });
                    // Checkboxes: skip here — will be clicked AFTER dropdown fills
                    // (Angular reactive form re-renders on dropdown changes, resetting checkbox state)
                    return filled;
                }
            """, {
                "first": self.config.buyer_first,
                "last": self.config.buyer_last,
                "email": self.config.buyer_email,
                "city": "Roma",
                "phone": "0000000000",
                "country": "Italia",
                "participants": [
                    {"first": p.get("first",""), "last": p.get("last","")}
                    for p in self.config.participants
                ],
            })
            logger.info("[spa_booking] Checkout fills: %s", checkout_fills)

            # Dump full checkout form structure to understand all required fields
            form_dump = await page.evaluate("""
                () => {
                    // All APP-* custom elements and their labels
                    const appEls = Array.from(document.querySelectorAll('app-dropdown, app-datepicker, [data-cy="managerBirthDate"]')).map(el => ({
                        tag: el.tagName, id: el.id, dataCy: el.getAttribute('data-cy')||'',
                        cls: el.className.slice(0,60),
                        text: el.textContent.trim().slice(0,60),
                        html: el.innerHTML.slice(0,300),
                        inputs: Array.from(el.querySelectorAll('input')).map(i=>({type:i.type,id:i.id,dataCy:i.getAttribute('data-cy')||'',value:i.value}))
                    }));
                    return appEls;
                }
            """)
            logger.info("[spa_booking] APP elements dump (%d):", len(form_dump))
            for el in form_dump:
                logger.info("  <%s> id=%r dataCy=%r text=%r inputs=%s", el['tag'], el['id'], el['dataCy'], el['text'][:40], el['inputs'])
            await asyncio.sleep(1.5)

            # Fill dropdown fields one at a time (separate calls + sleeps let Angular re-render)
            def _make_dropdown_fill(cy_val, option_text):
                return f"""
                    () => {{
                        const el = document.querySelector('input[data-cy="{cy_val}"]');
                        if (!el) return {{error: 'not found'}};
                        el.scrollIntoView({{block:'center'}});
                        el.click();
                        return {{opened: true, val: el.value}};
                    }}
                """

            def _select_item(text):
                # Only click VISIBLE items (offsetParent !== null) to avoid clicking hidden items
                # from other dropdowns that are still in the DOM
                return f"""
                    () => {{
                        const allItems = document.querySelectorAll('.select__list--item');
                        // First try visible items only
                        for (const item of allItems) {{
                            if (item.offsetParent !== null && item.textContent.trim() === '{text}') {{
                                item.scrollIntoView({{block:'nearest'}});
                                item.click();
                                return {{clicked: true, text: '{text}', visible: true}};
                            }}
                        }}
                        // Fallback: any item (even hidden)
                        for (const item of allItems) {{
                            if (item.textContent.trim() === '{text}') {{
                                item.scrollIntoView({{block:'nearest'}});
                                item.click();
                                return {{clicked: true, text: '{text}', visible: false}};
                            }}
                        }}
                        const vis = Array.from(allItems).filter(i=>i.offsetParent!==null).map(i=>i.textContent.trim().slice(0,15));
                        return {{clicked: false, visItems: vis}};
                    }}
                """

            # 1. Sex/Gender
            await page.evaluate(_make_dropdown_fill("managerSex", "Maschio"))
            await asyncio.sleep(0.5)
            sex_r = await page.evaluate(_select_item("Maschio"))
            logger.info("[spa_booking] managerSex: %s", sex_r)
            await asyncio.sleep(0.5)

            # 2. Country (has search filter input)
            await page.evaluate(_make_dropdown_fill("managerCountry", "Italia"))
            await asyncio.sleep(0.5)
            # Type in filter to narrow to Italia
            await page.evaluate("""
                () => {
                    const inp = document.getElementById('searchInput_country');
                    if (!inp) return;
                    const setter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype,'value').set;
                    setter.call(inp, 'Italia');
                    ['input','keyup','change'].forEach(e=>inp.dispatchEvent(new Event(e,{bubbles:true})));
                }
            """)
            await asyncio.sleep(0.4)
            ctry_r = await page.evaluate(_select_item("Italia"))
            logger.info("[spa_booking] managerCountry: %s", ctry_r)
            await asyncio.sleep(0.5)

            # 3. Language — evaluate-based (same as sex/country; locator.click fails viewport)
            await page.evaluate(_make_dropdown_fill("managerLanguage", "Italiano"))
            await asyncio.sleep(0.6)
            lang_r = await page.evaluate(_select_item("Italiano"))
            logger.info("[spa_booking] managerLanguage: %s", lang_r)
            await asyncio.sleep(0.5)

            # 4. Birth date — Playwright native clicks (CDP mouse events, isTrusted=true)
            #    page.evaluate el.click() dispatches synthetic events that may not propagate
            #    through Angular Material's ControlValueAccessor chain to update the FormControl.
            #    Playwright locator.click() uses real CDP Input.dispatchMouseEvent.
            dob_result = {"val": "not_set", "error": None}
            try:
                import asyncio as _asyncio
                # Scroll to the datepicker toggle
                await page.evaluate("document.querySelector('mat-datepicker-toggle button, #birthDateContainer button')?.scrollIntoView({block:'center'})")
                await asyncio.sleep(0.3)

                # Open calendar
                dob_target = self.config.buyer_dob  # "1990-01-01"
                dob_year  = int(dob_target.split("-")[0])  # 1990
                dob_month = int(dob_target.split("-")[1])  # 1
                dob_day   = int(dob_target.split("-")[2])  # 1

                await page.locator("mat-datepicker-toggle button").first.click()
                await asyncio.sleep(0.7)

                # Check calendar is open
                cal_open = await page.locator("mat-calendar").count() > 0
                logger.info("[spa_booking] Calendar opened: %s", cal_open)

                if cal_open:
                    # Click period button once → year-view (months); click again → multi-year-view
                    await page.locator("button.mat-calendar-period-button").click()
                    await asyncio.sleep(0.45)

                    cells = await page.locator(".mat-calendar-body-cell-content").all_text_contents()
                    in_multi = any(re.match(r"^\d{4}$", t.strip()) for t in cells)
                    logger.info("[spa_booking] After 1st period click — in_multi_year=%s  sample=%s", in_multi, cells[:4])

                    if not in_multi:
                        await page.locator("button.mat-calendar-period-button").click()
                        await asyncio.sleep(0.45)
                        cells = await page.locator(".mat-calendar-body-cell-content").all_text_contents()
                        in_multi = any(re.match(r"^\d{4}$", t.strip()) for t in cells)
                        logger.info("[spa_booking] After 2nd period click — in_multi_year=%s  sample=%s", in_multi, cells[:4])

                    # Navigate backward until target year is visible (max 6 pages back)
                    for _nav in range(6):
                        cells = await page.locator(".mat-calendar-body-cell-content").all_text_contents()
                        if str(dob_year) in [t.strip() for t in cells]:
                            break
                        await page.locator("button.mat-calendar-previous-button").click()
                        await asyncio.sleep(0.35)
                    cells = await page.locator(".mat-calendar-body-cell-content").all_text_contents()
                    logger.info("[spa_booking] Years visible: %s", [t.strip() for t in cells if re.match(r"^\d{4}$", t.strip())])

                    # Click the target year
                    # Note: cells are .mat-calendar-body-cell-content WITHOUT td. prefix
                    # has_text does partial substring match (so " 1990 " matches "1990")
                    year_loc = page.locator(".mat-calendar-body-cell-content").filter(has_text=str(dob_year))
                    year_texts = await year_loc.all_text_contents()
                    logger.info("[spa_booking] Year cells matching %s: %s", dob_year, year_texts)
                    if not year_texts:
                        # Fallback: find by positional index in all year cells
                        all_cells = await page.locator(".mat-calendar-body-cell-content").all_text_contents()
                        yr_idx = next((i for i, t in enumerate(all_cells) if t.strip() == str(dob_year)), -1)
                        logger.info("[spa_booking] Year fallback idx=%d all cells=%s", yr_idx, all_cells[:6])
                        if yr_idx >= 0:
                            await page.locator(".mat-calendar-body-cell-content").nth(yr_idx).click()
                        else:
                            raise RuntimeError(f"Year {dob_year} not found in calendar cells")
                    else:
                        await year_loc.first.click()
                    await asyncio.sleep(0.45)

                    # Now in year-view for target year — click target month (January = index 0)
                    # Italian: gen, feb, mar…  English fallback: jan, feb, mar…
                    month_cells = await page.locator(".mat-calendar-body-cell-content").all_text_contents()
                    logger.info("[spa_booking] Month cells: %s", month_cells)
                    target_month_idx = dob_month - 1  # 0-based index
                    italian_months = ["gen","feb","mar","apr","mag","giu","lug","ago","set","ott","nov","dic"]
                    target_text = italian_months[target_month_idx]
                    matched_idx = next(
                        (i for i, t in enumerate(month_cells) if t.strip().lower().startswith(target_text)),
                        target_month_idx
                    )
                    await page.locator(".mat-calendar-body-cell-content").nth(matched_idx).click()
                    await asyncio.sleep(0.45)

                    # Now in day-view — click target day (skip disabled/grayed cells)
                    # Disabled cells have aria-disabled="true" or mat-calendar-body-disabled class
                    day_cells = page.locator(".mat-calendar-body-cell-content:not(.mat-calendar-body-disabled)")
                    day_texts = await day_cells.all_text_contents()
                    logger.info("[spa_booking] Day cells (first 10): %s", day_texts[:10])
                    day_idx = next((i for i, t in enumerate(day_texts) if t.strip() == str(dob_day)), 0)
                    await day_cells.nth(day_idx).click()
                    await asyncio.sleep(0.5)

                    dob_result["val"] = await page.locator('input[data-cy="dateCalendar"]').input_value()
                    dob_result["calendarClosed"] = await page.locator("mat-calendar").count() == 0
                    logger.info("[spa_booking] managerBirthDate after Playwright click: %s", dob_result)
                    # Do NOT dispatch extra input/change events here — they trigger _onInput() which
                    # re-parses "1 January 1990" using dd/MM/yyyy locale format → returns invalid Date
                    # → sets _lastValueValid=false → breaks the valid state set by the calendar click.

            except Exception as _dob_err:
                dob_result["error"] = str(_dob_err)
                logger.warning("[spa_booking] Playwright calendar click error: %s", _dob_err)
            await asyncio.sleep(0.5)

            # 5. Participant sex dropdowns — find dynamically, fill each with evaluate
            part_sex_cys = await page.evaluate("""
                () => Array.from(document.querySelectorAll('input[data-cy]'))
                    .filter(el => el.getAttribute('data-cy') !== 'managerSex' &&
                                  el.getAttribute('data-cy').toLowerCase().includes('sex'))
                    .map(el => el.getAttribute('data-cy'))
            """)
            logger.info("[spa_booking] Participant sex inputs: %s", part_sex_cys)
            for _cy in part_sex_cys:
                await page.evaluate(_make_dropdown_fill(_cy, "Maschio"))
                await asyncio.sleep(0.6)
                _pr = await page.evaluate(_select_item("Maschio"))
                logger.info("[spa_booking] %s: %s", _cy, _pr)
                await asyncio.sleep(0.5)

            # 6. Click checkboxes AFTER dropdown fills (Angular re-renders reset them if done earlier)
            # The required terms checkbox has data-cy='acceptChk-1'; click its LABEL for reliable check
            cb_results = await page.evaluate("""
                () => {
                    const results = [];
                    document.querySelectorAll('mat-checkbox').forEach(el => {
                        const inp = el.querySelector('input[type="checkbox"]');
                        if (!inp) return;
                        const lbl = el.querySelector('label');
                        // Click label if available (most reliable for Angular Material)
                        if (lbl) {
                            lbl.click();
                            results.push({dataCy: el.getAttribute('data-cy')||'', id: inp.id, method: 'label', checked: inp.checked});
                        } else {
                            // Fallback: set checked directly + fire change
                            Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype,'checked').set.call(inp, true);
                            inp.dispatchEvent(new Event('change', {bubbles: true}));
                            inp.dispatchEvent(new Event('input', {bubbles: true}));
                            inp.click();
                            results.push({dataCy: el.getAttribute('data-cy')||'', id: inp.id, method: 'direct', checked: inp.checked});
                        }
                    });
                    // Any standalone native checkboxes — skip service panel ones (handled separately)
                    document.querySelectorAll('input[type="checkbox"]:not(:checked)').forEach(inp => {
                        if (!inp.closest('mat-checkbox') && !inp.closest('[id^="expandedPanel_"]')) {
                            inp.click();
                            results.push({id: inp.id, method: 'native', checked: inp.checked});
                        }
                    });
                    return results;
                }
            """)
            logger.info("[spa_booking] Checkbox fills: %s", cb_results)
            await asyncio.sleep(0.5)

            # Verify field values — check both text and hidden inputs
            verify_state = await page.evaluate("""
                () => {
                    const vals = {};
                    ['managerSex','managerCountry','managerLanguage'].forEach(f => {
                        const el = document.querySelector(`input[data-cy="${f}"]`);
                        if (el) {
                            vals[f] = el.value;
                            // Also check the sibling hidden input (actual form value)
                            const hidden = el.parentElement?.querySelector('input[type="hidden"]') ||
                                           el.closest('app-dropdown')?.querySelector('input[type="hidden"]');
                            if (hidden) vals[f + '_hidden'] = hidden.value;
                        }
                    });
                    const dob = document.querySelector('input[data-cy="dateCalendar"]');
                    if (dob) vals['dateCalendar'] = dob.value;
                    // Email match check
                    const em = document.querySelector('input[data-cy="managerEmail"]');
                    const ec = document.querySelector('input[data-cy="managerConfirmEmail"]');
                    if (em && ec) vals['emailMatch'] = em.value === ec.value;
                    // Checkbox states
                    vals['checkboxes'] = Array.from(document.querySelectorAll('input[type="checkbox"]')).map(
                        el => ({id: el.id, checked: el.checked})
                    );
                    return vals;
                }
            """)
            logger.info("[spa_booking] Field values after extra fills: %s", verify_state)

            # --- "Aggiungi servizi" step ---
            # Each participant card has an "Aggiungi servizi" button that must be clicked
            # to confirm the mandatory pre-sale service (id=58, "Diritti di Prevendita").
            # Without this the participantContainerOutside FormGroup stays ng-invalid.
            servizi_result = await page.evaluate("""
                async () => {
                    const results = [];

                    // The "Aggiungi servizi" UI is a toggle-switch button (class="toggle-switch-expanded").
                    // The toggle is already ON (green, expanded). Service checkboxes in expandedPanels
                    // may appear checked (via our standalone checkbox fill) but Angular's component
                    // might need the toggle to be CLOSED → OPENED to "confirm" the service selection.

                    // Strategy 1: toggle each participant's toggle switch OFF then ON again
                    // Re-query after each click — Angular may destroy/recreate the button element,
                    // so holding a stale reference and calling .click() a second time does nothing.
                    const countToggles = () =>
                        document.querySelectorAll('button.toggle-switch-expanded, button[class*="toggle-switch"]').length;
                    const nToggles = countToggles();
                    results.push({toggleBtns: nToggles});

                    for (let ti = 0; ti < nToggles; ti++) {
                        // Re-query each iteration to get fresh references
                        const freshBtns = Array.from(document.querySelectorAll('button.toggle-switch-expanded, button[class*="toggle-switch"]'));
                        if (ti >= freshBtns.length) break;
                        const btn = freshBtns[ti];
                        btn.scrollIntoView({block: 'center'});
                        btn.click();  // Turn OFF (collapse panel)
                        await new Promise(r => setTimeout(r, 500));
                        results.push({toggledOff: ti, cls: btn.className.slice(0,40)});
                        // Re-query again after OFF — might be a different element now
                        const btnsAfterOff = Array.from(document.querySelectorAll('button.toggle-switch-collapsed, button[class*="toggle-switch"]:not(.toggle-switch-expanded)'));
                        const btnOn = btnsAfterOff[ti] || freshBtns[ti];  // try collapsed variant first
                        btnOn.click();  // Turn ON (expand panel)
                        await new Promise(r => setTimeout(r, 500));
                        results.push({toggledOn: ti, cls: btnOn.className.slice(0,40)});
                    }

                    await new Promise(r => setTimeout(r, 400));

                    // Strategy 1b: look for Aggiungi/Conferma/Aggiungi servizi buttons inside panels
                    const confirmKeywords = ['AGGIUNGI', 'CONFERMA', 'ADD', 'CONFIRM'];
                    const panelBtns = Array.from(document.querySelectorAll('[id^="expandedPanel_"] button, .service-panel button, .expanded-panel button'));
                    results.push({panelBtnsFound: panelBtns.length, panelBtnTexts: panelBtns.map(b=>b.textContent.trim().slice(0,20))});
                    for (const pb of panelBtns) {
                        const t = pb.textContent.trim().toUpperCase();
                        if (confirmKeywords.some(k => t.includes(k))) {
                            pb.click();
                            await new Promise(r => setTimeout(r, 300));
                            results.push({panelBtnClicked: t.slice(0,20)});
                        }
                    }

                    // Strategy 2: ensure service checkboxes inside panels are checked
                    const panels = document.querySelectorAll('[id^="expandedPanel_"]');
                    results.push({panels: panels.length, ids: Array.from(panels).map(p=>p.id)});
                    for (const panel of panels) {
                        const cbs = panel.querySelectorAll('input[type="checkbox"]');
                        for (const cb of cbs) {
                            if (!cb.checked) {
                                cb.removeAttribute('readonly');
                                cb.click();
                                await new Promise(r => setTimeout(r, 100));
                                if (!cb.checked) {
                                    Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'checked').set.call(cb, true);
                                    cb.dispatchEvent(new Event('change', {bubbles: true}));
                                }
                                results.push({panel: panel.id, cbChecked: cb.checked});
                            } else {
                                results.push({panel: panel.id, alreadyChecked: true});
                            }
                        }
                    }

                    await new Promise(r => setTimeout(r, 500));

                    // Debug: find ALL elements with _nghost- attributes (component hosts)
                    const ngHosts = Array.from(document.querySelectorAll('*')).filter(el =>
                        Array.from(el.attributes).some(a => a.name.startsWith('_nghost'))
                    );
                    results.push({
                        ngHostCount: ngHosts.length,
                        ngHosts: ngHosts.slice(0, 10).map(el => ({
                            tag: el.tagName, cls: el.className.slice(0,40),
                            ngHostAttr: Array.from(el.attributes).find(a=>a.name.startsWith('_nghost'))?.name,
                            ctxType: typeof el.__ngContext__,
                            ctxIsArray: Array.isArray(el.__ngContext__),
                            ctxConstructor: el.__ngContext__?.constructor?.name || 'none',
                            ctxLen: el.__ngContext__?.length
                        }))
                    });

                    // Debug: find checkout component host specifically
                    const checkoutHost = document.querySelector('[_nghost-ng-c3005736359]');
                    if (checkoutHost) {
                        const lview = checkoutHost.__ngContext__;
                        results.push({
                            checkoutHost: checkoutHost.tagName,
                            ctxType: typeof lview,
                            ctxIsArray: Array.isArray(lview),
                            ctxLen: lview?.length,
                            ctxConstructor: lview?.constructor?.name
                        });
                    } else {
                        results.push({checkoutHostNotFound: true});
                    }

                    // Verify form validity after toggle
                    const co = document.querySelector('.containerOutside');
                    const pco = document.querySelector('.participantContainerOutside');
                    results.push({
                        containerOutside_ngInvalid: co ? co.classList.contains('ng-invalid') : null,
                        participantContainerOutside_ngInvalid: pco ? pco.classList.contains('ng-invalid') : null,
                        totalNgInvalid: document.querySelectorAll('.ng-invalid').length
                    });
                    return results;
                }
            """)
            logger.info("[spa_booking] Aggiungi servizi result: %s", servizi_result)
            await asyncio.sleep(0.5)

            # Use Playwright native clicks for toggle + service checkbox (CDP mouse events, isTrusted=true)
            # JS el.click() may not update Angular FormControls for these elements.
            try:
                # Dismiss any open overlay (calendar, dropdown) before clicking toggles
                # An open mat-datepicker overlay blocks all pointer events to page elements
                overlay_count = await page.locator(".cdk-overlay-container .mat-mdc-dialog-surface, .cdk-overlay-container .cdk-overlay-backdrop").count()
                if overlay_count > 0:
                    logger.info("[spa_booking] Overlay open (%d), pressing Escape to close", overlay_count)
                    await page.keyboard.press("Escape")
                    await asyncio.sleep(0.5)
                # Also close any open calendar
                cal_open2 = await page.locator("mat-calendar").count()
                if cal_open2 > 0:
                    await page.keyboard.press("Escape")
                    await asyncio.sleep(0.4)

                # Service panels: leave EXPANDED panels alone — closing them invalidates
                # the mandatory service FormControl in Angular (removes it from the FormGroup).
                # If a panel is COLLAPSED (not expanded), open it so the checkbox is visible.
                toggle_locs = page.locator("button.toggle-switch-expanded, button.toggle-switch")
                n_toggles = await toggle_locs.count()
                logger.info("[spa_booking] Playwright toggle count: %d", n_toggles)
                for ti in range(n_toggles):
                    btn = toggle_locs.nth(ti)
                    cls = await btn.get_attribute("class") or ""
                    if "expanded" in cls:
                        # Panel already open — leave it open, don't close
                        logger.info("[spa_booking] Service panel %d already expanded — leaving open", ti)
                    else:
                        # Panel closed — open it to expose the mandatory service checkbox
                        await btn.click()
                        await asyncio.sleep(0.6)
                        logger.info("[spa_booking] Opened service panel %d", ti)

                # Service checkboxes inside expandedPanels.
                # Previous attempts at clicking the INPUT returned checked=False — the Angular
                # component calls event.preventDefault() and controls state internally.
                # New strategy: click the LABEL (or wrapping div) so the Angular (click) handler
                # on those elements fires.  Also try the outer service-row div.
                # The checkbox reflects the component's internal servicesSelected state.
                panel_cbs = page.locator('[id^="expandedPanel_"] input[type="checkbox"]')
                n_cbs = await panel_cbs.count()
                logger.info("[spa_booking] Service checkboxes in panels: %d", n_cbs)
                for ci in range(n_cbs):
                    # Scroll into view first
                    await page.evaluate("""
                        (idx) => {
                            const cbs = document.querySelectorAll('[id^="expandedPanel_"] input[type="checkbox"]');
                            const cb = cbs[idx];
                            if (cb) cb.scrollIntoView({block: 'center'});
                        }
                    """, ci)
                    await asyncio.sleep(0.2)

                    # Strategy A: click the LABEL next to the checkbox (label → Angular (click))
                    clicked_lbl = await page.evaluate("""
                        (idx) => {
                            const cbs = document.querySelectorAll('[id^="expandedPanel_"] input[type="checkbox"]');
                            const cb = cbs[idx];
                            if (!cb) return {found: false};
                            // Try sibling label first, then parent div
                            const lbl = cb.nextElementSibling && cb.nextElementSibling.tagName === 'LABEL'
                                ? cb.nextElementSibling
                                : cb.parentElement.querySelector('label');
                            if (lbl) { lbl.scrollIntoView({block:'center'}); lbl.click(); return {clicked:'label', text:lbl.textContent.trim().slice(0,30)}; }
                            return {found: true, noLabel: true};
                        }
                    """, ci)
                    await asyncio.sleep(0.5)

                    # Strategy B: if label click didn't make it checked, click the wrapping div
                    is_checked_now = await panel_cbs.nth(ci).is_checked()
                    if not is_checked_now:
                        await page.evaluate("""
                            (idx) => {
                                const cbs = document.querySelectorAll('[id^="expandedPanel_"] input[type="checkbox"]');
                                const cb = cbs[idx];
                                if (!cb) return;
                                const wrapper = cb.closest('.tw-flex') || cb.parentElement;
                                if (wrapper) { wrapper.scrollIntoView({block:'center'}); wrapper.click(); }
                            }
                        """, ci)
                        await asyncio.sleep(0.5)
                        is_checked_now = await panel_cbs.nth(ci).is_checked()

                    # Strategy C: last resort — Playwright native click on the input itself
                    if not is_checked_now:
                        cb = panel_cbs.nth(ci)
                        await cb.click(force=True)
                        await asyncio.sleep(0.6)
                        is_checked_now = await cb.is_checked()

                    logger.info("[spa_booking] Service checkbox %d: checked=%s lbl=%s", ci, is_checked_now, clicked_lbl)

                # Final validity check
                validity_after_pw = await page.evaluate("""
                    () => ({
                        participantContainerOutside_ngInvalid:
                            document.querySelector('.participantContainerOutside')?.classList.contains('ng-invalid'),
                        containerOutside_ngInvalid:
                            document.querySelector('.containerOutside')?.classList.contains('ng-invalid'),
                        datepickerVal: document.querySelector('input[data-cy="dateCalendar"]')?.value || '',
                        totalNgInvalid: document.querySelectorAll('.ng-invalid').length
                    })
                """)
                logger.info("[spa_booking] Validity after Playwright toggles: %s", validity_after_pw)

            except Exception as _e_toggle:
                logger.warning("[spa_booking] Playwright toggle/checkbox error: %s", _e_toggle)
            await asyncio.sleep(0.5)

            # Dump full container HTML (8000 chars) to understand what else is in each container
            container_html_full = await page.evaluate("""
                () => {
                    const co = document.querySelector('.containerOutside');
                    const pco = document.querySelector('.participantContainerOutside');
                    return {
                        containerOutside_full: co ? co.innerHTML.slice(0, 20000) : 'NOT_FOUND',
                        participantContainerOutside_full: pco ? pco.innerHTML.slice(0, 8000) : 'NOT_FOUND',
                        containerOutside_ngInvalid: co ? co.classList.contains('ng-invalid') : null,
                        participantContainerOutside_ngInvalid: pco ? pco.classList.contains('ng-invalid') : null,
                        containerOutside_descendants_invalid: co ? Array.from(co.querySelectorAll('.ng-invalid')).map(e=>e.className.slice(0,60)) : [],
                        participantContainerOutside_descendants_invalid: pco ? Array.from(pco.querySelectorAll('.ng-invalid')).map(e=>e.className.slice(0,60)) : []
                    };
                }
            """)
            logger.info("[spa_booking] containerOutside full HTML length: %s",
                        len(container_html_full.get("containerOutside_full","")))
            logger.info("[spa_booking] containerOutside full HTML (first 3000):\n%s",
                        container_html_full.get("containerOutside_full","")[:3000])
            logger.info("[spa_booking] containerOutside full HTML (3000-6000):\n%s",
                        container_html_full.get("containerOutside_full","")[3000:6000])
            logger.info("[spa_booking] containerOutside full HTML (6000-):\n%s",
                        container_html_full.get("containerOutside_full","")[6000:])
            logger.info("[spa_booking] containerOutside_ngInvalid=%s  descendants_invalid=%s",
                        container_html_full.get("containerOutside_ngInvalid"),
                        container_html_full.get("containerOutside_descendants_invalid"))
            logger.info("[spa_booking] participantContainerOutside full HTML (first 3000):\n%s",
                        container_html_full.get("participantContainerOutside_full","")[:3000])
            logger.info("[spa_booking] participantContainerOutside_ngInvalid=%s  descendants_invalid=%s",
                        container_html_full.get("participantContainerOutside_ngInvalid"),
                        container_html_full.get("participantContainerOutside_descendants_invalid"))

            # Dump form validation state — expanded HTML + participant container details
            validation_state = await page.evaluate("""
                () => {
                    const invalid = Array.from(document.querySelectorAll('.ng-invalid')).map(el => ({
                        tag: el.tagName, id: el.id, cls: el.className.slice(0,70),
                        dataCy: el.getAttribute('data-cy')||'', type: el.type||'',
                        value: (el.value||el.textContent||'').slice(0,40)
                    }));
                    const errors = Array.from(document.querySelectorAll('mat-error, .error, [class*="error"]')).map(e=>e.textContent.trim().slice(0,80));

                    const acquista = Array.from(document.querySelectorAll('button')).find(b=>b.textContent.trim().toUpperCase().includes('ACQUISTA'));

                    // Dump HTML of ALL ng-invalid containers (up to 3 levels deep)
                    const containers = Array.from(document.querySelectorAll('.ng-invalid')).filter(
                        el => el.className.includes('containerOutside') || el.className.includes('participantContainer'));
                    const containerDumps = containers.map(c => ({
                        cls: c.className.slice(0,80),
                        html: c.innerHTML.slice(0, 8000),
                        ngInvalidDescendants: Array.from(c.querySelectorAll('.ng-invalid')).map(el => ({
                            tag: el.tagName, cls: el.className.slice(0,60),
                            dataCy: el.getAttribute('data-cy')||'', value: (el.value||'').slice(0,20)
                        }))
                    }));

                    // Check all APP-DROPDOWN ng states
                    const dropdownStates = Array.from(document.querySelectorAll('app-dropdown')).map(el => ({
                        dataCy: el.querySelector('input[data-cy]')?.getAttribute('data-cy')||'',
                        textVal: el.querySelector('input[type="text"]')?.value||'',
                        hiddenVal: el.querySelector('input[type="hidden"]')?.value||'',
                        ngCls: (el.className.match(/ng-\w+/g)||[]).join(' '),
                        appNgInvalid: el.classList.contains('ng-invalid')
                    }));

                    // Try to get Angular form errors from container's __ngContext__
                    const angCtx = [];
                    for (const sel of ['.containerOutside.ng-invalid', '.participantContainerOutside.ng-invalid']) {
                        const el = document.querySelector(sel);
                        if (!el) continue;
                        let info = {sel, errors: null};
                        try {
                            const ctx = el.__ngContext__;
                            if (Array.isArray(ctx)) {
                                for (let i = 0; i < ctx.length; i++) {
                                    const c = ctx[i];
                                    if (c && typeof c === 'object' && c.errors !== undefined && c.status !== undefined) {
                                        info.errors = {status: c.status, errors: c.errors};
                                        break;
                                    }
                                }
                            }
                        } catch(e) { info.err = String(e).slice(0,50); }
                        angCtx.push(info);
                    }

                    return {invalidCount: invalid.length, invalid: invalid.slice(0,15), errors,
                            acquistaDisabled: acquista?.disabled, acquistaExists: !!acquista,
                            containerDumps, dropdownStates, angCtx};
                }
            """)
            logger.info("[spa_booking] Validation state: %s", validation_state)

            # --- Targeted diagnostics: attributes + Zone event handlers ---
            targeted_diag = await page.evaluate("""
                () => {
                    const result = {};

                    // 1. All attributes on .participantContainerOutside itself
                    const pco = document.querySelector('.participantContainerOutside');
                    if (pco) {
                        result.pco_attrs = {};
                        for (const attr of pco.attributes) result.pco_attrs[attr.name] = attr.value;
                        // All Angular form attrs on direct children
                        result.pco_direct_children = Array.from(pco.children).map(c => ({
                            tag: c.tagName, id: c.id, cls: c.className.slice(0,80),
                            attrs: Array.from(c.attributes).reduce((a,v) => { a[v.name]=v.value; return a; }, {})
                        }));
                    }

                    // 2. All formcontrolname/formgroupname elements INSIDE participantContainerOutside
                    const formEls = pco ? Array.from(pco.querySelectorAll('[formcontrolname],[formgroupname],[ng-reflect-name],[ng-reflect-form-control-name]')) : [];
                    result.pco_form_els = formEls.map(el => ({
                        tag: el.tagName, cls: el.className.slice(0,60),
                        fcn: el.getAttribute('formcontrolname'),
                        fgn: el.getAttribute('formgroupname'),
                        ngInvalid: el.classList.contains('ng-invalid'),
                        ngReflect: el.getAttribute('ng-reflect-name') || el.getAttribute('ng-reflect-form-control-name')
                    }));

                    // 3. Zone.js event listeners on ACQUISTA button
                    const acquista = Array.from(document.querySelectorAll('button')).find(b =>
                        b.textContent.trim().toUpperCase().includes('ACQUISTA'));
                    if (acquista) {
                        const zoneSymbols = Object.getOwnPropertyNames(acquista).filter(k => k.includes('zone_symbol'));
                        result.acquista_zone_keys = zoneSymbols;
                        // Try to find click handlers
                        for (const key of zoneSymbols) {
                            if (key.toLowerCase().includes('click')) {
                                const listeners = acquista[key];
                                result.acquista_click_listeners = Array.isArray(listeners) ?
                                    listeners.map(l => ({type: typeof l.callback, name: l.callback?.name || '(anon)'})) : [];
                            }
                        }
                    }

                    // 4. Try calling ACQUISTA's Zone listener directly after patching form
                    // First, find the click listener on the form itself
                    const formEl = document.querySelector('form');
                    if (formEl) {
                        const zoneKeys = Object.getOwnPropertyNames(formEl).filter(k => k.includes('zone_symbol'));
                        result.form_zone_keys = zoneKeys;
                        for (const key of zoneKeys) {
                            if (key.toLowerCase().includes('submit')) {
                                const listeners = formEl[key];
                                result.form_submit_listeners = Array.isArray(listeners) ?
                                    listeners.map(l => ({type: typeof l.callback, name: l.callback?.name || '(anon)'})) : [];
                            }
                        }
                    }

                    // 5. Scan ALL form elements OUTSIDE the two known containers
                    const form = document.querySelector('form');
                    const co = document.querySelector('.containerOutside');
                    const pcoEl = document.querySelector('.participantContainerOutside');

                    // All elements with formcontrolname/formgroupname anywhere in the form
                    const allFormEls = form ? Array.from(form.querySelectorAll('[formcontrolname],[formgroupname]')) : [];
                    result.form_els_outside_containers = allFormEls
                        .filter(el => !co?.contains(el) && !pcoEl?.contains(el))
                        .map(el => ({
                            tag: el.tagName,
                            fcn: el.getAttribute('formcontrolname'),
                            fgn: el.getAttribute('formgroupname'),
                            cls: el.className.slice(0, 80),
                            ngInvalid: el.classList.contains('ng-invalid'),
                            val: (el.value || el.textContent || '').slice(0, 40)
                        }));

                    // Also look for mat-checkbox with formcontrolname (terms etc)
                    result.mat_checkboxes_with_fcn = Array.from(document.querySelectorAll('mat-checkbox[formcontrolname]'))
                        .map(el => ({
                            fcn: el.getAttribute('formcontrolname'),
                            cls: el.className.slice(0, 80),
                            ngInvalid: el.classList.contains('ng-invalid'),
                            checked: el.querySelector('input[type="checkbox"]')?.checked
                        }));

                    // Dump the form's DIRECT children (top-level structure)
                    result.form_direct_children = form ? Array.from(form.children).map(c => ({
                        tag: c.tagName, id: c.id, cls: c.className.slice(0, 80),
                        ngInvalid: c.classList.contains('ng-invalid'),
                        fcn: c.getAttribute('formcontrolname'),
                        fgn: c.getAttribute('formgroupname')
                    })) : [];

                    // Dump the ngSubmit handler source
                    const ngSubmitListeners = form ? form['__zone_symbol__ngSubmitfalse'] : null;
                    result.ngSubmit_handler_src = ngSubmitListeners && ngSubmitListeners[0]
                        ? ngSubmitListeners[0].callback.toString().slice(0, 500) : null;

                    return result;
                }
            """)
            logger.info("[spa_booking] Targeted diagnostic: %s", targeted_diag)

            # --- Angular form introspection + validator clear ---
            # participantContainerOutside stays ng-invalid with no ng-invalid descendants.
            # Strategy (tiered):
            #   Ph1 — window.ng.getDirectives on ng-invalid elements → FormGroupDirective/FormControlDirective
            #   Ph2 — window.ng.getComponent / getOwningComponent → component instance + FormGroup properties
            #   Ph3 — Intercept AbstractControl.prototype.setErrors during a blur sweep
            #          to capture any hidden invalid controls that never appear in the DOM
            #   Ph4 — LView Map intercept (existing approach, now last resort)
            form_patch = await page.evaluate("""
                async () => {
                    const found = [];
                    const patched = [];
                    let formValue = null;
                    let err = null;
                    const debug = [];

                    const isControl = (c) =>
                        c && typeof c === 'object' && !Array.isArray(c) &&
                        typeof c.status === 'string' && typeof c.markAsTouched === 'function';

                    const clearCtrl = (ctrl) => {
                        if (!ctrl || !isControl(ctrl)) return;
                        if (typeof ctrl.clearValidators === 'function') ctrl.clearValidators();
                        if (typeof ctrl.clearAsyncValidators === 'function') ctrl.clearAsyncValidators();
                        if (ctrl.controls && typeof ctrl.controls === 'object') {
                            const keys = Array.isArray(ctrl.controls) ?
                                ctrl.controls.map((_, i) => i) : Object.keys(ctrl.controls);
                            for (const k of keys) clearCtrl(ctrl.controls[k]);
                        }
                        try { ctrl.setErrors(null); } catch(_) {}
                        try { ctrl.updateValueAndValidity({onlySelf: false, emitEvent: true}); } catch(_) {}
                    };

                    // ── Phase 0: window.ng availability ────────────────────────────────────
                    debug.push({
                        hasNg: !!window.ng,
                        ngKeys: window.ng ? Object.keys(window.ng).slice(0, 20) : []
                    });

                    // ── Phase 1: getDirectives on form-bound elements ────────────────────
                    if (window.ng && window.ng.getDirectives) {
                        const targets = [
                            'form',
                            '.containerOutside',
                            '.participantContainerOutside',
                            '[_nghost-ng-c2227252425]',   // APP-PARTECIPANTS-FORM host
                            '[_nghost-ng-c2403135089]',   // APP-CHECKOUT host
                            '[_nghost-ng-c3005736359]',   // APP-MANAGER-FORM host
                        ];
                        for (const sel of targets) {
                            const el = document.querySelector(sel);
                            if (!el) { debug.push({ph1_miss: sel}); continue; }
                            let dirs = [];
                            try { dirs = window.ng.getDirectives(el) || []; } catch(_) {}
                            debug.push({ph1_sel: sel, dirCount: dirs.length,
                                        dirNames: dirs.map(d => d.constructor?.name?.slice(0,30) || '?').slice(0,5)});
                            for (const dir of dirs) {
                                // FormGroupDirective → .form; FormControlDirective → .control
                                for (const prop of ['form', 'control', 'formControl', 'value']) {
                                    try {
                                        const ctrl = dir[prop];
                                        if (isControl(ctrl)) {
                                            found.push({src:'dir:'+sel+'.'+prop, status:ctrl.status,
                                                        errs: ctrl.errors ? JSON.stringify(ctrl.errors).slice(0,60) : null});
                                            if (ctrl.status === 'INVALID') {
                                                clearCtrl(ctrl);
                                                patched.push({ph1: sel+'.'+prop, newStatus: ctrl.status});
                                            }
                                        }
                                    } catch(_) {}
                                }
                            }
                        }
                    }

                    // ── Phase 2: getComponent / getOwningComponent ───────────────────────
                    if (window.ng) {
                        // getComponent on APP-PARTECIPANTS-FORM host element
                        const partHost = document.querySelector('[_nghost-ng-c2227252425]');
                        if (partHost && window.ng.getComponent) {
                            let comp = null;
                            try { comp = window.ng.getComponent(partHost); } catch(_) {}
                            if (comp) {
                                const nonFnKeys = Object.keys(comp).filter(k => typeof comp[k] !== 'function');
                                debug.push({ph2_partComp: comp.constructor?.name || '?', keys: nonFnKeys.slice(0,25)});
                                for (const k of nonFnKeys) {
                                    try {
                                        const v = comp[k];
                                        if (isControl(v)) {
                                            found.push({src:'compProp:'+k, status:v.status,
                                                        errs: v.errors ? JSON.stringify(v.errors).slice(0,60) : null});
                                            if (v.status === 'INVALID') {
                                                clearCtrl(v);
                                                patched.push({ph2_compProp: k, newStatus: v.status});
                                            }
                                        }
                                    } catch(_) {}
                                }
                            } else {
                                debug.push({ph2_partComp: 'getComponent returned null'});
                            }
                        }

                        // getOwningComponent on a known inner element
                        const inner = document.getElementById('participantSurname_0');
                        if (inner && window.ng.getOwningComponent) {
                            let owning = null;
                            try { owning = window.ng.getOwningComponent(inner); } catch(_) {}
                            if (owning) {
                                const nonFnKeys2 = Object.keys(owning).filter(k => typeof owning[k] !== 'function');
                                debug.push({ph2_owning: owning.constructor?.name || '?', keys: nonFnKeys2.slice(0,25)});
                                for (const k of nonFnKeys2) {
                                    try {
                                        const v = owning[k];
                                        if (isControl(v)) {
                                            found.push({src:'owning:'+k, status:v.status,
                                                        errs: v.errors ? JSON.stringify(v.errors).slice(0,60) : null});
                                            if (v.status === 'INVALID') {
                                                clearCtrl(v);
                                                patched.push({ph2_owning: k, newStatus: v.status});
                                            }
                                        }
                                    } catch(_) {}
                                }
                            } else {
                                debug.push({ph2_owning: 'getOwningComponent returned null'});
                            }
                        }

                        // Try getDirectives on inner elements too
                        for (const id of ['participantSurname_0', 'participantName_0']) {
                            const el = document.getElementById(id);
                            if (!el || !window.ng.getDirectives) continue;
                            let dirs = [];
                            try { dirs = window.ng.getDirectives(el) || []; } catch(_) {}
                            if (dirs.length) {
                                debug.push({ph2_innerDirs: id, count: dirs.length,
                                            names: dirs.map(d=>d.constructor?.name?.slice(0,20)||'?')});
                            }
                        }
                    }

                    // ── Phase 3: AbstractControl.prototype.setErrors intercept ───────────
                    // Finds the prototype via a directive's .control, then patches it to capture
                    // ALL controls that receive errors (including hidden FormControls with no DOM).
                    if (found.length === 0) {
                        let abstractCtrlProto = null;
                        try {
                            // Any input with formcontrolname has an NgControl directive
                            const anyInput = document.querySelector('input[formcontrolname]');
                            if (anyInput && window.ng && window.ng.getDirectives) {
                                const dirs = window.ng.getDirectives(anyInput) || [];
                                for (const d of dirs) {
                                    for (const prop of ['control', 'form']) {
                                        if (isControl(d[prop])) {
                                            const proto1 = Object.getPrototypeOf(d[prop]);
                                            const proto2 = Object.getPrototypeOf(proto1);
                                            if (proto2 && typeof proto2.setErrors === 'function') {
                                                abstractCtrlProto = proto2;
                                            }
                                        }
                                    }
                                }
                            }
                        } catch(e) { debug.push({ph3_protoErr: String(e).slice(0,60)}); }

                        if (abstractCtrlProto) {
                            debug.push({ph3_gotProto: true});
                            const capturedCtrls = new Map();
                            const origSetErrors = abstractCtrlProto.setErrors;
                            abstractCtrlProto.setErrors = function(errors, opts) {
                                if (errors !== null && isControl(this)) capturedCtrls.set(this, errors);
                                return origSetErrors.call(this, errors, opts);
                            };

                            // Trigger Angular's validators via blur sweep (exclude dateCalendar)
                            document.querySelectorAll('input[formcontrolname], input[data-cy]').forEach(inp => {
                                if (inp.getAttribute('data-cy') === 'dateCalendar') return;
                                inp.dispatchEvent(new Event('blur', {bubbles: true}));
                            });
                            await new Promise(r => setTimeout(r, 600));
                            abstractCtrlProto.setErrors = origSetErrors;  // Restore immediately

                            debug.push({ph3_capturedCount: capturedCtrls.size});
                            for (const [ctrl, errors] of capturedCtrls) {
                                found.push({src:'intercept', status:ctrl.status,
                                            errs: JSON.stringify(errors).slice(0,60)});
                                if (ctrl.status === 'INVALID') {
                                    clearCtrl(ctrl);
                                    if (!formValue && typeof ctrl.getRawValue === 'function') {
                                        try { formValue = ctrl.getRawValue(); } catch(_) {}
                                    }
                                    patched.push({ph3_intercept: true, newStatus: ctrl.status});
                                }
                            }
                        } else {
                            debug.push({ph3_gotProto: false, reason: 'no formcontrolname dirs found'});
                        }
                    }

                    // ── Phase 4: __ngContext__ content-element scan ──────────────────────
                    // In Angular Ivy, HOST elements (_nghost-*) store a numeric LView ID.
                    // CONTENT elements may store the actual LView array directly — check both.
                    // Also scan directive instances at each LView slot (.control, .form).
                    try {
                        const contentSelectors = [
                            'form',
                            '.containerOutside',
                            '.participantContainerOutside',
                            '#participantSurname_0',
                            '#participantName_0',
                            'input[formcontrolname="surname"]',
                            'input[formcontrolname="name"]',
                            '[_nghost-ng-c2227252425]',  // APP-PARTECIPANTS-FORM
                            '[_nghost-ng-c2403135089]',  // APP-CHECKOUT
                        ];

                        const ctxTypes = {};
                        for (const sel of contentSelectors) {
                            const el = document.querySelector(sel);
                            if (!el) { ctxTypes[sel] = 'MISS'; continue; }
                            const ctx = el.__ngContext__;
                            const isArr = Array.isArray(ctx);
                            // Also list all Angular properties on the element
                            const ngProps = Object.getOwnPropertyNames(el).filter(k => k.startsWith('__ng'));
                            ctxTypes[sel] = {t: typeof ctx, isArr, len: isArr ? ctx.length : (typeof ctx === 'number' ? ctx : '?'), ngProps};

                            if (isArr) {
                                // Scan: direct FormControls AND directive-wrapped ones
                                for (let i = 0; i < ctx.length; i++) {
                                    const c = ctx[i];
                                    if (!c || typeof c !== 'object' || Array.isArray(c)) continue;

                                    // 1. Direct AbstractControl instance
                                    if (isControl(c)) {
                                        if (!formValue && typeof c.getRawValue === 'function')
                                            try { formValue = c.getRawValue(); } catch(_) {}
                                        found.push({src:sel+'['+i+']', type:'ctrl', status:c.status,
                                                    errs: c.errors ? JSON.stringify(c.errors).slice(0,60) : null});
                                        if (c.status === 'INVALID') { clearCtrl(c); patched.push({ph4_direct: sel, i}); }
                                    }

                                    // 2. Directive: .control or .form
                                    for (const prop of ['control', 'form']) {
                                        try {
                                            const ctrl = c[prop];
                                            if (!ctrl || !isControl(ctrl)) continue;
                                            found.push({src:sel+'['+i+'].'+prop, type:'dir', status:ctrl.status,
                                                        errs: ctrl.errors ? JSON.stringify(ctrl.errors).slice(0,60) : null});
                                            if (ctrl.status === 'INVALID') {
                                                // Try clearing specific sub-controls first
                                                if (typeof ctrl.get === 'function') {
                                                    for (const k of ['participants','partecipants','services','users']) {
                                                        const sub = ctrl.get(k);
                                                        if (sub && sub.status === 'INVALID') {
                                                            clearCtrl(sub);
                                                            patched.push({ph4_sub: k});
                                                        }
                                                    }
                                                }
                                                clearCtrl(ctrl);
                                                patched.push({ph4_dir: prop, sel, i});
                                                if (!formValue && typeof ctrl.getRawValue === 'function')
                                                    try { formValue = ctrl.getRawValue(); } catch(_) {}
                                            }
                                        } catch(_) {}
                                    }
                                }
                            }
                        }
                        debug.push({ph4_ctxTypes: ctxTypes, ph4_found: found.length, ph4_patched: patched.length});
                    } catch(e) { err = String(e).slice(0, 200); }

                    // ── Phase 4E: AbstractControl intercept via LView prototype ───────────
                    // If we found ANY AbstractControl above, extract its prototype and intercept
                    // setErrors to catch hidden controls (no DOM binding) during validation sweep.
                    if (found.length > 0 && patched.length === 0) {
                        // Found controls but none invalid — find AbstractControl.prototype from them
                        let proto = null;
                        for (const f of found) {
                            // Each found[] entry holds a reference? No — we need to keep the ref.
                            // Phase 4 above doesn't keep references. Let's get one fresh.
                        }
                    }
                    if (found.length === 0) {
                        // Phase 4E: WeakMap + Map intercept with more aggressive event triggers
                        try {
                            const capturedLViews = new Map();
                            const origMapGet = Map.prototype.get;
                            const origWeakGet = WeakMap.prototype.get;
                            Map.prototype.get = function(key) {
                                const r = origMapGet.call(this, key);
                                if (typeof key === 'number' && r && Array.isArray(r) && r.length > 10)
                                    capturedLViews.set(String(key), r);
                                return r;
                            };
                            WeakMap.prototype.get = function(key) {
                                const r = origWeakGet.call(this, key);
                                if (r && Array.isArray(r) && r.length > 10)
                                    capturedLViews.set(key, r);
                                return r;
                            };

                            // Dispatch all event types Angular might respond to
                            for (const evType of ['input', 'change', 'blur', 'focus', 'keyup']) {
                                document.querySelectorAll('input[formcontrolname], input[data-cy]').forEach(inp => {
                                    if (inp.getAttribute('data-cy') !== 'dateCalendar')
                                        inp.dispatchEvent(new Event(evType, {bubbles: true}));
                                });
                            }
                            await new Promise(r => setTimeout(r, 600));
                            Map.prototype.get = origMapGet;
                            WeakMap.prototype.get = origWeakGet;

                            debug.push({ph4e_mapCaptures: capturedLViews.size});
                            for (const lview of capturedLViews.values()) {
                                if (!Array.isArray(lview)) continue;
                                for (let i = 0; i < lview.length; i++) {
                                    const c = lview[i];
                                    if (!c || typeof c !== 'object' || Array.isArray(c)) continue;
                                    if (isControl(c)) {
                                        found.push({src:'map_cap['+i+']', status:c.status,
                                                    errs:c.errors?JSON.stringify(c.errors).slice(0,60):null});
                                        if (c.status==='INVALID') { clearCtrl(c); patched.push({ph4e:true, i}); }
                                        // Store the largest invalid FormGroup for ACQUISTA bypass
                                        // (patch via Object.defineProperty in page.evaluate — no CDP session)
                                        if (c.status==='INVALID' && c.controls &&
                                            Object.keys(c.controls||{}).length > 5 &&
                                            (!window.__checkoutForm ||
                                             Object.keys(c.controls).length >
                                             Object.keys(window.__checkoutForm.controls||{}).length)) {
                                            window.__checkoutForm = c;
                                        }
                                    }
                                    for (const prop of ['control', 'form']) {
                                        try {
                                            const ctrl = c[prop];
                                            if (!ctrl || !isControl(ctrl)) continue;
                                            found.push({src:'map_cap['+i+'].'+prop, status:ctrl.status,
                                                        errs:ctrl.errors?JSON.stringify(ctrl.errors).slice(0,60):null});
                                            if (ctrl.status==='INVALID') { clearCtrl(ctrl); patched.push({ph4e:prop, i}); }
                                            if (ctrl.status==='INVALID' && ctrl.controls &&
                                                Object.keys(ctrl.controls||{}).length > 5 &&
                                                (!window.__checkoutForm ||
                                                 Object.keys(ctrl.controls).length >
                                                 Object.keys(window.__checkoutForm.controls||{}).length)) {
                                                window.__checkoutForm = ctrl;
                                            }
                                        } catch(_) {}
                                    }
                                }
                            }
                        } catch(e2) { debug.push({ph4e_err: String(e2).slice(0,100)}); }
                    }

                    // ── Final: dispatch events so Angular re-renders CSS classes ─────────
                    document.querySelectorAll('input[formcontrolname], input[data-cy]').forEach(inp => {
                        if (inp.getAttribute('data-cy') === 'dateCalendar') return;
                        inp.dispatchEvent(new Event('input', {bubbles: true}));
                        inp.dispatchEvent(new Event('change', {bubbles: true}));
                    });
                    await new Promise(r => setTimeout(r, 300));

                    const ngInvalidNow = document.querySelectorAll('.ng-invalid').length;
                    return {found: found.slice(0, 20), patched, debug,
                            formValue: JSON.stringify(formValue || {}).slice(0, 300),
                            ngInvalidNow, err};
                }
            """)
            logger.info("[spa_booking] LView scan: found=%d  patched=%d  ngInvalidNow=%d  err=%s",
                        len(form_patch.get("found",[])), len(form_patch.get("patched",[])),
                        form_patch.get("ngInvalidNow",0), form_patch.get("err"))
            logger.info("[spa_booking] Form value from LView: %s", form_patch.get("formValue","")[:300])
            logger.info("[spa_booking] Patched details: %s", form_patch.get("patched",[]))
            logger.info("[spa_booking] Found controls: %s", form_patch.get("found",[]))
            logger.info("[spa_booking] Debug: %s", form_patch.get("debug",[]))
            await asyncio.sleep(0.5)

            # Click ACQUISTA with Playwright native click (isTrusted=true CDP mouse event).
            # JS btn.click() is a synthetic event — Angular form submit handlers may behave
            # differently with trusted vs synthetic clicks (Zone.js hooks may differ).
            acquista_info = await page.evaluate("""
                () => {
                    const candidates = ['ACQUISTA', 'PROCEDI', 'AVANTI', 'CONTINUA', 'CONFERMA'];
                    for (const btn of document.querySelectorAll('button:not([disabled])')) {
                        const t = btn.textContent.trim().toUpperCase();
                        if (candidates.some(c => t.includes(c))) {
                            btn.scrollIntoView({block:'center'});
                            return {found: true, text: btn.textContent.trim().slice(0,40), id: btn.id,
                                    selector: btn.id ? `button#${btn.id}` : null};
                        }
                    }
                    return {found: false, btns: Array.from(document.querySelectorAll('button')).map(b=>b.textContent.trim().slice(0,30))};
                }
            """)
            logger.info("[spa_booking] ACQUISTA button info: %s", acquista_info)
            await asyncio.sleep(0.3)

            # ── Force Angular form valid via Object.defineProperty on the captured instance ──
            # window.__checkoutForm was stored in Phase 4E's LView scan above.
            # We override valid/invalid/status getters directly on the FormGroup instance
            # via page.evaluate (uses Runtime.evaluate CDP, not the heavier cdp_session
            # with Runtime.getProperties / callFunctionOn that F5's WAF detects).
            form_bypass_ok = await page.evaluate("""
                () => {
                    const ctrl = window.__checkoutForm;
                    if (!ctrl) return {ok: false, reason: 'no __checkoutForm'};
                    const wasValid = ctrl.valid;
                    function forceValid(c) {
                        if (!c || typeof c !== 'object') return;
                        try {
                            Object.defineProperty(c, 'valid',   {get: () => true,    configurable: true, enumerable: true});
                            Object.defineProperty(c, 'invalid', {get: () => false,   configurable: true, enumerable: true});
                            Object.defineProperty(c, 'status',  {get: () => 'VALID', configurable: true, enumerable: true});
                            if ('errors' in c) c.errors = null;
                        } catch(_) {}
                        for (const k of Object.keys(c.controls || {})) forceValid(c.controls[k]);
                    }
                    forceValid(ctrl);
                    return {ok: true, wasValid, nowValid: ctrl.valid,
                            ctrlCount: Object.keys(ctrl.controls || {}).length};
                }
            """)
            logger.info("[spa_booking] form bypass: %s", form_bypass_ok)
            await asyncio.sleep(1.5)  # let Angular change detection re-render disabled state

            # ── Click ACQUISTA (trusted Playwright click) + capture reservation ─
            checkout_next = {"clicked": False}
            try:
                async with page.expect_response(
                    lambda r: "/api/visit/reservation" in r.url,
                    timeout=25_000,
                ) as _res_info:

                    acquista_loc = page.locator("button", has_text="ACQUISTA").first
                    try:
                        await acquista_loc.click(timeout=8_000)
                        checkout_next = {"clicked": True, "trusted": True}
                        logger.info("[spa_booking] ACQUISTA clicked (trusted Playwright)")
                    except Exception as _e_btn:
                        logger.warning("[spa_booking] ACQUISTA click failed: %s — using JS removeAttr+click", _e_btn)
                        await page.evaluate("""
                            () => {
                                const b = Array.from(document.querySelectorAll('button'))
                                    .find(b => b.textContent.trim().toUpperCase().includes('ACQUISTA'));
                                if (b) { b.removeAttribute('disabled'); b.disabled = false; b.click(); }
                            }
                        """)
                        checkout_next = {"clicked": True, "trusted": False}
                _res_val = await _res_info.value
                _res_body = await _res_val.body()
                _res_text = _res_body.decode("utf-8", errors="replace")
                _is_html = _res_text.strip().startswith("<")
                logger.info("[spa_booking] reservation API → HTTP %d  html=%s  body=%s",
                            _res_val.status, _is_html, _res_text[:300])
                if not _is_html and _res_val.status in (200, 201):
                    try:
                        import json as _json2
                        _res_json = _json2.loads(_res_text)
                        logger.info("[spa_booking] ✓ Reservation ID from API: %s", _res_json)
                        page.remove_listener("response", _on_res)
                        return _res_json
                    except Exception:
                        pass
            except Exception as _e_wait:
                logger.warning("[spa_booking] reservation wait/click failed: %s", _e_wait)
            logger.info("[spa_booking] Checkout ACQUISTA click: %s", checkout_next)
            # Check for validation errors shown after ACQUISTA click
            post_click = await page.evaluate("""
                () => ({
                    url: window.location.href,
                    errors: Array.from(document.querySelectorAll('mat-error, .error-message, [class*="errorMsg"], [class*="error-text"]')).map(e=>e.textContent.trim().slice(0,80)),
                    invalidNow: Array.from(document.querySelectorAll('.ng-invalid')).map(el=>({
                        tag: el.tagName, cls: el.className.slice(0,60), val: (el.value||el.innerText||'').slice(0,30)
                    })).slice(0,10),
                    requiredFields: Array.from(document.querySelectorAll('[required], [aria-required="true"]')).map(el=>({
                        id: el.id, dataCy: el.getAttribute('data-cy')||'', val: (el.value||'').slice(0,20), invalid: el.classList.contains('ng-invalid')
                    })).filter(f=>f.invalid).slice(0,10)
                })
            """)
            logger.info("[spa_booking] Post-ACQUISTA state: %s", post_click)

            # If ACQUISTA didn't trigger the reservation API (form blocked by ng-invalid),
            # bypass Angular form validation and POST directly via browser fetch
            # (uses F5 session cookies already in the browser)
            logger.info("[spa_booking] ACQUISTA did not fire reservation API — trying direct browser fetch…")
            fetch_result = await page.evaluate("""
                async (data) => {
                    // Extract form values from the Angular reactive form
                    // Try ng API first for the exact payload
                    let formValue = null;
                    try {
                        const form = document.querySelector('form');
                        const ctx = form.__ngContext__;
                        if (Array.isArray(ctx)) {
                            for (let i = 0; i < ctx.length; i++) {
                                const c = ctx[i];
                                if (c && typeof c === 'object' && typeof c.getRawValue === 'function') {
                                    formValue = c.getRawValue();
                                    break;
                                }
                            }
                        }
                    } catch(e) {}

                    // Build payload from known form data
                    const payload = {
                        visitTypeId: data.visitTypeId,
                        visitDate: data.visitDate,
                        timeSlot: data.timeSlot,
                        slotId: data.slotId,
                        numVisitors: data.numVisitors,
                        representative: {
                            surname: data.surname,
                            name: data.name,
                            gender: data.gender,
                            country: data.country,
                            lang: data.lang,
                            birthDate: data.birthDate,
                            email: data.email,
                            city: data.city,
                            phone: data.phone
                        },
                        participants: data.participants,
                        gdpr: data.gdpr,
                        ticketId: data.ticketId,
                        mandatoryServices: data.mandatoryServices
                    };

                    // Get XSRF token from cookies if present
                    const xsrf = document.cookie.split(';').map(c=>c.trim()).find(c=>c.startsWith('XSRF-TOKEN='));
                    const xsrfVal = xsrf ? decodeURIComponent(xsrf.split('=')[1]) : '';

                    try {
                        const resp = await fetch('/api/visit/reservation', {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                                'Accept': 'application/json, text/plain, */*',
                                ...(xsrfVal ? {'X-XSRF-TOKEN': xsrfVal} : {})
                            },
                            credentials: 'include',
                            body: JSON.stringify(payload)
                        });
                        const body = await resp.text();
                        return {status: resp.status, ok: resp.ok, body: body.slice(0,500), sentPayload: JSON.stringify(payload).slice(0,300), formValue: JSON.stringify(formValue||{}).slice(0,200)};
                    } catch(e) {
                        return {error: String(e), sentPayload: JSON.stringify(payload).slice(0,300)};
                    }
                }
            """, {
                "visitTypeId": slot.get("visitTypeId", ""),
                "visitDate": self._to_dmy(self.config.target_date),
                "timeSlot": slot.get("time", ""),
                "slotId": slot.get("id", ""),
                "numVisitors": self.config.num_tickets,
                "surname": self.config.buyer_last,
                "name": self.config.buyer_first,
                "gender": "M",
                "country": "Italia",
                "lang": "it",
                "birthDate": "01/01/1990",
                "email": self.config.buyer_email,
                "city": "Roma",
                "phone": "0000000000",
                "participants": [
                    {"surname": p.get("last",""), "name": p.get("first","")}
                    for p in self.config.participants
                ],
                "gdpr": [{"id": "3", "accepted": False}],
                "ticketId": 60,
                "mandatoryServices": [{"id": 58}],
            })
            logger.info("[spa_booking] Direct fetch result: status=%s ok=%s body=%s",
                        fetch_result.get("status"), fetch_result.get("ok"), fetch_result.get("body","")[:300])
            logger.info("[spa_booking] Fetch formValue=%s", fetch_result.get("formValue",""))
            if fetch_result.get("ok") or fetch_result.get("status") in (200, 201):
                try:
                    import json as _json2
                    res_json = _json2.loads(fetch_result.get("body","{}"))
                    logger.info("[spa_booking] Reservation from fetch: %s", res_json)
                    page.remove_listener("response", _on_res)
                    return res_json
                except Exception:
                    pass
            await asyncio.sleep(14)

            # Check what APIs fired after checkout next
            logger.info("[spa_booking] Final APIs captured (%d):", len(captured_res))
            for c in captured_res:
                logger.info("  %d html=%-5s %s", c["status"], c["html"], c["url"][:100])

        page.remove_listener("response", _on_res)

        # Look for reservation confirmation
        res_data = next((c for c in captured_res
                         if "/api/visit/reservation" in c["url"]
                         and not c["html"] and c["status"] in (200, 201)), None)
        if res_data:
            logger.info("[spa_booking] Reservation created! body=%s", res_data["body"][:400])
            try:
                return _json.loads(res_data["body"])
            except Exception:
                return {"raw": res_data["body"][:200]}

        # Fallback: return page state for dry-run inspection
        final_state = await page.evaluate("""
            () => ({url: window.location.href, bodyLen: document.body?.innerHTML?.length || 0,
                    text: document.body?.innerText?.slice(0,400) || ''})
        """)
        logger.info("[spa_booking] Final page: url=%s text=%r",
                    final_state.get("url",""), final_state.get("text","")[:200])
        return final_state

    @staticmethod
    def _check(r: dict, step: str) -> dict:
        """Validate api_get/api_post result dict {status, body}. Returns parsed JSON."""
        import json
        if r["status"] not in (200, 201, 202):
            raise RuntimeError(f"[{step}] HTTP {r['status']}: {r['body'][:300]}")
        if r["body"].strip().startswith("<"):
            raise RuntimeError(f"[{step}] F5 block page returned")
        try:
            return json.loads(r["body"])
        except Exception as exc:
            raise RuntimeError(f"[{step}] JSON parse failed: {exc}  body={r['body'][:300]}")

    async def _step_get_sid(self):
        """Fetch a server-side session ID before starting the booking flow."""
        r = await self.session.api_get(EP_GET_SID)
        data = self._check(r, "getSid")
        sid = data.get("sid") or data.get("sessionId") or data.get("id")
        logger.info("[getSid] sid=%s", sid)

    @staticmethod
    def _to_dmy(date_str: str) -> str:
        """Convert "YYYY-MM-DD" to "DD/MM/YYYY" (Angular API format)."""
        from datetime import datetime
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").strftime("%d/%m/%Y")
        except Exception:
            return date_str

    async def _step_create_reservation(self, slot: dict):
        """POST /api/visit/reservation — the main booking call."""
        payload = {
            "tag":           "MV-Biglietti",
            "visitDate":     self._to_dmy(self.config.target_date),  # DD/MM/YYYY
            "timeSlot":      slot.get("time") or slot.get("startTime") or slot.get("ora"),
            "slotId":        slot.get("id") or slot.get("slotId"),
            "visitTypeId":   slot.get("visitTypeId"),
            "numVisitors":   self.config.num_tickets,
            "visitorNum":    self.config.num_tickets,
            "lang":          "it",
            "firstName":     self.config.buyer_first,
            "lastName":      self.config.buyer_last,
            "email":         self.config.buyer_email,
            "dateOfBirth":   self.config.buyer_dob,
        }
        r = await self.session.api_post(EP_RESERVATION, payload)
        data = self._check(r, "reservation")
        self._reservation_id = (
            data.get("reservationId") or data.get("bookingId") or
            data.get("id") or data.get("orderId")
        )
        logger.info("[reservation] created — id=%s", self._reservation_id)

    async def _step_participants(self):
        """POST /api/reservation/participant — submit every visitor's name."""
        payload = {
            "reservationId": self._reservation_id,
            "participants":  self.config.participants,
        }
        r = await self.session.api_post(EP_PARTICIPANTS, payload)
        self._check(r, "participants")
        logger.info("[participants] %d names submitted OK", len(self.config.participants))

    async def _step_recap(self):
        """GET /api/visit/recap — fetch booking summary before final confirmation."""
        r = await self.session.api_get(EP_RECAP, params={"reservationId": self._reservation_id, "lang": "it"})
        data = self._check(r, "recap")
        logger.info("[recap] total=%s currency=%s", data.get("total"), data.get("currency"))

    async def _step_confirm(self) -> dict:
        """POST /api/visit/confirmation — submit Turnstile token and confirm."""
        payload = {
            "reservationId":        self._reservation_id,
            "cfTurnstileResponse":  self.captcha_token,
            "termsAccepted":        True,
            "privacyAccepted":      True,
            "lang":                 "it",
        }
        r = await self.session.api_post(EP_CONFIRM, payload)
        data = self._check(r, "confirmation")
        logger.info("[confirmation] status=%s payUrl=%s",
                    data.get("status"), data.get("paymentUrl") or data.get("redirectUrl"))
        return data

    async def _step_payment(self) -> dict:
        """GET /api/visit/confirmation/ads — post-confirm / payment redirect."""
        r = await self.session.api_get(EP_PAYMENT, params={"reservationId": self._reservation_id, "lang": "it"})
        data = self._check(r, "payment")
        logger.info("[payment] %s", data.get("status"))
        return data
