"""
F5 TSPD challenge solver.

The F5 bot-defense layer at tickets.museivaticani.va rejects plain HTTP clients
with a 245-byte block page. Browser-like requests receive an obfuscated JS
challenge that fingerprints the client (~200 attributes) and beacons telemetry
to re.security.f5aas.com before setting TS-prefixed session cookies.

Strategy: run Playwright once to solve the challenge and harvest the TS cookies,
then replay them in lightweight httpx calls for the rest of the funnel.
"""

import asyncio
import logging
from typing import Optional, Any

from playwright.async_api import async_playwright, BrowserContext, Page, APIResponse

from config import BASE_URL, Config
from proxy import ProxyManager

logger = logging.getLogger(__name__)

# Patches injected before page scripts run — suppress automation signals
STEALTH_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
window.chrome = {runtime: {}};
const origQuery = window.navigator.permissions.query;
window.navigator.permissions.query = (params) =>
    params.name === 'notifications'
        ? Promise.resolve({state: Notification.permission})
        : origQuery(params);
"""

# Intercepts Angular's AbstractControl.valid/status/invalid getters AT definition time.
# We patch Object.defineProperty so that when Angular's bundle runs and defines these
# getters on AbstractControl.prototype, our wrapped versions check window.__forceAngularFormValid
# first. Setting that flag to true makes checkoutForm.valid return true, bypassing the
# orphaned FormControl that keeps the form ng-invalid with no DOM representation.
ANGULAR_FORM_PATCH_SCRIPT = """
(function() {
    var origDefProp = Object.defineProperty;
    var patched = [];  // [{target, prop}]

    function alreadyPatched(target, prop) {
        for (var i = 0; i < patched.length; i++) {
            if (patched[i].target === target && patched[i].prop === prop) return true;
        }
        return false;
    }

    var patchedFn = function(target, prop, descriptor) {
        // Only intercept 'valid' getter definitions on non-built-in prototypes.
        // Intentionally NOT intercepting 'status' or 'invalid' to avoid breaking
        // Angular's router (ActivatedRoute.status), HttpClient response status, etc.
        if (prop === 'valid'
            && descriptor && typeof descriptor.get === 'function'
            && !descriptor.set
            && target !== Object.prototype
            && target !== Function.prototype
            && !alreadyPatched(target, prop)) {

            patched.push({target: target, prop: prop});
            var origGetter = descriptor.get;
            var patchedDesc = {
                get: (function(og) {
                    return function() {
                        if (window.__forceAngularFormValid === true) return true;
                        return og.call(this);
                    };
                })(origGetter),
                configurable: true,
                enumerable: !!descriptor.enumerable
            };
            return origDefProp.call(this, target, prop, patchedDesc);
        }
        return origDefProp.call(this, target, prop, descriptor);
    };

    // Spoof toString so F5's fingerprint check sees a native function, not our wrapper
    var origFnToStr = Function.prototype.toString;
    origDefProp(Function.prototype, 'toString', {
        value: function nativeToString() {
            if (this === patchedFn || this === Object.defineProperty) {
                return 'function defineProperty() { [native code] }';
            }
            return origFnToStr.call(this);
        },
        writable: true,
        configurable: true,
        enumerable: false
    });

    Object.defineProperty = patchedFn;

    window.__forceAngularFormValid = false;
    window.__patchedAngularProps = patched;
})();
"""

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


class F5Session:
    """
    Manages a live Playwright/Chrome session that has passed the F5 TSPD challenge.

    Keeps the browser context open after solving the challenge so that all
    subsequent API calls go through the same Chrome TLS/HTTP2 stack — the
    only reliable way to stay undetected by F5's per-request fingerprinting.

    Use as an async context manager:
        async with F5Session(cfg, proxy_mgr) as session:
            cookies = session.as_httpx_cookies()      # dict for logging
            data    = await session.api_get(url, params={...})
            result  = await session.api_post(url, data={...})
    """

    def __init__(self, config: Config, proxy_mgr: ProxyManager):
        self.config = config
        self.proxy_mgr = proxy_mgr
        self._cookies: list[dict] = []
        self.turnstile_site_key: Optional[str] = "0x4AAAAAAB2Edz1zEK7o5Rj1"
        # live Playwright objects — kept open until close()
        self._stealth_ctx = None
        self._pw = None
        self._browser = None
        self._ctx: Optional[BrowserContext] = None

    async def __aenter__(self):
        await self._launch()
        return self

    async def __aexit__(self, *_):
        await self.close()

    async def _launch(self):
        from playwright_stealth import Stealth
        self._stealth_ctx = Stealth().use_async(async_playwright())
        self._pw = await self._stealth_ctx.__aenter__()
        try:
            self._browser = await self._pw.chromium.launch(
                channel="chrome",
                headless=self.config.headless,
                args=["--disable-blink-features=AutomationControlled"],
            )
        except Exception:
            self._browser = await self._pw.chromium.launch(
                headless=self.config.headless,
                args=["--disable-blink-features=AutomationControlled"],
            )
        self._ctx = await self._browser.new_context(
            user_agent=UA,
            proxy=self.proxy_mgr.playwright_proxy(),
            viewport={"width": 1280, "height": 800},
            locale="en-US",
            ignore_https_errors=True,
        )
        # NOTE: ANGULAR_FORM_PATCH_SCRIPT intentionally NOT added here.
        # The script patches Object.defineProperty and Function.prototype.toString,
        # which interferes with Angular's page initialization (bodyLen=185, hasAngular=False).
        # Instead we capture the FormGroup reference at runtime (window.__checkoutForm)
        # during Phase 4E's MapGet interception and patch it directly in booking.py.
        # await self._ctx.add_init_script(ANGULAR_FORM_PATCH_SCRIPT)

    async def solve(self) -> dict:
        """
        Load the booking page, solve the F5 challenge, then navigate to the
        calendar section. All API calls F5 sees come from the SPA's own
        navigation — no extra synthetic requests.
        """
        self._page: Page = await self._ctx.new_page()
        self._intercepted: dict = {}

        # Step 1: solve F5 challenge on /home. Stay on /home — do NOT navigate to the
        # calendar URL here. A second goto causes F5 to re-score the session (10 → 8 cookies),
        # which degrades the session enough that /api/visit/reservation is blocked.
        # The monitor navigates to the calendar via SPA (no F5 re-challenge).
        logger.info("Loading /home to solve F5 challenge…")
        await self._page.goto(f"{BASE_URL}/home", wait_until="networkidle", timeout=60_000)
        await self._wait_for_ts_cookie(self._page)

        # Verify Angular actually loaded (not just F5's challenge page).
        # When F5's challenge page appears, bodyLen ≈ 185 bytes; the real Angular
        # app is 60 KB+. If we're on the challenge page, wait for the automatic
        # redirect to the real page (F5 redirects after the JS challenge completes).
        body_len = await self._page.evaluate("() => document.body.innerHTML.length")
        if body_len < 20_000:
            # Angular app is 60 KB+. < 20 KB means we're still on the F5 challenge page
            # (38 bytes = redirect stub, 4–5 KB = challenge JS page). Wait for the challenge
            # JS to run and redirect us to the real page. Never reload — that restarts the
            # challenge loop.
            logger.info("F5 challenge JS running (bodyLen=%d) — waiting for Angular redirect…", body_len)
            deadline = asyncio.get_event_loop().time() + 30
            while asyncio.get_event_loop().time() < deadline:
                await asyncio.sleep(1)
                try:
                    bl = await self._page.evaluate("() => document.body.innerHTML.length")
                    if bl >= 20_000:
                        logger.info("Angular app loaded after challenge (bodyLen=%d)", bl)
                        break
                except Exception:
                    pass
            else:
                logger.warning("Angular did not load after 30s wait (still on challenge page)")

        logger.info("F5 challenge passed.")

        await self._wait_for_session_cookie()

        raw = await self._ctx.cookies()
        self._cookies = raw
        logger.info("Session ready. Harvested %d cookies. Intercepted %d API calls.",
                    len(raw), len(self._intercepted))

        # DOM diagnostic: see if Angular rendered the calendar UI
        try:
            dom_state = await self._page.evaluate("""
                () => ({
                    bodyLen: document.body?.innerHTML?.length || 0,
                    matCells: document.querySelectorAll('button.mat-calendar-body-cell').length,
                    inputs: document.querySelectorAll('input').length,
                    buttons: document.querySelectorAll('button').length,
                    hasAngular: !!window.ng,
                    url: window.location.href,
                })
            """)
            logger.info("DOM state: %s", dom_state)
            if dom_state.get("matCells", 0) == 0:
                logger.warning("Angular calendar NOT rendered (matCells=0, bodyLen=%d)",
                               dom_state.get("bodyLen", 0))
                await self._page.screenshot(path="session_calendar_state.png")
        except Exception as exc:
            logger.warning("DOM diagnostic failed: %s", exc)

        # Confirm Turnstile site key from page evaluation as fallback
        try:
            init_r = await self._page.evaluate("""async () => {
                const r = await fetch('/api/config/initValues');
                return await r.json();
            }""")
            if init_r and init_r.get("captchaSiteKey"):
                self.turnstile_site_key = init_r["captchaSiteKey"]
                logger.info("Turnstile site key confirmed: %s", self.turnstile_site_key)
        except Exception:
            pass

        return self.as_httpx_cookies()

    def get_intercepted(self, key: str) -> Optional[dict]:
        """Return a previously-intercepted API response by path key, or None."""
        return self._intercepted.get(key)

    async def intercept_calendar(self, timeout: float = 20_000) -> bool:
        """
        Navigate to the calendar page and wait explicitly for the SPA's
        /api/search/calendar call. Returns True if captured successfully.
        """
        try:
            async with self._page.expect_response(
                lambda r: "/api/search/calendar" in r.url, timeout=timeout
            ) as cal_info:
                await self._page.goto(
                    f"{BASE_URL}/home/calendar/visit/MV-Biglietti/2",
                    wait_until="load", timeout=60_000,
                )
            cal_response = await cal_info.value
            body = await cal_response.body()
            self._intercepted["/api/search/calendar"] = {
                "status": cal_response.status,
                "body": body.decode("utf-8", errors="replace"),
                "url": cal_response.url,
            }
            logger.info("Calendar API intercepted → HTTP %d  body=%r",
                        cal_response.status, body[:120])
            return True
        except Exception as exc:
            logger.warning("intercept_calendar failed: %s", exc)
            return False

    # ── API call helpers — run fetch() INSIDE Chrome to reuse F5 session ─────

    async def api_get(self, url: str, params: Optional[dict] = None) -> dict:
        """
        GET via XMLHttpRequest (Angular's method) — runs inside Chrome's network stack.
        Uses a relative URL so no Origin header is sent (F5 WAF requirement).
        Returns dict with keys: status (int), body (str).
        """
        from urllib.parse import urlencode, urlparse
        parsed = urlparse(url)
        path = parsed.path
        if params:
            path = f"{path}?{urlencode(params)}"
        elif parsed.query:
            path = f"{path}?{parsed.query}"
        result = await self._page.evaluate("""
            (args) => new Promise((resolve, reject) => {
                const xhr = new XMLHttpRequest();
                xhr.open('GET', args.path, true);
                xhr.setRequestHeader('Accept', 'application/json, text/plain, */*');
                xhr.setRequestHeader('X-Requested-With', 'XMLHttpRequest');
                xhr.withCredentials = true;
                xhr.onload = () => resolve({ status: xhr.status, body: xhr.responseText });
                xhr.onerror = () => reject(new Error('XHR network error'));
                xhr.send();
            })
        """, {"path": path})
        logger.info("GET %s → HTTP %d  body=%r", path, result["status"], result["body"][:200])
        return result

    async def api_post(self, url: str, data: dict) -> dict:
        """
        POST via page.evaluate(fetch(...)) — runs inside Chrome's network stack.
        Uses a relative URL so no cross-origin headers are sent.
        Returns dict with keys: status (int), body (str).
        """
        import json as _json
        from urllib.parse import urlparse
        path = urlparse(url).path
        result = await self._page.evaluate("""
            async (args) => {
                const r = await fetch(args.path, {
                    method: 'POST',
                    headers: {
                        'Accept': 'application/json, text/plain, */*',
                        'Content-Type': 'application/json',
                        'X-Requested-With': 'XMLHttpRequest',
                    },
                    body: args.body,
                    credentials: 'include',
                });
                return { status: r.status, body: await r.text() };
            }
        """, {"path": path, "body": _json.dumps(data)})
        logger.info("POST %s → HTTP %d  body=%r", path, result["status"], result["body"][:200])
        return result

    @staticmethod
    async def _patch_angular_bundle(route):
        """
        Intercept Angular JS bundle responses and patch AbstractControl.valid.
        Only processes JS from the Vatican Museums domain; passes everything else
        through untouched so F5's own challenge scripts are never modified.
        """
        import re as _re
        url = route.request.url

        # Skip F5 / CDN / external JS — only touch Vatican Museums assets
        if "museivaticani.va" not in url:
            await route.continue_()
            return

        try:
            response = await route.fetch()
            body = await response.body()
            text = body.decode("utf-8", errors="replace")

            if "markAllAsTouched" in text and "VALID" in text:
                # Angular forms bundle: patch the valid getter
                patched, n = _re.subn(
                    r'get valid\(\)\{return("VALID"===this\.\w+|this\.\w+==="VALID")\}',
                    r'get valid(){if(window.__forceAngularFormValid===true)return true;return \1}',
                    text,
                )
                if n > 0:
                    logger.info("Patched %d valid getter(s) in %s", n, url.split("?")[0][-60:])
                    await route.fulfill(
                        status=response.status,
                        headers=dict(response.headers),
                        body=patched.encode("utf-8"),
                    )
                    return

            await route.fulfill(response=response)
        except Exception as exc:
            logger.warning("Bundle patch route error for %s: %s", url, exc)
            try:
                await route.continue_()
            except Exception:
                pass

    async def close(self):
        """Tear down browser and Playwright context."""
        try:
            if self._browser:
                await self._browser.close()
            if self._stealth_ctx:
                await self._stealth_ctx.__aexit__(None, None, None)
        except Exception:
            pass

    async def _wait_for_ts_cookie(self, page: Page, timeout: float = 30.0):
        """Poll until at least one TS-prefixed cookie appears (challenge solved)."""
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            cookies = await page.context.cookies()
            if any(c["name"].startswith("TS") for c in cookies):
                return
            await asyncio.sleep(0.5)
        raise TimeoutError("F5 challenge did not resolve within timeout.")

    async def _wait_for_session_cookie(self, timeout: float = 20.0):
        """
        Wait until JSESSIONID (or ticketmv) appears — Angular sets these when it
        completes its initialization API calls after the calendar page loads.
        Falls back after timeout so the bot continues even if they never appear.
        """
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            cookies = await self._ctx.cookies()
            names = {c["name"] for c in cookies}
            if "JSESSIONID" in names or "ticketmv" in names:
                logger.info("Session cookies ready (%d total).", len(cookies))
                return
            await asyncio.sleep(0.5)
        logger.warning("Session cookie wait timed out — continuing with %d cookies.",
                       len(await self._ctx.cookies()))

    def as_httpx_cookies(self) -> dict:
        return {c["name"]: c["value"] for c in self._cookies}

    @staticmethod
    def _extract_site_key(html: str) -> Optional[str]:
        import re
        # reCAPTCHA v2 site key appears as data-sitekey="..." or in grecaptcha.render calls
        m = re.search(r'data-sitekey=["\']([0-9A-Za-z_-]{40})["\']', html)
        if m:
            return m.group(1)
        m = re.search(r'grecaptcha\.render\([^,]+,\s*\{[^}]*["\']sitekey["\']\s*:\s*["\']([0-9A-Za-z_-]{40})["\']', html)
        if m:
            return m.group(1)
        return None
