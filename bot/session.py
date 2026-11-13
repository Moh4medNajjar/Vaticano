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

# NOTE: playwright_stealth (used below via Stealth().use_async) already
# installs a clean, toString-spoofed set of patches: navigator.webdriver=false
# on the prototype (NOT the instance), plugins length=5, and native-looking
# toString on patched methods. Verified via httpbin probe.
#
# Earlier versions of this file carried a hand-rolled STEALTH_SCRIPT that
# layered on top of playwright_stealth and INTRODUCED two F5 tells:
#   - navigator.webdriver became an OWN property returning undefined
#     (real Chrome has it on the prototype returning false)
#   - navigator.permissions.query.toString() returned our JS source instead of
#     "function query() { [native code] }" — directly catches on F5's
#     Function.prototype.toString spoof check.
# Do not re-introduce it. If something needs patching beyond stealth, wrap
# the patch with a toString proxy that returns the expected native string.

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

# Must match the actual Chromium version launched (verified via probe:
# installed Chrome is 157). Mismatch between UA header and Chrome's
# auto-populated sec-ch-ua: "Google Chrome";v="157" is a hard F5 signal.
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/157.0.0.0 Safari/537.36"
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
        # True after solve() returns if the Angular shell loaded (full F5 pass);
        # False if the IP produced only a challenge page / dead connection.
        self.challenge_passed: bool = False
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
        """
        Persistent Chrome profile per (proxy, user-data-dir). Inspired by the
        Colosseo bot at test-workspace/fast/* which keeps clearance cookies in
        a SQLite cache per proxy and seeds them on every session — the bot WAF
        then sees "trusted browser accumulating activity" instead of 20 fresh
        fingerprints an hour.

        Our equivalent: Playwright's launchPersistentContext writes TSPD_101,
        TSPD_101_DID, ticketmv, JSESSIONID etc. to disk. Next run reuses them —
        Vatican sees the same browser that already cleared F5 doing one more
        booking, not a brand-new session.

        One profile per proxy to keep the cookie↔IP binding stable. We stick
        to the first picked proxy for the profile's lifetime.
        """
        import hashlib as _hashlib
        import pathlib as _pl
        from playwright_stealth import Stealth

        proxy = self.proxy_mgr.playwright_proxy()
        # Derive a per-proxy profile dir — one browser history per exit IP
        if proxy and proxy.get("server"):
            proxy_tag = _hashlib.sha1(
                (proxy.get("server","") + (proxy.get("username") or "")).encode()
            ).hexdigest()[:10]
        else:
            proxy_tag = "noproxy"
        profiles_root = _pl.Path(__file__).resolve().parent.parent / ".pw_profiles"
        profiles_root.mkdir(exist_ok=True)
        user_data_dir = profiles_root / proxy_tag
        is_fresh = not user_data_dir.exists()
        logger.info("persistent profile: %s  (fresh=%s)", user_data_dir.name, is_fresh)
        user_data_dir.mkdir(exist_ok=True)

        # Stable viewport per profile (NOT random): randomness defeats the
        # "same browser over time" signal we're trying to build.
        viewport = {"width": 1536, "height": 864}

        self._stealth_ctx = Stealth().use_async(async_playwright())
        self._pw = await self._stealth_ctx.__aenter__()
        launch_kwargs = dict(
            user_data_dir=str(user_data_dir),
            headless=self.config.headless,
            args=["--disable-blink-features=AutomationControlled"],
            user_agent=UA,
            viewport=viewport,
            locale="en-US",
            timezone_id="Europe/Rome",
            ignore_https_errors=True,
            proxy=proxy,
        )
        try:
            self._ctx = await self._pw.chromium.launch_persistent_context(
                channel="chrome", **launch_kwargs)
        except Exception as exc:
            logger.info("launch_persistent_context(channel=chrome) failed: %s — trying bundled Chromium", exc)
            self._ctx = await self._pw.chromium.launch_persistent_context(**launch_kwargs)
        # No separate _browser when using persistent context — ctx IS the browser
        self._browser = self._ctx.browser

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
        try:
            await self._page.goto(f"{BASE_URL}/home", wait_until="networkidle", timeout=30_000)
        except Exception as exc:
            # ERR_TIMED_OUT / net errors mean the chosen Evomi IP is dead. Report
            # so the caller can rotate without a long wait.
            logger.warning("goto /home failed (dead IP?): %s", exc)
            self.challenge_passed = False
            self._cookies = []
            return self.as_httpx_cookies()
        try:
            await self._wait_for_ts_cookie(self._page)
        except TimeoutError:
            logger.warning("TS cookie never arrived (hard F5 reject)")
            self.challenge_passed = False
            self._cookies = []
            return self.as_httpx_cookies()

        # Verify Angular actually loaded (not just F5's challenge page).
        # Angular shell is 60 KB+; F5 challenge page is ~6 KB; redirect stub is ~185 bytes.
        # A good IP's challenge resolves in 2-4 s; cap the wait at 10 s so bad IPs
        # fail fast and the caller can rotate to a warmer one.
        body_len = await self._page.evaluate("() => document.body.innerHTML.length")
        self.challenge_passed = True   # optimistic; downgraded below if needed
        if body_len < 20_000:
            logger.info("F5 challenge JS running (bodyLen=%d) — waiting up to 10s for Angular…", body_len)
            deadline = asyncio.get_event_loop().time() + 10
            while asyncio.get_event_loop().time() < deadline:
                await asyncio.sleep(0.5)
                try:
                    bl = await self._page.evaluate("() => document.body.innerHTML.length")
                    if bl >= 20_000:
                        logger.info("Angular app loaded after challenge (bodyLen=%d)", bl)
                        break
                except Exception:
                    pass
            else:
                logger.warning("Angular did not load within 10s — IP is cold, rotating is advised")
                self.challenge_passed = False

        logger.info("F5 solve finished  challenge_passed=%s", self.challenge_passed)

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
        """Tear down the persistent context. The user-data-dir stays on disk
        so cookies + storage persist for the next run."""
        try:
            if self._ctx:
                await self._ctx.close()
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
