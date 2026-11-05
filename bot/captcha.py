"""
Cloudflare Turnstile solver via YesCaptcha API.

The Vatican Museums site uses Cloudflare Turnstile (site key 0x4AAAAAAB2Edz1zEK7o5Rj1),
NOT reCAPTCHA v2 as older sources claimed.  YesCaptcha supports Turnstile via
the TurnstileTaskProxyless task type.

Docs: https://yescaptcha.com/en/api
"""

import asyncio
import logging
import httpx

logger = logging.getLogger(__name__)

YESCAPTCHA_API = "https://api.yescaptcha.com"

# Hardcoded Turnstile site key — also returned by /api/config/initValues at runtime
TURNSTILE_SITE_KEY = "0x4AAAAAAB2Edz1zEK7o5Rj1"
TURNSTILE_PAGE_URL = "https://tickets.museivaticani.va/home"


class CaptchaSolver:

    def __init__(self, api_key: str):
        self.api_key = api_key

    async def solve_turnstile(
        self,
        site_key: str = TURNSTILE_SITE_KEY,
        page_url: str = TURNSTILE_PAGE_URL,
        timeout: int = 150,
        retries: int = 3,
    ) -> str:
        """
        Submit a Cloudflare Turnstile task to YesCaptcha and poll until solved.
        Retries up to `retries` times if the task times out.
        Returns the cf-turnstile-response token string.
        """
        last_exc = None
        for attempt in range(1, retries + 1):
            try:
                task_id = await self._create_task(site_key, page_url)
                logger.info("YesCaptcha Turnstile task created (attempt %d/%d): %s",
                            attempt, retries, task_id)
                token = await self._poll_result(task_id, timeout)
                logger.info("Turnstile solved.")
                return token
            except TimeoutError as exc:
                last_exc = exc
                logger.warning("Turnstile attempt %d/%d timed out — retrying…", attempt, retries)
                await asyncio.sleep(2)
        raise TimeoutError(f"Turnstile not solved after {retries} attempts: {last_exc}")

    async def _create_task(self, site_key: str, page_url: str) -> str:
        payload = {
            "clientKey": self.api_key,
            "task": {
                "type": "TurnstileTaskProxyless",
                "websiteURL": page_url,
                "websiteKey": site_key,
            },
        }
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(f"{YESCAPTCHA_API}/createTask", json=payload)
            r.raise_for_status()
            data = r.json()
        if data.get("errorId", 0) != 0:
            raise RuntimeError(f"YesCaptcha createTask error: {data}")
        return str(data["taskId"])

    async def _poll_result(self, task_id: str, timeout: int) -> str:
        payload = {"clientKey": self.api_key, "taskId": task_id}
        deadline = asyncio.get_event_loop().time() + timeout
        async with httpx.AsyncClient(timeout=30) as client:
            while asyncio.get_event_loop().time() < deadline:
                await asyncio.sleep(5)
                r = await client.post(f"{YESCAPTCHA_API}/getTaskResult", json=payload)
                r.raise_for_status()
                data = r.json()
                if data.get("errorId", 0) != 0:
                    raise RuntimeError(f"YesCaptcha poll error: {data}")
                if data.get("status") == "ready":
                    # Turnstile token is in "token" key
                    sol = data["solution"]
                    return sol.get("token") or sol.get("gRecaptchaResponse", "")
                logger.debug("YesCaptcha status: %s", data.get("status"))
        raise TimeoutError("Turnstile not solved within timeout.")
