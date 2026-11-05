"""Quick check: can local IP reach the Vatican site (not rate-limited)?"""
import asyncio, logging
from playwright.async_api import async_playwright

async def test():
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        )
        page = await ctx.new_page()
        await page.goto("https://tickets.museivaticani.va/api/search/calendar?tag=MV-Biglietti&lang=it&num=2", wait_until="load", timeout=15_000)
        body = await page.evaluate("() => document.body.innerText")
        print(f"bodyLen={len(body)}  preview={body[:150]!r}")
        blocked = "Request Rejected" in body or "rejected" in body.lower()
        print("STATUS:", "BLOCKED" if blocked else "OK")
        await browser.close()

asyncio.run(test())
