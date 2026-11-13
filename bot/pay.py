#!/usr/bin/env python3
"""
Vatican checkout helper — turn a captured reservation into something a human
can actually pay.

Input:
    reports/hold_<ts>_live.json (must contain reservation.referenceOrder +
    reservation.epay). Use --file to pick a specific file, else the newest
    hold_*_live.json with result='held'.

What it does:
    1. Prints the raw epay object so we can see every field Vatican returned.
    2. Builds a self-submitting HTML form that POSTs to epay.urlMs with the
       browser's default content-type (application/x-www-form-urlencoded or
       empty body — Vatican's bridge only cares about the cookie + URL match).
       Saves it to reports/pay_<ref>.html and opens it in the default browser.
    3. Also prints a Catholica direct-form (POST to epay.url) as fallback;
       exact field names TBD, we send the ones the inspiration repo README
       lists and tag any that come back wrong for the next iteration.

Usage:
    python pay.py                      # pick newest held reservation
    python pay.py --file reports/hold_1794257065_live.json
    python pay.py --ref 2L2NFGEV00…    # resolve by ref
    python pay.py --dry                # build HTML but don't open browser

Important: this MUST run on the same machine/browser profile that made the
hold (cookies are JSESSIONID-bound). If the Playwright session is already
closed, Vatican's bridge will reject the POST — in that case use --manual
to just print the form and paste it into a browser dev-tools tab.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys
import time
import webbrowser
from datetime import datetime
from typing import Optional

PROJECT = pathlib.Path(__file__).resolve().parent.parent
REPORTS = PROJECT / "reports"


def find_latest_held(explicit_file: Optional[str], ref: Optional[str]) -> pathlib.Path:
    if explicit_file:
        p = pathlib.Path(explicit_file)
        if not p.is_absolute():
            p = REPORTS / p.name
        if not p.exists():
            raise SystemExit(f"file not found: {p}")
        return p
    cands = sorted(REPORTS.glob("hold_*_live.json"), key=lambda p: p.stat().st_mtime)
    for p in reversed(cands):
        try:
            d = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        if d.get("result") != "held":
            continue
        got_ref = (d.get("reservation") or {}).get("referenceOrder")
        if ref and got_ref != ref:
            continue
        return p
    raise SystemExit(f"no held reservation found (ref={ref})" if ref else
                     "no reports/hold_*_live.json with result=held yet")


FORM_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Vatican checkout · {ref}</title>
  <style>
    body {{ font: 16px/1.5 system-ui, sans-serif; max-width: 680px; margin: 2rem auto; padding: 0 1rem; }}
    .meta {{ color: #666; font-size: 90%; }}
    .warn {{ background: #fff4c4; border-left: 4px solid #f0ad4e; padding: 12px 16px; margin: 1rem 0; }}
    .ok   {{ background: #d4edda; border-left: 4px solid #28a745; padding: 12px 16px; margin: 1rem 0; }}
    pre {{ background: #f6f6f6; padding: 1rem; overflow: auto; font-size: 85%; }}
    .btn {{ display: inline-block; padding: .9rem 1.4rem; background: #1a7f37; color: white;
            border: 0; border-radius: 6px; font-size: 1.05rem; cursor: pointer; text-decoration: none; }}
    .btn.alt {{ background: #0366d6; }}
    .btn.big {{ font-size: 1.15rem; padding: 1.1rem 1.6rem; }}
    code.url {{ display: block; background: #f6f6f6; padding: 8px; word-break: break-all; }}
  </style>
</head>
<body>
  <h1>🎟️ Vatican reservation ready to pay</h1>
  <p class="meta">ref: <code>{ref}</code> · total: <b>€{total_eur}</b> · expires ~15 min after capture</p>

  <div class="ok">
    <b>Catholica payment form will auto-submit in 2 seconds.</b> If your browser blocks
    it, click the green button below to submit manually. After paying, you may hit a
    "potentially malicious String ';'" F5 error on the redirect back — that's harmless,
    your card is still charged. See bottom of page for how to confirm the booking.
  </div>

  <h2>Pay via Catholica direct (auto-submits)</h2>
  <form id="to_catholica" method="POST" action="{catholica_url}">
    {catholica_fields}
    <button type="submit" class="btn alt big">💳 Pay via Catholica now</button>
  </form>
  <p class="meta">
    Posts directly to Catholica's payment page with the MAC auth token. Verified
    working end-to-end on {ref} — Catholica returns a 302 to the card-entry form.
  </p>
  <script>
    // Auto-submit after 2s so the user lands directly on Catholica's card form.
    // Small delay so they have a chance to see the ref + total + warning box.
    setTimeout(function(){{ document.getElementById('to_catholica').submit(); }}, 2000);
  </script>

  <details style="margin-top: 1.5rem;">
    <summary>Advanced: pay via Vatican's <code>urlMs</code> (not recommended)</summary>
    <form id="to_urlms" method="POST" action="{url_ms}" style="margin-top: 1rem;">
      <button type="submit" class="btn">Pay via Vatican → Catholica</button>
    </form>
    <p class="meta">
      POSTs to Vatican's <code>/epay/process/{ref}</code> which redirects to Catholica.
      This endpoint requires the SAME browser session that caught the reservation —
      your regular Chrome doesn't have those cookies (our bot's Playwright profile does).
      Will almost certainly return "No message available". Use the Catholica direct form
      above instead.
    </p>
  </details>

  <h2>What to do if you hit the F5 <code>";"</code> rejection</h2>
  <div class="ok">
    <p>The error means Catholica redirected back to Vatican with
      <code>…confirm/en/{ref};jsessionid=…</code> and F5 refused the semicolon.</p>
    <p><b>Your payment likely succeeded regardless.</b> To verify, open this URL
      directly in a new tab (no semicolon):</p>
    <code class="url">{confirm_url}</code>
    <p>That's Vatican's confirmation page for your reference. If payment was captured,
      you'll see the booking details and a PDF download. Otherwise, you'll see "booking
      not yet paid" — in which case, retry a fresh <code>/hold</code>.</p>
    <p>Also check the email inbox used for the booking — Vatican sends a "Conferma
      prenotazione / Booking confirmation" email with the PDF attached independent of
      the browser redirect.</p>
  </div>

  <h2>Raw <code>epay</code> object (for debugging)</h2>
  <pre>{epay_pretty}</pre>
</body>
</html>
"""


def _hidden(name: str, value) -> str:
    return (f'    <input type="hidden" name="{name}" '
            f'value="{"" if value is None else str(value)}"/>\n')


def build_html(payload: dict) -> str:
    res   = payload["reservation"]
    ref   = res["referenceOrder"]
    total = res.get("total")
    epay  = res["epay"]

    # Total in cents: the Vatican returns it either as an int/float of euros
    # (50.0 for €50) or as a string of cents ("2500" for €25.00). Normalise.
    if isinstance(total, (int, float)):
        total_eur = f"{float(total):.2f}"
        total_cents = int(float(total) * 100)
    else:
        try:
            cents = int(str(total))
            total_cents = cents
            total_eur = f"{cents/100:.2f}"
        except Exception:
            total_cents = 0
            total_eur = str(total)

    # Catholica field names — EXACT uppercase names captured from the user's
    # successful manual payment HAR (epay.catholica.va.har entry 0, 2026-11-11).
    # These are what Vatican's SPA JS posts to Catholica; using them makes the
    # form equivalent to clicking ACQUISTA in the real site.
    catholica_fields = "".join([
        _hidden("URLMS",     epay.get("urlMs",    "")),
        _hidden("URLBACK",   epay.get("urlback",  "")),
        _hidden("URLDONE",   epay.get("urldone",  "")),
        _hidden("IDNEGOZIO", epay.get("idnegozio", "SIV001")),
        _hidden("VALUTA",    epay.get("valuta",    "978")),
        _hidden("TCONTAB",   epay.get("tcontab",   "D")),
        _hidden("TAUTOR",    epay.get("tautor",    "I")),
        _hidden("MAC",       epay.get("mac_avvio", "")),
        _hidden("NUMORD",    ref),
        _hidden("IMPORTO",   total_cents),
        _hidden("LINGUA",    "en"),
    ])

    # Clean confirmation URL (no ;jsessionid) for manual fallback
    confirm_url = epay.get("urldone") or f"https://tickets.museivaticani.va/home/checkout/confirm/en/{ref}"
    return FORM_HTML.format(
        ref=ref,
        total_eur=total_eur,
        url_ms=epay.get("urlMs", ""),
        catholica_url=epay.get("url", "https://epay.catholica.va/pay/SIV001/upp/auth/start.page"),
        catholica_fields=catholica_fields,
        confirm_url=confirm_url,
        epay_pretty=json.dumps(epay, indent=2),
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", help="Specific hold_*_live.json")
    ap.add_argument("--ref", help="Resolve by referenceOrder")
    ap.add_argument("--response", help="Paste a raw Vatican /reservation response "
                                       "JSON (bypasses our reports/ lookup, useful "
                                       "for paying a manual-caught reservation).")
    ap.add_argument("--dry", action="store_true", help="Build HTML but do not open browser")
    args = ap.parse_args()

    # Direct-response mode: paste the JSON Vatican returned and pay it.
    if args.response:
        raw = json.loads(pathlib.Path(args.response).read_text(encoding="utf-8")
                         if pathlib.Path(args.response).exists() else args.response)
        if "referenceOrder" not in raw and "reservation" in raw:
            raw = raw["reservation"]
        d = {"result": "held", "reservation": raw}
        src = REPORTS / f"pay_input_{raw['referenceOrder']}.json"
        src.write_text(json.dumps(d, indent=2), encoding="utf-8")
    else:
        src = find_latest_held(args.file, args.ref)
        d = json.loads(src.read_text(encoding="utf-8"))
    if (d.get("result") != "held") or not (d.get("reservation") or {}).get("referenceOrder"):
        print(f"{src.name} has no held reservation; aborting", file=sys.stderr)
        sys.exit(1)

    res   = d["reservation"]
    ref   = res["referenceOrder"]
    epay  = res.get("epay") or {}
    html  = build_html(d)
    out   = REPORTS / f"pay_{ref}.html"
    out.write_text(html, encoding="utf-8")
    print(f"ref:    {ref}")
    print(f"total:  €{res.get('total')}")
    print(f"urlMs:  {epay.get('urlMs')}")
    print(f"urldone:{epay.get('urldone')}")
    print(f"wrote:  {out}")
    if not args.dry:
        webbrowser.open(out.as_uri())
        print("Opened in default browser. Click 'Pay via Vatican → Catholica'.")


if __name__ == "__main__":
    main()
