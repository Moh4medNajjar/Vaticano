import os
from dataclasses import dataclass, field
from typing import Optional

BASE_URL = "https://tickets.museivaticani.va"

@dataclass
class Config:
    # --- Target ---
    target_date: str = ""          # "YYYY-MM-DD"
    ticket_type: str = "standard"  # standard | early_entry
    num_tickets: int = 2
    time_slot: Optional[str] = None  # None = take first available

    # --- Buyer (used for the "Manager data" step) ---
    buyer_first: str = ""
    buyer_last: str = ""
    buyer_email: str = ""
    buyer_dob: str = ""            # "YYYY-MM-DD"

    # --- Participants (every visitor, nominative since Jan 2024) ---
    # List of {"first": str, "last": str}
    participants: list = field(default_factory=list)

    # --- CAPTCHA ---
    yescaptcha_key: str = os.getenv("YESCAPTCHA_KEY", "")
    recaptcha_site_key: str = ""   # discovered at runtime from page source

    # --- Proxies ---
    proxy_list: list = field(default_factory=list)  # ["http://user:pass@host:port", ...]
    rotate_every: int = 1          # rotate proxy every N requests

    # --- Timing ---
    poll_interval: float = 5.0     # seconds between availability checks
    request_timeout: int = 30      # seconds
    max_poll_attempts: int = 0     # 0 = infinite; set > 0 to cap polling (useful for dry-runs)

    # --- Mode ---
    dry_run: bool = True           # True = stop before payment step
    headless: bool = True          # Playwright headless mode
