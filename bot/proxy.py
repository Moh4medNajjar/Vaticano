import itertools
from typing import Optional

class ProxyManager:
    """Round-robin residential proxy rotation."""

    def __init__(self, proxy_list: list[str], rotate_every: int = 1):
        self._proxies = proxy_list
        self._rotate_every = rotate_every
        self._cycle = itertools.cycle(proxy_list) if proxy_list else None
        self._count = 0
        self._current: Optional[str] = None

    def get(self) -> Optional[str]:
        if not self._cycle:
            return None
        if self._current is None or self._count % self._rotate_every == 0:
            self._current = next(self._cycle)
        self._count += 1
        return self._current

    def httpx_proxy(self) -> Optional[str]:
        """Returns a single proxy URL string for httpx's `proxy=` parameter."""
        return self.get()

    def playwright_proxy(self) -> Optional[dict]:
        """Return a proxy dict formatted for Playwright."""
        p = self.get()
        if not p:
            return None
        # "http://user:pass@host:port" → {"server": ..., "username": ..., "password": ...}
        from urllib.parse import urlparse
        parsed = urlparse(p)
        proxy = {"server": f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"}
        if parsed.username:
            proxy["username"] = parsed.username
        if parsed.password:
            proxy["password"] = parsed.password
        return proxy
