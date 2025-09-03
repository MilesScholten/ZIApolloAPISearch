from __future__ import annotations
import os, time, json
from typing import Any, Dict

def sanitize_domain(website: str) -> str:
    if not website:
        return ""
    w = website.strip()
    if w.startswith("http://") or w.startswith("https://"):
        try:
            from urllib.parse import urlparse
            host = urlparse(w).netloc
        except Exception:
            host = w
    else:
        host = w
    host = host.split("/")[0]
    host = host[4:] if host.lower().startswith("www.") else host
    return host.lower()

def flatten(prefix: str, obj: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    def _rec(base, val):
        if isinstance(val, dict):
            for k, v in val.items():
                _rec(f"{base}{k}.", v)
        elif isinstance(val, list):
            for i, v in enumerate(val):
                _rec(f"{base}{i}.", v)
        else:
            out[base[:-1]] = val
    _rec(prefix + ".", obj)
    return {k.replace(".", "_"): v for k, v in out.items()}

def rate_limiter(per_minute: int) -> float:
    if per_minute <= 0:
        return 0.0
    return 60.0 / float(per_minute)
