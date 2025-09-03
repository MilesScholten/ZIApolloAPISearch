from __future__ import annotations
from typing import Any, Dict, Optional, Tuple
import os, requests

class ZoomInfoClient:
    def __init__(self, api_key: Optional[str] = None, *, base_url: str = "https://api.zoominfo.com",
                 company_by_id_path: str = "/company/detail",
                 company_lookup_path: str = "/lookup/company",
                 search_by_name_path: str = "/search/company",
                 timeout: int = 30):
        self.api_key = api_key or os.getenv("ZOOMINFO_API_KEY", "")
        self.base_url = base_url.rstrip("/")
        self.company_by_id_path = company_by_id_path
        self.company_lookup_path = company_lookup_path
        self.search_by_name_path = search_by_name_path
        self.timeout = timeout

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def company_by_id(self, company_id: str, retries: int = 3):
        if not company_id:
            return None, "missing company_id"
        url = f"{self.base_url}{self.company_by_id_path}"
        params = {"companyId": company_id}
        for attempt in range(1, retries + 1):
            try:
                r = requests.get(url, headers=self._headers(), params=params, timeout=self.timeout)
                if r.status_code == 200:
                    return r.json(), None
                elif r.status_code == 404:
                    return None, "not found"
                elif r.status_code in (429, 500, 502, 503, 504):
                    from time import sleep
                    sleep(min(60, 1.0 * (2 ** (attempt - 1))))
                else:
                    return None, f"HTTP {r.status_code}: {r.text[:300]}"
            except requests.RequestException:
                from time import sleep
                sleep(min(60, 1.0 * (2 ** (attempt - 1))))
        return None, "max attempts reached"

    def company_by_domain(self, domain: str, retries: int = 3):
        if not domain:
            return None, "missing domain"
        url = f"{self.base_url}{self.company_lookup_path}"
        params = {"domain": domain}
        for attempt in range(1, retries + 1):
            try:
                r = requests.get(url, headers=self._headers(), params=params, timeout=self.timeout)
                if r.status_code == 200:
                    return r.json(), None
                elif r.status_code == 404:
                    return None, "not found"
                elif r.status_code in (429, 500, 502, 503, 504):
                    from time import sleep
                    sleep(min(60, 1.0 * (2 ** (attempt - 1))))
                else:
                    return None, f"HTTP {r.status_code}: {r.text[:300]}"
            except requests.RequestException:
                from time import sleep
                sleep(min(60, 1.0 * (2 ** (attempt - 1))))
        return None, "max attempts reached"

    def company_by_name(self, name: str, retries: int = 3):
        if not name:
            return None, "missing name"
        url = f"{self.base_url}{self.search_by_name_path}"
        payload = {"companyName": name}
        for attempt in range(1, retries + 1):
            try:
                r = requests.post(url, headers=self._headers(), json=payload, timeout=self.timeout)
                if r.status_code == 200:
                    data = r.json()
                    if isinstance(data, dict) and "data" in data and data["data"]:
                        return data["data"][0], None
                    return None, "not found"
                elif r.status_code in (429, 500, 502, 503, 504):
                    from time import sleep
                    sleep(min(60, 1.0 * (2 ** (attempt - 1))))
                else:
                    return None, f"HTTP {r.status_code}: {r.text[:300]}"
            except requests.RequestException:
                from time import sleep
                sleep(min(60, 1.0 * (2 ** (attempt - 1))))
        return None, "max attempts reached"
