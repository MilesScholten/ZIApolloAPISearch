from __future__ import annotations
from typing import Any, Dict, Optional, Tuple
import os, requests

class ApolloClient:
    def __init__(self, api_key: Optional[str] = None, *, base_url: str = "https://api.apollo.io/v1",
                 enrich_by_domain_path: str = "/companies/enrich",
                 enrich_by_id_path: str = "/companies/enrich",
                 search_by_name_path: str = "/mixed_companies/search",
                 search_by_salesforce_id_path: str = "/companies/search",
                 timeout: int = 30):
        self.api_key = api_key or os.getenv("APOLLO_API_KEY", "")
        self.base_url = base_url.rstrip("/")
        self.enrich_by_domain_path = enrich_by_domain_path
        self.enrich_by_id_path = enrich_by_id_path
        self.search_by_name_path = search_by_name_path
        self.search_by_salesforce_id_path = search_by_salesforce_id_path
        self.timeout = timeout

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def company_by_domain(self, domain: str, retries: int = 3):
        if not domain:
            return None, "missing domain"
        url = f"{self.base_url}{self.enrich_by_domain_path}"
        payload = {"domain": domain}
        for attempt in range(1, retries + 1):
            try:
                r = requests.post(url, headers=self._headers(), json=payload, timeout=self.timeout)
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

    def company_by_id(self, apollo_id: str, retries: int = 3):
        if not apollo_id:
            return None, "missing apollo_id"
        url = f"{self.base_url}{self.enrich_by_id_path}"
        payload = {"id": apollo_id}
        for attempt in range(1, retries + 1):
            try:
                r = requests.post(url, headers=self._headers(), json=payload, timeout=self.timeout)
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
        payload = {"q_organization_name": name, "page": 1, "per_page": 1}
        for attempt in range(1, retries + 1):
            try:
                r = requests.post(url, headers=self._headers(), json=payload, timeout=self.timeout)
                if r.status_code == 200:
                    data = r.json()
                    for key in ("companies", "organizations", "data", "results"):
                        if isinstance(data, dict) and key in data and data[key]:
                            return data[key][0], None
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

    def company_by_salesforce_id(self, sf_id: str, retries: int = 3):
        if not sf_id:
            return None, "missing salesforce_id"
        url = f"{self.base_url}{self.search_by_salesforce_id_path}"
        payload = {"filters": {"salesforce_id": sf_id}, "page": 1, "per_page": 1}
        for attempt in range(1, retries + 1):
            try:
                r = requests.post(url, headers=self._headers(), json=payload, timeout=self.timeout)
                if r.status_code == 200:
                    data = r.json()
                    for key in ("companies", "organizations", "data", "results"):
                        if isinstance(data, dict) and key in data and data[key]:
                            return data[key][0], None
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
