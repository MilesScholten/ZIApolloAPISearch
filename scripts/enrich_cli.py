#!/usr/bin/env python3
import os, sys
HERE = os.path.dirname(__file__)
ROOT = os.path.abspath(os.path.join(HERE, ".."))
SRC = os.path.join(ROOT, 'src')
if SRC not in sys.path:
    sys.path.insert(0, SRC)

import argparse, os, sys, csv, json, time, pandas as pd
try:
    import yaml
except ImportError:
    yaml = None
from enrichment.clients.zoominfo import ZoomInfoClient
from enrichment.clients.apollo import ApolloClient
from enrichment.logic import do_enrich_row
from enrichment.utils import rate_limiter

def load_config(path: str | None) -> dict:
    cfg = {
        "zoominfo": {
            "base_url": "https://api.zoominfo.com",
            "company_by_id_path": "/company/detail",
            "company_lookup_path": "/lookup/company",
            "search_by_name_path": "/search/company",
            "api_key_env": "ZOOMINFO_API_KEY",
        },
        "apollo": {
            "base_url": "https://api.apollo.io/v1",
            "company_enrich_by_domain_path": "/companies/enrich",
            "company_enrich_by_id_path": "/companies/enrich",
            "company_search_by_name_path": "/mixed_companies/search",
            "company_search_by_salesforce_id_path": "/companies/search",
            "api_key_env": "APOLLO_API_KEY",
        },
        "rate_limits": {"zoominfo_per_min": 50, "apollo_per_min": 50},
        "mapping": {"zoominfo_id": None, "apollo_id": None, "salesforce_id": None, "name": None, "website": None},
        "output": {"include_input_columns": True, "prefix_zoominfo": "zi", "prefix_apollo": "ap", "add_vendor_json_columns": False},
        "retries": {"max_attempts": 5, "base_delay_seconds": 1.0},
        "http": {"timeout_seconds": 30},
    }
    if path and yaml:
        with open(path, "r", encoding="utf-8") as f:
            user = yaml.safe_load(f) or {}
        # shallow merge for brevity
        for k, v in user.items():
            if isinstance(v, dict) and isinstance(cfg.get(k), dict):
                cfg[k].update(v)
            else:
                cfg[k] = v
    return cfg

def main():
    p = argparse.ArgumentParser(description="Company enrichment via ZoomInfo + Apollo (CLI)")
    p.add_argument("-i", "--input", required=True, help="Path to input CSV")
    p.add_argument("-o", "--output", required=True, help="Path to output CSV")
    p.add_argument("-c", "--config", default=None, help="Path to YAML config (optional)")
    args = p.parse_args()

    cfg = load_config(args.config)

    # Instantiate clients with configured endpoints and API keys
    zi_cfg = cfg.get("zoominfo", {})
    ap_cfg = cfg.get("apollo", {})
    zi = ZoomInfoClient(
        api_key=os.getenv(zi_cfg.get("api_key_env", "ZOOMINFO_API_KEY"), ""),
        base_url=zi_cfg.get("base_url", "https://api.zoominfo.com"),
        company_by_id_path=zi_cfg.get("company_by_id_path", "/company/detail"),
        company_lookup_path=zi_cfg.get("company_lookup_path", "/lookup/company"),
        search_by_name_path=zi_cfg.get("search_by_name_path", "/search/company"),
        timeout=cfg.get("http", {}).get("timeout_seconds", 30),
    )
    ap = ApolloClient(
        api_key=os.getenv(ap_cfg.get("api_key_env", "APOLLO_API_KEY"), ""),
        base_url=ap_cfg.get("base_url", "https://api.apollo.io/v1"),
        enrich_by_domain_path=ap_cfg.get("company_enrich_by_domain_path", "/companies/enrich"),
        enrich_by_id_path=ap_cfg.get("company_enrich_by_id_path", "/companies/enrich"),
        search_by_name_path=ap_cfg.get("company_search_by_name_path", "/mixed_companies/search"),
        search_by_salesforce_id_path=ap_cfg.get("company_search_by_salesforce_id_path", "/companies/search"),
        timeout=cfg.get("http", {}).get("timeout_seconds", 30),
    )

    df = pd.read_csv(args.input, dtype=str, keep_default_na=False).fillna("")

    mapping = cfg.get("mapping", {})
    if not any(mapping.values()):
        # simple interactive mapping if nothing set
        cols = list(df.columns)
        print("No mapping provided; select columns by number or press Enter to skip.")
        for key in ["zoominfo_id", "apollo_id", "salesforce_id", "name", "website"]:
            print(f"Select column for {key}:")
            for i, c in enumerate(cols):
                print(f"  [{i}] {c}")
            sel = input("Enter number (or blank to skip): ").strip()
            mapping[key] = cols[int(sel)] if sel.isdigit() and 0 <= int(sel) < len(cols) else None

    sleep_zi = rate_limiter(cfg.get("rate_limits", {}).get("zoominfo_per_min", 50))
    sleep_ap = rate_limiter(cfg.get("rate_limits", {}).get("apollo_per_min", 50))

    out_rows = []
    total = len(df)
    for i, row in df.iterrows():
        enriched = do_enrich_row(
            row,
            mapping,
            {"zoominfo": zi, "apollo": ap},
            {
                "prefix_zoominfo": cfg.get("output", {}).get("prefix_zoominfo", "zi"),
                "prefix_apollo": cfg.get("output", {}).get("prefix_apollo", "ap"),
                "include_input_columns": cfg.get("output", {}).get("include_input_columns", True),
                "max_attempts": cfg.get("retries", {}).get("max_attempts", 5),
            },
        )
        out_rows.append(enriched)
        time.sleep(max(sleep_zi, sleep_ap))

    if not out_rows:
        print("No rows processed; nothing to write.")
        return

    # Write CSV with deterministic column order
    base_cols = list(df.columns) if cfg.get("output", {}).get("include_input_columns", True) else []
    all_keys = set().union(*[set(r.keys()) for r in out_rows])
    vendor_cols = sorted([k for k in all_keys if k not in base_cols])
    columns = base_cols + vendor_cols

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for r in out_rows:
            writer.writerow(r)

    print(f"Wrote {args.output}")

if __name__ == "__main__":
    main()
