from __future__ import annotations
from typing import Dict, Any, Optional, List
import pandas as pd
from enrichment.utils import sanitize_domain, flatten

def do_enrich_row(row: pd.Series,
                  mapping_in: Dict[str, Optional[str]],
                  vendors: Dict[str, Any],
                  cfg: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}

    if cfg.get("include_input_columns", True):
        for c in row.index:
            out[c] = row[c]

    # Extract fields
    zi_id = str(row[mapping_in.get("zoominfo_id")]).strip() if mapping_in.get("zoominfo_id") else ""
    apollo_id = str(row[mapping_in.get("apollo_id")]).strip() if mapping_in.get("apollo_id") else ""
    sf_id = str(row[mapping_in.get("salesforce_id")]).strip() if mapping_in.get("salesforce_id") else ""
    name = str(row[mapping_in.get("name")]).strip() if mapping_in.get("name") else ""
    website = str(row[mapping_in.get("website")]).strip() if mapping_in.get("website") else ""
    domain = sanitize_domain(website)

    zc = vendors["zoominfo"]
    ac = vendors["apollo"]

    prefix_zi = cfg.get("prefix_zoominfo", "zi")
    prefix_ap = cfg.get("prefix_apollo", "ap")
    retries = cfg.get("max_attempts", 5)

    zi_obj, zi_err = None, None
    ap_obj, ap_err = None, None

    # ZoomInfo: ID -> domain -> name
    if zi_id:
        zi_obj, zi_err = zc.company_by_id(zi_id, retries=retries)
    if zi_obj is None and domain:
        zi_obj, zi_err = zc.company_by_domain(domain, retries=retries)
    if zi_obj is None and name:
        zi_obj, zi_err = zc.company_by_name(name, retries=retries)

    # Apollo: Apollo ID -> Salesforce ID -> domain -> name
    if apollo_id:
        ap_obj, ap_err = ac.company_by_id(apollo_id, retries=retries)
    if ap_obj is None and sf_id:
        ap_obj, ap_err = ac.company_by_salesforce_id(sf_id, retries=retries)
    if ap_obj is None and domain:
        ap_obj, ap_err = ac.company_by_domain(domain, retries=retries)
    if ap_obj is None and name:
        ap_obj, ap_err = ac.company_by_name(name, retries=retries)

    if zi_obj:
        out[f"{prefix_zi}_match"] = True
        out[f"{prefix_zi}_error"] = ""
        out.update(flatten(prefix_zi, zi_obj))
    else:
        out[f"{prefix_zi}_match"] = False
        out[f"{prefix_zi}_error"] = zi_err or "not_found"

    if ap_obj:
        out[f"{prefix_ap}_match"] = True
        out[f"{prefix_ap}_error"] = ""
        out.update(flatten(prefix_ap, ap_obj))
    else:
        out[f"{prefix_ap}_match"] = False
        out[f"{prefix_ap}_error"] = ap_err or "not_found"

    return out
