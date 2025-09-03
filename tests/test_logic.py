import pandas as pd
from enrichment.logic import do_enrich_row

class StubZ:
    def __init__(self): pass
    def company_by_id(self, cid, retries=3): return ({"id": cid, "name": "Z by id"}, None) if cid == "ZI-OK" else (None, "not found")
    def company_by_domain(self, d, retries=3): return ({"domain": d, "name": "Z by domain"}, None) if d == "ok.com" else (None, "not found")
    def company_by_name(self, n, retries=3): return ({"name": n}, None) if n == "Okay" else (None, "not found")

class StubA:
    def __init__(self): pass
    def company_by_id(self, aid, retries=3): return ({"id": aid, "name": "A by id"}, None) if aid == "AP-OK" else (None, "not found")
    def company_by_salesforce_id(self, s, retries=3): return ({"sfid": s, "name": "A by sfid"}, None) if s == "SF-OK" else (None, "not found")
    def company_by_domain(self, d, retries=3): return ({"domain": d, "name": "A by domain"}, None) if d == "ok.com" else (None, "not found")
    def company_by_name(self, n, retries=3): return ({"name": n}, None) if n == "Okay" else (None, "not found")

def test_do_enrich_row_priority():
    row = pd.Series({"zi_id": "ZI-OK", "ap_id": "", "sf_id": "", "name": "Okay", "website": "ok.com"})
    mapping = {"zoominfo_id": "zi_id", "apollo_id": "ap_id", "salesforce_id": "sf_id", "name": "name", "website": "website"}
    out = do_enrich_row(row, mapping, {"zoominfo": StubZ(), "apollo": StubA()}, {"prefix_zoominfo": "zi", "prefix_apollo": "ap"})
    assert out["zi_match"] is True
    assert out["ap_match"] is True  # Apollo domain should also match

def test_do_enrich_row_fallbacks():
    row = pd.Series({"zi_id": "", "ap_id": "AP-OK", "sf_id": "", "name": "Okay", "website": ""})
    mapping = {"zoominfo_id": "zi_id", "apollo_id": "ap_id", "salesforce_id": "sf_id", "name": "name", "website": "website"}
    out = do_enrich_row(row, mapping, {"zoominfo": StubZ(), "apollo": StubA()}, {"prefix_zoominfo": "zi", "prefix_apollo": "ap"})
    assert out["ap_match"] is True
    # ZoomInfo falls back to name
    assert out["zi_match"] is True
