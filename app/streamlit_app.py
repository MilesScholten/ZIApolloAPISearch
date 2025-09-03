#!/usr/bin/env python3
import os, sys
HERE = os.path.dirname(__file__)
ROOT = os.path.abspath(os.path.join(HERE, ".."))
SRC = os.path.join(ROOT, 'src')
if SRC not in sys.path:
    sys.path.insert(0, SRC)

# streamlit run app/streamlit_app.py
import os, sys
HERE = os.path.dirname(__file__)
ROOT = os.path.abspath(os.path.join(HERE, ".."))
SRC = os.path.join(ROOT, 'src')
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from __future__ import annotations
import os, time, json, pandas as pd, streamlit as st
from typing import Dict, Any, Optional, List
from enrichment.utils import rate_limiter, sanitize_domain
from enrichment.clients.zoominfo import ZoomInfoClient
from enrichment.clients.apollo import ApolloClient
from enrichment.logic import do_enrich_row

st.set_page_config(page_title="Company Enrichment (ZoomInfo + Apollo)", layout="wide")
st.title("Company Enrichment — ZoomInfo + Apollo")
st.caption("Matching priorities: ZoomInfo ID → Domain → Name; Apollo ID → Salesforce ID → Domain → Name")

with st.sidebar:
    st.header("API Keys")
    zi_key = st.text_input("ZoomInfo API Key", type="password", value=os.getenv("ZOOMINFO_API_KEY", ""))
    ap_key = st.text_input("Apollo API Key", type="password", value=os.getenv("APOLLO_API_KEY", ""))

    st.header("Options")
    zi_per_min = st.number_input("ZoomInfo max calls / min", min_value=0, value=50)
    ap_per_min = st.number_input("Apollo max calls / min", min_value=0, value=50)
    max_attempts = st.number_input("Max retry attempts", min_value=1, value=5)
    prefix_zi = st.text_input("ZoomInfo prefix", value="zi")
    prefix_ap = st.text_input("Apollo prefix", value="ap")
    include_inputs = st.checkbox("Include input columns in output", value=True)

tab_upload, tab_mapping, tab_run, tab_output = st.tabs(["1) Upload", "2) Map Columns", "3) Run", "4) Output & Rename"])

with tab_upload:
    st.subheader("Upload Source CSV")
    src = st.file_uploader("CSV with identifiers (IDs, names, websites)", type=["csv"])
    st.caption("Headers can be anything — you'll map them in the next step.")
    st.divider()
    st.subheader("Optional: Salesforce Accounts CSV (for backfilling domain by SF Account ID)")
    sf = st.file_uploader("Salesforce Accounts CSV (optional)", type=["csv"])

    if src:
        df_src = pd.read_csv(src, dtype=str, keep_default_na=False).fillna("")
        st.write("Sample rows:")
        st.dataframe(df_src.head(10))
        st.session_state["df_src"] = df_src
    if sf:
        df_sf = pd.read_csv(sf, dtype=str, keep_default_na=False).fillna("")
        st.write("Salesforce sample rows:")
        st.dataframe(df_sf.head(10))
        st.session_state["df_sf"] = df_sf

with tab_mapping:
    st.subheader("Map Input Columns")
    df_src = st.session_state.get("df_src")
    if df_src is None:
        st.info("Upload your source CSV in the 'Upload' tab first.")
    else:
        columns = [""] + list(df_src.columns)
        c1, c2, c3 = st.columns(3)
        with c1:
            col_zoominfo_id = st.selectbox("ZoomInfo Company ID", options=columns, index=0)
            col_apollo_id = st.selectbox("Apollo Company ID", options=columns, index=0)
        with c2:
            col_salesforce_id = st.selectbox("Salesforce Account ID", options=columns, index=0)
            col_name = st.selectbox("Account / Company Name", options=columns, index=0)
        with c3:
            col_website = st.selectbox("Website", options=columns, index=0)

        st.session_state["mapping_in"] = {
            "zoominfo_id": col_zoominfo_id or None,
            "apollo_id": col_apollo_id or None,
            "salesforce_id": col_salesforce_id or None,
            "name": col_name or None,
            "website": col_website or None
        }

        st.divider()
        st.subheader("Optional: Map Salesforce fields → Source")
        df_sf = st.session_state.get("df_sf")
        if df_sf is not None:
            sf_cols = [""] + list(df_sf.columns)
            c4, c5 = st.columns(2)
            with c4:
                sf_id_col = st.selectbox("SF: Account ID column", options=sf_cols, index=0, key="sf_id")
            with c5:
                sf_domain_col = st.selectbox("SF: Website/Domain column", options=sf_cols, index=0, key="sf_domain")

            st.session_state["sf_mapping"] = {
                "sf_id": sf_id_col or None,
                "sf_domain": sf_domain_col or None
            }

with tab_run:
    st.subheader("Run Enrichment")
    df_src = st.session_state.get("df_src")
    if df_src is None:
        st.info("Upload your source CSV in the 'Upload' tab first.")
    else:
        run_btn = st.button("Start Enrichment")
        if run_btn:
            mapping_in = st.session_state.get("mapping_in", {})
            sf_mapping = st.session_state.get("sf_mapping", {})
            df_sf = st.session_state.get("df_sf")

            # Optional backfill from Salesforce CSV
            if df_sf is not None and mapping_in.get("salesforce_id") and sf_mapping.get("sf_id"):
                sf_id_col = sf_mapping.get("sf_id")
                sf_dom_col = sf_mapping.get("sf_domain")
                if sf_id_col and sf_dom_col and sf_id_col in df_sf.columns and sf_dom_col in df_sf.columns:
                    sf_map = dict(zip(df_sf[sf_id_col].astype(str), df_sf[sf_dom_col].astype(str)))
                else:
                    sf_map = {}
            else:
                sf_map = {}

            cfg = {
                "prefix_zoominfo": prefix_zi,
                "prefix_apollo": prefix_ap,
                "include_input_columns": include_inputs,
                "max_attempts": int(max_attempts),
                "sleep_zi": rate_limiter(int(zi_per_min)),
                "sleep_ap": rate_limiter(int(ap_per_min))
            }

            vendors = {
                "zoominfo": ZoomInfoClient(zi_key),
                "apollo": ApolloClient(ap_key)
            }

            out_rows: List[Dict[str, Any]] = []
            total = len(df_src)
            prog = st.progress(0, text="Starting...")
            for i, row in df_src.iterrows():
                # Backfill website from SF if missing
                if mapping_in.get("salesforce_id") and mapping_in.get("website"):
                    sfid = str(row[mapping_in["salesforce_id"]]) if mapping_in["salesforce_id"] else ""
                    if sfid and (not str(row[mapping_in["website"]]).strip()):
                        if sfid in sf_map:
                            row[mapping_in["website"]] = sf_map[sfid]

                enriched = do_enrich_row(row, mapping_in, vendors, cfg)
                out_rows.append(enriched)
                pct = int(((i + 1) / total) * 100)
                prog.progress(min(pct, 100), text=f"Processed {i+1}/{total} rows")
                time.sleep(max(cfg["sleep_zi"], cfg["sleep_ap"]))

            df_out = pd.DataFrame(out_rows)
            st.session_state["df_out"] = df_out
            st.success(f"Done. Enriched {len(df_out)} rows. Move to the 'Output & Rename' tab.")

with tab_output:
    st.subheader("Preview & Output")
    df_out = st.session_state.get("df_out")
    if df_out is None:
        st.info("Run enrichment first.")
    else:
        st.write("Preview of enriched output:")
        st.dataframe(df_out.head(20))

        st.markdown("#### Optional: Rename Output Columns")
        st.caption("Provide a JSON object mapping CURRENT column names to DESIRED names.")
        # Seed with some common suggestions
        common_suggestions = {}
        for col in df_out.columns:
            if col.endswith("_match") or col.endswith("_error"):
                common_suggestions[col] = col
            if ".name" in col or "_name" in col:
                common_suggestions[col] = "Company Name"
            if ".domain" in col or "_domain" in col:
                common_suggestions[col] = "Domain"
            if ".website" in col or "_website" in col:
                common_suggestions[col] = "Website"
        seed = json.dumps(common_suggestions, indent=2)
        rename_json = st.text_area("Rename mapping JSON", value=seed, height=200)
        try:
            rename_map = json.loads(rename_json) if rename_json.strip() else {}
        except json.JSONDecodeError:
            st.warning("Invalid JSON; ignoring renames.")
            rename_map = {}

        df_final = df_out.rename(columns={k: v for k, v in rename_map.items() if k in df_out.columns and v})
        st.write("Final preview:")
        st.dataframe(df_final.head(20))

        csv_bytes = df_final.to_csv(index=False).encode("utf-8")
        st.download_button("Download CSV", csv_bytes, file_name="enriched_companies.csv", mime="text/csv")
