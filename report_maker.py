#!/usr/bin/env python3
"""
report_maker.py — FINAL per-column report (plus Producer sheet) with scan logs.

Excel output:
  • Sheet "Producer": Invoice No., Producer, Fail Reason
  • Sheet "Final"   : Invoice No., Mirakl Order, Mirakl Refund, Vertex,
                      IP - US, IP - UK, PIX, Fail Reason

Config json supports ${ROOT_PATH} and ~.
Expected headers in each file (exact spellings):
  Producer.xlsx                -> ['Invoice No.', 'Producer', 'Fail Reason']
  Mirakl_NewRelic.xlsx         -> ['Invoice No.', 'Mirakl Order', 'Mirakl Refund', 'Fail Reason']
  JSON_Comparator.xlsx         -> ['Invoice No.', 'Mirakl Order', 'Mirakl Refund', 'Fail Reason']
  Vertex_Consumer.xlsx         -> ['Invoice No.', 'Vertex Consumer', 'Fail Reason']
  IP_US_Consumer.xlsx          -> ['Invoice No.', 'IP-US Consumer', 'Fail Reason']
  IP_UK_Consumer.xlsx          -> ['Invoice No.', 'IP-UK Consumer', 'Fail Reason']
  PIX_Consumer.xlsx            -> ['Invoice No.', 'PIX Consumer', 'Fail Reason']
  Vertex_File_Comparator.xlsx  -> ['Invoice No.', 'Vertex', 'Fail Reason']
  IP_File_Comparator.xlsx      -> ['Invoice No.', 'IP-US', 'IP-UK', 'Fail Reason']
  PIX_XML_Comparator.xlsx      -> ['Invoice No.', 'PIX', 'Fail Reason']
"""

from __future__ import annotations
import argparse, json, os
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import pandas as pd

# ---- mapping: which source file/column feed each FINAL column ----
FINAL_GROUPS = {
    "Mirakl Order": [("Mirakl_NewRelic", "Mirakl Order"),
                     ("JSON_Comparator", "Mirakl Order")],
    "Mirakl Refund": [("Mirakl_NewRelic", "Mirakl Refund"),
                      ("JSON_Comparator", "Mirakl Refund")],
    "Vertex": [("Vertex_Consumer", "Vertex Consumer"),
               ("Vertex_File_Comparator", "Vertex")],
    "IP - US": [("IP_US_Consumer", "IP-US Consumer"),
                ("IP_File_Comparator", "IP-US")],
    "IP - UK": [("IP_UK_Consumer", "IP-UK Consumer"),
                ("IP_File_Comparator", "IP-UK")],
    "PIX": [("PIX_Consumer", "PIX Consumer"),
            ("PIX_XML_Comparator", "PIX")],
}
FINAL_ORDER = ["Invoice No.", "Mirakl Order", "Mirakl Refund", "Vertex", "IP - US", "IP - UK", "PIX", "Fail Reason"]

# ---------------- config + utils ----------------
def expand_env_str(s: str) -> str:
    return os.path.expanduser(os.path.expandvars(s))

def expand_env_deep(obj):
    if isinstance(obj, dict):  return {k: expand_env_deep(v) for k, v in obj.items()}
    if isinstance(obj, list):  return [expand_env_deep(v) for v in obj]
    if isinstance(obj, str):   return expand_env_str(obj)
    return obj

def load_config(path: Path) -> dict:
    print(f"[CONFIG] Using: {path}", flush=True)
    cfg = json.loads(Path(path).read_text())
    for k, v in cfg.get("env", {}).items():
        os.environ[str(k)] = expand_env_str(str(v))
        print(f"[ENV] {k}={os.environ[str(k)]}", flush=True)
    return expand_env_deep(cfg)

def _norm_status(val: Optional[str]) -> str:
    if val is None: return "NA"
    s = str(val).strip().lower()
    if s in {"pass","passed","ok","success"}: return "Pass"
    if s in {"fail","failed","error","ko"}:   return "Fail"
    if s in {"","na","n/a","none","null"}:    return "NA"
    return "NA"

def _load_df(p: Path) -> Optional[pd.DataFrame]:
    if not p.exists():
        print(f"[SCAN] MISSING: {p}", flush=True)
        return None
    try:
        df = pd.read_excel(p) if p.suffix.lower() in {".xlsx",".xls"} else pd.read_csv(p)
        print(f"[SCAN] LOADED : {p} (rows={len(df)}, cols={len(df.columns)})", flush=True)
        return df
    except Exception as e:
        print(f"[SCAN] ERROR  : {p} ({type(e).__name__}: {e})", flush=True)
        return None

def _load_datasets(cfg: dict) -> Dict[str, Optional[pd.DataFrame]]:
    out = {}
    for key, file_path in cfg.get("datasets", {}).items():
        p = Path(file_path)
        print(f"[SCAN] {key} -> {p}", flush=True)
        out[key] = _load_df(p)
    return out

def _lookup(df: pd.DataFrame, invoice: str, status_col: str) -> Tuple[Optional[str], Optional[str]]:
    if df is None or "Invoice No." not in df.columns: return None, None
    m = df[df["Invoice No."].astype(str) == str(invoice)]
    if m.empty: return None, None
    row = m.iloc[0]
    return (None if pd.isna(row.get(status_col)) else str(row.get(status_col)),
            None if pd.isna(row.get("Fail Reason")) else str(row.get("Fail Reason")))

def _derive_invoices(cfg: dict, datasets: Dict[str, Optional[pd.DataFrame]]) -> List[str]:
    ds_cfg = cfg.get("datasets", {})
    if not ds_cfg: raise SystemExit("No datasets configured.")
    source_key = "Producer" if "Producer" in ds_cfg else next(iter(ds_cfg))
    df = datasets.get(source_key)
    if df is None: raise SystemExit(f"Dataset '{source_key}' could not be loaded.")
    if "Invoice No." not in df.columns: raise SystemExit(f"Dataset '{source_key}' missing 'Invoice No.'")
    seen, invs = set(), []
    for v in df["Invoice No."]:
        if pd.isna(v): continue
        s = str(v)
        if s not in seen:
            seen.add(s); invs.append(s)
    print(f"[INVOICES] Using {len(invs)} from {source_key}", flush=True)
    return invs

def _summarize_missing(datasets: Dict[str, Optional[pd.DataFrame]], invoices: List[str]):
    for name, df in datasets.items():
        if df is None: print(f"[MISS] {name}: file not loaded", flush=True); continue
        if "Invoice No." not in df.columns: print(f"[MISS] {name}: no 'Invoice No.' column", flush=True); continue
        have = set(str(x) for x in df["Invoice No."].dropna().astype(str))
        miss = [inv for inv in invoices if inv not in have]
        if miss: print(f"[MISS] {name}: missing {len(miss)} -> {', '.join(miss)}", flush=True)
        else:    print(f"[OK]   {name}: all {len(invoices)} invoices present", flush=True)

# --------------- build sheets ---------------
def build_producer_sheet(invoices: List[str], producer_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for inv in invoices:
        if producer_df is None or "Invoice No." not in producer_df.columns:
            rows.append({"Invoice No.": inv, "Producer": "Fail", "Fail Reason": "[Producer] file not loaded or schema mismatch"})
            continue
        m = producer_df[producer_df["Invoice No."].astype(str) == str(inv)]
        if m.empty:
            rows.append({"Invoice No.": inv, "Producer": "Fail", "Fail Reason": f"[Producer] Missing invoice {inv}"})
        else:
            r = m.iloc[0]
            s = _norm_status(r.get("Producer"))
            reason = str(r.get("Fail Reason")) if (s == "Fail" and str(r.get("Fail Reason")).strip()) else ""
            rows.append({"Invoice No.": inv, "Producer": s, "Fail Reason": reason})
    return pd.DataFrame(rows, columns=["Invoice No.", "Producer", "Fail Reason"])

def build_final_sheet(invoices: List[str], datasets: Dict[str, Optional[pd.DataFrame]]) -> pd.DataFrame:
    out_rows = []
    for inv in invoices:
        row = {c: "" for c in FINAL_ORDER}
        row["Invoice No."] = inv
        reasons: List[str] = []

        for final_col, providers in FINAL_GROUPS.items():
            all_present = True
            all_pass = True
            all_na = True
            for ds_name, status_col in providers:
                status_raw, fail_raw = _lookup(datasets.get(ds_name), inv, status_col)
                if status_raw is None:
                    all_present = False
                    all_pass = False
                    all_na = False
                    reasons.append(f"[{ds_name}] Missing invoice {inv}")
                    continue
                status = _norm_status(status_raw)
                if status != "Pass": all_pass = False
                if status != "NA":   all_na = False
                if status == "Fail":
                    if fail_raw and str(fail_raw).strip():
                        reasons.append(f"[{ds_name}] {fail_raw}")
                    else:
                        reasons.append(f"[{ds_name}] {status_col}: Fail")

            # decide final status for the column
            if not all_present:
                col_status = "Fail"
            elif all_pass:
                col_status = "Pass"
            elif all_na:
                col_status = "NA"
            else:
                col_status = "Fail"
            row[final_col] = col_status

        # de-dup reasons keep order
        if reasons:
            seen = set(); uniq = []
            for r in reasons:
                if r not in seen:
                    seen.add(r); uniq.append(r)
            row["Fail Reason"] = ", ".join(uniq)
        out_rows.append(row)

    return pd.DataFrame(out_rows, columns=FINAL_ORDER)

# --------------- main ---------------
def main():
    ap = argparse.ArgumentParser(description="Final per-column report (plus Producer sheet).")
    ap.add_argument("--config", required=True, help="Path to config.json")
    ap.add_argument("--invoice", action="append", help="Invoice No. (repeatable). If omitted, derive from Producer/first dataset.")
    ap.add_argument("--excel", default=None, help="Excel output path override")
    args = ap.parse_args()

    cfg = load_config(Path(args.config))
    datasets = _load_datasets(cfg)

    invoices = args.invoice if args.invoice else _derive_invoices(cfg, datasets)
    _summarize_missing(datasets, invoices)

    producer_df = datasets.get("Producer")
    producer_sheet = build_producer_sheet(invoices, producer_df)
    final_sheet    = build_final_sheet(invoices, datasets)

    excel_out = args.excel or cfg.get("output", {}).get("excel")
    if not excel_out:
        base = Path(args.config).parent
        excel_out = str(base / f"report_{invoices[0]}.xlsx")

    p = Path(excel_out); p.parent.mkdir(parents=True, exist_ok=True)
    for eng in ("xlsxwriter","openpyxl"):
        try:
            with pd.ExcelWriter(p, engine=eng) as w:
                producer_sheet.to_excel(w, sheet_name="Producer", index=False)
                final_sheet.to_excel(w, sheet_name="Final", index=False)
            print(f"[WRITE] Excel: {p}", flush=True)
            break
        except Exception as e:
            print(f"[WRITE] Excel engine error ({eng}): {e}", flush=True)

    print("Done.", flush=True)

if __name__ == "__main__":
    main()
