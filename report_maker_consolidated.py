#!/usr/bin/env python3
import json
from pathlib import Path
from typing import Dict, List, Optional
import argparse
import pandas as pd

# ----------------- Helpers -----------------
def norm_str(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    return str(v).strip()

def is_yes(v: str) -> bool:
    return norm_str(v).lower() in {"yes", "y", "true", "1"}

def is_no(v: str) -> bool:
    return norm_str(v).lower() in {"no", "n", "false", "0"}

def is_na(v: str) -> bool:
    return norm_str(v).lower() in {"na", "n/a", "not applicable", "none", ""}

def is_failed_pattern(v: str) -> bool:
    s = norm_str(v).lower()
    return s.startswith("failed-") or s.startswith("fail-") or s.startswith("error") or s.startswith("exception")

def read_excel(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        print(f"[WARN] Missing file: {path}")
        return None
    try:
        df = pd.read_excel(path)
        df.columns = [str(c).strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"[ERROR] Failed to read {path}: {e}")
        return None

def pick_id_col(cols: List[str]) -> Optional[str]:
    preferred = [
        "Tracking_ID_OR_Unique_Key",
        "Tracking_ID",
        "Unique_Key",
        "Invoice No.",
        "Invoice_No",
        "Invoice",
        "Tracking ID",
    ]
    exact = {c.strip(): c for c in cols}
    for p in preferred:
        if p in exact:
            return exact[p]
    for c in cols:
        cl = c.lower().replace("_", " ")
        if "invoice" in cl or "tracking" in cl or "unique" in cl:
            return c
    return None

def detect_kind(key: str, path: str) -> str:
    s = f"{key} {path}".lower()
    if "producer" in s: return "producer"
    if "consumer" in s: return "consumer"
    if "comparator" in s: return "comparator"
    if "newrelic" in s or "new_relic" in s: return "newrelic"
    return "newrelic"

def first_row_for_id(df: pd.DataFrame, id_col: str, id_val):
    try:
        sub = df.loc[df[id_col] == id_val]
        if not sub.empty:
            return sub.iloc[0]
    except Exception:
        pass
    return None

# ----------------- Scoring -----------------
def score_producer(value) -> str:
    if is_yes(value): return "Pass"
    if is_failed_pattern(value) or is_no(value) or is_na(value) or norm_str(value) == "": return "Fail"
    return "Fail"

def score_consumer(applicable, posted) -> str:
    if is_no(applicable): return "NA"
    if is_yes(applicable) and is_yes(posted): return "Pass"
    if is_failed_pattern(applicable) or is_failed_pattern(posted): return "Fail"
    return "Fail"

def score_comparator(val) -> str:
    s = norm_str(val).lower()
    if s == "yes": return "Pass"
    if s in {"na", "n/a", "missing"}: return "NA"
    if s == "no" or is_failed_pattern(s): return "Fail"
    return "Fail"

def score_newrelic_row(row: pd.Series, id_col: str) -> str:
    for c in row.index:
        if c == id_col: continue
        if not is_yes(row[c]):
            return "Fail"
    return "Pass"

# ----------------- Main consolidate -----------------
def consolidate(cfg_path: Path) -> Path:
    cfg = json.loads(cfg_path.read_text())

    prod_path = Path(cfg["producer"]).expanduser()
    files_map: Dict[str, str] = cfg.get("files", {})
    out_dir = Path(cfg.get("output") or ".").expanduser()

    # Producer for IDs
    df_prod = read_excel(prod_path)
    if df_prod is None:
        raise SystemExit(f"Producer file not found or unreadable: {prod_path}")
    id_col = pick_id_col(list(df_prod.columns))
    if not id_col:
        raise SystemExit("Could not detect Tracking/Invoice column in producer file")
    invoices = df_prod[id_col].dropna().astype(str).map(str).tolist()

    # Producer status col
    prod_status_cols = [c for c in df_prod.columns if c.lower().startswith("posted_to_producer_topic")]
    prod_status_col = prod_status_cols[0] if prod_status_cols else None

    # Preload other files in config order
    keys_in_order = list(files_map.keys())
    dfs: Dict[str, Optional[pd.DataFrame]] = {}
    id_cols: Dict[str, Optional[str]] = {}
    kinds: Dict[str, str] = {}
    for key in keys_in_order:
        p = Path(files_map[key]).expanduser()
        df = read_excel(p)
        dfs[key] = df
        id_cols[key] = pick_id_col(list(df.columns)) if df is not None else None
        kinds[key] = detect_kind(key, str(p))

    # Build rows
    rows = []
    for inv in invoices:
        row = {"Invoice No.": inv}

        # Producer column first
        prod_row = first_row_for_id(df_prod, id_col, inv)
        if prod_row is None:
            row["producer"] = "Fail"
        else:
            val = prod_row[prod_status_col] if (prod_status_col and prod_status_col in prod_row.index) else None
            row["producer"] = score_producer(val)

        # Then configured files in order
        for key in keys_in_order:
            df = dfs.get(key)
            kind = kinds.get(key, "newrelic")
            invc = id_cols.get(key)

            if df is None or not invc or invc not in df.columns:
                row[key] = "Fail"
                continue

            sub = df.loc[df[invc] == inv]
            if sub.empty:
                row[key] = "Fail"
                continue

            r = sub.iloc[0]
            if kind == "consumer":
                app_cols = [c for c in df.columns if c.lower().startswith("applicable_for_consumer_topic")]
                post_cols = [c for c in df.columns if c.lower().startswith("posted_to_consumer_topic")]
                app_col = app_cols[0] if app_cols else None
                post_col = post_cols[0] if post_cols else None
                app_val = r[app_col] if (app_col and app_col in r.index) else None
                post_val = r[post_col] if (post_col and post_col in r.index) else None
                row[key] = score_consumer(app_val, post_val)
            elif kind == "comparator":
                comp_cols = [c for c in df.columns if "expected" in c.lower() and "observed" in c.lower() and "match" in c.lower()]
                comp_col = comp_cols[0] if comp_cols else None
                v = r[comp_col] if (comp_col and comp_col in r.index) else None
                row[key] = score_comparator(v)
            elif kind == "producer":
                pcols = [c for c in df.columns if c.lower().startswith("posted_to_producer_topic")]
                pcol = pcols[0] if pcols else None
                val = r[pcol] if (pcol and pcol in r.index) else None
                row[key] = score_producer(val)
            else:
                row[key] = score_newrelic_row(r, invc)

        rows.append(row)

    # DataFrame in desired order: Invoice No., producer, files..., Final Result
    final_df = pd.DataFrame(rows)
    # Ensure the column order
    columns = ["Invoice No.", "producer"] + keys_in_order
    final_df = final_df.reindex(columns=columns)

    # Final Result (NA counts as Fail, as requested)
    per_cols = [c for c in final_df.columns if c != "Invoice No."]
    def final_result(sr: pd.Series) -> str:
        vals = [norm_str(sr[c]) for c in per_cols]
        return "Pass" if vals and all(v == "Pass" for v in vals) else "Fail"
    final_df["Final Result"] = final_df.apply(final_result, axis=1)

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "consolidated_report.xlsx"
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        final_df.to_excel(writer, index=False, sheet_name="Consolidated")

    print(str(out_path))
    return out_path

def main():
    ap = argparse.ArgumentParser(description="Report Maker Consolidated (single-sheet)")
    ap.add_argument("--config", required=True, help="Path to config JSON")
    args = ap.parse_args()
    consolidate(Path(args.config).expanduser())

if __name__ == "__main__":
    main()
