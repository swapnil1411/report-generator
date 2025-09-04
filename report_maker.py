#!/usr/bin/env python3
import json
import os
from pathlib import Path
from typing import Dict, List, Optional
import argparse
import pandas as pd

# ----------------- Helpers -----------------
def expand_env_str(s: str) -> str:
    root_path = os.getenv("ROOT_PATH", ".")
    s = s.replace("${ROOT_PATH}", root_path)
    return os.path.expanduser(os.path.expandvars(s))

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
        "Tracking_ID_OR_Unique_Key","Tracking_ID","Unique_Key",
        "Invoice No.","Invoice_No","Invoice","Tracking ID",
    ]
    exact = {c.strip(): c for c in cols}
    for p in preferred:
        if p in exact: return exact[p]
    for c in cols:
        cl = c.lower().replace("_", " ")
        if "invoice" in cl or "tracking" in cl or "unique" in cl:
            return c
    return None

def detect_kind_from_key(key: str) -> str:
    k = key.lower()
    if "json_comparator" in k: return "json_comparator"
    if "comparator" in k: return "comparator"
    if "producer" in k: return "producer"
    if "consumer" in k: return "consumer"
    if "newrelic" in k or "new_relic" in k: return "newrelic"
    return "newrelic"

def first_row_for_id(df: pd.DataFrame, id_col: str, id_val):
    try:
        sub = df.loc[df[id_col] == id_val]
        if not sub.empty:
            return sub.iloc[0]
    except Exception:
        pass
    return None

def non_id_columns(row: pd.Series, id_col: str) -> List[str]:
    return [c for c in row.index if c != id_col]

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

def score_comparator_classic_value(val) -> str:
    s = norm_str(val).lower()
    if s == "yes": return "Pass"
    if s in {"na", "n/a", "missing"}: return "NA"
    if s == "no" or is_failed_pattern(s): return "Fail"
    return "Fail"

def score_comparator_json_status(status_val) -> str:
    s = norm_str(status_val).lower()
    pass_tokens = {"yes","y","true","1","pass","passed","success","ok"}
    fail_tokens = {"no","n","false","0","fail","failed","error",""}
    if s in pass_tokens: return "Pass"
    if s in fail_tokens or is_failed_pattern(s): return "Fail"
    return "Fail"  # conservative

def score_all_yes(row: pd.Series, id_col: str) -> str:
    for c in non_id_columns(row, id_col):
        if not is_yes(row[c]):
            return "Fail"
    return "Pass"

# --- Reason extraction: ONLY explicit reasons present in fields ---
def maybe_reason_from_value(value: str) -> Optional[str]:
    """
    Return a reason string only if the cell contains an explicit failure reason,
    e.g., 'failed-str{...}', 'error...', 'exception...'.
    Otherwise return None (no reason appended).
    """
    s = norm_str(value)
    if not s:
        return None
    if is_failed_pattern(s):
        return s
    return None

# ----------------- Main consolidate -----------------
def consolidate(cfg_path: Path) -> Path:
    cfg_text = Path(cfg_path).read_text()
    cfg_text = expand_env_str(cfg_text)
    cfg = json.loads(cfg_text)

    prod_path = Path(expand_env_str(cfg["producer"]))
    files_map: Dict[str, str] = {k: expand_env_str(v) for k, v in cfg.get("files", {}).items()}
    out_dir = Path(expand_env_str(cfg.get("output") or "."))

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
        p = Path(files_map[key])
        df = read_excel(p)
        dfs[key] = df
        id_cols[key] = pick_id_col(list(df.columns)) if df is not None else None
        kinds[key] = detect_kind_from_key(key)

    # Build rows
    rows = []
    for inv in invoices:
        row = {"Invoice No.": inv}
        reasons: Dict[str, List[str]] = {}

        # Producer
        prod_row = first_row_for_id(df_prod, id_col, inv)
        if prod_row is None:
            row["producer"] = "Fail"
            # NO diagnostic reason appended
        else:
            val = prod_row[prod_status_col] if (prod_status_col and prod_status_col in prod_row.index) else None
            row["producer"] = score_producer(val)
            # Append explicit reason only if value itself carries one
            rtxt = maybe_reason_from_value(val)
            if rtxt:
                reasons.setdefault("producer", []).append(f"Posted_To_Producer_Topic?={rtxt}")

        # Other files
        for key in keys_in_order:
            df = dfs.get(key)
            kind = kinds.get(key, "newrelic")
            invc = id_cols.get(key)

            if df is None or not invc or invc not in (df.columns if df is not None else []):
                row[key] = "Fail"
                # NO diagnostic reason appended
                continue

            sub = df.loc[df[invc] == inv] if df is not None else pd.DataFrame()
            if sub.empty:
                row[key] = "Fail"
                # NO diagnostic reason appended
                continue

            r = sub.iloc[0]

            if kind == "consumer":
                app_col = next((c for c in r.index if "applicable_for_consumer_topic" in c.lower()), None)
                post_col = next((c for c in r.index if "posted_to_consumer_topic" in c.lower()), None)
                app_val = r[app_col] if app_col else None
                post_val = r[post_col] if post_col else None
                status = score_consumer(app_val, post_val)
                row[key] = status
                # Only append explicit reasons
                for col_name, cell in (("Applicable", app_val), ("Posted", post_val)):
                    rr = maybe_reason_from_value(cell)
                    if rr:
                        reasons.setdefault(key, []).append(f"{col_name}={rr}")

            elif kind == "comparator":
                cmp_col = next((c for c in r.index if "expected" in c.lower() and "observed" in c.lower() and "match" in c.lower()), None)
                if cmp_col:
                    v = r[cmp_col]
                    status = score_comparator_classic_value(v)
                    row[key] = status
                    rr = maybe_reason_from_value(v)
                    if rr:
                        reasons.setdefault(key, []).append(f"{cmp_col}={rr}")
                else:
                    # Fallback: all non-ID columns must be Yes
                    status = score_all_yes(r, invc)
                    row[key] = status
                    if status != "Pass":
                        for c in non_id_columns(r, invc):
                            rr = maybe_reason_from_value(r[c])
                            if rr:
                                reasons.setdefault(key, []).append(f"{c}={rr}")

            elif kind == "json_comparator":
                # Prefer Status/Reason if present
                status_col = next((c for c in r.index if c.lower() == "status"), None)
                reason_col = next((c for c in r.index if c.lower() == "reason"), None)
                if status_col:
                    status_val = r[status_col]
                    status = score_comparator_json_status(status_val)
                    row[key] = status
                    # reason from Status if it's a failed-* or error-like
                    rr = maybe_reason_from_value(status_val)
                    if rr:
                        reasons.setdefault(key, []).append(f"{status_col}={rr}")
                    # reason column text (always include if non-empty and row failed)
                    if status != "Pass" and reason_col:
                        reason_text = norm_str(r[reason_col])
                        if reason_text:
                            reasons.setdefault(key, []).append(f"{reason_col}={reason_text}")
                else:
                    # Fallback: all non-ID columns must be Yes
                    status = score_all_yes(r, invc)
                    row[key] = status
                    if status != "Pass":
                        for c in non_id_columns(r, invc):
                            rr = maybe_reason_from_value(r[c])
                            if rr:
                                reasons.setdefault(key, []).append(f"{c}={rr}")

            elif kind == "producer":
                pcol = next((c for c in r.index if c.lower().startswith("posted_to_producer_topic")), None)
                val2 = r[pcol] if pcol else None
                status = score_producer(val2)
                row[key] = status
                rr = maybe_reason_from_value(val2)
                if rr:
                    reasons.setdefault(key, []).append(f"{pcol or 'Posted_To_Producer_Topic?'}={rr}")

            else:  # newrelic-like
                status = score_all_yes(r, invc)
                row[key] = status
                if status != "Pass":
                    for c in non_id_columns(r, invc):
                        rr = maybe_reason_from_value(r[c])
                        if rr:
                            reasons.setdefault(key, []).append(f"{c}={rr}")

        # Final Result (NA still counts as Fail)
        per_cols = [c for c in row.keys() if c != "Invoice No."]
        final_pass = all(str(row[c]).strip() == "Pass" for c in per_cols)
        row["Final Result"] = "Pass" if final_pass else "Fail"

        # Reason column (only explicit reasons we collected)
        row["Reason"] = "; ".join(f"{k}=[{', '.join(v)}]" for k, v in reasons.items()) if reasons else ""

        rows.append(row)

    # Output
    columns = ["Invoice No.", "producer"] + keys_in_order + ["Final Result", "Reason"]
    final_df = pd.DataFrame(rows)[columns]

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "consolidated_report.xlsx"
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        final_df.to_excel(writer, index=False, sheet_name="Consolidated")

    print(str(out_path))
    return out_path

def main():
    ap = argparse.ArgumentParser(description="Report Maker Consolidated (explicit reasons only)")
    ap.add_argument("--config", help="Path to config JSON (defaults to ${ROOT_PATH}/config.json)")
    args = ap.parse_args()

    root_path = os.getenv("ROOT_PATH", ".")
    cfg_path = Path(args.config if args.config else f"{root_path}/config.json")
    consolidate(cfg_path)

if __name__ == "__main__":
    main()
