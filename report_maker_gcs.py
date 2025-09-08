#!/usr/bin/env python3
import json
import os
from typing import Dict, List, Optional, Tuple
import re
import argparse
import pandas as pd

# --- NEW: unified local + GCS I/O ---
from gcs_utils import (
    expand_env_str,          # expands ${ROOT_PATH}, env vars, ~ (works with gs:// too)
    read_text_any,           # read text from local or gs://
    read_excel_any,          # read excel from local or gs://
    write_excel_any,         # write excel to local or gs://
    is_gcs_path,             # detect gs://
)

# ----------------- Helpers -----------------
def norm_str(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    return str(v).strip()

# Positive / Negative token sets (case-insensitive)
POS_TOKENS = {"yes", "y", "true", "1", "pass", "passed", "success", "ok","Yes"}
NEG_TOKENS = {"no", "n", "false", "0", "fail", "failed", "na", "n/a", "not applicable", "none", ""}

def is_yes(v: str) -> bool:
    return norm_str(v).lower() in POS_TOKENS

def is_no(v: str) -> bool:
    s = norm_str(v).lower()
    return s in NEG_TOKENS

def is_na(v: str) -> bool:
    return norm_str(v).lower() in {"na", "n/a", "not applicable", "none", ""}

def is_failed_pattern(v: str) -> bool:
    s = norm_str(v).lower()
    return s.startswith("failed-") or s.startswith("fail-") or s.startswith("error") or s.startswith("exception")

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
    if "file_comparator" in k or ("file" in k and "comparator" in k):
        return "file_comparator"  # dedicated kind, handled like json_comparator
    if "json_comparator" in k:
        return "json_comparator"
    if "comparator" in k:
        return "comparator"
    if "producer" in k:
        return "producer"
    if "consumer" in k:
        return "consumer"
    if "newrelic" in k or "new_relic" in k:
        return "newrelic"
    return "newrelic"

def non_id_columns(row: pd.Series, id_col: str) -> List[str]:
    return [c for c in row.index if c != id_col]

# --- Robust ID matching ---
def norm_id_series(s: pd.Series) -> pd.Series:
    return s.astype(str).map(lambda x: str(x).strip())

def first_row_for_id(df: pd.DataFrame, id_col: str, id_val) -> Optional[pd.Series]:
    try:
        sub = df.loc[norm_id_series(df[id_col]) == str(id_val).strip()]
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
    if is_no(applicable) or is_na(applicable):
        return "Fail"
    if is_yes(applicable) and is_yes(posted):
        return "Pass"
    if is_failed_pattern(applicable) or is_failed_pattern(posted):
        return "Fail"
    return "Fail"

def score_comparator_classic_value(val) -> str:
    s = norm_str(val).lower()
    if s in POS_TOKENS: return "Pass"
    if s in {"na", "n/a", "missing"}: return "NA"
    if s in NEG_TOKENS or is_failed_pattern(s): return "Fail"
    return "Fail"

def score_comparator_json_status(status_val) -> str:
    s = norm_str(status_val).lower()
    if s in POS_TOKENS: return "Pass"
    if s in NEG_TOKENS or is_failed_pattern(s): return "Fail"
    return "Fail"

def score_all_yes(row: pd.Series, id_col: str) -> str:
    for c in non_id_columns(row, id_col):
        if not is_yes(row[c]):
            return "Fail"
    return "Pass"

# --- New Relic specifics ---
def _is_flag_value(v: str) -> bool:
    s = norm_str(v).lower()
    if s == "":
        return False
    return (s in POS_TOKENS) or (s in {"na","n/a","not applicable","no","n","false","0","fail","failed"}) or is_failed_pattern(s)

def score_newrelic_row(row: pd.Series, id_col: str) -> str:
    saw_positive = False
    for c in non_id_columns(row, id_col):
        s = norm_str(row[c]).lower()
        if not _is_flag_value(s):
            continue
        if is_failed_pattern(s) or s in {"no","n","false","0","fail","failed","na","n/a","not applicable"}:
            return "Fail"
        if s in POS_TOKENS:
            saw_positive = True
    return "Pass" if saw_positive else "Fail"

# --- Extract explicit failure reasons (supports failed-str{...}) ---
_BRACED_FAIL = re.compile(r"""^fail(?:ed)?[-_\s]?[a-z]*\{(.*?)\}""", re.IGNORECASE | re.DOTALL)

def maybe_reason_from_value(value: str) -> Optional[str]:
    raw = norm_str(value)
    if not raw:
        return None
    low = raw.lower()
    m = _BRACED_FAIL.match(raw)
    if m:
        return m.group(1).strip()
    if low.startswith("failed") or low.startswith("fail-"):
        after = re.sub(r"^fail(?:ed)?[:\-\s_]*", "", raw, flags=re.IGNORECASE).strip()
        return after if after else raw
    if low.startswith("error") or low.startswith("exception"):
        return raw
    return None

# ----------------- Row evaluation per kind -----------------
def eval_row_for_kind(kind: str, row: Optional[pd.Series], id_col: Optional[str]) -> Tuple[str, List[str]]:
    reasons: List[str] = []
    if row is None or id_col is None:
        return ("Fail", reasons)

    if kind == "producer":
        pcol = next((c for c in row.index if c.lower().startswith("posted_to_producer_topic")), None)
        val = row[pcol] if pcol else None
        status = score_producer(val)
        rtxt = maybe_reason_from_value(val)
        if rtxt:
            reasons.append(f"{pcol or 'Posted_To_Producer_Topic?'}={rtxt}")
        return (status, reasons)

    if kind == "consumer":
        app_col = next((c for c in row.index if "applicable_for_consumer_topic" in c.lower()), None)
        post_col = next((c for c in row.index if "posted_to_consumer_topic" in c.lower()), None)
        app_val = row[app_col] if app_col else None
        post_val = row[post_col] if post_col else None
        status = score_consumer(app_val, post_val)
        for col_name, cell in ((app_col or "Applicable", app_val), (post_col or "Posted", post_val)):
            rr = maybe_reason_from_value(cell)
            if rr:
                reasons.append(f"{col_name}={rr}")
        return (status, reasons)

    if kind == "comparator":
        cmp_col = next((c for c in row.index if "expected" in c.lower() and "observed" in c.lower() and "match" in c.lower()), None)
        if cmp_col:
            v = row[cmp_col]
            status = score_comparator_classic_value(v)
            rr = maybe_reason_from_value(v)
            if rr:
                reasons.append(f"{cmp_col}={rr}")
        else:
            status = score_all_yes(row, id_col)
            if status != "Pass":
                for c in non_id_columns(row, id_col):
                    rr = maybe_reason_from_value(row[c])
                    if rr:
                        reasons.append(f"{c}={rr}")
        return (status, reasons)

    if kind in {"json_comparator", "file_comparator"}:
        # Prefer strict 'Status' / then add 'reason/details/diff' if provided
        status_col = next((c for c in row.index if c.lower() == "status"), None)
        reason_col = next((c for c in row.index if c.lower() in {"reason", "details", "diff"}), None)
        if status_col:
            status_val = row[status_col]
            status = score_comparator_json_status(status_val)
            rr = maybe_reason_from_value(status_val)
            if rr:
                reasons.append(f"{status_col}={rr}")
            if status != "Pass" and reason_col:
                reason_text = norm_str(row[reason_col])
                if reason_text:
                    reasons.append(f"{reason_col}={reason_text}")
        else:
            status = score_all_yes(row, id_col)
            if status != "Pass":
                for c in non_id_columns(row, id_col):
                    rr = maybe_reason_from_value(row[c])
                    if rr:
                        reasons.append(f"{c}={rr}")
        return (status, reasons)

    # newrelic (default)
    status = score_newrelic_row(row, id_col)
    if status != "Pass":
        for c in non_id_columns(row, id_col):
            rr = maybe_reason_from_value(row[c])
            if rr:
                reasons.append(f"{c}={rr}")
    return (status, reasons)

# ----------------- Report writer (local or GCS) -----------------
def write_single_report(df: pd.DataFrame, out_path: str) -> None:
    """
    Writes a single-sheet Excel named by out_path (supports local and gs://).
    """
    write_excel_any({"Report": df}, out_path)
    print(out_path)

def join_out_path(out_dir: str, filename: str) -> str:
    """
    Join a filename to an output dir that may be local or gs://.
    """
    if is_gcs_path(out_dir):
        return out_dir.rstrip("/") + "/" + filename
    # local
    return os.path.join(out_dir, filename)

# ----------------- Build one report -----------------
def build_report_for_file(
    key: str,
    path_str: str,
    kind: str,
    df_file: Optional[pd.DataFrame],
    prod_df: pd.DataFrame,
    prod_id_col: str,
    out_dir: str,
) -> None:
    report_rows: List[Dict[str, str]] = []
    file_read_ok = df_file is not None
    file_id_col = pick_id_col(list(df_file.columns)) if df_file is not None else None

    file_df = df_file if df_file is not None else pd.DataFrame()
    if file_read_ok and file_id_col:
        file_df = file_df.assign(__norm_id__=norm_id_series(file_df[file_id_col])).set_index("__norm_id__", drop=True)

    for _, prow in prod_df.iterrows():
        inv = str(prow[prod_id_col]).strip()
        reason_list: List[str] = []
        status: str = "Fail"

        file_row: Optional[pd.Series] = None
        if not file_read_ok:
            reason_list.append("File missing or unreadable")
        elif not file_id_col:
            reason_list.append("ID column not found")
        else:
            file_row = file_df.loc[file_df.index == inv]
            if file_row is not None and not isinstance(file_row, pd.Series) and not file_row.empty:
                file_row = file_row.iloc[0]
            if file_row is None or (not isinstance(file_row, pd.Series)):
                reason_list.append(f"Invoice {inv} missing in {key}")

        if file_row is not None and isinstance(file_row, pd.Series):
            st, rs = eval_row_for_kind(kind, file_row, file_id_col)
            status = st
            reason_list.extend(rs)
        else:
            status = "Fail"

        report_rows.append({
            "Invoice No.": inv,
            "Status": status,
            "Reason": "; ".join(reason_list) if reason_list else "",
        })

    out_path = join_out_path(out_dir, f"{key}_report.xlsx")
    write_single_report(pd.DataFrame(report_rows, columns=["Invoice No.", "Status", "Reason"]), out_path)

# ----------------- Main -----------------
def main():
    ap = argparse.ArgumentParser(description="Make individual reports per file (producer keyed)")
    ap.add_argument("--config", help="Path to config JSON (defaults to ${ROOT_PATH}/config.json)")
    args = ap.parse_args()

    cfg_path = expand_env_str(args.config if args.config else "${ROOT_PATH}/config.json")
    cfg_text = read_text_any(cfg_path)  # works for local and gs://
    cfg = json.loads(cfg_text)

    prod_path = expand_env_str(cfg["producer"])
    files_map: Dict[str, str] = {k: expand_env_str(v) for k, v in cfg.get("files", {}).items()}
    out_dir = expand_env_str(cfg.get("output") or ".")

    # Load producer and find IDs (from local or gs://)
    df_prod = read_excel_any(prod_path)
    if df_prod is None:
        raise SystemExit(f"Producer file not found or unreadable: {prod_path}")
    prod_id_col = pick_id_col(list(df_prod.columns))
    if not prod_id_col:
        raise SystemExit("Could not detect Tracking/Invoice column in producer file")

    # Producer-only report
    prod_rows: List[Dict[str, str]] = []
    p_status_col = next((c for c in df_prod.columns if c.lower().startswith("posted_to_producer_topic")), None)
    for _, prow in df_prod.iterrows():
        inv = str(prow[prod_id_col]).strip()
        val = prow[p_status_col] if (p_status_col and p_status_col in prow.index) else None
        status = score_producer(val)
        reason = maybe_reason_from_value(val)
        prod_rows.append({
            "Invoice No.": inv,
            "Status": status,
            "Reason": (f"{p_status_col or 'Posted_To_Producer_Topic?'}={reason}" if reason else "")
        })
    prod_out = join_out_path(out_dir, "producer_report.xlsx")
    write_single_report(pd.DataFrame(prod_rows, columns=["Invoice No.", "Status", "Reason"]), prod_out)

    # Per-file reports (each key → 1 XLSX)
    for key, pstr in files_map.items():
        kind = detect_kind_from_key(key)
        df_f = read_excel_any(pstr)  # local or gs://
        build_report_for_file(
            key=key,
            path_str=pstr,
            kind=kind,
            df_file=df_f,
            prod_df=df_prod,
            prod_id_col=prod_id_col,
            out_dir=out_dir,
        )

if __name__ == "__main__":
    main()
