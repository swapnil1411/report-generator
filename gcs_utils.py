#!/usr/bin/env python3
"""
Unified local + GCS file utilities.

Requirements (only if you use gs://):
  pip install fsspec gcsfs openpyxl pandas

Auth for GCS:
  - Locally: export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
  - Or: `gcloud auth application-default login`
  - In GCE/GKE/CloudRun: use the default service account with proper permissions.
"""

from __future__ import annotations
import os
from io import BytesIO
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

# ----------------- Path helpers -----------------
def is_gcs_path(p: str) -> bool:
    return str(p).startswith("gs://")

def _strip_gcs(p: str) -> str:
    """Return bucket and key for a gs://bucket/key path."""
    assert p.startswith("gs://")
    no_scheme = p[len("gs://"):]
    parts = no_scheme.split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    return bucket, key

def ensure_gcs():
    try:
        import fsspec  # noqa
        import gcsfs   # noqa
    except Exception as e:
        raise RuntimeError(
            "GCS paths require: fsspec and gcsfs (pip install fsspec gcsfs)\n"
            f"Import error: {e}"
        )

# ----------------- Text I/O -----------------
def read_text_any(path_str: str, encoding: str = "utf-8") -> str:
    """
    Read a UTF-8 text file from local or gs://.
    """
    if is_gcs_path(path_str):
        ensure_gcs()
        import fsspec
        with fsspec.open(path_str, mode="rt", encoding=encoding) as f:
            return f.read()
    else:
        return Path(path_str).read_text(encoding=encoding)

def write_text_any(path_str: str, text: str, encoding: str = "utf-8") -> None:
    """
    Write a UTF-8 text file to local or gs://.
    """
    data = text.encode(encoding)
    write_bytes_any(path_str, data)

# ----------------- Bytes I/O -----------------
def read_bytes_any(path_str: str) -> bytes:
    """
    Read bytes from local or gs://.
    """
    if is_gcs_path(path_str):
        ensure_gcs()
        import fsspec
        with fsspec.open(path_str, mode="rb") as f:
            return f.read()
    else:
        return Path(path_str).read_bytes()

def write_bytes_any(path_str: str, data: bytes) -> None:
    """
    Write bytes to local or gs://.
    """
    if is_gcs_path(path_str):
        ensure_gcs()
        import fsspec
        # parent dirs in GCS are logical; no need to mkdir
        with fsspec.open(path_str, mode="wb") as f:
            f.write(data)
    else:
        p = Path(path_str)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)

# ----------------- Excel I/O -----------------
def read_excel_any(path_str: str, sheet_name=0) -> Optional[pd.DataFrame]:
    """
    Read an Excel file from local or gs:// into a DataFrame.
    Returns None if the file does not exist or fails to read.
    """
    try:
        if is_gcs_path(path_str):
            ensure_gcs()
            # pandas + fsspec/gcsfs works with storage_options
            df = pd.read_excel(path_str, engine="openpyxl", sheet_name=sheet_name, storage_options={})
        else:
            p = Path(path_str)
            if not p.exists():
                print(f"[WARN] Missing file: {p}")
                return None
            df = pd.read_excel(p, engine="openpyxl", sheet_name=sheet_name)
        df.columns = [str(c).strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"[ERROR] Failed to read {path_str}: {e}")
        return None

def write_excel_any(dfs: Dict[str, pd.DataFrame], out_path: str) -> None:
    """
    Write one or more DataFrames to an Excel workbook (one sheet per key).
    Works for both local and gs:// destinations.
    """
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        for sheet, df in dfs.items():
            df.to_excel(writer, index=False, sheet_name=sheet)
    write_bytes_any(out_path, bio.getvalue())

# ----------------- Convenience: download/upload -----------------
def download_gcs_to_local(src_gs_path: str, dst_local_path: str) -> str:
    """
    Download a gs:// object to a local file path.
    Returns the local path.
    """
    ensure_gcs()
    data = read_bytes_any(src_gs_path)
    write_bytes_any(dst_local_path, data)
    return dst_local_path

def upload_local_to_gcs(src_local_path: str, dst_gs_path: str) -> str:
    """
    Upload a local file path to a gs:// destination.
    Returns the gs path.
    """
    ensure_gcs()
    data = read_bytes_any(src_local_path)
    write_bytes_any(dst_gs_path, data)
    return dst_gs_path

# ----------------- Env/path expansion -----------------
def expand_env_str(s: str) -> str:
    """
    Expand ${ROOT_PATH}, $VARS, and ~, but keep gs:// intact.
    This lets you set ROOT_PATH='gs://my-bucket/some/prefix' in the environment.
    """
    root_path = os.getenv("ROOT_PATH", ".")
    s = s.replace("${ROOT_PATH}", root_path)
    # NOTE: os.path.expanduser/vars are safe for gs:// because they don't strip schemes.
    return os.path.expanduser(os.path.expandvars(s))
