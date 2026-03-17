
import os
import json
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime
 
print("Running From:", Path.cwd())


# ----------------------------
# CONFIG
# ----------------------------

URL = (
        "https://wd5-impl-services1.url"  # replace this with your URL 
)

 
# Option A (recommended): credentials from environment variables
# set WORKDAY_USER and WORKDAY_PASS in your environment
# WORKDAY_USER = os.getenv("WORKDAY_USER")
# WORKDAY_PASS = os.getenv("WORKDAY_PASS")

# Option B (not recommended): hardcode (you can temporarily use this, but don't commit it) # replace these with username/password
WORKDAY_USER = "username"
WORKDAY_PASS = "password"

if not WORKDAY_USER or not WORKDAY_PASS:
    raise ValueError(
        "Missing credentials. Set WORKDAY_USER and WORKDAY_PASS environment variables "
        "or (temporarily) hardcode them in the script."
    )

AUTH = (WORKDAY_USER, WORKDAY_PASS)

# Where to drop JSON + Excel
OUTPUT_JSON_DIR = Path(r"C:\00_MAIN\000_DataSpring 2.0\000_WD CE Updates\00_JSON Files\FV8")
OUTPUT_XLSX_DIR = Path(r"C:\00_MAIN\000_DataSpring 2.0\000_WD CE Updates\00_JSON Files\FV8")

# Optional: choose specific columns; set to None to export ALL columns

COLUMNS_TO_EXTRACT = None
# Example:
# COLUMNS_TO_EXTRACT = ["ClientNbr", "Customer_WID"]

# ----------------------------
# HELPERS
# ----------------------------

def find_records(obj):
    """
    Try to locate the list of row-records inside a JSON structure.
    Returns a list[dict] if found, else None.

    """
    if isinstance(obj, list) and obj and isinstance(obj[0], dict):
        return obj
    if isinstance(obj, dict):
        for key in ["Report_Entry", "data", "rows", "results", "items", "value"]:
            if key in obj:
                found = find_records(obj[key])
                if found is not None:
                    return found
        for v in obj.values():
            found = find_records(v)
            if found is not None:
                return found
    return None

def download_json(url: str, auth: tuple, output_path: Path) -> Path:
    """
    Download JSON payload from API and write to output_path. Returns output_path.
    Uses streaming write to avoid memory issues on large payloads.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with requests.get(url, auth=auth, stream=True) as r:
        print("HTTP Status:", r.status_code)
        print("Content-Type:", r.headers.get("Content-Type"))
        r.raise_for_status()

        with output_path.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

    print("JSON written to:", output_path)
    print("File size (bytes):", output_path.stat().st_size)
    return output_path

def json_to_excel(input_json: Path, output_excel: Path, columns=None) -> Path:
    """
    Convert a JSON file to Excel. Locates record list dynamically and flattens it.
    If columns is provided, exports only those columns (when present).
    """
    output_excel.parent.mkdir(parents=True, exist_ok=True)
    with input_json.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    records = find_records(raw)
    if records is None:
        raise ValueError("Could not find a list of record objects in the JSON.")

    df = pd.json_normalize(records)
    df = df.reindex(sorted(df.columns), axis=1)
 
    if columns:
        missing = [c for c in columns if c not in df.columns]
        if missing:
            print("WARNING: Missing requested columns:", missing)
            print("Available columns sample:", list(df.columns)[:40])
        keep = [c for c in columns if c in df.columns]
        df = df[keep]
    df.to_excel(output_excel, index=False)
    print(f"Exported {len(df)} rows and {len(df.columns)} columns to: {output_excel.resolve()}")
    return output_excel


# ----------------------------
# MAIN
# ----------------------------

def main():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_json = OUTPUT_JSON_DIR / f"workday_report_{timestamp}.json"
    output_xlsx = OUTPUT_XLSX_DIR / f"workday_report_{timestamp}.xlsx"

    # 1) Download JSON
    downloaded_json_path = download_json(URL, AUTH, output_json)
    # 2) Convert that exact JSON file to Excel
    json_to_excel(downloaded_json_path, output_xlsx, columns=COLUMNS_TO_EXTRACT)


if __name__ == "__main__":

    main()