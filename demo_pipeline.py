from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from db import initialise_database, get_connection
from outlook_scanner import scan_outlook_to_db
from po_detection import run_po_detection
from po_validation import run_po_validation
from value_extraction import run_value_extraction
from worklist import refresh_worklist_tables
from load_po_master import load_po_master

from dashboard_data import (
    load_overview_data,
    load_status_breakdown_data,
    load_ageing_buckets_data,
    load_trends_data,
    load_worklist_data,
)

# -------------------------------------------------------------------------
# Debug toggle
# -------------------------------------------------------------------------
DEBUG = os.getenv("ICS_DEBUG", "").strip().lower() in ("1", "true", "yes", "y", "on")


def dprint(*args, **kwargs) -> None:
    if DEBUG:
        print(*args, **kwargs)


BASE_DIR = Path(__file__).resolve().parent
STAGING_DIR = BASE_DIR / "staging"
EXPORTS_DIR = BASE_DIR / "exports"
EXPORTS_DIR.mkdir(exist_ok=True)

# -------------------------------------------------------------------------
# Public-safe data contract (NO demo/ folder)
# -------------------------------------------------------------------------
DATA_DIR = BASE_DIR / "data"
ATTACHMENTS_DIR = DATA_DIR / "attachments"
INBOX_JSON = DATA_DIR / "inbox.json"
PO_MASTER_CSV = DATA_DIR / "po_master.csv"

DB_PATH = Path(os.getenv("ICS_DB_PATH", str(BASE_DIR / "inbox.db")))
SNAPSHOT_JSON = Path(os.getenv("ICS_SNAPSHOT_JSON", str(EXPORTS_DIR / "snapshot.json")))


def print_tables() -> None:
    conn = get_connection()
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
    tables = [row["name"] for row in cur.fetchall()]
    conn.close()
    dprint("DB tables:", tables)


def write_local_snapshot(*, db_path: Path, out_path: Path, include_trends: bool = False, include_worklist: bool = True) -> None:
    """
    Public-safe local snapshot export for the dashboard (no networking).
    """
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    overview = load_overview_data(db_path)
    if "_error" in overview:
        payload = {"generated_at": now, "_error": overview["_error"]}
    else:
        payload = {
            "generated_at": now,
            "overview": overview,
            "status_breakdown": load_status_breakdown_data(db_path),
            "ageing_buckets": load_ageing_buckets_data(db_path),
            "worklist": load_worklist_data(db_path) if include_worklist else [],
            "trends": load_trends_data(db_path) if include_trends else [],
        }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _assert_demo_inputs_exist() -> None:
    """
    Hard fail early with a clear message if required synthetic inputs are missing.
    """
    missing = []
    if not PO_MASTER_CSV.exists():
        missing.append(str(PO_MASTER_CSV))
    if not INBOX_JSON.exists():
        missing.append(str(INBOX_JSON))
    if not ATTACHMENTS_DIR.exists():
        missing.append(str(ATTACHMENTS_DIR))

    if missing:
        raise FileNotFoundError(
            "Missing required synthetic demo inputs:\n"
            + "\n".join(f" - {p}" for p in missing)
            + "\n\nExpected structure:\n"
              "data/\n"
              "  po_master.csv\n"
              "  inbox.json\n"
              "  attachments/\n"
              "    *.pdf\n"
        )


def run_pipeline() -> dict:
    """
    Public-safe end-to-end pipeline (NO demo/ folder).

    Inputs (synthetic):
      - data/po_master.csv
      - data/inbox.json
      - data/attachments/*.pdf

    Outputs:
      - inbox.db
      - staging/ (local copies of PDFs)
      - exports/snapshot.json
    """
    boot_ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    print(f"[BOOT] {boot_ts} pipeline starting", flush=True)
    dprint("[DEBUG] Enabled via ICS_DEBUG=1")

    _assert_demo_inputs_exist()

    # Force the public scanner to look in data/ (no env setup required by users)
    os.environ["ICS_INBOX_ADAPTER"] = "demo"
    os.environ["ICS_DEMO_INBOX_JSON"] = str(INBOX_JSON)
    os.environ["ICS_DEMO_ATTACHMENTS_DIR"] = str(ATTACHMENTS_DIR)

    # 1) Ensure schema exists
    initialise_database()
    print_tables()

    # 2) Load synthetic PO master
    print("\n--- Stage 1: PO Master Load ---")
    po_master_summary = load_po_master(PO_MASTER_CSV)
    print(po_master_summary)

    # 3) Scan inbox -> SQLite (demo adapter)
    print("\n--- Stage 2: Inbox Scan ---", flush=True)
    scan_summary = scan_outlook_to_db()
    print(
        {
            "messages_seen": scan_summary.get("messages_seen"),
            "pdfs_saved": scan_summary.get("pdfs_saved"),
            "staging_dir": scan_summary.get("staging_dir"),
            "adapter": scan_summary.get("adapter"),
            "inbox_json": str(INBOX_JSON),
            "attachments_dir": str(ATTACHMENTS_DIR),
        }
    )
    dprint("Full scan summary:", scan_summary)

    # 4) PO Detection
    print("\n--- Stage 3: PO Detection ---")
    po_detect_summary = run_po_detection(staging_dir=STAGING_DIR)
    print(po_detect_summary)

    # 5) PO Validation
    print("\n--- Stage 4: PO Validation ---")
    po_validation_summary = run_po_validation()
    print(po_validation_summary)

    # 6) Value Extraction
    print("\n--- Stage 5: Value Extraction ---")
    value_summary = run_value_extraction(staging_dir=STAGING_DIR)
    print(value_summary)

    # 7) Worklist refresh
    print("\n--- Stage 6: Worklist Refresh ---")
    conn = get_connection()
    try:
        run_id = refresh_worklist_tables(conn)
    finally:
        conn.close()
    print(f"Worklist refreshed. run_id={run_id}")

    # 8) Snapshot export (local)
    print("\n--- Stage 7: Snapshot Export (local) ---")
    write_local_snapshot(db_path=DB_PATH, out_path=SNAPSHOT_JSON, include_trends=False, include_worklist=True)
    print(f"Snapshot written: {SNAPSHOT_JSON}")

    print("\n=== Pipeline complete ===")

    return {
        "boot_ts": boot_ts,
        "po_master": po_master_summary,
        "scan": scan_summary,
        "po_detection": po_detect_summary,
        "po_validation": po_validation_summary,
        "value_extraction": value_summary,
        "worklist_run_id": run_id,
        "snapshot_path": str(SNAPSHOT_JSON),
        "db_path": str(DB_PATH),
        "data_dir": str(DATA_DIR),
    }


if __name__ == "__main__":
    run_pipeline()
