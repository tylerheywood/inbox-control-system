from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path

from db import get_connection


_CANONICAL_HEADERS = {"po_number", "supplier_account", "po_status", "approval_status"}

_EXPORT_ALIASES = {
    "po_number": ["po_number", "Purchase order", "Purchase Order", "PO Number", "PO"],
    "supplier_account": ["supplier_account", "Supplier account", "Supplier Account"],
    "po_status": ["po_status", "Purchase order status", "PO status", "PO Status"],
    "approval_status": ["approval_status", "Approval status", "Approval Status"],
}


def _pick_field(fieldnames: set[str], candidates: list[str]) -> str | None:
    for c in candidates:
        if c in fieldnames:
            return c
    return None


def load_po_master(csv_path: Path) -> dict:
    """
    Load a PO master snapshot into SQLite (public-safe).

    Supports two CSV styles:
    1) Demo/canonical headers:
       - po_number, supplier_account, po_status, approval_status
    2) Export-like headers (common ERP export naming):
       - Purchase order, Supplier account, Purchase order status, Approval status

    Behaviour:
    - Full refresh per run (snapshot semantics)
    - Safe to re-run
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"PO master CSV not found: {csv_path}")

    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    conn = get_connection()
    cur = conn.cursor()
    inserted = 0

    try:
        conn.execute("BEGIN")
        cur.execute("DELETE FROM po_master")

        with csv_path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = set(reader.fieldnames or [])

            # Resolve columns (canonical or export-like)
            col_po = _pick_field(fieldnames, _EXPORT_ALIASES["po_number"])
            col_supp = _pick_field(fieldnames, _EXPORT_ALIASES["supplier_account"])
            col_status = _pick_field(fieldnames, _EXPORT_ALIASES["po_status"])
            col_appr = _pick_field(fieldnames, _EXPORT_ALIASES["approval_status"])

            missing = [k for k, v in [("po_number", col_po), ("supplier_account", col_supp), ("po_status", col_status), ("approval_status", col_appr)] if v is None]
            if missing:
                raise ValueError(
                    "PO master CSV missing required columns. "
                    f"Missing: {missing}. Found headers: {sorted(fieldnames)}"
                )

            for row in reader:
                po_number = (row.get(col_po) or "").strip()
                if not po_number:
                    continue

                cur.execute(
                    """
                    INSERT INTO po_master (
                        po_number,
                        supplier_account,
                        po_status,
                        approval_status,
                        last_import_datetime
                    )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        po_number,
                        (row.get(col_supp) or "").strip(),
                        (row.get(col_status) or "").strip(),
                        (row.get(col_appr) or "").strip(),
                        now,
                    ),
                )
                inserted += 1

        conn.commit()

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return {"rows_loaded": inserted, "source": str(csv_path), "imported_at": now}
