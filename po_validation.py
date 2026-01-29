from __future__ import annotations

from db import get_connection

# Truth values expected in po_master (demo-safe)
VALID_OPEN_STATUS = "Open order"
VALID_APPROVAL_STATUS = "Confirmed"

# Validation statuses written into inbox_invoice.po_validation_status
STATUS_UNVALIDATED = "UNVALIDATED"
STATUS_PO_NOT_IN_MASTER = "PO_NOT_IN_MASTER"
STATUS_PO_NOT_OPEN = "PO_NOT_OPEN"
STATUS_PO_NOT_CONFIRMED = "PO_NOT_CONFIRMED"
STATUS_VALID_PO = "VALID_PO"

# Detection status expected in inbox_invoice.po_match_status
STATUS_SINGLE_PO_DETECTED = "SINGLE_PO_DETECTED"


def run_po_validation() -> dict:
    """
    Validate detected POs against po_master.

    Rules:
    - Only invoices with po_match_status == SINGLE_PO_DETECTED are eligible
    - PO must exist in po_master
    - PO must be Open (po_status == VALID_OPEN_STATUS)
    - PO must be Confirmed (approval_status == VALID_APPROVAL_STATUS)

    Writes:
    - inbox_invoice.po_validation_status
    - inbox_invoice.ready_to_post (canonical dashboard/worklist flag)

    V1 behaviour:
    - Re-validates all currently-present SINGLE_PO_DETECTED invoices each run
      (reflects any changes in po_master)
    - Excludes invoices with posted_datetime NOT NULL (terminal)
    """
    conn = get_connection()
    cur = conn.cursor()

    validated = 0
    valid = 0
    not_in_master = 0
    not_open = 0
    not_confirmed = 0

    try:
        conn.execute("BEGIN")

        # Defensive: anything not SINGLE_PO_DETECTED cannot be "ready"
        cur.execute(
            """
            UPDATE inbox_invoice
            SET ready_to_post = 0
            WHERE is_currently_present = 1
              AND posted_datetime IS NULL
              AND (po_match_status IS NULL OR po_match_status <> ?)
            """,
            (STATUS_SINGLE_PO_DETECTED,),
        )

        # Reset current SINGLE_PO_DETECTED invoices each run (live validation)
        cur.execute(
            """
            UPDATE inbox_invoice
            SET po_validation_status = ?,
                ready_to_post = 0
            WHERE is_currently_present = 1
              AND posted_datetime IS NULL
              AND po_match_status = ?
            """,
            (STATUS_UNVALIDATED, STATUS_SINGLE_PO_DETECTED),
        )

        # Pull the PO + master truth for all eligible invoices
        rows = cur.execute(
            """
            SELECT
                ii.document_hash,
                ip.po_number,
                pm.po_status,
                pm.approval_status
            FROM inbox_invoice ii
            JOIN invoice_po ip
                ON ii.document_hash = ip.document_hash
            LEFT JOIN po_master pm
                ON ip.po_number = pm.po_number
            WHERE ii.is_currently_present = 1
              AND ii.posted_datetime IS NULL
              AND ii.po_match_status = ?
            """,
            (STATUS_SINGLE_PO_DETECTED,),
        ).fetchall()

        for row in rows:
            document_hash = row["document_hash"]
            po_status = row["po_status"]
            approval_status = row["approval_status"]

            if po_status is None:
                new_status = STATUS_PO_NOT_IN_MASTER
                ready_to_post = 0
                not_in_master += 1

            elif po_status != VALID_OPEN_STATUS:
                new_status = STATUS_PO_NOT_OPEN
                ready_to_post = 0
                not_open += 1

            elif approval_status != VALID_APPROVAL_STATUS:
                new_status = STATUS_PO_NOT_CONFIRMED
                ready_to_post = 0
                not_confirmed += 1

            else:
                new_status = STATUS_VALID_PO
                ready_to_post = 1
                valid += 1

            cur.execute(
                """
                UPDATE inbox_invoice
                SET po_validation_status = ?,
                    ready_to_post = ?
                WHERE document_hash = ?
                """,
                (new_status, ready_to_post, document_hash),
            )

            validated += 1

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()

    return {
        "validated": validated,
        "valid": valid,
        "po_not_in_master": not_in_master,
        "po_not_open": not_open,
        "po_not_confirmed": not_confirmed,
    }
