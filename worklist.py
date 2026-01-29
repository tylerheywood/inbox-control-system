"""
V1 Worklist (Joblist) for ICS

- Deterministic, auditable, precedence-based "next action" per invoice
- Produces:
    - next_action
    - action_reason
- Writes:
    - invoice_worklist (current cache, full-replace per run)
    - invoice_worklist_history (append-only snapshots per run)

V1 model:
- No manual dismissal state
- Items disappear when truth changes or invoice is no longer present in the inbox scan

Outlook usability:
- Rows include identifiers that help AP users locate the invoice in Outlook:
    - sender_domain  (best-effort; "internal" for Exchange legacy DNs)
    - email_subject
    - attachment_name
    - received_datetime

Debug:
- Controlled via ICS_DEBUG env var
- Prints summary of action changes vs previous run (history table)
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from po_validation import (
    STATUS_PO_NOT_CONFIRMED,
    STATUS_PO_NOT_IN_MASTER,
    STATUS_PO_NOT_OPEN,
    STATUS_SINGLE_PO_DETECTED,
    STATUS_UNVALIDATED,
    STATUS_VALID_PO,
)

# Debug toggle (matches project pattern)
_ENV_DEBUG = os.getenv("ICS_DEBUG", "").strip().lower()
DEBUG = _ENV_DEBUG in ("1", "true", "yes", "y", "on")

# If you want to hard-link these, you can import from po_detection instead of strings:
STATUS_NO_TEXT_LAYER = "NO_TEXT_LAYER"
STATUS_MISSING_PO = "MISSING_PO"
STATUS_MULTIPLE_POS = "MULTIPLE_POS"


@dataclass(frozen=True)
class WorkItem:
    document_hash: str

    # Outlook identifiers
    sender_domain: str | None
    email_subject: str | None
    attachment_name: str | None
    received_datetime: str | None

    # Worklist classification
    next_action: str
    action_reason: str
    priority: int  # lower = earlier attention in the queue
    generated_at_utc: str
    is_currently_present: int


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _new_run_id() -> str:
    # Unique ID per refresh so history snapshots are queryable
    return uuid.uuid4().hex


def _extract_sender_domain(sender_address: Optional[str]) -> Optional[str]:
    """
    Extract domain from a sender identifier.

    Cases:
    - SMTP sender: "john@acme.com" -> "acme.com"
    - Exchange legacy DN: "/O=EXCH.../CN=..." -> "internal"
    - Anything else -> None
    """
    if not sender_address:
        return None

    s = str(sender_address).strip()
    if not s:
        return None

    if s.startswith("/O=") or s.startswith("\\O="):
        return "internal"

    if "@" in s:
        dom = s.split("@", 1)[1].strip().lower()
        return dom or None

    return None


def build_worklist(
    conn: sqlite3.Connection,
    *,
    only_currently_present: bool = True,
    include_ready_to_post: bool = True,
) -> Tuple[str, List[WorkItem]]:
    """
    Compute the current worklist deterministically from inbox_invoice truth columns.

    Returns:
        (run_id, items)

    Notes:
    - Does NOT write to DB.
    - Precedence-based: first blocker wins.
    - Includes Outlook identifiers so AP users can locate the invoice in Outlook.
    """
    generated_at_utc = _utc_now_iso()
    run_id = _new_run_id()

    where_clause = "WHERE ii.is_currently_present = 1" if only_currently_present else ""

    rows = conn.execute(
        f"""
        SELECT
            ii.document_hash,
            ii.is_currently_present,
            ii.po_match_status,
            ii.po_validation_status,
            ii.ready_to_post,
            ii.gross_total,

            ii.attachment_file_name AS attachment_name,
            im.sender_address       AS sender_address,
            im.subject              AS email_subject,
            im.received_datetime    AS received_datetime
        FROM inbox_invoice ii
        LEFT JOIN inbox_message im
               ON im.message_id = ii.message_id
        {where_clause}
        """
    ).fetchall()

    items: List[WorkItem] = []
    for r in rows:
        next_action, action_reason, priority = _classify_invoice(r)

        if not include_ready_to_post and next_action == "READY TO POST":
            continue

        items.append(
            WorkItem(
                document_hash=r["document_hash"],
                sender_domain=_extract_sender_domain(r["sender_address"]),
                email_subject=r["email_subject"],
                attachment_name=r["attachment_name"],
                received_datetime=r["received_datetime"],
                next_action=next_action,
                action_reason=action_reason,
                priority=priority,
                generated_at_utc=generated_at_utc,
                is_currently_present=int(r["is_currently_present"]),
            )
        )

    # Deterministic ordering: priority then hash
    items.sort(key=lambda x: (x.priority, x.document_hash))
    return run_id, items


def refresh_worklist_tables(
    conn: sqlite3.Connection,
    *,
    only_currently_present: bool = True,
    include_ready_to_post: bool = True,
) -> str:
    """
    Full-replace refresh of invoice_worklist + append-only snapshot to history.

    Returns:
        run_id for this refresh.
    """
    run_id, items = build_worklist(
        conn,
        only_currently_present=only_currently_present,
        include_ready_to_post=include_ready_to_post,
    )

    with conn:
        conn.execute("DELETE FROM invoice_worklist;")

        conn.executemany(
            """
            INSERT INTO invoice_worklist (
                document_hash,
                sender_domain,
                email_subject,
                attachment_name,
                received_datetime,
                next_action,
                action_reason,
                priority,
                generated_at_utc,
                is_currently_present
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            [
                (
                    i.document_hash,
                    i.sender_domain,
                    i.email_subject,
                    i.attachment_name,
                    i.received_datetime,
                    i.next_action,
                    i.action_reason,
                    i.priority,
                    i.generated_at_utc,
                    i.is_currently_present,
                )
                for i in items
            ],
        )

        conn.executemany(
            """
            INSERT INTO invoice_worklist_history (
                run_id,
                document_hash,
                sender_domain,
                email_subject,
                attachment_name,
                received_datetime,
                next_action,
                action_reason,
                priority,
                generated_at_utc,
                is_currently_present
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            [
                (
                    run_id,
                    i.document_hash,
                    i.sender_domain,
                    i.email_subject,
                    i.attachment_name,
                    i.received_datetime,
                    i.next_action,
                    i.action_reason,
                    i.priority,
                    i.generated_at_utc,
                    i.is_currently_present,
                )
                for i in items
            ],
        )

    if DEBUG:
        _debug_worklist_delta(conn, run_id, total_items=len(items))

    return run_id


def fetch_current_worklist(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """
    Convenience reader (dashboard/CLI): returns dict rows from invoice_worklist.
    Ordered by priority then document_hash (lower = earlier attention).
    """
    rows = conn.execute(
        """
        SELECT
            document_hash,
            sender_domain,
            email_subject,
            attachment_name,
            received_datetime,
            next_action,
            action_reason,
            priority,
            generated_at_utc,
            is_currently_present
        FROM invoice_worklist
        ORDER BY priority ASC, document_hash ASC;
        """
    ).fetchall()
    return [dict(r) for r in rows]


# ----------------------------
# Debug helpers
# ----------------------------
def _debug_worklist_delta(conn: sqlite3.Connection, run_id: str, *, total_items: int) -> None:
    prev = conn.execute(
        """
        SELECT run_id
        FROM invoice_worklist_history
        WHERE run_id <> ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (run_id,),
    ).fetchone()

    print("\n[WORKLIST DEBUG]")
    print(f"Run ID: {run_id}")
    print(f"Total items this run: {total_items}")

    if prev is None:
        print("First run — no prior worklist to compare.")
        return

    prev_run_id = prev["run_id"]

    changes = conn.execute(
        """
        SELECT
            prev.next_action AS prev_action,
            curr.next_action AS curr_action,
            COUNT(*) AS count
        FROM invoice_worklist_history prev
        JOIN invoice_worklist_history curr
          ON prev.document_hash = curr.document_hash
        WHERE prev.run_id = ?
          AND curr.run_id = ?
          AND prev.next_action <> curr.next_action
        GROUP BY prev.next_action, curr.next_action
        ORDER BY count DESC;
        """,
        (prev_run_id, run_id),
    ).fetchall()

    total_changed = sum(r["count"] for r in changes)
    print(f"Changed since last run: {total_changed}")

    if total_changed == 0:
        print("  No action changes detected.")
        return

    for r in changes:
        print(f"  {r['prev_action']} → {r['curr_action']}: {r['count']}")


# ----------------------------
# Classification rules (V1)
# ----------------------------
def _gross_missing(row: sqlite3.Row) -> bool:
    return row["gross_total"] is None


def _classify_invoice(row: sqlite3.Row) -> Tuple[str, str, int]:
    """
    Return (next_action, action_reason, priority).

    Priority: lower = earlier attention in the queue.

    Precedence (V1):
    1) No text layer
    2) Missing PO
    3) Multiple POs
    4) PO validation blockers (single PO detected)
    5) Ready to post
    6) Gross missing (needs value entry / confirmation)
    7) Catch-all manual review
    """
    if int(row["is_currently_present"]) == 0:
        return ("NOT CURRENTLY PRESENT", "NOT IN INBOX THIS SCAN", 90)

    po_match_status = (row["po_match_status"] or "").strip()
    po_validation_status = (row["po_validation_status"] or "").strip()
    ready_to_post = int(row["ready_to_post"]) if row["ready_to_post"] is not None else 0

    # Highest urgency blockers first (lowest priority number)
    if po_match_status == STATUS_NO_TEXT_LAYER:
        return ("MANUAL REVIEW", "NO TEXT LAYER", 10)

    if po_match_status == STATUS_MISSING_PO:
        return ("MANUAL REVIEW", "MISSING PO", 20)

    if po_match_status == STATUS_MULTIPLE_POS:
        return ("MANUAL REVIEW", "MULTIPLE POS DETECTED", 30)

    if po_match_status == STATUS_SINGLE_PO_DETECTED:
        if po_validation_status == STATUS_PO_NOT_OPEN:
            return ("MANUAL REVIEW", "PO NOT OPEN", 35)
        if po_validation_status == STATUS_PO_NOT_CONFIRMED:
            return ("MANUAL REVIEW", "PO NOT CONFIRMED", 36)
        if po_validation_status == STATUS_PO_NOT_IN_MASTER:
            return ("MANUAL REVIEW", "PO NOT IN MASTER", 40)
        if po_validation_status == STATUS_UNVALIDATED:
            return ("MANUAL REVIEW", "PO NOT VALIDATED YET", 50)

        if po_validation_status not in (
            STATUS_UNVALIDATED,
            STATUS_PO_NOT_IN_MASTER,
            STATUS_PO_NOT_OPEN,
            STATUS_PO_NOT_CONFIRMED,
            STATUS_VALID_PO,
        ):
            return ("MANUAL REVIEW", "UNKNOWN PO VALIDATION STATUS", 55)

    # Green lane
    if ready_to_post == 1:
        return ("READY TO POST", "VALID PO", 5)

    # Value missing (only matters if not ready)
    if _gross_missing(row):
        return ("MANUAL REVIEW", "GROSS TOTAL NOT EXTRACTED", 60)

    return ("MANUAL REVIEW", "UNCLASSIFIED STATE", 80)
