from __future__ import annotations

"""
Public-Safe Inbox Scanner (JSON Adapter)

This module intentionally excludes Outlook/COM integration for the public repo.

What it does:
- Reads inbox messages from a synthetic JSON feed (data/inbox.json)
- Copies PDF attachments from data/attachments/ into staging/
- Computes document_hash (SHA-256)
- Persists scan results into SQLite (inbox_message + inbox_invoice)
- Implements presence reset semantics (begin_scan) per run

What it does NOT do (public version):
- Access Outlook profiles
- Use pywin32 / COM / MAPI
- Require tenant permissions

Default demo input structure (public-safe):
- data/inbox.json
- data/attachments/*.pdf
- data/po_master.csv (loaded by pipeline, not this module)

Environment overrides:
- ICS_DATA_DIR=path/to/data               (default: ./data)
- ICS_INBOX_JSON=path/to/inbox.json       (default: <data_dir>/inbox.json)
- ICS_ATTACHMENTS_DIR=path/to/attachments (default: <data_dir>/attachments)
- ICS_STAGING_DIR=path/to/staging         (default: ./staging)
- ICS_MAX_ITEMS_PER_FOLDER=50
- ICS_DEBUG=1
"""

import json
import os
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from db import get_connection, initialise_database
from fingerprint import sha256_file


# =============================================================================
# Config
# =============================================================================

@dataclass(frozen=True)
class ScannerConfig:
    data_dir: Path
    inbox_json: Path
    attachments_dir: Path
    staging_dir: Path
    max_items_per_folder: int
    debug: bool


def _get_config() -> ScannerConfig:
    base_dir = Path(__file__).resolve().parent

    data_dir = Path(os.getenv("ICS_DATA_DIR", str(base_dir / "data")))
    inbox_json = Path(os.getenv("ICS_INBOX_JSON", str(data_dir / "inbox.json")))
    attachments_dir = Path(os.getenv("ICS_ATTACHMENTS_DIR", str(data_dir / "attachments")))

    staging_dir = Path(os.getenv("ICS_STAGING_DIR", str(base_dir / "staging")))
    staging_dir.mkdir(parents=True, exist_ok=True)

    max_items = int(os.getenv("ICS_MAX_ITEMS_PER_FOLDER", "50"))

    debug = os.getenv("ICS_DEBUG", "").strip().lower() in ("1", "true", "yes", "y", "on")

    return ScannerConfig(
        data_dir=data_dir,
        inbox_json=inbox_json,
        attachments_dir=attachments_dir,
        staging_dir=staging_dir,
        max_items_per_folder=max_items,
        debug=debug,
    )


def _debug(cfg: ScannerConfig, msg: str) -> None:
    if cfg.debug:
        print(msg, flush=True)


# =============================================================================
# Models
# =============================================================================

@dataclass(frozen=True)
class PdfAttachment:
    attachment_index: int
    file_name: str
    size_bytes: int
    save_path: Path
    document_hash: str


@dataclass(frozen=True)
class JsonAttachmentRef:
    file_name: str
    source_path: Path


@dataclass(frozen=True)
class JsonMessage:
    message_id: str
    folder_path: str
    received_datetime: Optional[str]
    sender_address: Optional[str]
    subject: Optional[str]
    attachments: list[JsonAttachmentRef]


# =============================================================================
# Helpers
# =============================================================================

def now_iso_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def safe_filename(name: str) -> str:
    """
    Windows-safe, deterministic. Keep extension. Replace weird chars.
    """
    s = re.sub(r"[^\w\-. ]+", "_", str(name)).strip()
    return s or "attachment.pdf"


def short_message_id(message_id: str, n: int = 12) -> str:
    s = str(message_id)
    return s[-n:] if len(s) >= n else s


# =============================================================================
# DB helpers (presence + upserts)
# =============================================================================

def begin_scan(conn, scan_ts: str) -> None:
    """
    Presence reset for a new scan.

    V1 behaviour:
    - Mark all currently-present messages/invoices as not present.
    - Upserts during this scan set is_currently_present back to 1.
    """
    cur = conn.cursor()

    # Messages
    cur.execute(
        """
        UPDATE inbox_message
        SET is_currently_present = 0,
            last_scan_datetime = ?
        WHERE is_currently_present = 1
        """,
        (scan_ts,),
    )

    # Invoices (exclude posted invoices)
    cur.execute(
        """
        UPDATE inbox_invoice
        SET is_currently_present = 0,
            last_scan_datetime = ?
        WHERE is_currently_present = 1
          AND posted_datetime IS NULL
        """,
        (scan_ts,),
    )


def upsert_message(
    conn,
    *,
    message_id: str,
    current_location: str,
    scan_ts: str,
    received_datetime: Optional[str],
    sender_address: Optional[str],
    subject: Optional[str],
    has_attachments: bool,
    attachment_count: int,
) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO inbox_message (
            message_id,
            current_location,
            first_seen_datetime,
            last_seen_datetime,
            last_scan_datetime,
            is_currently_present,
            received_datetime,
            sender_address,
            subject,
            has_attachments,
            attachment_count
        )
        VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?)
        ON CONFLICT(message_id) DO UPDATE SET
            current_location      = excluded.current_location,
            last_seen_datetime    = excluded.last_seen_datetime,
            last_scan_datetime    = excluded.last_scan_datetime,
            is_currently_present  = 1,
            received_datetime     = excluded.received_datetime,
            sender_address        = excluded.sender_address,
            subject               = excluded.subject,
            has_attachments       = excluded.has_attachments,
            attachment_count      = excluded.attachment_count
        """,
        (
            message_id,
            current_location,
            scan_ts,  # first_seen on insert
            scan_ts,  # last_seen
            scan_ts,  # last_scan
            received_datetime,
            sender_address,
            subject,
            1 if has_attachments else 0,
            int(attachment_count),
        ),
    )


def upsert_invoice(
    conn,
    *,
    document_hash: str,
    message_id: str,
    attachment_file_name: str,
    scan_ts: str,
    source_folder_path: str,
) -> None:
    """
    Upsert into inbox_invoice (presence + linkage + timestamps only).
    Other truth columns are handled by later pipeline stages.
    """
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO inbox_invoice (
            document_hash,
            message_id,
            attachment_file_name,
            first_seen_datetime,
            last_seen_datetime,
            last_scan_datetime,
            is_currently_present,
            source_folder_path,
            po_count,
            po_match_status,
            supplier_account_expected,
            supplier_validation_status,
            processing_status,
            posted_datetime,
            net_total,
            vat_total,
            gross_total,
            review_outcome,
            reviewed_datetime,
            reviewed_by,
            review_note
        )
        VALUES (?, ?, ?, ?, ?, ?, 1, ?, 0, 'UNSCANNED', NULL, NULL, 'NEW', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
        ON CONFLICT(document_hash) DO UPDATE SET
            message_id            = excluded.message_id,
            attachment_file_name  = excluded.attachment_file_name,
            last_seen_datetime    = excluded.last_seen_datetime,
            last_scan_datetime    = excluded.last_scan_datetime,
            is_currently_present  = 1,
            source_folder_path    = excluded.source_folder_path
        """,
        (
            document_hash,
            message_id,
            attachment_file_name,
            scan_ts,
            scan_ts,
            scan_ts,
            source_folder_path,
        ),
    )


# =============================================================================
# JSON adapter (data/inbox.json)
# =============================================================================

def _load_json_messages(
    inbox_json_path: Path,
    attachments_dir: Path,
    *,
    tracked_folders: list[str],
    max_items_per_folder: int,
) -> list[JsonMessage]:
    """
    Expected JSON schema (synthetic):

    [
      {
        "message_id": "MSG-0001",
        "folder_path": "Inbox",
        "received_datetime": "2026-01-01T09:15:00Z",
        "sender_address": "ap@vendor.example",
        "subject": "Invoice INV-1001",
        "attachments": [
          {"file_name": "INV-1001.pdf", "source_file": "INV-1001.pdf"}
        ]
      }
    ]
    """
    if not inbox_json_path.exists():
        raise FileNotFoundError(
            f"Inbox JSON not found: {inbox_json_path}. "
            f"Create {inbox_json_path} or set ICS_INBOX_JSON."
        )

    if not attachments_dir.exists():
        raise FileNotFoundError(
            f"Attachments directory not found: {attachments_dir}. "
            f"Create {attachments_dir} or set ICS_ATTACHMENTS_DIR."
        )

    raw = json.loads(inbox_json_path.read_text(encoding="utf-8"))

    by_folder: dict[str, list[dict]] = {}
    for m in raw:
        folder = str(m.get("folder_path", "") or "Inbox")
        by_folder.setdefault(folder, []).append(m)

    messages: list[JsonMessage] = []
    for folder in tracked_folders:
        items = (by_folder.get(folder, []) or [])[: int(max_items_per_folder)]

        for m in items:
            mid = str(m.get("message_id", "")).strip() or f"MSG-{len(messages)+1:04d}"
            subject = m.get("subject") or None
            sender = m.get("sender_address") or None
            received = m.get("received_datetime") or None

            atts: list[JsonAttachmentRef] = []
            for a in (m.get("attachments") or []):
                file_name = str(a.get("file_name", "") or "").strip()
                src_file = str(a.get("source_file", "") or "").strip()

                if not file_name or not src_file:
                    continue

                if not file_name.lower().endswith(".pdf"):
                    continue

                src_path = attachments_dir / src_file
                if not src_path.exists():
                    raise FileNotFoundError(
                        f"Attachment missing: {src_path} (referenced by message_id={mid})"
                    )

                atts.append(JsonAttachmentRef(file_name=file_name, source_path=src_path))

            messages.append(
                JsonMessage(
                    message_id=mid,
                    folder_path=str(m.get("folder_path") or folder),
                    received_datetime=str(received) if received else None,
                    sender_address=str(sender) if sender else None,
                    subject=str(subject) if subject else None,
                    attachments=atts,
                )
            )

    return messages


def _save_and_hash_pdf(
    staging_dir: Path,
    message_id: str,
    attachment_index: int,
    att: JsonAttachmentRef,
) -> PdfAttachment:
    """
    Copy attachment into staging with deterministic filename, then hash.
    """
    staging_dir.mkdir(parents=True, exist_ok=True)

    mid_short = short_message_id(message_id)
    safe_name = safe_filename(att.file_name)

    save_name = f"{mid_short}_{attachment_index:02d}_{safe_name}"
    save_path = staging_dir / save_name

    shutil.copyfile(att.source_path, save_path)
    document_hash = sha256_file(save_path)

    return PdfAttachment(
        attachment_index=attachment_index,
        file_name=att.file_name,
        size_bytes=save_path.stat().st_size,
        save_path=save_path,
        document_hash=document_hash,
    )


# =============================================================================
# Public entrypoint
# =============================================================================

def scan_outlook_to_db(
    *,
    mailbox_name: str = "DEMO_MAILBOX",
    tracked_folders: Optional[list[str]] = None,
    max_items_per_folder: Optional[int] = None,
) -> dict:
    """
    Scan inbox messages from JSON + attachments folder and persist results into SQLite.

    Public repo:
    - JSON-based adapter only
    - No Outlook integration
    """
    cfg = _get_config()

    if tracked_folders is None:
        tracked_folders = ["Inbox"]

    cap = int(max_items_per_folder) if max_items_per_folder is not None else cfg.max_items_per_folder

    initialise_database()

    scan_ts = now_iso_utc()
    messages_seen = 0
    pdfs_saved = 0

    conn = get_connection()
    try:
        conn.execute("BEGIN")
        begin_scan(conn, scan_ts)

        messages = _load_json_messages(
            cfg.inbox_json,
            cfg.attachments_dir,
            tracked_folders=list(tracked_folders),
            max_items_per_folder=cap,
        )

        folders_scanned = len({m.folder_path for m in messages})

        for msg in messages:
            messages_seen += 1
            attachment_count = len(msg.attachments)

            pdf_count_for_message = 0
            for idx, att in enumerate(msg.attachments, start=1):
                pdf_count_for_message += 1
                pdf = _save_and_hash_pdf(cfg.staging_dir, msg.message_id, idx, att)
                pdfs_saved += 1

                upsert_message(
                    conn,
                    message_id=msg.message_id,
                    current_location=msg.folder_path,
                    scan_ts=scan_ts,
                    received_datetime=msg.received_datetime,
                    sender_address=msg.sender_address,
                    subject=msg.subject,
                    has_attachments=attachment_count > 0,
                    attachment_count=pdf_count_for_message,  # PDFs only
                )

                upsert_invoice(
                    conn,
                    document_hash=pdf.document_hash,
                    message_id=msg.message_id,
                    attachment_file_name=att.file_name,
                    scan_ts=scan_ts,
                    source_folder_path=msg.folder_path,
                )

        conn.commit()

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return {
        "messages_seen": messages_seen,
        "pdfs_saved": pdfs_saved,
        "staging_dir": str(cfg.staging_dir),
        "scan_ts": scan_ts,
        "mailbox": mailbox_name,
        "folders_scanned": folders_scanned,
        "tracked_folders": list(tracked_folders),
        "max_items_per_folder": cap,
        "adapter": "json",
        "inbox_json": str(cfg.inbox_json),
        "attachments_dir": str(cfg.attachments_dir),
    }


if __name__ == "__main__":
    print(scan_outlook_to_db())
