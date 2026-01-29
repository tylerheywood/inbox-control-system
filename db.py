from __future__ import annotations

import os
import sqlite3
from pathlib import Path

"""
db.py (public-safe)

- Creates tables
- Enforces constraints
- Provides connection helper
- Performs lightweight, idempotent schema guards (SQLite-friendly)

Public demo convention:
- DB path can be overridden with ICS_DB_PATH
- Default DB is ./inbox.db (repo root)
"""

BASE_DIR = Path(__file__).resolve().parent


def _resolve_db_path() -> Path:
    raw = os.getenv("ICS_DB_PATH", "").strip()
    return Path(raw) if raw else (BASE_DIR / "inbox.db")


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(_resolve_db_path())
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------
def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1;",
        (name,),
    ).fetchone()
    return row is not None


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    if not _table_exists(conn, table):
        return False
    rows = conn.execute(f"PRAGMA table_info({table});").fetchall()
    return any(r["name"] == column for r in rows)


# ---------------------------------------------------------------------------
# Lightweight migrations / schema guards (idempotent)
# ---------------------------------------------------------------------------
def _migrate_add_po_validation_status(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "inbox_invoice"):
        return
    if not _column_exists(conn, "inbox_invoice", "po_validation_status"):
        conn.execute(
            "ALTER TABLE inbox_invoice ADD COLUMN po_validation_status TEXT NOT NULL DEFAULT 'UNVALIDATED';"
        )


def _migrate_add_ready_to_post(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "inbox_invoice"):
        return

    if not _column_exists(conn, "inbox_invoice", "ready_to_post"):
        conn.execute("ALTER TABLE inbox_invoice ADD COLUMN ready_to_post INTEGER;")

    # Backfill once (only NULL), based on canonical validation truth
    if _column_exists(conn, "inbox_invoice", "po_validation_status"):
        conn.execute(
            """
            UPDATE inbox_invoice
            SET ready_to_post = CASE
                WHEN po_validation_status = 'VALID_PO' THEN 1
                ELSE 0
            END
            WHERE ready_to_post IS NULL
            """
        )

    # Defensive normalisation
    conn.execute(
        """
        UPDATE inbox_invoice
        SET ready_to_post = 0
        WHERE ready_to_post IS NOT NULL AND ready_to_post NOT IN (0,1)
        """
    )


def _migrate_add_po_master_approval_status(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "po_master"):
        return
    if not _column_exists(conn, "po_master", "approval_status"):
        conn.execute("ALTER TABLE po_master ADD COLUMN approval_status TEXT;")


def _migrate_add_worklist_identity_columns(conn: sqlite3.Connection) -> None:
    identity_cols = (
        ("sender_domain", "TEXT"),
        ("email_subject", "TEXT"),
        ("attachment_name", "TEXT"),
        ("received_datetime", "TEXT"),
    )

    if _table_exists(conn, "invoice_worklist"):
        for col, col_type in identity_cols:
            if not _column_exists(conn, "invoice_worklist", col):
                conn.execute(f"ALTER TABLE invoice_worklist ADD COLUMN {col} {col_type};")

    if _table_exists(conn, "invoice_worklist_history"):
        for col, col_type in identity_cols:
            if not _column_exists(conn, "invoice_worklist_history", col):
                conn.execute(f"ALTER TABLE invoice_worklist_history ADD COLUMN {col} {col_type};")


def _ensure_indexes(conn: sqlite3.Connection) -> None:
    # Safe indexes that only reference columns guaranteed by base schema or migrations
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_message_location
        ON inbox_message (current_location, is_currently_present);
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_message_next_step ON inbox_message (next_step);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_message_received ON inbox_message (received_datetime);")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_invoice_message ON inbox_invoice (message_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_invoice_presence ON inbox_invoice (is_currently_present, processing_status);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_invoice_po_count_status ON inbox_invoice (po_count, po_match_status);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_invoice_supplier_status ON inbox_invoice (supplier_account_expected, supplier_validation_status);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_invoice_posting ON inbox_invoice (processing_status, posted_datetime);")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_invoice_po_po ON invoice_po (po_number);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_po_supplier ON po_master (supplier_account);")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_worklist_action ON invoice_worklist (next_action, priority);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_worklist_present ON invoice_worklist (is_currently_present, priority);")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_worklist_hist_run ON invoice_worklist_history (run_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_worklist_hist_doc ON invoice_worklist_history (document_hash);")
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_worklist_hist_run_doc
        ON invoice_worklist_history (run_id, document_hash);
        """
    )

    # Ready index only if column exists
    if _column_exists(conn, "inbox_invoice", "ready_to_post"):
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_invoice_ready_present
            ON inbox_invoice (is_currently_present, ready_to_post);
            """
        )


# ---------------------------------------------------------------------------
# Create schema
# ---------------------------------------------------------------------------
def initialise_database() -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.executescript(
        """
        -- =========================================================
        -- Inbox Message
        -- =========================================================
        CREATE TABLE IF NOT EXISTS inbox_message (
            message_id TEXT PRIMARY KEY,

            current_location TEXT NOT NULL,
            first_seen_datetime TEXT NOT NULL,
            last_seen_datetime  TEXT NOT NULL,
            last_scan_datetime  TEXT NOT NULL,

            is_currently_present INTEGER NOT NULL CHECK (is_currently_present IN (0,1)),

            received_datetime TEXT,
            sender_address    TEXT,
            subject           TEXT,

            has_attachments   INTEGER NOT NULL CHECK (has_attachments IN (0,1)),
            attachment_count  INTEGER NOT NULL CHECK (attachment_count >= 0),

            next_step TEXT,
            automation_status TEXT,
            automation_error_detail TEXT,
            last_action_datetime TEXT
        );

        -- =========================================================
        -- Inbox Invoice
        -- =========================================================
        CREATE TABLE IF NOT EXISTS inbox_invoice (
            document_hash TEXT PRIMARY KEY,
            message_id TEXT NOT NULL,
            attachment_file_name TEXT NOT NULL,

            first_seen_datetime TEXT NOT NULL,
            last_seen_datetime  TEXT NOT NULL,
            last_scan_datetime  TEXT NOT NULL,

            is_currently_present INTEGER NOT NULL CHECK (is_currently_present IN (0,1)),
            source_folder_path TEXT,

            po_count INTEGER NOT NULL CHECK (po_count >= 0),
            po_match_status TEXT NOT NULL,

            -- If present, dashboard/worklist prefers this (migrated for older DBs)
            ready_to_post INTEGER NOT NULL DEFAULT 0 CHECK (ready_to_post IN (0,1)),

            supplier_account_expected   TEXT,
            supplier_validation_status  TEXT,

            processing_status TEXT NOT NULL,
            posted_datetime   TEXT,

            net_total   INTEGER,
            vat_total   INTEGER,
            gross_total INTEGER,

            review_outcome    TEXT,
            reviewed_datetime TEXT,
            reviewed_by       TEXT,
            review_note       TEXT,

            FOREIGN KEY (message_id) REFERENCES inbox_message(message_id)
                ON UPDATE CASCADE
                ON DELETE RESTRICT
        );

        -- =========================================================
        -- Detected PO evidence
        -- =========================================================
        CREATE TABLE IF NOT EXISTS invoice_po (
            document_hash TEXT NOT NULL,
            po_number     TEXT NOT NULL,
            detected_datetime TEXT,

            PRIMARY KEY (document_hash, po_number),
            FOREIGN KEY (document_hash) REFERENCES inbox_invoice(document_hash)
                ON UPDATE CASCADE
                ON DELETE CASCADE
        );

        -- =========================================================
        -- PO master snapshot
        -- =========================================================
        CREATE TABLE IF NOT EXISTS po_master (
            po_number TEXT PRIMARY KEY,
            supplier_account TEXT NOT NULL,
            po_status TEXT,
            approval_status TEXT,
            last_import_datetime TEXT NOT NULL
        );

        -- =========================================================
        -- Supplier master snapshot
        -- =========================================================
        CREATE TABLE IF NOT EXISTS supplier_master (
            supplier_account TEXT PRIMARY KEY,
            supplier_name TEXT NOT NULL,
            payment_hold INTEGER CHECK (payment_hold IN (0,1)),
            registered_address TEXT,
            last_import_datetime TEXT NOT NULL
        );

        -- =========================================================
        -- Human resolution (optional)
        -- =========================================================
        CREATE TABLE IF NOT EXISTS invoice_resolution (
            document_hash TEXT PRIMARY KEY,
            resolve_po_number TEXT,
            resolution_status TEXT NOT NULL,
            resolved_by TEXT,
            resolved_datetime TEXT,
            resolution_note TEXT,

            FOREIGN KEY (document_hash) REFERENCES inbox_invoice(document_hash)
                ON UPDATE CASCADE
                ON DELETE CASCADE
        );

        -- =========================================================
        -- Worklist cache + history
        -- =========================================================
        CREATE TABLE IF NOT EXISTS invoice_worklist (
            document_hash TEXT PRIMARY KEY,

            sender_domain TEXT,
            email_subject TEXT,
            attachment_name TEXT,
            received_datetime TEXT,

            next_action TEXT NOT NULL,
            action_reason TEXT NOT NULL,
            priority INTEGER NOT NULL,
            generated_at_utc TEXT NOT NULL,
            is_currently_present INTEGER NOT NULL CHECK (is_currently_present IN (0,1)),

            FOREIGN KEY (document_hash) REFERENCES inbox_invoice(document_hash)
                ON UPDATE CASCADE
                ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS invoice_worklist_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            document_hash TEXT NOT NULL,

            sender_domain TEXT,
            email_subject TEXT,
            attachment_name TEXT,
            received_datetime TEXT,

            next_action TEXT NOT NULL,
            action_reason TEXT NOT NULL,
            priority INTEGER NOT NULL,
            generated_at_utc TEXT NOT NULL,
            is_currently_present INTEGER NOT NULL CHECK (is_currently_present IN (0,1)),

            FOREIGN KEY (document_hash) REFERENCES inbox_invoice(document_hash)
                ON UPDATE CASCADE
                ON DELETE CASCADE
        );
        """
    )

    # Migrations for older DBs (idempotent)
    _migrate_add_po_master_approval_status(conn)
    _migrate_add_po_validation_status(conn)
    _migrate_add_ready_to_post(conn)
    _migrate_add_worklist_identity_columns(conn)

    # Indexes (safe, idempotent)
    _ensure_indexes(conn)

    conn.commit()
    conn.close()


def reset_database() -> None:
    conn = get_connection()
    cur = conn.cursor()
    cur.executescript(
        """
        DROP TABLE IF EXISTS invoice_worklist_history;
        DROP TABLE IF EXISTS invoice_worklist;
        DROP TABLE IF EXISTS invoice_resolution;
        DROP TABLE IF EXISTS invoice_po;
        DROP TABLE IF EXISTS inbox_invoice;
        DROP TABLE IF EXISTS inbox_message;
        DROP TABLE IF EXISTS po_master;
        DROP TABLE IF EXISTS supplier_master;
        """
    )
    conn.commit()
    conn.close()
