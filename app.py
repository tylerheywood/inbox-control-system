from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List, Dict

import pandas as pd
import streamlit as st

from dashboard_data import (
    load_overview_data,
    load_status_breakdown_data,
    load_ageing_buckets_data,
    load_trends_data,
    load_worklist_data,
)

# =============================================================================
# AP Inbox Control — Dashboard (Public Demo)
#
# Public-safe rules:
# - Read-only UI (no writeback to DB)
# - DB path configurable via ICS_DB_PATH (defaults to ./inbox.db)
# - Assumes all data is synthetic when used in the public repo demo
# =============================================================================

APP_TITLE = "AP Inbox Control"
DB_PATH = Path(os.getenv("ICS_DB_PATH", str(Path(__file__).resolve().parent / "inbox.db")))

# -------------------- Formatting helpers --------------------
def fmt_dt(value: Any) -> str:
    if not value:
        return "—"
    s = str(value)
    return s.replace("T", " ").replace("+00:00", "").replace("Z", "")


def pence_or_zero(pence: Any) -> int:
    try:
        return int(pence) if pence is not None else 0
    except (TypeError, ValueError):
        return 0


def pence_to_gbp_value(pence: Any) -> float:
    return pence_or_zero(pence) / 100.0


def pence_to_gbp_str(pence: Any) -> str:
    v = pence_to_gbp_value(pence)
    return f"£{v:,.2f}"


def pct_str(value: Any) -> str:
    if value is None:
        return "—"
    try:
        return f"{float(value):.1f}%"
    except (TypeError, ValueError):
        return "—"


def days_str(value: Any) -> str:
    if value is None:
        return "—"
    try:
        return f"{int(value)} days"
    except (TypeError, ValueError):
        return "—"


def parse_iso_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        s = str(value).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


# -------------------- Cached data access --------------------
@st.cache_data(show_spinner=False, ttl=10)
def load_overview():
    return load_overview_data(DB_PATH)


@st.cache_data(show_spinner=False, ttl=10)
def load_status_breakdown():
    return load_status_breakdown_data(DB_PATH)


@st.cache_data(show_spinner=False, ttl=10)
def load_ageing_buckets():
    return load_ageing_buckets_data(DB_PATH)


@st.cache_data(show_spinner=False, ttl=10)
def load_worklist():
    return load_worklist_data(DB_PATH)


@st.cache_data(show_spinner=False, ttl=30)
def load_trends():
    return load_trends_data(DB_PATH)


def _worklist_to_dataframe(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Convert worklist rows into a dataframe suitable for sorting/filtering.

    Adds:
    - Gross £ (derived if gross_pence exists)
    - Received (parsed datetime for proper sorting; displayed as string)
    """
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # Normalise expected columns (public-safe; tolerate missing)
    expected = [
        "priority",
        "next_action",
        "action_reason",
        "sender_domain",
        "email_subject",
        "attachment_name",
        "received_datetime",
        "document_hash",
        "is_currently_present",
        "gross_pence",
    ]
    for col in expected:
        if col not in df.columns:
            df[col] = None

    # Friendly fields
    df["Gross £"] = df["gross_pence"].apply(lambda x: pence_to_gbp_str(x) if x is not None else "—")
    df["Received"] = df["received_datetime"].apply(fmt_dt)

    # Parsed datetime for correct sorting (hidden column)
    df["_received_dt"] = df["received_datetime"].apply(parse_iso_dt)

    # Choose display order (keep hash at the end)
    display_cols = [
        "priority",
        "next_action",
        "action_reason",
        "Gross £",
        "sender_domain",
        "Received",
        "email_subject",
        "attachment_name",
        "document_hash",
    ]

    # Only keep columns that exist (defensive)
    display_cols = [c for c in display_cols if c in df.columns]
    df = df[display_cols + (["_received_dt"] if "_received_dt" in df.columns else [])]

    return df


# -------------------- UI --------------------
st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)

with st.sidebar:
    st.subheader("Data source")
    st.caption("Set `ICS_DB_PATH` to point to a different SQLite DB.")
    st.code(str(DB_PATH), language="text")
    st.divider()
    st.caption("Run")
    st.code("streamlit run app.py", language="bash")

m = load_overview()
if "_error" in m:
    st.error(m["_error"])
    st.stop()

st.caption(f"Most Recent Scan: {fmt_dt(m.get('last_scan'))}")
st.caption("Disclosure: Exposure values may include a median-based estimate where invoices are missing extracted £ totals.")

tabs = st.tabs(["Overview", "Worklist", "Exceptions", "Ageing", "Trends"])

# ---------------- Tab 1: Overview ----------------
with tabs[0]:
    show_breakdown = st.toggle("Show exposure breakdown (Known vs Estimated)", value=False)

    c1, c2, c3, c4 = st.columns([1.4, 1.2, 1.2, 0.9])

    with c1:
        st.metric(
            "Total Estimated Exposure",
            pence_to_gbp_str(m.get("total_estimated_exposure_pence")),
            f"{m.get('total_present', 0)} invoices",
        )

        if m.get("missing_value_count", 0) > 0 and m.get("estimated_missing_exposure_pence") is not None:
            st.caption(
                f"Value coverage: {m.get('value_covered', 0)}/{m.get('total_present', 0)} ({pct_str(m.get('value_coverage_pct'))}). "
                f"Includes estimate for {m.get('missing_value_count', 0)} invoice(s) missing £ values "
                f"using median ({pence_to_gbp_str(m.get('median_gross_pence'))})."
            )
        else:
            st.caption(
                f"Value coverage: {m.get('value_covered', 0)}/{m.get('total_present', 0)} ({pct_str(m.get('value_coverage_pct'))}). "
                "No estimation required."
            )

        st.caption(
            f"Known: {pence_to_gbp_str(m.get('known_exposure_pence'))} · "
            f"Estimated: {pence_to_gbp_str(m.get('estimated_missing_exposure_pence'))}"
        )

        if show_breakdown:
            st.caption("Estimate method: median of invoices with extracted £ totals × count of missing values.")

    with c2:
        st.metric(
            "Invoices awaiting manual review",
            pence_to_gbp_str(m.get("manual_total_estimated_exposure_pence")),
            f"{m.get('manual_count', 0)} invoices",
        )

        st.caption(
            f"Known: {pence_to_gbp_str(m.get('manual_known_exposure_pence'))} · "
            f"Estimated: {pence_to_gbp_str(m.get('manual_estimated_missing_exposure_pence'))}"
        )

        if show_breakdown and m.get("manual_missing_value_count", 0) > 0 and m.get("median_gross_pence") is not None:
            st.caption(
                f"Manual lane estimate covers {m.get('manual_missing_value_count', 0)} invoice(s) missing £ values "
                f"using median ({pence_to_gbp_str(m.get('median_gross_pence'))})."
            )

    with c3:
        st.metric(
            "Invoices ready to be posted",
            pence_to_gbp_str(m.get("ready_exposure_pence")),
            f"{m.get('ready_count', 0)} invoices",
        )

        st.caption(f"Known: {pence_to_gbp_str(m.get('ready_known_exposure_pence'))} · Estimated: £0.00")

        if show_breakdown:
            st.caption("Ready lane uses extracted totals only (no estimation).")

    with c4:
        st.subheader("Signals")
        st.metric("PO confidence", pct_str(m.get("po_confidence")))
        st.metric("Biggest invoice", pence_to_gbp_str(m.get("biggest_invoice_pence")))
        st.metric("Oldest invoice", days_str(m.get("oldest_days")))

    st.divider()
    st.subheader("Total Estimated Exposure Over Time")
    st.caption("Use the Trends tab once snapshotting is enabled.")

# ---------------- Tab 2: Worklist ----------------
with tabs[1]:
    st.subheader("Worklist")
    st.caption(
        "This is the current AP queue computed from the database truth. "
        "Click a column header to sort. Use filters to narrow down."
    )

    rows = load_worklist()
    if not rows:
        st.info("No worklist rows available. Run the pipeline to generate invoice_worklist.")
    else:
        df = _worklist_to_dataframe(rows)

        # Filters (simple, fast, public-friendly)
        c1, c2, c3 = st.columns([1.0, 1.2, 1.2])

        with c1:
            actions = ["All"] + sorted([a for a in df["next_action"].dropna().unique().tolist() if str(a).strip()])
            action_filter = st.selectbox("Next action", actions, index=0)

        with c2:
            reasons = ["All"] + sorted([a for a in df["action_reason"].dropna().unique().tolist() if str(a).strip()])
            reason_filter = st.selectbox("Reason", reasons, index=0)

        with c3:
            domain_val = st.text_input("Sender domain contains", value="")

        filtered = df.copy()

        if action_filter != "All":
            filtered = filtered[filtered["next_action"] == action_filter]

        if reason_filter != "All":
            filtered = filtered[filtered["action_reason"] == reason_filter]

        if domain_val.strip():
            needle = domain_val.strip().lower()
            filtered = filtered[
                filtered["sender_domain"].fillna("").astype(str).str.lower().str.contains(needle, na=False)
            ]

        # Default sort (priority asc, then received desc)
        if "_received_dt" in filtered.columns:
            filtered = filtered.sort_values(by=["priority", "_received_dt"], ascending=[True, False])

        # Hide helper sort column
        if "_received_dt" in filtered.columns:
            filtered_display = filtered.drop(columns=["_received_dt"])
        else:
            filtered_display = filtered

        st.dataframe(
            filtered_display,
            use_container_width=True,
            hide_index=True,
        )

        st.caption(f"Showing {len(filtered_display)} of {len(df)} worklist item(s).")

# ---------------- Tab 3: Exceptions ----------------
with tabs[2]:
    st.subheader("Exceptions & Status Breakdown")
    st.caption(
        "Counts are for invoices currently present in the inbox. "
        "Values include only invoices where a gross total is available."
    )

    breakdown = load_status_breakdown()
    if not breakdown:
        st.info("No status breakdown available (missing `po_match_status` or no invoices present).")
    else:
        c1, c2, c3 = st.columns([1.1, 1.1, 1.1])
        with c1:
            st.metric(
                "Unreadable (NO_TEXT_LAYER)",
                str(m.get("ocr_needed_count")) if m.get("ocr_needed_count") is not None else "—",
            )
        with c2:
            st.metric("Manual review invoices", str(m.get("manual_count", 0)))
        with c3:
            st.metric("Ready invoices", str(m.get("ready_count", 0)))

        st.divider()

        table = [
            {
                "Status": r.get("status"),
                "Count": int(r.get("cnt", 0)),
                "Known total (£)": pence_to_gbp_str(r.get("gross_pence")),
            }
            for r in breakdown
        ]
        st.dataframe(table, use_container_width=True, hide_index=True)

# ---------------- Tab 4: Ageing ----------------
with tabs[3]:
    st.subheader("Ageing Buckets")
    st.caption("Age is calculated from first seen datetime. Values include only invoices where a gross total is available.")

    rows = load_ageing_buckets()
    if not rows:
        st.info("No invoices currently present.")
    else:
        buckets: dict[str, dict[str, dict[str, Any]]] = {}
        for r in rows:
            b = r.get("age_bucket")
            lane = r.get("lane")
            buckets.setdefault(b, {})
            buckets[b][lane] = {
                "cnt": int(r.get("cnt", 0)),
                "gross": pence_to_gbp_str(r.get("gross_pence")),
            }

        out = []
        order = ["0-1 days", "2-3 days", "4-7 days", "8-14 days", "15+ days"]
        for b in order:
            ready = buckets.get(b, {}).get("Ready", {"cnt": 0, "gross": pence_to_gbp_str(0)})
            manual = buckets.get(b, {}).get("Manual", {"cnt": 0, "gross": pence_to_gbp_str(0)})
            out.append(
                {
                    "Age bucket": b,
                    "Ready count": ready["cnt"],
                    "Ready £": ready["gross"],
                    "Manual count": manual["cnt"],
                    "Manual £": manual["gross"],
                }
            )

        st.dataframe(out, use_container_width=True, hide_index=True)

# ---------------- Tab 5: Trends ----------------
with tabs[4]:
    st.subheader("Trends (Daily Snapshots)")
    st.caption(
        "This tab becomes active once daily snapshotting is enabled. "
        "Snapshots let you track exposure and workload trends over time."
    )

    trends = load_trends()
    if not trends:
        st.info("Snapshotting not enabled yet.")
    else:
        st.dataframe(trends, use_container_width=True, hide_index=True)
        st.caption("Showing most recent snapshots (latest first).")
