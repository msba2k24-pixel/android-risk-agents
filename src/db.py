# src/db.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from supabase import create_client

from .config import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, validate_env


def get_supabase_client():
    validate_env()
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)


@dataclass
class ChangeRow:
    id: int
    source_id: int
    url: str
    old_snapshot_id: Optional[int]
    new_snapshot_id: int


def _safe_first(data: Any) -> Optional[Dict[str, Any]]:
    if isinstance(data, list) and len(data) > 0:
        return data[0]
    return None


def get_snapshot_text_by_id(snapshot_id: int) -> str:
    sb = get_supabase_client()
    resp = (
        sb.table("snapshots")
        .select("clean_text")
        .eq("id", snapshot_id)
        .limit(1)
        .execute()
    )
    row = _safe_first(resp.data)
    if not row:
        return ""
    return row.get("clean_text") or ""


def _get_source_url(source_id: int) -> str:
    sb = get_supabase_client()
    resp = (
        sb.table("sources")
        .select("url")
        .eq("id", source_id)
        .limit(1)
        .execute()
    )
    row = _safe_first(resp.data)
    if not row:
        return ""
    return row.get("url") or ""


def get_uninsighted_changes(limit: int = 25) -> List[ChangeRow]:
    """
    Returns latest changes that do not yet have an insights row.
    Schema:
      changes: id, source_id, prev_snapshot_id, new_snapshot_id, created_at
      insights: change_id (UNIQUE)
    """
    sb = get_supabase_client()

    changes_resp = (
        sb.table("changes")
        .select("id, source_id, prev_snapshot_id, new_snapshot_id, created_at")
        .order("created_at", desc=True)
        .limit(max(limit * 3, limit))
        .execute()
    )

    changes = changes_resp.data or []
    if not changes:
        return []

    change_ids = [c["id"] for c in changes if c.get("id") is not None]
    if not change_ids:
        return []

    insights_resp = (
        sb.table("insights")
        .select("change_id")
        .in_("change_id", change_ids)
        .execute()
    )
    existing = {
        r["change_id"]
        for r in (insights_resp.data or [])
        if r.get("change_id") is not None
    }

    out: List[ChangeRow] = []
    for c in changes:
        cid = c.get("id")
        if cid is None or cid in existing:
            continue

        source_id = c.get("source_id")
        if source_id is None:
            continue

        new_snapshot_id = c.get("new_snapshot_id")
        if new_snapshot_id is None:
            continue

        url = _get_source_url(int(source_id))

        out.append(
            ChangeRow(
                id=int(cid),
                source_id=int(source_id),
                url=url,
                old_snapshot_id=c.get("prev_snapshot_id"),
                new_snapshot_id=int(new_snapshot_id),
            )
        )

        if len(out) >= limit:
            break

    return out


def create_baseline_changes(limit: int = 10) -> int:
    """
    First-run demo helper:
    changes.prev_snapshot_id is NOT NULL, so baseline uses prev=new=latest snapshot id.
    Creates "baseline" change rows for sources that have snapshots but no changes yet.
    """
    sb = get_supabase_client()

    snaps_resp = (
        sb.table("snapshots")
        .select("id, source_id, fetched_at")
        .order("fetched_at", desc=True)
        .limit(300)
        .execute()
    )
    snaps = snaps_resp.data or []
    if not snaps:
        return 0

    latest_by_source: Dict[int, Dict[str, Any]] = {}
    for s in snaps:
        sid = s.get("source_id")
        snap_id = s.get("id")
        if sid is None or snap_id is None:
            continue
        sid_i = int(sid)
        if sid_i not in latest_by_source:
            latest_by_source[sid_i] = s

    source_ids = list(latest_by_source.keys())
    if not source_ids:
        return 0

    ch_resp = (
        sb.table("changes")
        .select("source_id")
        .in_("source_id", source_ids)
        .execute()
    )
    existing_sources = {
        int(r["source_id"])
        for r in (ch_resp.data or [])
        if r.get("source_id") is not None
    }

    to_insert: List[Dict[str, Any]] = []
    now = datetime.now(timezone.utc).isoformat()

    for sid, snap in latest_by_source.items():
        if sid in existing_sources:
            continue

        snap_id = int(snap["id"])

        to_insert.append(
            {
                "source_id": sid,
                "prev_snapshot_id": snap_id,
                "new_snapshot_id": snap_id,
                "diff_json": {"type": "baseline", "note": "Initial baseline change for demo"},
                "created_at": now,
            }
        )

        if len(to_insert) >= limit:
            break

    if not to_insert:
        return 0

    sb.table("changes").insert(to_insert).execute()
    return len(to_insert)


def insert_insight(
    change_id: int,
    agent_name: str,
    title: str,
    summary: str,
    confidence: float = 0.6,
    category: Optional[str] = None,
    affected_signals: Optional[List[str]] = None,
    recommended_actions: Optional[List[str]] = None,
    risk_score: Optional[int] = None,
) -> None:
    """
    Inserts an insight row. Since you added UNIQUE(change_id),
    we UPSERT on change_id to be idempotent on reruns.

    Option 1 DB: defaults exist for category, jsonb fields, created_at.
    We only include optional fields if provided so DB defaults can apply.
    """
    sb = get_supabase_client()

    payload: Dict[str, Any] = {
        "change_id": int(change_id),
        "agent_name": agent_name,
        "title": title,
        "summary": summary,
        "confidence": float(confidence),
    }

    if category is not None:
        payload["category"] = str(category)

    if affected_signals is not None:
        payload["affected_signals"] = affected_signals

    if recommended_actions is not None:
        payload["recommended_actions"] = recommended_actions

    if risk_score is not None:
        payload["risk_score"] = int(risk_score)

    # ✅ idempotent insert/update by change_id
    sb.table("insights").upsert(payload, on_conflict="change_id").execute()