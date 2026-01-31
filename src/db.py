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
    Your schema:
      changes: id, source_id, prev_snapshot_id, new_snapshot_id, created_at
      insights: change_id
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

    insights_resp = (
        sb.table("insights")
        .select("change_id")
        .in_("change_id", change_ids)
        .execute()
    )
    existing = {r["change_id"] for r in (insights_resp.data or []) if r.get("change_id") is not None}

    out: List[ChangeRow] = []
    for c in changes:
        cid = c.get("id")
        if cid is None or cid in existing:
            continue

        source_id = c.get("source_id")
        if source_id is None:
            continue

        url = _get_source_url(int(source_id))

        new_snapshot_id = c.get("new_snapshot_id")
        if new_snapshot_id is None:
            continue

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


def insert_insight(
    change_id: int,
    agent_name: str,
    title: str,
    summary: str,
    confidence: float,
) -> None:
    """
    Option A (demo): write only minimal fields to insights table.
    """
    sb = get_supabase_client()

    payload = {
        "change_id": int(change_id),
        "agent_name": agent_name,
        "title": title,
        "summary": summary,
        "confidence": float(confidence),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    sb.table("insights").insert(payload).execute()