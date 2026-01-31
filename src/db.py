# src/db.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from supabase import create_client

from .config import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, validate_env


def get_supabase_client():
    validate_env()
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)


@dataclass
class ChangeRow:
    id: str
    source_id: str
    url: str
    old_snapshot_id: Optional[str]
    new_snapshot_id: str


def _safe_first(data: Any) -> Optional[Dict[str, Any]]:
    if isinstance(data, list) and len(data) > 0:
        return data[0]
    return None


def get_snapshot_text_by_id(snapshot_id: str) -> str:
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


def _get_source_url(source_id: str) -> str:
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
    Strategy:
    - Fetch recent changes
    - Fetch insights for those change ids
    - Filter out changes that already have insights
    """
    sb = get_supabase_client()

    changes_resp = (
        sb.table("changes")
        .select("id, source_id, url, old_snapshot_id, new_snapshot_id, created_at_utc, created_at")
        .order("created_at_utc", desc=True)
        .limit(max(limit * 3, limit))
        .execute()
    )

    changes = changes_resp.data or []
    if not changes:
        return []

    change_ids = [c["id"] for c in changes if c.get("id")]

    # Pull any existing insights for these change ids
    insights_resp = (
        sb.table("insights")
        .select("change_id")
        .in_("change_id", change_ids)
        .execute()
    )
    existing = set()
    for r in (insights_resp.data or []):
        cid = r.get("change_id")
        if cid:
            existing.add(cid)

    out: List[ChangeRow] = []
    for c in changes:
        cid = c.get("id")
        if not cid or cid in existing:
            continue

        source_id = c.get("source_id") or ""
        url = c.get("url") or ""
        if not url and source_id:
            url = _get_source_url(source_id)

        new_snapshot_id = c.get("new_snapshot_id") or ""
        if not new_snapshot_id:
            continue

        out.append(
            ChangeRow(
                id=cid,
                source_id=source_id,
                url=url,
                old_snapshot_id=c.get("old_snapshot_id"),
                new_snapshot_id=new_snapshot_id,
            )
        )

        if len(out) >= limit:
            break

    return out


def insert_insight(
    source_id: str,
    change_id: str,
    snapshot_id: str,
    insight_json: Dict[str, Any],
    generated_at_utc: str,
    model: str,
) -> None:
    sb = get_supabase_client()

    payload = {
        "source_id": source_id,
        "change_id": change_id,
        "snapshot_id": snapshot_id,
        "insight_json": insight_json,  # JSONB column recommended
        "generated_at_utc": generated_at_utc,
        "model": model,
    }

    sb.table("insights").insert(payload).execute()