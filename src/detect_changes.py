from datetime import datetime, timezone
from .db import get_supabase_client

def main():
    sb = get_supabase_client()
    now = datetime.now(timezone.utc).isoformat()

    sources = sb.table("sources").select("id,name").eq("active", True).execute().data

    for src in sources:
        snaps = (
            sb.table("snapshots")
            .select("id,content_hash,fetched_at")
            .eq("source_id", src["id"])
            .order("fetched_at", desc=True)
            .limit(2)
            .execute()
            .data
        )

        if len(snaps) < 2:
            continue

        latest, previous = snaps

        if latest["content_hash"] == previous["content_hash"]:
            continue

        payload = {
            "source_id": src["id"],
            "prev_snapshot_id": previous["id"],
            "new_snapshot_id": latest["id"],
            "diff_json": {
                "prev_hash": previous["content_hash"],
                "new_hash": latest["content_hash"],
            },
            "created_at": now,
        }

        sb.table("changes").insert(payload).execute()
        print(f"🚨 Change detected for {src['name']}")

if __name__ == "__main__":
    main()