from datetime import datetime, timezone
from .db import get_supabase_client

def main():
    sb = get_supabase_client()
    now = datetime.now(timezone.utc).isoformat()

    sources = sb.table("sources").select("id,name").eq("enabled", True).execute().data
    if not sources:
        print("No enabled sources.")
        return

    for src in sources:
        snapshots = (
            sb.table("snapshots")
            .select("id,content_hash,fetched_at")
            .eq("source_id", src["id"])
            .order("fetched_at", desc=True)
            .limit(2)
            .execute()
            .data
        )

        if len(snapshots) < 2:
            print(f"Skipping {src['name']} (not enough snapshots)")
            continue

        latest, previous = snapshots[0], snapshots[1]

        if latest["content_hash"] == previous["content_hash"]:
            print(f"No change detected for {src['name']}")
            continue

        payload = {
            "source_id": src["id"],
            "prev_snapshot_id": previous["id"],
            "new_snapshot_id": latest["id"],
            "detected_at": now,
            "status": "new",
        }

        sb.table("changes").insert(payload).execute()
        print(f"🚨 Change detected for {src['name']}")

if __name__ == "__main__":
    main()