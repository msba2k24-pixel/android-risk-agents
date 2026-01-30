from datetime import datetime, timezone
from .db import get_supabase_client

SOURCES = [
    {
        "name": "Android Security Bulletins",
        "url": "https://source.android.com/docs/security/bulletin/asb-overview",
        "type": "security",
    },
    {
        "name": "Android Developers Blog",
        "url": "https://android-developers.googleblog.com/",
        "type": "blog",
    },
    {
        "name": "Google Play Developer Policy Center",
        "url": "https://play.google/developer-content-policy/",
        "type": "policy",
    },
    {
        "name": "Play Integrity API Docs",
        "url": "https://developer.android.com/google/play/integrity",
        "type": "api",
    },
]

def main():
    sb = get_supabase_client()
    now = datetime.now(timezone.utc).isoformat()

    for source in SOURCES:
        payload = {
            "name": source["name"],
            "url": source["url"],
            "type": source["type"],
            "enabled": True,
            "cadence": "daily",
            "created_at": now,
            "updated_at": now,
        }

        sb.table("sources").upsert(payload, on_conflict="url").execute()

    print("✅ Sources seeded successfully.")

if __name__ == "__main__":
    main()