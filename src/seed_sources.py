from datetime import datetime, timezone
from .db import get_supabase_client

AGENT_NAME = "android-risk-agent"

SOURCES = [
    {
        "name": "Android Security Bulletins",
        "url": "https://source.android.com/docs/security/bulletin/asb-overview",
        "fetch_type": "html",
    },
    {
        "name": "Android Developers Blog",
        "url": "https://android-developers.googleblog.com/",
        "fetch_type": "html",
    },
    {
        "name": "Google Play Developer Policy Center",
        "url": "https://play.google/developer-content-policy/",
        "fetch_type": "html",
    },
    {
        "name": "Play Integrity API Docs",
        "url": "https://developer.android.com/google/play/integrity",
        "fetch_type": "html",
    },
]

def main():
    sb = get_supabase_client()
    now = datetime.now(timezone.utc).isoformat()

    for src in SOURCES:
        payload = {
            "agent_name": AGENT_NAME,
            "name": src["name"],
            "url": src["url"],
            "fetch_type": src["fetch_type"],
            "active": True,
            "created_at": now,
        }

        sb.table("sources").upsert(payload, on_conflict="url").execute()

    print("✅ Sources seeded")

if __name__ == "__main__":
    main()