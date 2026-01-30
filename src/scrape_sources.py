import hashlib
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup

from .db import get_supabase_client
from .config import USER_AGENT

def clean_html(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator="\n")
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    return "\n".join(lines)

def hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def main():
    sb = get_supabase_client()
    now = datetime.now(timezone.utc).isoformat()

    sources = sb.table("sources").select("*").eq("active", True).execute().data
    headers = {"User-Agent": USER_AGENT}

    for src in sources:
        print(f"Fetching {src['name']}")

        r = requests.get(src["url"], headers=headers, timeout=30)
        r.raise_for_status()

        raw_text = r.text
        clean_text = clean_html(raw_text)
        content_hash = hash_text(clean_text)

        payload = {
            "source_id": src["id"],
            "fetched_at": now,
            "content_hash": content_hash,
            "raw_text": raw_text,
            "clean_text": clean_text,
        }

        sb.table("snapshots").insert(payload).execute()
        print(f"✅ Snapshot stored for {src['name']}")

if __name__ == "__main__":
    main()