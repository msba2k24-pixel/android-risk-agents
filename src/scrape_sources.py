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
    lines = [line.strip() for line in text.splitlines()]
    return "\n".join([l for l in lines if l])

def hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def main():
    sb = get_supabase_client()
    now = datetime.now(timezone.utc).isoformat()

    sources = sb.table("sources").select("*").eq("enabled", True).execute().data
    if not sources:
        print("No enabled sources found.")
        return

    headers = {"User-Agent": USER_AGENT}

    for src in sources:
        print(f"Fetching {src['name']} -> {src['url']}")

        response = requests.get(src["url"], headers=headers, timeout=30)
        response.raise_for_status()

        extracted_text = clean_html(response.text)
        content_hash = hash_text(extracted_text)

        payload = {
            "source_id": src["id"],
            "fetched_at": now,
            "content_hash": content_hash,
            "raw_url": src["url"],
            "extracted_text": extracted_text,
        }

        sb.table("snapshots").insert(payload).execute()
        print(f"✅ Snapshot stored for {src['name']} (hash={content_hash[:10]}...)")

if __name__ == "__main__":
    main()