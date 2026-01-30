import hashlib
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
from readability import Document

from .db import get_supabase_client
from .config import USER_AGENT


def html_to_main_text(html: str) -> str:
    """
    Extract the main content from an HTML page using Readability.
    This removes most nav, sidebars, footer junk.
    """
    doc = Document(html)
    main_html = doc.summary(html_partial=True)
    soup = BeautifulSoup(main_html, "html.parser")
    return soup.get_text("\n", strip=True)


def hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def quality_gate(url: str, clean_text: str) -> None:
    """
    Fail the run if scrape is likely junk.
    This prevents silently storing useless snapshots.
    """
    # Too short often means blocked, empty, or only navigation extracted
    if len(clean_text) < 800:
        raise ValueError(f"Scrape too short for {url}. len={len(clean_text)}")

    bad_markers = [
        "Manage cookies",
        "unusual traffic",
        "enable cookies",
        "your request has been blocked",
        "Sign in",
    ]
    lowered = clean_text.lower()
    hits = [m for m in bad_markers if m.lower() in lowered]
    if hits:
        raise ValueError(f"Scrape looks like junk/block page for {url}. markers={hits}")


def main():
    sb = get_supabase_client()
    now = datetime.now(timezone.utc).isoformat()

    sources = (
        sb.table("sources")
        .select("*")
        .eq("active", True)
        .execute()
        .data
    )

    headers = {"User-Agent": USER_AGENT}

    for src in sources:
        url = src["url"]
        print(f"Fetching {src['name']} | {url}")

        r = requests.get(url, headers=headers, timeout=30)
        r.raise_for_status()

        raw_text = r.text
        clean_text = html_to_main_text(raw_text)

        # Make sure it is meaningful content
        quality_gate(url, clean_text)

        content_hash = hash_text(clean_text)

        payload = {
            "source_id": src["id"],
            "fetched_at": now,
            "content_hash": content_hash,
            "raw_text": raw_text,
            "clean_text": clean_text,
        }

        sb.table("snapshots").insert(payload).execute()
        print(f"✅ Snapshot stored for {src['name']} (len={len(clean_text)})")


if __name__ == "__main__":
    main()