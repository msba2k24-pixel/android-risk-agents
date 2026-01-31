# src/scrape_sources.py

import hashlib
import re
from datetime import datetime, timezone
from typing import Tuple

import requests
from bs4 import BeautifulSoup

from .db import get_supabase_client
from .config import USER_AGENT


HEADERS = {"User-Agent": USER_AGENT}

MIN_CLEAN_TEXT_LEN = 1200
MAX_CLEAN_TEXT_CHARS = 25000
REQUEST_TIMEOUT_S = 30


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def _normalize_ws(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _remove_noise(root) -> None:
    for tag in root.find_all(["script", "style", "noscript", "svg", "canvas"]):
        tag.decompose()
    # optional extra noise removal (safe)
    for tag in root.find_all(["nav", "footer", "header", "aside"]):
        tag.decompose()


def _pick_root(soup: BeautifulSoup):
    return soup.find("main") or soup.find("article") or soup.find("body") or soup


def _cap_text(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    head = text[: int(max_chars * 0.7)]
    tail = text[-int(max_chars * 0.3):]
    return head.rstrip() + "\n\n[...truncated...]\n\n" + tail.lstrip()


def fetch_raw_and_clean(url: str) -> Tuple[str, str]:
    """
    Returns (raw_html, clean_text).
    Raises requests exceptions upward so the job fails loudly (better for debugging).
    """
    resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT_S, allow_redirects=True)
    resp.raise_for_status()

    raw_html = resp.text or ""

    soup = BeautifulSoup(raw_html, "html.parser")
    root = _pick_root(soup)
    _remove_noise(root)

    clean_text = _normalize_ws(root.get_text("\n", strip=True))
    clean_text = _cap_text(clean_text, MAX_CLEAN_TEXT_CHARS)

    return raw_html, clean_text


def main():
    sb = get_supabase_client()
    now = _utc_now_iso()

    sources = (
        sb.table("sources")
        .select("id,name,url")
        .eq("active", True)
        .execute()
        .data
    )

    print(f"Found {len(sources)} active sources", flush=True)

    inserted = 0
    skipped = 0

    for s in sources:
        src_id = s["id"]
        name = s["name"]
        url = s["url"]

        try:
            raw_html, clean_text = fetch_raw_and_clean(url)
        except Exception as e:
            # fail loudly so you can see the real cause in Actions logs
            raise RuntimeError(f"Fetch failed for source='{name}' url='{url}': {e}")

        if len(clean_text) < MIN_CLEAN_TEXT_LEN:
            skipped += 1
            print(f"Skipped (too short): {name} len={len(clean_text)}", flush=True)
            continue

        payload = {
            "source_id": src_id,
            "fetched_at": now,
            "content_hash": _sha256(clean_text),
            "raw_text": raw_html,
            "clean_text": clean_text,
        }

        res = sb.table("snapshots").insert(payload).execute()

        if not res.data:
            raise RuntimeError(f"Insert failed for source='{name}' url='{url}'")

        inserted += 1
        print(f"Inserted snapshot: {name}", flush=True)

    print(f"✅ Done. inserted={inserted} skipped={skipped}", flush=True)


if __name__ == "__main__":
    main()