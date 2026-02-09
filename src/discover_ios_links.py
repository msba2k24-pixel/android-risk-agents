# src/discover_ios_links.py
"""
iOS discovery (two-stage):
1) Start from Apple's iOS release notes index (121161), find unique support.apple.com/<id> links.
2) Expand a limited number of those discovered pages and find MORE unique support.apple.com/<id> links.
3) Upsert all discovered URLs into `sources` with agent_name="ios-risk-agent".

Goal: populate sources with iOS-related pages so scrape can snapshot/clean them.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Iterable, Set, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from .db import get_supabase_client
from .config import USER_AGENT

HEADERS = {"User-Agent": USER_AGENT}
TIMEOUT_S = 30

AGENT_NAME = "ios-risk-agent"
FETCH_TYPE = "html"

START_URL = "https://support.apple.com/en-us/121161"

# Stage limits (keep small for dry run)
TOP_LEVEL_MAX_LINKS = 30      # max links taken from 121161
EXPAND_MAX_PAGES = 10         # how many of those links to expand for second-stage discovery
SECOND_LEVEL_MAX_LINKS = 80   # max new links gathered from expanded pages

# Accept both /100100 and /en-us/100100 styles
APPLE_ID_RE = re.compile(r"^/((en-us|en-gb|en-in)/)?(\d{5,6})/?$")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_support_apple(url: str) -> bool:
    try:
        p = urlparse(url)
        return p.scheme in ("http", "https") and p.netloc.endswith("support.apple.com")
    except Exception:
        return False


def _canonicalize_support_apple(url: str) -> str | None:
    """
    Convert any support.apple.com URL into canonical form:
      https://support.apple.com/en-us/<id>
    If not an ID page, return None.
    """
    if not url:
        return None

    try:
        p = urlparse(url)
    except Exception:
        return None

    if not p.netloc.endswith("support.apple.com"):
        return None

    m = APPLE_ID_RE.match(p.path.strip())
    if not m:
        return None

    doc_id = m.group(3)
    return f"https://support.apple.com/en-us/{doc_id}"


def _extract_support_id_links(html: str, base_url: str) -> Set[str]:
    soup = BeautifulSoup(html, "html.parser")
    root = soup.find("main") or soup.find("article") or soup.find("body") or soup

    out: Set[str] = set()
    for a in root.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href:
            continue

        abs_url = urljoin(base_url, href)
        if not _is_support_apple(abs_url):
            continue

        canon = _canonicalize_support_apple(abs_url)
        if canon:
            out.add(canon)

    return out


def _fetch(url: str) -> str:
    resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT_S, allow_redirects=True)
    resp.raise_for_status()
    return resp.text or ""


def _upsert_sources(urls: Iterable[str]) -> int:
    sb = get_supabase_client()
    now = _utc_now_iso()

    inserted = 0
    for u in urls:
        doc_id = u.rstrip("/").split("/")[-1]
        name = f"Apple Support Doc - {doc_id}"

        payload = {
            "agent_name": AGENT_NAME,
            "name": name,
            "url": u,
            "fetch_type": FETCH_TYPE,
            "active": True,
            "created_at": now,
        }

        sb.table("sources").upsert(payload, on_conflict="url").execute()
        inserted += 1

    return inserted


def main():
    print(f"iOS discovery start: {START_URL}", flush=True)

    # ---- Stage 1: discover from 121161
    html = _fetch(START_URL)
    level1 = _extract_support_id_links(html, START_URL)

    # Keep deterministic order: sort and cap
    level1_sorted = sorted(level1)
    level1_sorted = level1_sorted[:TOP_LEVEL_MAX_LINKS]

    print(f"Stage1 found={len(level1)} using={len(level1_sorted)}", flush=True)

    # ---- Stage 2: expand a subset of stage1 pages to find more links
    level2: Set[str] = set()
    to_expand = level1_sorted[:EXPAND_MAX_PAGES]

    for i, url in enumerate(to_expand, start=1):
        try:
            page_html = _fetch(url)
            found = _extract_support_id_links(page_html, url)
            level2 |= found
            print(f"Expanded {i}/{len(to_expand)}: {url} -> +{len(found)} links", flush=True)
        except Exception as e:
            print(f"Expand failed: {url} err={e}", flush=True)

        if len(level2) >= SECOND_LEVEL_MAX_LINKS:
            break

    # Remove anything already in level1 to avoid noisy repeats
    level2_only = sorted(level2 - set(level1_sorted))[:SECOND_LEVEL_MAX_LINKS]

    print(f"Stage2 new_links={len(level2_only)} (raw_stage2={len(level2)})", flush=True)

    # ---- Upsert all
    all_urls = list(dict.fromkeys(level1_sorted + level2_only))  # stable dedupe, keep order
    upserted = _upsert_sources(all_urls)

    print(f"✅ iOS discovery done. upserted={upserted} total_urls={len(all_urls)}", flush=True)


if __name__ == "__main__":
    main()
