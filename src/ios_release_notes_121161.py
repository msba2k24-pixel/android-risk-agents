# src/ios_release_notes_121161.py
# Dry-run iOS ingestion: fetch Apple release notes page once, split into per-patch sections,
# and insert snapshots into the SAME snapshots table.

import hashlib
import re
from datetime import datetime, timezone
from typing import List, Dict, Optional

import requests
from bs4 import BeautifulSoup, Tag

from .db import get_supabase_client
from .config import USER_AGENT

HEADERS = {"User-Agent": USER_AGENT}
BASE_URL = "https://support.apple.com/en-us/121161"

AGENT_NAME = "ios-risk-agent"
SOURCE_NAME = "Apple iOS Release Notes (121161)"
FETCH_TYPE = "html"

REQUEST_TIMEOUT_S = 30
MAX_CLEAN_TEXT_CHARS = 25000

# Keep small for dry run, increase later
TOP_N = 8

IOS_HEADING_RE = re.compile(r"^iOS\s+(\d+(?:\.\d+){0,2})\s*$", re.IGNORECASE)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def _normalize_ws(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _cap_text(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    head = text[: int(max_chars * 0.7)]
    tail = text[-int(max_chars * 0.3) :]
    return head.rstrip() + "\n\n[...truncated...]\n\n" + tail.lstrip()


def _is_ios_heading(tag: Tag) -> Optional[str]:
    if tag.name not in ("h1", "h2", "h3", "h4"):
        return None
    txt = tag.get_text(" ", strip=True)
    m = IOS_HEADING_RE.match(txt)
    if not m:
        return None
    return m.group(1)


def _remove_noise(root: Tag) -> None:
    for t in root.find_all(["script", "style", "noscript", "svg", "canvas"]):
        t.decompose()
    for t in root.find_all(["nav", "footer", "header", "aside"]):
        t.decompose()


def _pick_root(soup: BeautifulSoup) -> Tag:
    return soup.find("main") or soup.find("article") or soup.find("body") or soup


def _extract_sections(html: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    root = _pick_root(soup)
    _remove_noise(root)

    headings: List[Tag] = []
    versions: List[str] = []

    for h in root.find_all(["h1", "h2", "h3", "h4"]):
        v = _is_ios_heading(h)
        if v:
            headings.append(h)
            versions.append(v)

    out: List[Dict[str, str]] = []

    for i, h in enumerate(headings):
        version = versions[i]

        section_nodes: List[Tag] = []
        for sib in h.next_siblings:
            if isinstance(sib, Tag):
                if _is_ios_heading(sib):
                    break
                section_nodes.append(sib)

        section_text_parts: List[str] = []
        for node in section_nodes:
            if isinstance(node, Tag):
                section_text_parts.append(node.get_text("\n", strip=True))

        section_text = _normalize_ws("\n".join(section_text_parts))
        section_text = _cap_text(section_text, MAX_CLEAN_TEXT_CHARS)

        # Keep a compact HTML for raw_text so DB is not huge
        section_html = str(h) + "\n" + "\n".join([str(n) for n in section_nodes])

        out.append(
            {
                "version": version,
                "section_html": section_html,
                "section_text": section_text,
            }
        )

    return out


def _get_or_create_source_id(sb) -> int:
    now = _utc_now_iso()
    payload = {
        "agent_name": AGENT_NAME,
        "name": SOURCE_NAME,
        "url": BASE_URL,
        "fetch_type": FETCH_TYPE,
        "active": True,
        "created_at": now,
    }

    sb.table("sources").upsert(payload, on_conflict="url").execute()

    row = (
        sb.table("sources")
        .select("id")
        .eq("url", BASE_URL)
        .limit(1)
        .execute()
        .data
    )
    if not row:
        raise RuntimeError("Could not read back source id after upsert.")
    return int(row[0]["id"])


def main():
    sb = get_supabase_client()
    now = _utc_now_iso()

    source_id = _get_or_create_source_id(sb)

    resp = requests.get(BASE_URL, headers=HEADERS, timeout=REQUEST_TIMEOUT_S, allow_redirects=True)
    resp.raise_for_status()

    sections = _extract_sections(resp.text or "")
    if not sections:
        raise RuntimeError("No iOS sections found. Heading parsing failed.")

    # newest are usually at top, keep first TOP_N
    sections = sections[:TOP_N]

    inserted = 0
    for s in sections:
        version = s["version"]
        clean_text = s["section_text"]
        raw_html = s["section_html"]

        # Some patches are short. Still store for dry run, but you can add a min length if needed.
        content_hash = _sha256(clean_text)

        payload = {
            "source_id": source_id,
            "fetched_at": now,
            "content_hash": content_hash,
            "raw_text": raw_html,
            "clean_text": clean_text,
        }

        sb.table("snapshots").insert(payload).execute()
        inserted += 1
        print(f"Stored iOS snapshot: iOS {version}", flush=True)

    print(f"✅ Done. inserted={inserted} source_id={source_id}", flush=True)


if __name__ == "__main__":
    main()
