# src/scrape_sources.py
import hashlib
import re
from datetime import datetime, timezone
from typing import Tuple, Optional, Dict, Any, List

import requests
from bs4 import BeautifulSoup

from .db import get_supabase_client, get_latest_snapshot_for_source, upsert_vector_chunks
from .config import USER_AGENT, CHUNK_SIZE_CHARS, CHUNK_OVERLAP_CHARS, EMBED_BASELINE_ON_FIRST_SNAPSHOT
from .embedder import chunk_text, embed_texts

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
    for tag in root.find_all(["nav", "footer", "header", "aside"]):
        tag.decompose()


def _pick_root(soup: BeautifulSoup):
    return soup.find("main") or soup.find("article") or soup.find("body") or soup


def _cap_text(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    head = text[: int(max_chars * 0.7)]
    tail = text[-int(max_chars * 0.3) :]
    return head.rstrip() + "\n\n[...truncated...]\n\n" + tail.lstrip()


def fetch_raw_and_clean(url: str) -> Tuple[str, str]:
    resp = requests.get(
        url,
        headers=HEADERS,
        timeout=REQUEST_TIMEOUT_S,
        allow_redirects=True,
    )
    resp.raise_for_status()

    raw_html = resp.text or ""
    soup = BeautifulSoup(raw_html, "html.parser")

    root = _pick_root(soup)
    _remove_noise(root)

    clean_text = _normalize_ws(root.get_text("\n", strip=True))
    clean_text = _cap_text(clean_text, MAX_CLEAN_TEXT_CHARS)

    return raw_html, clean_text


def _store_vectors_for_snapshot(
    source_id: str,
    snapshot_sha: str,
    kind: str,
    clean_text: str,
) -> int:
    chunks = chunk_text(clean_text, chunk_size_chars=CHUNK_SIZE_CHARS, overlap_chars=CHUNK_OVERLAP_CHARS)
    if not chunks:
        return 0

    embs = embed_texts(chunks)
    rows: List[Dict[str, Any]] = []
    for i, (ch, emb) in enumerate(zip(chunks, embs)):
        rows.append(
            {
                "source_id": str(source_id),
                "snapshot_sha": str(snapshot_sha),
                "kind": kind,
                "chunk_index": int(i),
                "chunk_text": ch,
                "embedding": emb,
            }
        )

    upsert_vector_chunks(rows)
    return len(rows)


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
    embedded = 0

    for s in sources:
        src_id = s["id"]
        name = s["name"]
        url = s["url"]

        # Determine if this is a first snapshot (baseline) BEFORE inserting
        prev_latest = get_latest_snapshot_for_source(int(src_id))
        is_first_snapshot = prev_latest is None

        try:
            raw_html, clean_text = fetch_raw_and_clean(url)
        except Exception as e:
            raise RuntimeError(f"Fetch failed for source='{name}' url='{url}': {e}")

        if len(clean_text) < MIN_CLEAN_TEXT_LEN:
            skipped += 1
            print(f"Skipped (too short): {name} len={len(clean_text)}", flush=True)
            continue

        content_hash = _sha256(clean_text)

        payload = {
            "source_id": src_id,
            "fetched_at": now,
            "content_hash": content_hash,
            "raw_text": raw_html,
            "clean_text": clean_text,
        }

        # Insert snapshot
        sb.table("snapshots").insert(payload).execute()
        inserted += 1
        print(f"Stored snapshot: {name}", flush=True)

        # Embed baseline on first snapshot (optional), otherwise embed as snapshot
        if is_first_snapshot and not EMBED_BASELINE_ON_FIRST_SNAPSHOT:
            continue

        kind = "baseline" if is_first_snapshot else "snapshot"
        try:
            nvec = _store_vectors_for_snapshot(
                source_id=str(src_id),
                snapshot_sha=content_hash,
                kind=kind,
                clean_text=clean_text,
            )
            embedded += nvec
            print(f"Embedded {nvec} chunks ({kind}): {name}", flush=True)
        except Exception as e:
            # Do not fail the whole run if embeddings fail
            print(f"Vector embed failed for source='{name}': {e}", flush=True)

    print(f"✅ Done. inserted={inserted} skipped={skipped} embedded_chunks={embedded}", flush=True)


if __name__ == "__main__":
    main()