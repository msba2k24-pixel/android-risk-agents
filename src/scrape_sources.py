# src/scrape_sources.py

import hashlib
import re
import time
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

from .db import get_supabase_client
from .config import USER_AGENT


HEADERS = {"User-Agent": USER_AGENT}
MIN_CLEAN_TEXT_LEN = 1200
MAX_CLEAN_TEXT_CHARS = 25000


def _utc_now_iso():
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
    tail = text[-int(max_chars * 0.3):]
    return head.rstrip() + "\n\n[...truncated...]\n\n" + tail.lstrip()


def fetch_clean_text(url: str, timeout_s: int = 30) -> dict:
    t0 = time.time()
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout_s, allow_redirects=True)
        fetch_ms = int((time.time() - t0) * 1000)
    except Exception as e:
        return {
            "ok": False,
            "status_code": 0,
            "final_url": url,
            "fetch_ms": int((time.time() - t0) * 1000),
            "clean_text": "",
            "skip_reason": f"request_error:{type(e).__name__}",
        }

    html_raw = resp.text or ""
    soup = BeautifulSoup(html_raw, "html.parser")
    root = _pick_root(soup)
    _remove_noise(root)

    clean_text = _normalize_ws(root.get_text("\n", strip=True))
    clean_text = _cap_text(clean_text, MAX_CLEAN_TEXT_CHARS)

    if resp.status_code >= 400:
        return {
            "ok": False,
            "status_code": resp.status_code,
            "final_url": str(resp.url),
            "fetch_ms": fetch_ms,
            "clean_text": clean_text,
            "skip_reason": f"http_error:{resp.status_code}",
        }

    if len(clean_text) < MIN_CLEAN_TEXT_LEN:
        return {
            "ok": False,
            "status_code": resp.status_code,
            "final_url": str(resp.url),
            "fetch_ms": fetch_ms,
            "clean_text": clean_text,
            "skip_reason": f"clean_text_too_short:{len(clean_text)}",
        }

    return {
        "ok": True,
        "status_code": resp.status_code,
        "final_url": str(resp.url),
        "fetch_ms": fetch_ms,
        "clean_text": clean_text,
        "skip_reason": None,
    }


def main():
    sb = get_supabase_client()
    now = _utc_now_iso()

    sources = (
        sb.table("sources")
        .select("id,name,url,active")
        .eq("active", True)
        .execute()
        .data
    )

    print(f"Found {len(sources)} active sources", flush=True)

    inserted = 0

    for s in sources:
        url = s["url"]
        r = fetch_clean_text(url)

        content_hash = _sha256(r["clean_text"] or "")

        payload = {
            "source_id": s["id"],
            "fetched_at": now,
            "content_hash": content_hash,
            "content": r["clean_text"],
            "status_code": r["status_code"],
            "final_url": r["final_url"],
            "fetch_ms": r["fetch_ms"],
            "should_skip": (not r["ok"]),
            "skip_reason": r["skip_reason"],
        }

        res = sb.table("snapshots").insert(payload).execute()

        if not res.data:
            raise RuntimeError(f"Insert failed for source={s['name']} url={url}")

        inserted += 1
        print(f"Inserted snapshot for {s['name']} skip={payload['should_skip']}", flush=True)

    print(f"✅ Done. Inserted {inserted} snapshots.", flush=True)


if __name__ == "__main__":
    main()