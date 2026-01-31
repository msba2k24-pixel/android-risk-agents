# scrape.py
# Minimal fetch + clean + skip small pages + insert into Supabase (no OpenAI)

import hashlib
import os
import re
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional, Dict, Any

import requests
from bs4 import BeautifulSoup

# ---------- Config ----------
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )
}

MIN_CLEAN_TEXT_LEN = 1200
MAX_CLEAN_TEXT_CHARS = 25000

# Storing raw HTML is the #1 reason inserts fail (size).
STORE_HTML_RAW = False
MAX_HTML_CHARS = 200000  # only used if STORE_HTML_RAW = True

# Supabase settings
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "snapshots")

# Use service role key in CI for ingestion (bypasses RLS).
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", os.getenv("SUPABASE_KEY", ""))

# If True, we upsert to avoid duplicates (recommended)
USE_UPSERT = True

# Choose a conflict key that exists in your table.
# Recommended: create a UNIQUE constraint on (url, clean_text_sha256)
UPSERT_ON_CONFLICT = os.getenv("UPSERT_ON_CONFLICT", "url,clean_text_sha256")


# ---------- Data model ----------
@dataclass
class ScrapeResult:
    url: str
    final_url: str
    status_code: int
    fetched_at_utc: str
    fetch_ms: int
    html_raw: str
    html_sha256: str
    clean_text: str
    clean_text_len: int
    clean_text_sha256: str
    should_skip: bool
    skip_reason: Optional[str]

    def to_record(self) -> Dict[str, Any]:
        return asdict(self)


# ---------- Helpers ----------
def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", errors="ignore")).hexdigest()


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


# ---------- Supabase I/O ----------
def _get_supabase_client():
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError(
            "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY). "
            "Set these as environment variables."
        )

    try:
        from supabase import create_client  # pip install supabase
    except Exception as e:
        raise RuntimeError(
            "supabase client not installed. Run: pip install supabase"
        ) from e

    return create_client(SUPABASE_URL, SUPABASE_KEY)


def insert_to_supabase(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Insert (or upsert) record into Supabase.
    Returns a small status dict that you can print/log in GitHub Actions.
    """
    sb = _get_supabase_client()

    try:
        if USE_UPSERT:
            res = sb.table(SUPABASE_TABLE).upsert(
                record,
                on_conflict=UPSERT_ON_CONFLICT
            ).execute()
        else:
            res = sb.table(SUPABASE_TABLE).insert(record).execute()

        inserted_count = len(res.data or [])
        return {"ok": True, "inserted": inserted_count}

    except Exception as e:
        # Make errors very visible in logs
        return {"ok": False, "error": str(e)}


# ---------- Main scrape ----------
def fetch_and_clean(
    url: str,
    timeout_s: int = 30,
    min_clean_len: int = MIN_CLEAN_TEXT_LEN,
    max_clean_chars: int = MAX_CLEAN_TEXT_CHARS,
    headers: Optional[Dict[str, str]] = None,
) -> ScrapeResult:
    hdrs = dict(HEADERS)
    if headers:
        hdrs.update(headers)

    t0 = time.time()
    try:
        resp = requests.get(url, headers=hdrs, timeout=timeout_s, allow_redirects=True)
        fetch_ms = int((time.time() - t0) * 1000)
    except requests.RequestException as e:
        return ScrapeResult(
            url=url,
            final_url=url,
            status_code=0,
            fetched_at_utc=_utc_now(),
            fetch_ms=int((time.time() - t0) * 1000),
            html_raw="",
            html_sha256=_sha256(""),
            clean_text="",
            clean_text_len=0,
            clean_text_sha256=_sha256(""),
            should_skip=True,
            skip_reason=f"request_error:{type(e).__name__}",
        )

    html_raw = resp.text or ""
    html_sha = _sha256(html_raw)

    soup = BeautifulSoup(html_raw, "html.parser")
    root = _pick_root(soup)
    _remove_noise(root)

    clean_text = _normalize_ws(root.get_text("\n", strip=True))
    clean_text = _cap_text(clean_text, max_clean_chars)

    clean_len = len(clean_text)
    clean_sha = _sha256(clean_text)

    should_skip = False
    skip_reason = None

    if resp.status_code >= 400:
        should_skip = True
        skip_reason = f"http_error:{resp.status_code}"
    elif clean_len < min_clean_len:
        should_skip = True
        skip_reason = f"clean_text_too_short:{clean_len}<min:{min_clean_len}"

    # Avoid oversized inserts
    if not STORE_HTML_RAW:
        html_raw_to_store = ""
    else:
        html_raw_to_store = _cap_text(html_raw, MAX_HTML_CHARS)

    return ScrapeResult(
        url=url,
        final_url=str(resp.url),
        status_code=resp.status_code,
        fetched_at_utc=_utc_now(),
        fetch_ms=fetch_ms,
        html_raw=html_raw_to_store,
        html_sha256=html_sha,
        clean_text=clean_text,
        clean_text_len=clean_len,
        clean_text_sha256=clean_sha,
        should_skip=should_skip,
        skip_reason=skip_reason,
    )


def scrape_and_store(url: str) -> Dict[str, Any]:
    """
    One-call function: scrape + insert record into Supabase.
    """
    result = fetch_and_clean(url)
    record = result.to_record()

    # Optional: still store skip records so you can debug coverage
    out = insert_to_supabase(record)
    return {"url": url, "should_skip": result.should_skip, "skip_reason": result.skip_reason, **out}


# ---------- CLI ----------
if __name__ == "__main__":
    test_url = os.getenv(
        "TEST_URL",
        "https://source.android.com/docs/security/bulletin/"
    )
    out = scrape_and_store(test_url)
    print(out)
