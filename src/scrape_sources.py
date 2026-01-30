# scrape.py
# Minimal fetch + clean + skip small pages (no OpenAI)

import hashlib
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Dict

import requests
from bs4 import BeautifulSoup


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )
}

MIN_CLEAN_TEXT_LEN = 1200
MAX_CLEAN_TEXT_CHARS = 25000


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
    resp = requests.get(url, headers=hdrs, timeout=timeout_s, allow_redirects=True)
    fetch_ms = int((time.time() - t0) * 1000)

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

    return ScrapeResult(
        url=url,
        final_url=str(resp.url),
        status_code=resp.status_code,
        fetched_at_utc=_utc_now(),
        fetch_ms=fetch_ms,
        html_raw=html_raw,
        html_sha256=html_sha,
        clean_text=clean_text,
        clean_text_len=clean_len,
        clean_text_sha256=clean_sha,
        should_skip=should_skip,
        skip_reason=skip_reason,
    )