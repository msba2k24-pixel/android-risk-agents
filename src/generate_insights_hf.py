# src/generate_insights_hf.py
import os
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

from huggingface_hub import InferenceClient

from .db import get_uninsighted_changes, get_snapshot_text_by_id, insert_insight


HF_TOKEN = os.getenv("HF_TOKEN")
if not HF_TOKEN:
    raise RuntimeError("HF_TOKEN is missing. Add it as a GitHub Actions secret and pass env HF_TOKEN.")

MODEL = os.getenv("HF_MODEL", "HuggingFaceH4/zephyr-7b-beta")

SYSTEM = (
    "You are a security research assistant. "
    "Given old and new text from a monitored Android security source, "
    "produce structured, concise insights about what changed. "
    "Do not invent facts. If unknown, say unknown. "
    "Return only valid JSON."
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def clamp_list(xs: List[str], max_items: int, max_len: int) -> List[str]:
    out: List[str] = []
    for x in xs[:max_items]:
        out.append(str(x)[:max_len])
    return out


def extract_json_only(s: str) -> Dict[str, Any]:
    s = s.strip()
    try:
        return json.loads(s)
    except Exception:
        start = s.find("{")
        end = s.rfind("}")
        if start == -1 or end == -1:
            raise
        return json.loads(s[start : end + 1])


def build_prompt(old_text: str, new_text: str, url: str) -> str:
    schema_hint = {
        "summary": "string, 1-3 sentences",
        "key_points": ["string, up to 6 bullets"],
        "cves": ["string, items like CVE-2025-12345"],
        "severity": "low|medium|high|critical|unknown",
        "confidence": "number 0..1"
    }

    return (
        f"SOURCE: {url}\n\n"
        f"OLD TEXT (trimmed):\n{old_text[:6000]}\n\n"
        f"NEW TEXT (trimmed):\n{new_text[:6000]}\n\n"
        "Return JSON only.\n"
        f"Schema:\n{json.dumps(schema_hint)}"
    )


def safe_insight(insight: Dict[str, Any]) -> Dict[str, Any]:
    summary = str(insight.get("summary", ""))[:1200]

    key_points = insight.get("key_points", [])
    if not isinstance(key_points, list):
        key_points = []
    key_points = clamp_list([str(x) for x in key_points], max_items=6, max_len=220)

    cves = insight.get("cves", [])
    if not isinstance(cves, list):
        cves = []
    cves_clean: List[str] = []
    for x in cves:
        sx = str(x).strip().upper()
        if "CVE-" in sx:
            cves_clean.append(sx[:30])
    cves_clean = cves_clean[:20]

    severity = str(insight.get("severity", "unknown")).lower()
    if severity not in {"low", "medium", "high", "critical", "unknown"}:
        severity = "unknown"

    try:
        confidence = float(insight.get("confidence", 0.5))
    except Exception:
        confidence = 0.5
    confidence = max(0.0, min(1.0, confidence))

    return {
        "summary": summary,
        "key_points": key_points,
        "cves": cves_clean,
        "severity": severity,
        "confidence": confidence,
    }


def run() -> int:
    client = InferenceClient(model=MODEL, token=HF_TOKEN)

    changes = get_uninsighted_changes(limit=25)
    if not changes:
        print("No changes pending insights.")
        return 0

    created = 0
    for ch in changes:
        try:
            # old_snapshot_id can be None for first snapshot - handle safely
            old_text = ""
            if getattr(ch, "old_snapshot_id", None):
                old_text = get_snapshot_text_by_id(ch.old_snapshot_id) or ""

            new_text = get_snapshot_text_by_id(ch.new_snapshot_id) or ""
            prompt = build_prompt(old_text, new_text, ch.url)

            resp = client.chat_completion(
                messages=[
                    {"role": "system", "content": SYSTEM},
                    {"role": "user", "content": prompt},
                ],
                max_tokens=450,
                temperature=0.2,
            )

            content = resp.choices[0].message["content"]
            raw = extract_json_only(content)
            insight = safe_insight(raw)

            insert_insight(
                source_id=ch.source_id,
                change_id=ch.id,
                snapshot_id=ch.new_snapshot_id,
                insight_json=insight,
                generated_at_utc=utc_now(),
                model=f"hf:{MODEL}",
            )

            created += 1
            print(f"Insight created for change_id={ch.id}")

            # Light throttling to reduce rate-limit risk
            time.sleep(0.5)

        except Exception as e:
            print(f"Insight failed for change_id={getattr(ch, 'id', 'unknown')}: {e}")
            continue

    print(f"Done. Created {created}/{len(changes)} insights.")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())