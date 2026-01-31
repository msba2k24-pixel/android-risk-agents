# src/generate_insights_hf.py
import os
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict

from huggingface_hub import InferenceClient

from .db import (
    get_uninsighted_changes,
    get_snapshot_text_by_id,
    insert_insight,
    create_baseline_changes,
)


HF_TOKEN = os.getenv("HF_TOKEN")
if not HF_TOKEN:
    raise RuntimeError("HF_TOKEN is missing. Add it as a GitHub Actions secret and pass env HF_TOKEN.")

MODEL = os.getenv("HF_MODEL", "mistralai/Mistral-7B-Instruct-v0.2")
AGENT_NAME = os.getenv("AGENT_NAME", "hf-demo")

SYSTEM = (
    "You are a security research assistant. "
    "Given old and new text from a monitored Android security source, "
    "summarize what changed for a demo. "
    "Do not invent facts. If uncertain, say unknown. "
    "Return only valid JSON."
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


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
        "summary": "string, 1 to 3 sentences",
        "confidence": "number 0..1"
    }

    return (
        f"SOURCE: {url}\n\n"
        f"OLD TEXT (trimmed):\n{old_text[:6000]}\n\n"
        f"NEW TEXT (trimmed):\n{new_text[:6000]}\n\n"
        "Return JSON only.\n"
        f"Schema:\n{json.dumps(schema_hint)}"
    )


def safe_output(obj: Dict[str, Any]) -> Dict[str, Any]:
    summary = str(obj.get("summary", "")).strip()
    if not summary:
        summary = "Update detected. Details unknown."

    try:
        confidence = float(obj.get("confidence", 0.5))
    except Exception:
        confidence = 0.5

    confidence = max(0.0, min(1.0, confidence))
    return {"summary": summary[:1200], "confidence": confidence}


def run() -> int:
    client = InferenceClient(model=MODEL, token=HF_TOKEN)

    changes = get_uninsighted_changes(limit=25)

    # First-run demo fallback
    if not changes:
        created = create_baseline_changes(limit=10)
        print(f"No changes pending insights. Created baseline changes: {created}")
        changes = get_uninsighted_changes(limit=25)

    if not changes:
        print("Still no changes available after baseline creation.")
        return 0

    created_insights = 0
    for ch in changes:
        try:
            old_text = ""
            if ch.old_snapshot_id is not None:
                old_text = get_snapshot_text_by_id(ch.old_snapshot_id) or ""

            new_text = get_snapshot_text_by_id(ch.new_snapshot_id) or ""
            prompt = build_prompt(old_text, new_text, ch.url)

            resp = client.chat_completion(
                messages=[
                    {"role": "system", "content": SYSTEM},
                    {"role": "user", "content": prompt},
                ],
                max_tokens=250,
                temperature=0.2,
            )

            content = resp.choices[0].message["content"]
            raw = extract_json_only(content)
            out = safe_output(raw)

            insert_insight(
                change_id=ch.id,
                agent_name=AGENT_NAME,
                title="Update detected",
                summary=out["summary"],
                confidence=out["confidence"],
            )

            created_insights += 1
            print(f"Insight created for change_id={ch.id}")

            time.sleep(0.4)

        except Exception as e:
            print(f"Insight failed for change_id={getattr(ch, 'id', 'unknown')}: {e}")
            continue

    print(f"Done. Created {created_insights}/{len(changes)} insights.")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())