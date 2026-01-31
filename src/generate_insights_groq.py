# src/generate_insights_groq.py
import os
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict

from openai import OpenAI

from .db import (
    get_uninsighted_changes,
    get_snapshot_text_by_id,
    insert_insight,
    create_baseline_changes,
)

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
if not GROQ_API_KEY:
    raise RuntimeError("GROQ_API_KEY is missing. Add it as a GitHub Actions secret.")

GROQ_BASE_URL = os.getenv("GROQ_BASE_URL", "https://api.groq.com/openai/v1")
MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
AGENT_NAME = os.getenv("AGENT_NAME", "groq-demo")

SYSTEM = (
    "You are a security research assistant. "
    "Given OLD and NEW text from a monitored Android security source, "
    "produce structured insights about what changed. "
    "Do not invent facts. If unknown, say unknown. "
    "Return ONLY valid JSON."
)


def extract_json_only(s: str) -> Dict[str, Any]:
    s = (s or "").strip()
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
        "title": "string, short headline",
        "summary": "string, 1-3 sentences",
        "category": "one of: bulletin_update, cve_update, policy_update, tooling_update, general",
        "affected_signals": ["string, up to 5 items"],
        "recommended_actions": ["string, up to 5 items"],
        "risk_score": "integer 1..5",
        "confidence": "number 0..1",
    }

    return (
        f"SOURCE: {url}\n\n"
        f"OLD TEXT (trimmed):\n{old_text[:4500]}\n\n"
        f"NEW TEXT (trimmed):\n{new_text[:4500]}\n\n"
        "Return JSON only.\n"
        f"Schema:\n{json.dumps(schema_hint)}"
    )


def safe_output(obj: Dict[str, Any]) -> Dict[str, Any]:
    title = str(obj.get("title", "")).strip() or "Update detected"
    summary = str(obj.get("summary", "")).strip() or "Update detected. Details unknown."

    category = str(obj.get("category", "general")).strip()
    allowed = {"bulletin_update", "cve_update", "policy_update", "tooling_update", "general"}
    if category not in allowed:
        category = "general"

    affected_signals = obj.get("affected_signals", [])
    if not isinstance(affected_signals, list):
        affected_signals = []
    affected_signals = [str(x)[:120] for x in affected_signals][:5]

    recommended_actions = obj.get("recommended_actions", [])
    if not isinstance(recommended_actions, list):
        recommended_actions = []
    recommended_actions = [str(x)[:140] for x in recommended_actions][:5]

    try:
        risk_score = int(obj.get("risk_score", 1))
    except Exception:
        risk_score = 1
    risk_score = max(1, min(5, risk_score))

    try:
        confidence = float(obj.get("confidence", 0.6))
    except Exception:
        confidence = 0.6
    confidence = max(0.0, min(1.0, confidence))

    return {
        "title": title[:120],
        "summary": summary[:1200],
        "category": category,
        "affected_signals": affected_signals,
        "recommended_actions": recommended_actions,
        "risk_score": risk_score,
        "confidence": confidence,
    }


def run() -> int:
    client = OpenAI(api_key=GROQ_API_KEY, base_url=GROQ_BASE_URL)

    changes = get_uninsighted_changes(limit=25)

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
                old_text = get_snapshot_text_by_id(int(ch.old_snapshot_id)) or ""

            new_text = get_snapshot_text_by_id(int(ch.new_snapshot_id)) or ""
            prompt = build_prompt(old_text, new_text, ch.url)

            resp = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,
                max_tokens=450,
            )

            content = resp.choices[0].message.content or "{}"
            raw = extract_json_only(content)
            out = safe_output(raw)

            insert_insight(
                change_id=ch.id,
                agent_name=AGENT_NAME,
                title=out["title"],
                summary=out["summary"],
                confidence=out["confidence"],
                category=out["category"],
                affected_signals=out["affected_signals"],
                recommended_actions=out["recommended_actions"],
                risk_score=out["risk_score"],
            )

            created_insights += 1
            print(f"Insight created for change_id={ch.id}")
            time.sleep(0.25)

        except Exception as e:
            print(f"Insight failed for change_id={getattr(ch, 'id', 'unknown')}: {e}")
            continue

    print(f"Done. Created {created_insights}/{len(changes)} insights.")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())