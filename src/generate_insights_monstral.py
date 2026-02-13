# src/generate_insights_monstral.py
import os
import json
import time
from typing import Any, Dict, Optional, List

from openai import OpenAI

from .db import (
    get_uninsighted_changes,
    get_snapshot_text_by_id,
    insert_insight,
    create_baseline_changes,
)

# Modal vLLM (OpenAI-compatible) endpoint + key
LLM_BASE_URL = os.getenv("LLM_BASE_URL")
LLM_API_KEY = os.getenv("LLM_API_KEY")

if not LLM_BASE_URL:
    raise RuntimeError("LLM_BASE_URL is missing. Add it as a GitHub Actions secret.")
if not LLM_API_KEY:
    raise RuntimeError("LLM_API_KEY is missing. Add it as a GitHub Actions secret.")

# Models served by your Modal vLLM deployment
MODEL_TRIAGE = os.getenv("MODEL_TRIAGE", "mistral-small")   # fast/cheap
MODEL_ANALYZE = os.getenv("MODEL_ANALYZE", "mistral-large") # stronger

AGENT_NAME = os.getenv("AGENT_NAME", "modal-digital-risk-agent")
RELEVANCE_THRESHOLD = int(os.getenv("RELEVANCE_THRESHOLD", "70"))

REQUEST_TIMEOUT_S = float(os.getenv("LLM_TIMEOUT_S", "60"))
MAX_RETRIES = int(os.getenv("LLM_MAX_RETRIES", "4"))
RETRY_BASE_S = float(os.getenv("LLM_RETRY_BASE_S", "1.25"))


client = OpenAI(
    base_url=LLM_BASE_URL.rstrip("/"),
    api_key=LLM_API_KEY,
    timeout=REQUEST_TIMEOUT_S,
)


def _sleep_backoff(attempt: int) -> None:
    time.sleep(RETRY_BASE_S * (2 ** attempt))


def _safe_json_loads(s: str) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(s)
    except Exception:
        return None


def _call_chat_json(model: str, system: str, user: str, max_tokens: int = 900) -> Dict[str, Any]:
    """
    Calls an OpenAI-compatible chat endpoint and returns parsed JSON.
    Retries on transient failures.
    """
    last_err: Optional[Exception] = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                temperature=0.2,
                max_tokens=max_tokens,
            )
            text = (resp.choices[0].message.content or "").strip()

            # If the model wraps JSON in code fences, strip them.
            if text.startswith("```"):
                text = text.strip("`")
                # common pattern: ```json ... ```
                text = text.replace("json\n", "", 1).strip()

            parsed = _safe_json_loads(text)
            if parsed is None:
                raise ValueError(f"Model did not return valid JSON. Raw: {text[:300]}")

            return parsed
        except Exception as e:
            last_err = e
            if attempt < MAX_RETRIES - 1:
                _sleep_backoff(attempt)
            else:
                raise RuntimeError(f"LLM call failed after retries: {e}") from e

    raise RuntimeError(f"LLM call failed: {last_err}")


TRIAGE_SYSTEM = """You are a digital risk triage agent.
Given a platform change (Android security bulletin/CVE/dev blog doc diff summary),
decide if it is relevant to fraud, abuse, identity, device trust, account takeover, or detection signals.

Return ONLY valid JSON with this schema:
{
  "relevance_score": 0-100,
  "decision": "keep" | "discard",
  "reasons": ["...", "..."],
  "risk_tags": ["permissions", "device_integrity", "network", "biometrics", "wallet", "policy", "privacy", "sandbox", "webview", "other"]
}
"""

ANALYZE_SYSTEM = """You are a senior digital risk analyst.
You will write an actionable insight for fraud and risk stakeholders.

Return ONLY valid JSON with this schema:
{
  "title": "short",
  "summary": "2-4 sentences",
  "why_it_matters": ["...", "..."],
  "affected_signals": ["device_id", "attestation", "network_fingerprinting", "permissions", "behavioral", "cookies", "push_tokens", "other"],
  "recommended_actions": ["...", "..."],
  "severity": "low" | "medium" | "high" | "critical",
  "confidence": 0-100
}
"""

def triage_change(change: Dict[str, Any], snapshot_text: str) -> Dict[str, Any]:
    user = f"""
Change record:
{json.dumps(change, ensure_ascii=False)[:4000]}

Snapshot text excerpt:
{snapshot_text[:6000]}
"""
    return _call_chat_json(model=MODEL_TRIAGE, system=TRIAGE_SYSTEM, user=user, max_tokens=450)


def analyze_change(change: Dict[str, Any], snapshot_text: str, triage: Dict[str, Any]) -> Dict[str, Any]:
    user = f"""
Triage result:
{json.dumps(triage, ensure_ascii=False)}

Change record:
{json.dumps(change, ensure_ascii=False)[:4000]}

Snapshot text excerpt:
{snapshot_text[:9000]}
"""
    return _call_chat_json(model=MODEL_ANALYZE, system=ANALYZE_SYSTEM, user=user, max_tokens=900)


def run_insights_pipeline() -> Dict[str, Any]:
    # If you use a baseline mechanism, keep it idempotent
    create_baseline_changes()

    changes = get_uninsighted_changes(limit=50)  # adjust if you want
    kept = 0
    discarded = 0
    inserted = 0

    for ch in changes:
        snapshot_id = ch.get("snapshot_id")
        if not snapshot_id:
            continue

        snapshot_text = get_snapshot_text_by_id(snapshot_id) or ""
        if not snapshot_text.strip():
            continue

        triage = triage_change(ch, snapshot_text)

        score = int(triage.get("relevance_score", 0) or 0)
        decision = triage.get("decision", "discard")

        if decision != "keep" or score < RELEVANCE_THRESHOLD:
            discarded += 1
            # Optional: you can still store triage notes somewhere if you have a column/table
            continue

        kept += 1
        insight = analyze_change(ch, snapshot_text, triage)

        insert_insight(
            change_id=ch["id"],
            agent_name=AGENT_NAME,
            model_triage=MODEL_TRIAGE,
            model_analyze=MODEL_ANALYZE,
            relevance_score=score,
            triage_json=triage,
            insight_json=insight,
        )
        inserted += 1

        # small pacing to reduce burst load on vLLM autoscale
        time.sleep(0.3)

    return {
        "total_candidates": len(changes),
        "kept": kept,
        "discarded": discarded,
        "inserted": inserted,
        "agent": AGENT_NAME,
        "triage_model": MODEL_TRIAGE,
        "analyze_model": MODEL_ANALYZE,
        "threshold": RELEVANCE_THRESHOLD,
    }


if __name__ == "__main__":
    out = run_insights_pipeline()
    print(json.dumps(out, indent=2))