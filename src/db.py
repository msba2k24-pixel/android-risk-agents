def insert_insight(
    change_id: int,
    agent_name: str,
    title: str,
    summary: str,
    confidence: float = 0.6,
    category: Optional[str] = None,
    affected_signals: Optional[List[str]] = None,
    recommended_actions: Optional[List[str]] = None,
    risk_score: Optional[int] = None,
) -> None:
    """
    Option 1 DB: category can be NULL and has default 'general'.
    JSONB columns have defaults. created_at has default now().
    This insert is safe even if some fields are missing.
    """
    sb = get_supabase_client()

    payload: Dict[str, Any] = {
        "change_id": int(change_id),
        "agent_name": agent_name,
        "title": title,
        "summary": summary,
        "confidence": float(confidence),
    }

    # Only include optional fields if provided.
    # This lets DB defaults apply cleanly.
    if category is not None:
        payload["category"] = str(category)

    if affected_signals is not None:
        payload["affected_signals"] = affected_signals

    if recommended_actions is not None:
        payload["recommended_actions"] = recommended_actions

    if risk_score is not None:
        payload["risk_score"] = int(risk_score)

    sb.table("insights").insert(payload).execute()