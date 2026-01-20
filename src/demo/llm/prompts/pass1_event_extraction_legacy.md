You are an expert operations analyst. Extract a single structured event from the message below.

RULES:
- Output JSON ONLY. No prose. No markdown. No commentary.
- Schema:
  {
    "message_id": str,
    "event_type": "process_signal" | "status_update" | "blocker" | "decision" | "scheduling" | "handoff" | "doc_shared" | "request" | "unrelated",
    "confidence": float (0..1),
    "evidence": { "message_id": str, "timestamp": str | null, "snippet": str (<=240 chars) },
    "candidate_client": str | null,
    "candidate_process": str | null,
    "candidate_role": str | null,
    "status": str | null,
    "step_signals": [ { "step_name": str | null, "direction": "started" | "completed" | "blocked" | "mentioned", "details": str | null } ] | null,
    "entities": object | null,
    "notes": str | null
  }
- Do not hallucinate facts not in the text.
- If unrelated to any process → event_type="unrelated", confidence low (<=0.2).
- Always include an evidence snippet copied from the message text (<=240 chars).

INPUT (JSON):
{{INPUT_JSON}}

