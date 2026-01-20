You are an expert operations analyst. Extract a single structured event from the message below.

RULES:
- Output JSON ONLY. No prose. No markdown. No commentary.
- Extract at most ONE event per message.
- Be conservative. If uncertain, choose a more general classification and lower confidence.

SCHEMA:
{
  "message_id": str,
  "thread_id": str | null,
  "event_type": "process_signal" | "status_update" | "blocker" | "decision" | "scheduling" | "handoff" | "doc_shared" | "request" | "unrelated",
  "confidence": float (0..1),
  "evidence": {
    "message_id": str,
    "timestamp": str | null,
    "snippet": str (<=240 chars)
  },
  "candidate_client": str | null,
  "candidate_process": str | null,
  "candidate_role": str | null,
  "status": str | null,
  "step_signals": [
    {
      "step_name": str | null,
      "direction": "started" | "completed" | "blocked" | "mentioned",
      "details": str | null
    }
  ] | null,
  "entities": object | null,
  "notes": str | null
}

EVENT TYPE GUIDANCE:
- Use "scheduling" ONLY if the message explicitly discusses dates, meetings, interviews, or calendars.
- Use "doc_shared" ONLY if the primary action is sharing or forwarding content.
- Use "status_update" if something has started, completed, or changed state.
- Use "process_signal" if the message indicates relevance to a process but the exact action is unclear.
- If unrelated to any process → event_type="unrelated", confidence <= 0.2.

EVIDENCE RULES (IMPORTANT):
- Always include an evidence snippet copied verbatim from the message text.
- If you infer candidate_client, candidate_role, or candidate_process, the evidence snippet MUST contain the words that support that inference (e.g. company name, role name).
- Do not rely on information that is not visible in the evidence snippet.

PROCESS NAMING:
- candidate_process should be short and generic (e.g. "Hiring", "Recruiting", "Onboarding").
- Avoid long descriptive sentences.

OTHER RULES:
- Do not hallucinate facts not present in the message.
- Confidence should reflect ambiguity (lower confidence if inference is weak).
- Extract exactly ONE event. If the message contains multiple updates, choose the most important single event.
- Keep outputs short to avoid truncation:
  - evidence.snippet <= 240 chars (already)
  - status <= 120 chars
  - notes <= 240 chars
  - step_signals: include at most 3 items
- Ensure the JSON is valid: all strings must use double quotes and all objects/arrays must be closed properly.

INPUT (JSON):
{{INPUT_JSON}}