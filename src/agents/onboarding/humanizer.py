import re
import json
import logging
from typing import List, Dict, Tuple, Optional

from .config import settings
import httpx
import uuid

MCP_BASE = settings.MCP_BASE
MCP_JSONRPC_ENDPOINT = f"{MCP_BASE}/mcp/v1/jsonrpc"


async def _mcp_call(tool_name: str, arguments: dict, timeout: int = 12) -> dict:
    request_id = str(uuid.uuid4())
    payload = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "tools/call",
        "params": {"name": tool_name, "arguments": arguments},
    }
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            r = await client.post(MCP_JSONRPC_ENDPOINT, json=payload)
            r.raise_for_status()
            response = r.json()
            if "error" in response:
                err = response["error"]
                raise RuntimeError(f"MCP tool '{tool_name}' failed: {err.get('message')}")
            # Extract flexible content
            if "result" in response and "content" in response["result"]:
                content = response["result"]["content"]
                if content and len(content) > 0:
                    text = content[0].get("text", "{}")
                    try:
                        return json.loads(text)
                    except Exception:
                        return {"text": text}
            return response.get("result", {})
    except Exception as e:
        log.error(f"[HUMANIZER] MCP call error for {tool_name}: {e}")
        raise

log = logging.getLogger(__name__)


# --------- Micro-prompts (Drafter) ---------
MICRO_PROMPTS: List[str] = [
    # Empathetic
    (
        "Role: Humanizer (Empathetic). You will produce JSON only with keys: reply, bridge_question. "
        "Acknowledge the user's constraint in warm, encouraging tone. Give one helpful fact or reassurance. "
        "End with ONE concrete question that advances this step. Keep <= 2 sentences before the question. "
        "Respect policy: live sessions/meetings happen only on weekdays 8–15. Output strict JSON."
    ),
    # Concise
    (
        "Role: Humanizer (Concise). Output JSON with reply, bridge_question. Reply in <= 2 short sentences, simple words, no filler. "
        "Ask ONE clear next question tied to this step. Respect policy (weekday 8–15). Do not restate already-captured data."
    ),
    # Persuasive (gentle nudge)
    (
        "Role: Humanizer (Persuasive). Output JSON with reply, bridge_question. Validate the user's point, then offer a tiny low-effort option "
        "(e.g., 20–30 min lunch slot, give two concrete choices). Friendly and brief, <= 2 sentences before the question. ONE question only. Policy-safe (weekday 8–15)."
    ),
    # Playful-light
    (
        "Role: Humanizer (Playful-light). Output JSON with reply, bridge_question. Be friendly and human, one quick positive note or empathy, then a crisp bridge question with 2 choices. "
        "<= 2 sentences before the question, no confusing slang. Policy-safe (weekday 8–15)."
    ),
    # Reassuring/Clarity
    (
        "Role: Humanizer (Reassuring). Output JSON with reply, bridge_question. Normalize uncertainty in one line, clarify simply, then ask ONE yes/no or A/B question that moves forward. "
        "Keep it short and policy-safe (weekday 8–15)."
    ),
]


# --------- Regex Rule Sheet ---------
RE_YES = re.compile(r"\b(yes|yeah|yup|ok|okay|sure|fine|works|agree)\b", re.I)
RE_NO = re.compile(r"\b(no|nope|not now|can't|cannot|won't)\b", re.I)
RE_DEFER = re.compile(r"\b(later|think|decide|remind|get back)\b", re.I)
RE_CONSTRAINT = re.compile(r"(weekend|saturday|sunday|no time|busy|meeting)", re.I)
RE_SMALL_TALK = re.compile(r"\b(hi|hello|thanks|thank you)\b|[😊👍]", re.I)


def detect_label(text: str) -> str:
    if RE_SMALL_TALK.search(text or ""):
        return "SMALL_TALK"
    if RE_DEFER.search(text or ""):
        return "DEFER"
    if RE_YES.search(text or ""):
        return "YES"
    if RE_NO.search(text or ""):
        return "NO"
    if RE_CONSTRAINT.search(text or ""):
        return "CONSTRAINT"
    return "AMBIGUOUS"


# --------- Time Phrase Normalizer ---------
TIME_NORMALIZER = [
    (re.compile(r"\b(lunch|noon|midday)\b", re.I), "12:30–13:00"),
    (re.compile(r"\b(morning|first half)\b", re.I), "08:00–11:00"),
    (re.compile(r"\b(afternoon|after 2|post[- ]?lunch)\b", re.I), "14:00–15:00"),
]


def normalize_time_phrase(text: str) -> Optional[str]:
    t = text or ""
    for pattern, window in TIME_NORMALIZER:
        if pattern.search(t):
            return window
    if re.search(r"\bevening\b", t, re.I):
        return None  # outside policy; caller can redirect
    return None


# --------- Drafter via MCP LLM (Claude) ---------
async def draft_candidates(flow_state_summary: str, user_input: str, max_candidates: int = 5) -> List[Dict]:
    candidates: List[Dict] = []
    for idx, micro in enumerate(MICRO_PROMPTS[:max_candidates]):
        try:
            system = (
                "You are the Humanizer layer for an onboarding agent. Respond with strict JSON only: "
                "{\"reply\": string, \"bridge_question\": string}. Keep policy: weekdays 8–15 only."
            )
            user = (
                f"Flow: {flow_state_summary}\n"
                f"User: {user_input}\n"
                f"Instruction: {micro}"
            )
            resp = await _mcp_call(
                "llm.call",
                {
                    "messages": [
                        {"role": "system", "content": system},
                        {"role": "user", "content": user},
                    ],
                    "max_tokens": 180,
                    "temperature": 0.6,
                },
                timeout=12,
            )
            text = resp.get("content") or resp.get("message") or resp.get("text", "")
            obj = json.loads(text) if isinstance(text, str) else {}
            reply = (obj.get("reply") or "").strip()
            bridge = (obj.get("bridge_question") or "").strip()
            if reply and bridge:
                candidates.append({"id": idx, "reply": reply, "bridge_question": bridge})
        except Exception as e:
            log.warning(f"[HUMANIZER] Draft failed for prompt {idx}: {e}")
    return candidates


# --------- Reranker (Rubric) ---------
def score_candidate(c: Dict, user_input: str) -> Dict:
    reply = c.get("reply", "")
    bridge = c.get("bridge_question", "")

    # Heuristics
    helpfulness = 1.0 if len(reply) > 0 else 0.0
    warmth = 1.0 if re.search(r"\b(thank|understand|appreciate|happy|glad)\b", reply, re.I) else 0.6
    brevity = 1.0 if len(reply.split()) <= 40 and len(bridge.split()) <= 20 else 0.5
    bridge_clarity = 1.0 if bridge.endswith("?") and len(bridge) <= 120 else 0.6
    # Policy safety: ensure no weekend suggestion and stay within 8–15
    policy_safe = 1.0
    if re.search(r"\b(weekend|saturday|sunday|evening)\b", reply + " " + bridge, re.I):
        policy_safe = 0.2
    if re.search(r"\b(6\s*pm|7\s*pm|8\s*pm|evening)\b", reply + " " + bridge, re.I):
        policy_safe = 0.2

    total = (helpfulness + warmth + brevity + bridge_clarity + policy_safe) / 5.0
    return {
        "id": c["id"],
        "helpfulness": helpfulness,
        "warmth": warmth,
        "brevity": brevity,
        "bridge_clarity": bridge_clarity,
        "policy_safe": policy_safe,
        "total": total,
    }


def rerank(candidates: List[Dict], user_input: str) -> Tuple[Dict, Dict]:
    if not candidates:
        return {}, {"candidates": [], "scores": [], "winner_id": None}
    pairs = [
        (score_candidate(c, user_input), c)
        for c in candidates
    ]
    pairs.sort(
        key=lambda p: (
            p[0]["total"],
            p[0]["policy_safe"],
            -len((p[1].get("reply") or ""))
        ),
        reverse=True,
    )
    top_score, top_cand = pairs[0]
    result = {
        "candidates": candidates,
        "scores": [p[0] for p in pairs],
        "winner_id": top_cand.get("id"),
    }
    return top_cand, result


# --------- Seed Pack (Gold Replies) ---------
def load_seed_pack() -> List[Dict]:
    try:
        import pathlib
        path = pathlib.Path(__file__).parent / "humanizer_seed.jsonl"
        seeds: List[Dict] = []
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        seeds.append(json.loads(line))
                    except Exception:
                        continue
        return seeds
    except Exception as e:
        log.warning(f"[HUMANIZER] Failed to load seed pack: {e}")
        return []


async def humanize_weekday_confirmation(flow_state_summary: str, user_input: str) -> Dict:
    """Thin wrapper to call MCP tool llm.humanize_weekday_confirmation and return structured JSON."""
    try:
        result = await _mcp_call(
            "llm.humanize_weekday_confirmation",
            {
                "flow_state_summary": flow_state_summary,
                "user_input": user_input,
                "locale": "en-IN",
            },
            timeout=12,
        )
        # Expecting keys: label, tone_prefix, reply, bridge_question
        if all(k in result for k in ["label", "tone_prefix", "reply", "bridge_question"]):
            return result
        # Graceful fallback if server returns text blob
        text = result.get("text", "") if isinstance(result, dict) else ""
        if text:
            return {
                "label": detect_label(user_input),
                "tone_prefix": "",
                "reply": text[:280],
                "bridge_question": "Could you share a small weekday window between 8–15 that could work?",
            }
        # Final safe fallback
        return {
            "label": detect_label(user_input),
            "tone_prefix": "",
            "reply": "Understood. Live sessions run on weekdays (8–15). Even a short lunch slot works.",
            "bridge_question": "Would Tue 12:30–13:00 or Wed 12:30–13:00 work for you?",
        }
    except Exception as e:
        log.error(f"[HUMANIZER] MCP tool error: {e}")
        return {
            "label": detect_label(user_input),
            "tone_prefix": "",
            "reply": "Got it—weekdays can be tight. Even a 20–30 min weekday slot helps us match school hours.",
            "bridge_question": "Could you try a short lunch window next week—Tue or Thu 12:30–13:00?",
        }


