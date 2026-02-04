"""
Knowing Volunteer Engine - Signal extraction and completion logic

Ports the KNOWING_VOLUNTEER loop from selectionagent.py into the agents service.
"""
import json
import logging
import re
from typing import Dict, List, Optional, Literal
from enum import Enum

log = logging.getLogger(__name__)

# Import MCP call function (lazy to avoid circular deps)
_mcp_call = None

def _get_mcp_call():
    """Lazy import of _mcp_call to avoid circular dependencies"""
    global _mcp_call
    if _mcp_call is None:
        from agents.onboarding.wa_loop import _mcp_call as mcp_call_func
        _mcp_call = mcp_call_func
    return _mcp_call


# Emoji pattern for stripping (matches most emoji ranges)
_EMOJI_PATTERN = re.compile(
    "["
    "\U0001F600-\U0001F64F"  # emoticons
    "\U0001F300-\U0001F5FF"  # symbols & pictographs
    "\U0001F680-\U0001F6FF"  # transport & map symbols
    "\U0001F1E0-\U0001F1FF"  # flags (iOS)
    "\U00002702-\U000027B0"  # dingbats
    "\U000024C2-\U0001F251"  # enclosed characters
    "]+",
    flags=re.UNICODE
)


def _strip_emojis(text: str) -> str:
    """Strip emojis from text string"""
    if not text or not isinstance(text, str):
        return text
    return _EMOJI_PATTERN.sub('', text).strip()


class KnowingVolunteerResult(str, Enum):
    """Result of evaluating knowing volunteer step"""
    STOP = "STOP"
    COMPLETE = "COMPLETE"
    COMPLETE_INSUFFICIENT_INFO = "COMPLETE_INSUFFICIENT_INFO"
    CONTINUE = "CONTINUE"


# Master system prompt (from selectionagent.py)
MASTER_SYSTEM_PROMPT = """You are SIA, a warm, respectful, purpose-driven conversational agent for Sunbird SERVE.

Your role is to onboard volunteers through a single, natural WhatsApp conversation.

You must sound human, encouraging, and calm — never procedural or robotic.

Core principles:

- Start with purpose before asking for details.

- Convert intent -> interest through clarity, not pressure.

- Never mention internal concepts like onboarding, registration, FSM, states, or selection.

- Keep messages short (1–3 lines), WhatsApp-friendly.

- Ask only one question at a time.

- Be honest and transparent about non-negotiables.

- If a volunteer cannot proceed, exit gracefully and share the SERVE community link.

Non-negotiables:

- Eligibility (18+, device + internet, voluntary role) must be met.

- Phone number and email are required to proceed to classroom volunteering.

- If a volunteer refuses required information, do not persuade beyond one gentle explanation.

Tone:

- Warm, respectful, optimistic

- Never salesy or pushy

- Always respond in simple English (no regional scripts). Do NOT switch languages or scripts.
- You may use at most ONE emoji per assistant message.

Context you will receive:

- Current state

- Known volunteer details (if any)

- Previous messages (summary)

- SERVE community link

Never invent facts.

Never assume consent.

Never store or repeat sensitive information unnecessarily.

You are guiding a human, not completing a form."""


# Planner prompt for next question selection
PLANNER_SYSTEM_PROMPT = """You are a planning module for a volunteer conversation.
You MUST return ONLY JSON with the required schema.
Rules:
- English only, one question per message.
- Keep tone warm and human.
- Prioritize decision-critical rubrics first if unknown/partial: commitment_horizon, teaching_readiness.
- Avoid asking resolved rubrics.
- For commitment_horizon: ask ONLY about continuing for ~3 months; do NOT mention hours/week, weekly time, or availability.
- If critical rubrics are resolved and remaining questions are optional, you may return next_target="stop".

Output JSON schema:
{
  "next_target": "motivation|commitment_horizon|teaching_readiness|teaching_experience|language|stop",
  "question": "string",
  "expected_answer_type": "free_text|yes_no_maybe|multi_choice",
  "stop_reason": "optional string",
  "why_internal": "string"
}
"""


EXTRACTOR_SYSTEM_PROMPT = """You are an extraction module. Return ONLY JSON with required schema.
Rules:
- Use last_target to focus extraction.
- Do NOT invent facts. If unclear, use value "unknown" or "maybe".
- Only ask clarification for critical rubrics (commitment_horizon, teaching_readiness) and only once.

Output JSON schema:
{
  "extracted": {
    "motivation": {"value":"high|medium|low|unknown","confidence":0-1},
    "commitment_horizon": {"value":"yes|no|maybe|unknown","confidence":0-1},
    "teaching_readiness": {"value":"yes|no|maybe|unknown","confidence":0-1},
    "teaching_experience": {"value":"yes|no|maybe|unknown","confidence":0-1},
    "language": {"value":"resolved|unknown","confidence":0-1}
  },
  "rubric_status_updates": {"rubric":"unknown|partial|resolved"},
  "needs_clarification": true|false,
  "clarification_question": "string",
  "notes_internal": "string"
}
"""


# KNOWING_VOLUNTEER state prompt (from selectionagent.py, cleaned)
def get_knowing_volunteer_prompt(
    next_target: Optional[str] = None,
    collected_fields: Optional[Dict] = None,
    discussed_fields: Optional[set] = None,
    preferred_language: Optional[str] = None
) -> str:
    """
    Get the KNOWING_VOLUNTEER prompt with optional NEXT_TARGET instruction and profile state.
    
    Args:
        next_target: Next rubric to ask about (from RUBRIC_ORDER)
        collected_fields: Dict of fields that have been collected (field: value)
        discussed_fields: Set of fields that have been discussed (even if not extracted)
    
    Returns:
        Prompt string
    """
    # Build profile state section
    profile_state_section = ""
    if collected_fields or discussed_fields:
        collected_list = []
        if collected_fields:
            for field, value in collected_fields.items():
                collected_list.append(f"- {field} = {value}")
        
        discussed_list = []
        if discussed_fields:
            for field in discussed_fields:
                if field not in (collected_fields or {}):
                    discussed_list.append(f"- {field} (discussed but not extracted)")
        
        if collected_list or discussed_list:
            profile_state_section = "\n\nCurrent profile state:\n"
            if collected_list:
                profile_state_section += "Already collected:\n" + "\n".join(collected_list) + "\n"
            if discussed_list:
                profile_state_section += "Already discussed:\n" + "\n".join(discussed_list) + "\n"
            if preferred_language:
                profile_state_section += f"Preferred language from preferences: {preferred_language}\n"
            profile_state_section += "\nIMPORTANT: Do NOT ask about fields that are already collected or discussed.\n"
    elif preferred_language:
        profile_state_section = f"\n\nPreferred language from preferences: {preferred_language}\n"
    
    # Build NEXT_TARGET instruction (prominent, at the top)
    next_target_section = ""
    if next_target:
        if preferred_language:
            language_instruction = (
                "Ask about comfort only in the already-chosen language. "
                f"Use {preferred_language} in your question and offer these options: Read, Write, Speak, All. "
                "Do NOT ask which language they prefer (it is already known). "
                "Capture language_comfort as one of: Read, Write, Speak, All (case-insensitive)."
            )
        else:
            language_instruction = (
                "Ask which language they are most comfortable teaching in, in a natural way. "
                "If they reply, capture language and language_comfort if mentioned. "
                "Do NOT evaluate grammar, fluency, or correctness."
            )

        target_questions = {
            "motivation": "Ask a warm question about why they want to volunteer and what drew them to SERVE.",
            "teaching_experience": "Ask about any prior teaching or mentoring experience (formal or informal).",
            "teaching_readiness": (
                "Ask how they feel about teaching children in a live class. "
                "You can include cues like: Excited to try / Comfortable with guidance / A bit unsure but open."
            ),
            "commitment_horizon": "Ask if they feel they can continue volunteering for about 3 months (no dates or guarantees).",
            "language": language_instruction
        }
        instruction = target_questions.get(next_target, "")
        if instruction:
            next_target_section = f"""
================================================================================
CRITICAL INSTRUCTION - READ THIS FIRST:
================================================================================
NEXT_TARGET = {next_target}

Your tone_reply MUST include a natural, conversational question about {next_target}.

{instruction}

IMPORTANT:
- Ask about {next_target} in a warm, natural way that fits the conversation context
- Adapt your question based on what the volunteer has already shared
- Make it feel like a genuine conversation, not an interrogation
- Your question should help extract information about {next_target}

DO NOT ask about fields that are already filled (check the conversation context).

Example tone_reply format (adapt naturally to context):
"<warm acknowledgement of their response> <natural question about {next_target}>"

Remember: Be conversational, warm, and human. The question should feel natural, not forced.

================================================================================
"""
    
    return f"""
You are SIA, the Sunbird SERVE volunteer onboarding guide.

Current state: KNOWING_VOLUNTEER.
{next_target_section}{profile_state_section}
Context:
- The volunteer has already completed eligibility, identity, and preference collection.
- Basic onboarding steps are complete.
- This step is to understand the volunteer as a person in a light, respectful way.
- You are NOT evaluating or filtering the volunteer at this stage.
- You are only understanding these signals:
  1) motivation - why they want to volunteer, what drew them to SERVE
  2) language - comfort in the preferred language (Read/Write/Speak/All)
  3) commitment_horizon - willingness to continue for ~3 months (ask naturally, no dates or guarantees)
  4) teaching_readiness - how they feel about teaching children live
  5) teaching_experience - any prior teaching / mentoring experience (formal or informal)
  
- The orchestrator controls which question was last asked in this state via `last_agent_prompt`.

Your goal:
Classify the user's latest message and produce:
- a single intent label,
- a confidence score (0.0–1.0),
- a short, warm WhatsApp-style acknowledgement ("tone_reply").

Allowed intents:
- MOTIVATION_SHARED         -> explains why they want to volunteer / help / give back
- EXPERIENCE_SHARED         -> mentions teaching, tutoring, mentoring, training, or helping others learn
- NO_EXPERIENCE             -> explicitly states no teaching or mentoring experience
- COMMITMENT_SHARED         -> mentions willingness or inability to continue for ~3 months
- LANGUAGE_SHARED           -> mentions language preference or comfort level
- TEACHING_READINESS_SHARED -> expresses excitement, comfort, or hesitation about teaching children live
- QUERY                     -> asks a question instead of answering
- AMBIGUOUS                 -> vague, off-topic, or unclear response
- DEFERRAL                  -> "not now", "later", "idk", "unsure", etc.
- STOP                      -> stop / unsubscribe / leave

Classification rules:
- Do NOT judge or filter based on experience; beginners are welcome.
- If the user explicitly says they have no experience -> NO_EXPERIENCE.
- Use `last_agent_prompt` to infer whether the response relates to motivation, experience, or comfort.
- If the message does not clearly map to any category -> AMBIGUOUS.
- Do NOT infer or invent information not explicitly stated.

Conversation boundaries:
- Do NOT ask personal questions (email, phone number, family, marital status, children, health, finances, etc.).
- Ask questions only around their work experience, teaching or mentoring experience, experience working with children, willingness to continue for ~3 months, and language they are comfortable teaching in

Critical rule (very important):
- Never mention onboarding steps, evaluation, selection, states, or internal processes.

Tone rules:
- 1–3 short lines.
- Warm, calm, and human.
- Reassuring, especially for NO_EXPERIENCE.
- Never evaluative, formal, or procedural.
- CRITICAL: Do NOT use emojis in tone_reply (encoding limitations). Never include emojis, even if previous messages contained them.
- CRITICAL: When decision=CONTINUE, tone_reply MUST always include a follow-up question (ending with "?"). Never leave the conversation without asking the next question.

Signal extraction rules:
- Extract signals ONLY if the user explicitly mentions them.
- Do NOT infer or guess.
- If a signal is not mentioned, return null.
- IMPORTANT: If the user's message clearly relates to a signal, extract it. Don't be overly strict.

Allowed values with examples:
  - teaching_experience: true / false / null
    Examples: "I taught math" -> true, "No experience" -> false, "I'm a teacher" -> true
  
  - teaching_readiness: "excited" / "comfortable_with_guidance" / "unsure_but_open" / "no" / null
    Examples: "Excited to try" -> "excited", "Comfortable with guidance" -> "comfortable_with_guidance",
              "A bit unsure but open" -> "unsure_but_open", "Not interested" -> "no"
  
  - commitment_horizon:
      "yes"    -> confident willingness to continue for ~3 months
      "unsure" -> hesitant / trying / not sure
      "no"     -> clearly cannot commit
      null
    Examples: "Yes, I can do 3 months" -> "yes", "I'm not sure" -> "unsure", "I can only do 1 month" -> "no"
  
  - language: string (e.g., "English", "Kannada", "Telugu", "Hindi", "Tamil", "Other") or null
    Examples: User says "English" -> "English", "I speak Kannada" -> "Kannada", "Telugu is my mother tongue" -> "Telugu"
  
  - language_comfort:
      "Read"  -> prefers reading
      "Write" -> prefers writing
      "Speak" -> prefers speaking
      "All"   -> comfortable with all
      null
    Examples: "Read" -> "Read", "write" -> "Write", "speak" -> "Speak", "all of them" -> "All"
  
  - motivation: null / help / serve others / empower / uplift / bring joy / happiness / give
    Examples: "I want to help" -> "help", "To serve others" -> "serve others", "To empower students" -> "empower"

Output ONLY valid JSON (all string values must be in double quotes):
  {{
  "intent": "EXPERIENCE_SHARED",
  "confidence": 0.7,
  "tone_reply": "Great to hear about your background! What inspired you to consider teaching?",

  "signals": {{
    "teaching_experience": true,
    "teaching_readiness": "unsure_but_open",
    "motivation": "help",
    "commitment_horizon": "yes",
    "language": "English",
    "language_comfort": "All"
  }}
}}

IMPORTANT JSON rules:
- All string values MUST be in double quotes: "yes", "no", "maybe", "help", etc.
- Boolean values: true or false (no quotes)
- null values: null (no quotes, lowercase)
- Array values: ["item1", "item2"] (strings in array must be quoted)
- If a signal is not mentioned, use null (not "null" as a string)
"""


# Ordered list of rubrics to fill (deterministic questioning)
RUBRIC_ORDER = ["motivation", "commitment_horizon", "teaching_readiness", "teaching_experience", "language"]
CRITICAL_RUBRICS = {"commitment_horizon", "teaching_readiness"}


# Few-shot examples (keep compact to avoid token bloat)
PLANNER_FEW_SHOTS = [
    {
        "role": "user",
        "content": json.dumps(
            {
                "open_rubrics": ["commitment_horizon", "teaching_readiness"],
                "rubric_status": {"commitment_horizon": "unknown", "teaching_readiness": "unknown"},
                "question_index": 1,
                "remaining_questions": 5,
                "fatigue": False,
                "last_target": "motivation",
                "last_agent_prompt": "What inspired you to volunteer?",
            }
        ),
    },
    {
        "role": "assistant",
        "content": (
            "{\"next_target\":\"commitment_horizon\",\"question\":\"Thanks for sharing that. "
            "Do you feel you could continue volunteering for about 3 months?\","
            "\"expected_answer_type\":\"yes_no_maybe\",\"why_internal\":\"commitment_horizon is critical\"}"
        ),
    },
    {
        "role": "user",
        "content": json.dumps(
            {
                "open_rubrics": ["teaching_readiness", "teaching_experience"],
                "rubric_status": {"teaching_readiness": "unknown", "teaching_experience": "unknown"},
                "question_index": 2,
                "remaining_questions": 4,
                "fatigue": False,
                "last_target": "commitment_horizon",
                "last_agent_prompt": "Do you feel you could continue volunteering for about 3 months?",
            }
        ),
    },
    {
        "role": "assistant",
        "content": (
            "{\"next_target\":\"teaching_readiness\",\"question\":\"How do you feel about teaching "
            "children in a live class — excited to try, comfortable with guidance, or a bit unsure but open?\","
            "\"expected_answer_type\":\"free_text\",\"why_internal\":\"readiness is critical\"}"
        ),
    },
    {
        "role": "user",
        "content": json.dumps(
            {
                "open_rubrics": [],
                "rubric_status": {
                    "motivation": "resolved",
                    "commitment_horizon": "resolved",
                    "teaching_readiness": "resolved",
                    "teaching_experience": "resolved",
                    "language": "resolved",
                },
                "question_index": 5,
                "remaining_questions": 1,
                "fatigue": False,
                "last_target": "language",
                "last_agent_prompt": "Which option fits you best: Read, Write, Speak, or All?",
            }
        ),
    },
    {
        "role": "assistant",
        "content": (
            "{\"next_target\":\"stop\",\"question\":\"Thanks for sharing — I have enough to suggest a next step.\","
            "\"expected_answer_type\":\"free_text\",\"why_internal\":\"all rubrics resolved\"}"
        ),
    },
]

EXTRACTOR_FEW_SHOTS = [
    {"role": "user", "content": "User: I can give around 2 hours a week, mostly weekdays."},
    {
        "role": "assistant",
        "content": (
            "{\"extracted\":{\"commitment_horizon\":{\"value\":\"unknown\",\"confidence\":0.2}},"
            "\"rubric_status_updates\":{},\"needs_clarification\":false,"
            "\"clarification_question\":\"\",\"notes_internal\":\"Hours/week is not commitment horizon\"}"
        ),
    },
    {"role": "user", "content": "User: Yes, I can commit for about 3 months."},
    {
        "role": "assistant",
        "content": (
            "{\"extracted\":{\"commitment_horizon\":{\"value\":\"yes\",\"confidence\":0.9}},"
            "\"rubric_status_updates\":{\"commitment_horizon\":\"resolved\"},\"needs_clarification\":false,"
            "\"clarification_question\":\"\",\"notes_internal\":\"Clear 3-month commitment\"}"
        ),
    },
    {"role": "user", "content": "User: I feel a bit unsure but I am open to try."},
    {
        "role": "assistant",
        "content": (
            "{\"extracted\":{\"teaching_readiness\":{\"value\":\"maybe\",\"confidence\":0.85}},"
            "\"rubric_status_updates\":{\"teaching_readiness\":\"partial\"},\"needs_clarification\":false,"
            "\"clarification_question\":\"\",\"notes_internal\":\"Unsure but open\"}"
        ),
    },
]


def _init_rubric_trackers(session: Dict) -> None:
    selection = session.setdefault("tool_state", {}).setdefault("selection", {})
    rubric_status = selection.setdefault("rubric_status", {})
    rubric_confidence = selection.setdefault("rubric_confidence", {})
    clarification_count = selection.setdefault("clarification_count", {})
    for rubric in RUBRIC_ORDER:
        rubric_status.setdefault(rubric, "unknown")
        rubric_confidence.setdefault(rubric, 0.0)
        clarification_count.setdefault(rubric, 0)
    # Preserve existing behavior: skip language unless explicitly enabled
    if selection.get("skip_language", True):
        rubric_status["language"] = "resolved"


def _rubric_open(
    rubric: str,
    rubric_status: Dict[str, str],
    clarification_count: Dict[str, int],
) -> bool:
    status = rubric_status.get(rubric, "unknown")
    if status == "resolved":
        return False
    if status == "partial" and clarification_count.get(rubric, 0) >= 1:
        return False
    return True


def _get_open_rubrics(
    rubric_status: Dict[str, str],
    clarification_count: Dict[str, int],
) -> List[str]:
    return [r for r in RUBRIC_ORDER if _rubric_open(r, rubric_status, clarification_count)]


def _parse_llm_json(raw_text: str) -> Dict:
    if not raw_text:
        raise ValueError("LLM returned empty response")
    repaired_text = raw_text
    repaired_text = re.sub(
        r'```(?:json)?\s*({.*?})\s*```',
        r"\1",
        repaired_text,
        flags=re.DOTALL,
    )
    try:
        return json.loads(repaired_text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"LLM response is not valid JSON: {raw_text[:500]}") from exc


async def _llm_call_json(messages: List[Dict], schema: Optional[Dict] = None, timeout: int = 20) -> Dict:
    _mcp_call = _get_mcp_call()
    payload = {
        "messages": messages,
        "temperature": 0.4,
        "max_tokens": 300,
        "response_format": "json_object",
    }
    result = await _mcp_call("llm.call", payload, timeout=timeout)
    raw_text = ""
    if isinstance(result, dict):
        if "content" in result:
            content = result["content"]
            if isinstance(content, list):
                for item in content:
                    if isinstance(item, dict) and item.get("type") == "text":
                        raw_text = item.get("text", "")
                        break
            elif isinstance(content, str):
                raw_text = content
        elif "text" in result:
            raw_text = result["text"]
        elif "reply" in result:
            raw_text = result["reply"]
        elif "result" in result and isinstance(result["result"], dict):
            nested = result["result"]
            if "content" in nested and isinstance(nested["content"], list):
                for item in nested["content"]:
                    if isinstance(item, dict) and item.get("type") == "text":
                        raw_text = item.get("text", "")
                        break
    parsed = _parse_llm_json(raw_text)
    if schema:
        try:
            import jsonschema
            jsonschema.validate(parsed, schema)
        except Exception:
            pass
    return parsed


async def _compose_ack_and_question(user_text: str, question: str) -> str:
    if not user_text or not question:
        return question
    prompt = (
        "You are crafting a single WhatsApp message.\n"
        "Add a short, warm acknowledgement of the user's reply, then ask the provided question.\n"
        "Rules:\n"
        "- 1-2 short lines total\n"
        "- No emojis\n"
        "- Do NOT add any other questions\n"
        "- Use the question text exactly as given and place it at the end\n"
        "Return ONLY valid JSON: {\"text\": \"...\"}"
    )
    schema = {
        "type": "object",
        "required": ["text"],
        "properties": {"text": {"type": "string"}},
    }
    messages = [
        {"role": "system", "content": MASTER_SYSTEM_PROMPT},
        {"role": "system", "content": prompt},
        {"role": "user", "content": f"User reply: {user_text}\nQuestion: {question}"},
    ]
    try:
        parsed = await _llm_call_json(messages, schema=schema, timeout=15)
        combined = parsed.get("text")
        if isinstance(combined, str) and combined.strip():
            return combined.strip()
    except Exception as e:
        log.warning(f"[KNOWING_VOLUNTEER] Ack+question composition failed: {e}")
    return f"Thanks for sharing. {question}"


def _detect_fatigue(user_text: str, session: Dict) -> bool:
    text_lower = (user_text or "").lower().strip()
    low_info = len(text_lower.split()) <= 2 or re.search(r"\b(idk|not sure|unsure|no idea)\b", text_lower)
    selection = session.setdefault("tool_state", {}).setdefault("selection", {})
    streak = selection.get("low_info_streak", 0)
    if low_info:
        streak += 1
    else:
        streak = 0
    selection["low_info_streak"] = streak
    return bool(low_info or streak >= 2)


def _rule_extract(user_text: str, target: Optional[str]) -> Dict[str, Dict]:
    """High-precision rule extraction. Returns rubric -> {value, confidence}."""
    text_lower = (user_text or "").lower().strip()
    extracted: Dict[str, Dict] = {}
    if target == "teaching_readiness":
        if re.search(r"\b(very\s+)?comfortable|confident|ready|okay|ok|sure|excited\b", text_lower):
            extracted["teaching_readiness"] = {"value": "yes", "confidence": 0.9}
        elif re.search(r"\b(not comfortable|uncomfortable|not confident)\b", text_lower):
            extracted["teaching_readiness"] = {"value": "no", "confidence": 0.9}
        elif re.search(r"\b(unsure|not sure|maybe|nervous)\b", text_lower):
            extracted["teaching_readiness"] = {"value": "maybe", "confidence": 0.9}
    elif target == "commitment_horizon":
        if re.search(r"\b(yes|ok|okay|sure|can|will|possible|fine)\b", text_lower):
            extracted["commitment_horizon"] = {"value": "yes", "confidence": 0.9}
        elif re.search(r"\b(not sure|maybe|unsure)\b", text_lower):
            extracted["commitment_horizon"] = {"value": "maybe", "confidence": 0.9}
        elif re.search(r"\b(no|cannot|can't|cant)\b", text_lower):
            extracted["commitment_horizon"] = {"value": "no", "confidence": 0.9}
    elif target == "teaching_experience":
        if re.search(r"\b(yes|have|taught|teaching|mentor|mentored|trained|experience)\b", text_lower):
            extracted["teaching_experience"] = {"value": "yes", "confidence": 0.9}
        elif re.search(r"\b(no|not really|never|haven't|have not|didn't|did not)\b", text_lower):
            extracted["teaching_experience"] = {"value": "no", "confidence": 0.9}
    elif target == "motivation":
        if re.search(r"\b(help|teach|support|give back|contribute|volunteer|kids|children|students|education)\b", text_lower):
            extracted["motivation"] = {"value": "high", "confidence": 0.9}
    elif target == "language":
        if re.search(r"\b(read|write|speak|all)\b", text_lower):
            extracted["language"] = {"value": "resolved", "confidence": 0.9}
    return extracted


def _merge_extractions(
    rule_extracted: Dict[str, Dict],
    llm_extracted: Dict[str, Dict],
) -> Dict[str, Dict]:
    merged = {}
    for rubric in set(rule_extracted.keys()) | set(llm_extracted.keys()):
        rule_val = rule_extracted.get(rubric)
        llm_val = llm_extracted.get(rubric)
        if not rule_val:
            merged[rubric] = llm_val
        elif not llm_val:
            merged[rubric] = rule_val
        else:
            merged[rubric] = rule_val if rule_val["confidence"] >= llm_val["confidence"] else llm_val
    return merged


def _status_from_value(rubric: str, value: str) -> str:
    if value in {"unknown"}:
        return "unknown"
    if value in {"maybe"}:
        return "partial"
    return "resolved"


def _clean_question(text: str) -> str:
    if not text:
        return text
    cleaned = text.strip()
    # Collapse multiple question marks at the end
    cleaned = re.sub(r"\?{2,}$", "?", cleaned)
    return cleaned


def _is_affirmative(text: str) -> bool:
    return bool(re.search(r"\b(yes|yeah|yep|sure|ok|okay|confirm|correct)\b", text, re.I))


def _is_negative(text: str) -> bool:
    return bool(re.search(r"\b(no|nope|not really|dont|don't)\b", text, re.I))

# Confidence threshold for trusting new extractions
LOW_CONF_THRESHOLD = 0.55


def _get_next_missing_rubric(
    rubric_status: Dict[str, str],
    clarification_count: Dict[str, int],
) -> Optional[str]:
    """Find the first rubric that is not resolved (partial allowed once)."""
    for rubric in RUBRIC_ORDER:
        if _rubric_open(rubric, rubric_status, clarification_count):
            return rubric
    return None


# Intent to field mapping (for marking fields as discussed)
INTENT_TO_FIELD_MAP = {
    "MOTIVATION_SHARED": "motivation",
    "EXPERIENCE_SHARED": "teaching_experience",
    "NO_EXPERIENCE": "teaching_experience",
    "TEACHING_READINESS_SHARED": "teaching_readiness",
    "INTEREST_SHARED": "teaching_readiness",
    "COMMITMENT_SHARED": "commitment_horizon",
    "LANGUAGE_SHARED": "language"
}

# Volunteer profile structure
def init_volunteer_profile() -> Dict:
    """Initialize empty volunteer profile structure"""
    return {
        "motivation": None,
        "teaching_experience": None,
        "commitment_horizon": None,
        "teaching_readiness": None,
        "language": None,
        "language_comfort": None
    }


def knowing_volunteer_complete(profile: Dict) -> bool:
    """
    Check if knowing volunteer is complete.
    
    Returns True if all 5 rubrics are present (language counts as present if either
    language or language_comfort is set).
    
    Args:
        profile: Volunteer profile dict
    
    Returns:
        bool: True if complete, False otherwise
    """
    signals_present = _count_signals_present(profile)
    # Complete only when all 5 rubrics are present
    return signals_present >= 5


def _count_signals_present(profile: Dict) -> int:
    """
    Count how many signals are present in the profile.
    
    Args:
        profile: Volunteer profile dict
    
    Returns:
        Number of signals present (0-5)
    """
    signals = [
        profile.get("motivation"),
        profile.get("teaching_experience"),
        profile.get("commitment_horizon"),
        profile.get("language") or profile.get("language_comfort"),  # Language signal counts if either field is present
        profile.get("teaching_readiness")
    ]
    
    count = 0
    for signal in signals:
        if signal is not None:
            if isinstance(signal, list):
                if len(signal) > 0:  # Non-empty list counts
                    count += 1
            else:
                count += 1
    
    return count


def _is_deferral_intent(intent: str, user_text: str) -> bool:
    """
    Check if the intent or user text indicates deferral.
    
    Args:
        intent: Intent classification
        user_text: User's message text
    
    Returns:
        True if deferral detected
    """
    if intent == "STOP":
        return True
    
    # Check for deferral patterns in text
    deferral_patterns = [
        r"\b(not now|not right now|later|after|next month|next week)\b",
        r"\b(idk|i don't know|i dont know|unsure|not sure)\b",
        r"\b(maybe later|some other time|another time)\b"
    ]
    
    text_lower = user_text.lower()
    for pattern in deferral_patterns:
        if re.search(pattern, text_lower):
            return True
    
    return False


def evaluate_knowing_volunteer(
    intent: str,
    question_index: int,
    profile: Dict,
    user_text: str = "",
    max_questions: int = 12,  # Hard cap at 12 questions
    min_questions: int = 3   # Lowered minimum
) -> KnowingVolunteerResult:
    """
    Decide flow outcome for KNOWING_VOLUNTEER.
    
    Args:
        intent: Intent from LLM classification
        question_index: Current question index (0-based)
        profile: Volunteer profile dict
        user_text: User's message text (for deferral detection)
        max_questions: Maximum questions to ask (default: 6)
        min_questions: Minimum questions before allowing completion (default: 3)
    
    Returns:
        KnowingVolunteerResult enum value
    """
    # 1️⃣ Explicit stop
    if intent == "STOP":
        return KnowingVolunteerResult.STOP
    
    # 2️⃣ Deferral + sufficient signals: stop gracefully
    signals_present = _count_signals_present(profile)
    if _is_deferral_intent(intent, user_text) and signals_present >= 3:
        return KnowingVolunteerResult.COMPLETE_INSUFFICIENT_INFO
    
    # 3️⃣ If profile is sufficiently filled (all 5 rubrics present)
    if knowing_volunteer_complete(profile):
        return KnowingVolunteerResult.COMPLETE
    
    # 4️⃣ Hard cap: stop after max_questions
    if question_index >= max_questions:
        return KnowingVolunteerResult.COMPLETE_INSUFFICIENT_INFO
    
    # 5️⃣ Continue asking
    return KnowingVolunteerResult.CONTINUE


async def run_knowing_volunteer_step(
    session: Dict,
    user_text: str,
    last_agent_prompt: Optional[str],
    history_messages: Optional[List[Dict]] = None
) -> Dict:
    """Run one step of the knowing volunteer loop (planner -> extractor)."""
    if "tool_state" not in session:
        session["tool_state"] = {}
    if "selection" not in session["tool_state"]:
        session["tool_state"]["selection"] = {}
    if "profile" not in session["tool_state"]["selection"]:
        session["tool_state"]["selection"]["profile"] = init_volunteer_profile()
    _init_rubric_trackers(session)
    selection = session["tool_state"]["selection"]
    profile = selection["profile"]
    rubric_status = selection["rubric_status"]
    rubric_confidence = selection["rubric_confidence"]
    clarification_count = selection["clarification_count"]
    question_index = selection.get("question_index", 0)
    last_target = selection.get("last_target")
    preferred_language = session.get("profile", {}).get("preferences", {}).get("language")
    fatigue = _detect_fatigue(user_text, session) if user_text != "__kick__" else False

    # Extractor step (only if we have a previous target)
    rule_extracted = _rule_extract(user_text, last_target) if last_target and user_text != "__kick__" else {}
    llm_extracted: Dict[str, Dict] = {}
    rubric_status_updates: Dict[str, str] = {}
    needs_clarification = False
    clarification_question = ""
    if last_target and user_text != "__kick__":
        extractor_schema = {
            "type": "object",
            "required": ["extracted", "rubric_status_updates", "needs_clarification"],
            "properties": {
                "extracted": {"type": "object"},
                "rubric_status_updates": {"type": "object"},
                "needs_clarification": {"type": "boolean"},
                "clarification_question": {"type": "string"},
                "notes_internal": {"type": "string"},
            },
        }
        messages = [
            {"role": "system", "content": EXTRACTOR_SYSTEM_PROMPT},
            {"role": "system", "content": f"Last target: {last_target}. Preferred language: {preferred_language}."},
            *EXTRACTOR_FEW_SHOTS,
            {"role": "user", "content": user_text},
        ]
        try:
            parsed = await _llm_call_json(messages, schema=extractor_schema, timeout=20)
            extracted = parsed.get("extracted", {})
            for rubric, payload in extracted.items():
                if isinstance(payload, dict) and "value" in payload:
                    llm_extracted[rubric] = {
                        "value": payload.get("value", "unknown"),
                        "confidence": float(payload.get("confidence", 0.0) or 0.0),
                    }
            rubric_status_updates = parsed.get("rubric_status_updates", {}) or {}
            needs_clarification = bool(parsed.get("needs_clarification", False))
            clarification_question = (parsed.get("clarification_question") or "").strip()
            selection["last_extractor"] = parsed
        except Exception as e:
            log.warning(f"[KNOWING_VOLUNTEER] Extractor failed: {e}")

    merged_extracted = _merge_extractions(rule_extracted, llm_extracted)
    # Guard: when last_target is teaching_experience, do not let readiness update
    if last_target == "teaching_experience" and "teaching_readiness" in merged_extracted:
        merged_extracted.pop("teaching_readiness", None)
    for rubric, payload in merged_extracted.items():
        value = payload.get("value", "unknown")
        confidence = float(payload.get("confidence", 0.0) or 0.0)
        if confidence >= rubric_confidence.get(rubric, 0.0):
            rubric_confidence[rubric] = confidence
            if rubric == "teaching_experience" and value in {"yes", "no", "maybe"}:
                profile["teaching_experience"] = value
            elif rubric == "teaching_readiness" and value in {"yes", "no", "maybe"}:
                profile["teaching_readiness"] = value
            elif rubric == "commitment_horizon" and value in {"yes", "no", "maybe"}:
                profile["commitment_horizon"] = value
            elif rubric == "motivation" and value in {"high", "medium", "low"}:
                profile["motivation"] = value
        status = _status_from_value(rubric, value)
        if status == "resolved":
            rubric_status[rubric] = "resolved"
        elif status == "partial" and rubric_status.get(rubric) != "resolved":
            rubric_status[rubric] = "partial"

    for rubric, status in rubric_status_updates.items():
        if status in {"unknown", "partial", "resolved"}:
            if status == "resolved":
                rubric_status[rubric] = "resolved"
            elif status == "partial" and rubric_status.get(rubric) != "resolved":
                rubric_status[rubric] = "partial"

    # Clarification for critical rubrics
    if (
        last_target in CRITICAL_RUBRICS
        and needs_clarification
        and clarification_count.get(last_target, 0) < 1
        and rubric_status.get(last_target, "unknown") != "resolved"
    ):
        clarification_count[last_target] = clarification_count.get(last_target, 0) + 1
        selection["last_target"] = last_target
        assistant_text = clarification_question or "Just to confirm — does that work for you?"
        selection["question_index"] = question_index + 1
        return {
            "intent": "CLARIFY",
            "confidence": 1.0,
            "assistant_text": assistant_text,
            "signals": profile.copy(),
            "decision": KnowingVolunteerResult.CONTINUE.value,
        }

    open_rubrics = _get_open_rubrics(rubric_status, clarification_count)
    remaining_questions = max(0, 6 - question_index)
    planner_schema = {
        "type": "object",
        "required": ["next_target", "question", "expected_answer_type"],
        "properties": {
            "next_target": {"type": "string"},
            "question": {"type": "string"},
            "expected_answer_type": {"type": "string"},
            "stop_reason": {"type": "string"},
            "why_internal": {"type": "string"},
        },
    }
    planner_messages = [
        {"role": "system", "content": MASTER_SYSTEM_PROMPT},
        {"role": "system", "content": PLANNER_SYSTEM_PROMPT},
        *PLANNER_FEW_SHOTS,
        {
            "role": "user",
            "content": json.dumps(
                {
                    "open_rubrics": open_rubrics,
                    "rubric_status": rubric_status,
                    "question_index": question_index,
                    "remaining_questions": remaining_questions,
                    "fatigue": fatigue,
                    "last_target": last_target,
                    "last_agent_prompt": last_agent_prompt,
                }
            ),
        },
    ]
    try:
        planner = await _llm_call_json(planner_messages, schema=planner_schema, timeout=20)
    except Exception as e:
        log.warning(f"[KNOWING_VOLUNTEER] Planner failed: {e}")
        planner = {
            "next_target": open_rubrics[0] if open_rubrics else "stop",
            "question": "Could you share a bit more?",
            "expected_answer_type": "free_text",
        }

    selection["last_planner"] = planner
    next_target = (planner.get("next_target") or "").strip()
    question = _clean_question((planner.get("question") or "").strip())

    if question_index >= 6:
        return {
            "intent": "STOP",
            "confidence": 1.0,
            "assistant_text": "Thanks for sharing — I have enough to suggest a next step.",
            "signals": profile.copy(),
            "decision": KnowingVolunteerResult.COMPLETE_INSUFFICIENT_INFO.value,
        }

    if next_target == "summary_confirm":
        next_target = open_rubrics[0] if open_rubrics else "stop"

    if next_target == "stop":
        return {
            "intent": "STOP",
            "confidence": 1.0,
            "assistant_text": question or "Thanks for sharing — I have enough to suggest a next step.",
            "signals": profile.copy(),
            "decision": KnowingVolunteerResult.COMPLETE.value,
        }

    selection["last_target"] = next_target
    selection["question_index"] = question_index + 1
    assistant_text = question
    if user_text != "__kick__":
        assistant_text = await _compose_ack_and_question(user_text, question)

    return {
        "intent": "CONTINUE",
        "confidence": 1.0,
        "assistant_text": assistant_text,
        "signals": profile.copy(),
        "decision": KnowingVolunteerResult.CONTINUE.value,
    }

