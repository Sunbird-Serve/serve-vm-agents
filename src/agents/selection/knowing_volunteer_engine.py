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
- Output must be English-only and ASCII-safe. Do NOT include any non-ASCII characters.

- CRITICAL: Do NOT use emojis in ANY responses (encoding limitations). Never include emojis, even if you see them in conversation history.

Context you will receive:

- Current state

- Known volunteer details (if any)

- Previous messages (summary)

- SERVE community link

Never invent facts.

Never assume consent.

Never store or repeat sensitive information unnecessarily.

You are guiding a human, not completing a form."""


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

# Confidence threshold for trusting new extractions
LOW_CONF_THRESHOLD = 0.55


def _get_next_missing_rubric(profile: Dict, discussed_fields: Optional[set] = None) -> Optional[str]:
    """
    Find the first missing rubric in ORDER.
    
    Language counts as present if either language OR language_comfort is set.
    Skips fields that are already filled OR already discussed.
    
    Args:
        profile: Volunteer profile dict
        discussed_fields: Set of field names that have been discussed (even if not extracted)
    
    Returns:
        Next missing rubric name, or None if all are filled or discussed
    """
    if discussed_fields is None:
        discussed_fields = set()
    
    for rubric in RUBRIC_ORDER:
        # Skip if already discussed
        if rubric in discussed_fields:
            continue
        
        # Skip if already filled
        if rubric == "language":
            # Language counts if either field is present
            if profile.get("language") or profile.get("language_comfort"):
                continue
        else:
            if profile.get(rubric) is not None:
                continue
        
        # This rubric is missing and not discussed - return it
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
    """
    Run one step of the knowing volunteer loop.
    
    Args:
        session: Session dict (modified in place)
        user_text: User's message text
        last_agent_prompt: Last prompt/question sent to user (optional)
        history_messages: List of previous messages in format [{"role": "user|assistant", "content": "..."}, ...] (optional)
    
    Returns:
        Dict with keys: intent, confidence, assistant_text, signals, decision
    """
    # Get LLM calling function (use existing infrastructure)
    _mcp_call = _get_mcp_call()
    
    # Get current profile to determine next target
    if "tool_state" not in session:
        session["tool_state"] = {}
    if "selection" not in session["tool_state"]:
        session["tool_state"]["selection"] = {}
    if "profile" not in session["tool_state"]["selection"]:
        session["tool_state"]["selection"]["profile"] = init_volunteer_profile()
        session["tool_state"]["selection"]["discussed_fields"] = set()
    
    current_profile = session["tool_state"]["selection"]["profile"]
    
    # Get discussed fields (fields that have been discussed even if not extracted)
    discussed_fields = session["tool_state"]["selection"].get("discussed_fields", set())
    if not isinstance(discussed_fields, set):
        discussed_fields = set(discussed_fields) if discussed_fields else set()
    # Pre-mark language as discussed to skip asking it for now
    discussed_fields.add("language")
    session["tool_state"]["selection"]["discussed_fields"] = discussed_fields
    
    # Calculate next target (skip discussed fields)
    next_target = _get_next_missing_rubric(current_profile, discussed_fields)
    
    # Store expected target for validation later
    session["tool_state"]["selection"]["expected_target"] = next_target
    
    # Build profile state summary for LLM
    collected_fields = {k: v for k, v in current_profile.items() if v is not None}
    missing_fields = [r for r in RUBRIC_ORDER if r not in collected_fields and r not in discussed_fields]
    
    log.info(f"[KNOWING_VOLUNTEER] Before LLM call: next_target={next_target}, collected={list(collected_fields.keys())}, discussed={list(discussed_fields)}, missing={missing_fields}")
    
    preferred_language = session.get("profile", {}).get("preferences", {}).get("language")

    # Build messages with dynamic prompt (include profile state)
    messages = [
        {"role": "system", "content": MASTER_SYSTEM_PROMPT},
        {"role": "system", "content": get_knowing_volunteer_prompt(
            next_target=next_target,
            collected_fields=collected_fields,
            discussed_fields=discussed_fields,
            preferred_language=preferred_language
        )}
    ]
    
    # Add last agent prompt if available (strip emojis to prevent LLM from copying them)
    if last_agent_prompt:
        cleaned_prompt = _strip_emojis(last_agent_prompt)
        if cleaned_prompt:  # Only add if there's content after stripping
            messages.append({"role": "assistant", "content": cleaned_prompt})
    
    # Add history messages (last 6) - strip emojis from all history messages
    if history_messages:
        for msg in history_messages[-6:]:
            if isinstance(msg, dict) and "role" in msg and "content" in msg:
                cleaned_content = _strip_emojis(msg["content"])
                if cleaned_content:  # Only add if there's content after stripping
                    messages.append({
                        "role": msg["role"],
                        "content": cleaned_content
                    })
    
    # Add current user message (strip emojis from user input too, just to be safe)
    cleaned_user_text = _strip_emojis(user_text)
    messages.append({"role": "user", "content": cleaned_user_text})
    
    # Define response schema
    response_schema = {
        "type": "object",
        "required": ["intent", "confidence"],
        "properties": {
            "intent": {"type": "string"},
            "confidence": {"type": ["number", "string"]},
            "tone_reply": {"type": ["string", "null"]},
            "signals": {
                "type": "object",
                "properties": {
                    "teaching_experience": {"type": ["boolean", "null"]},
                    "teaching_readiness": {"type": ["string", "null"]},
                    "motivation": {"type": ["string", "null"]},
                    "commitment_horizon": {"type": ["string", "null"]},
                    "language": {"type": ["string", "null"]},
                    "language_comfort": {"type": ["string", "null"]}
                }
            }
        }
    }
    
    # Call LLM with JSON response format
    try:
        import jsonschema
        from jsonschema import ValidationError
        
        # Use MCP llm.call with response_format=json_object
        _mcp_call = _get_mcp_call()
        payload = {
            "messages": messages,
            "temperature": 0.4,
            "max_tokens": 300,
            "response_format": "json_object"
        }
        
        mcp_result = await _mcp_call("llm.call", payload, timeout=20)
        
        # Extract text from MCP response (use same logic as _extract_llm_text)
        raw_text = ""
        if isinstance(mcp_result, dict):
            if "content" in mcp_result:
                content = mcp_result["content"]
                if isinstance(content, list):
                    for item in content:
                        if isinstance(item, dict) and item.get("type") == "text":
                            raw_text = item.get("text", "")
                            break
                elif isinstance(content, str):
                    raw_text = content
            elif "text" in mcp_result:
                raw_text = mcp_result["text"]
            elif "reply" in mcp_result:
                raw_text = mcp_result["reply"]
            elif "result" in mcp_result:
                # Try nested result
                result_data = mcp_result["result"]
                if isinstance(result_data, dict):
                    if "content" in result_data:
                        content = result_data["content"]
                        if isinstance(content, list):
                            for item in content:
                                if isinstance(item, dict) and item.get("type") == "text":
                                    raw_text = item.get("text", "")
                                    break
                        elif isinstance(content, str):
                            raw_text = content
        
        if not raw_text:
            raise ValueError("LLM returned empty response")
        
        # Repair common JSON issues before parsing
        # Fix unquoted string values (e.g., maybe -> "maybe", yes -> "yes", no -> "no")
        repaired_text = raw_text
        # Replace unquoted string values in teaching_readiness field
        repaired_text = re.sub(
            r'"teaching_readiness"\s*:\s*(excited|comfortable_with_guidance|unsure_but_open|no)(?=\s*[,}])',
            r'"teaching_readiness": "\1"',
            repaired_text,
            flags=re.IGNORECASE
        )
        # Replace unquoted string values in motivation field (common values)
        repaired_text = re.sub(
            r'"motivation"\s*:\s*(help|serve|uplift|outreach|empower)(?=\s*[,}])',
            r'"motivation": "\1"',
            repaired_text,
            flags=re.IGNORECASE
        )
        # Replace unquoted string values in commitment_horizon field
        repaired_text = re.sub(
            r'"commitment_horizon"\s*:\s*(yes|unsure|no)(?=\s*[,}])',
            r'"commitment_horizon": "\1"',
            repaired_text,
            flags=re.IGNORECASE
        )
        # Replace unquoted string values in language_comfort field
        repaired_text = re.sub(
            r'"language_comfort"\s*:\s*(read|write|speak|all)(?=\s*[,}])',
            r'"language_comfort": "\1"',
            repaired_text,
            flags=re.IGNORECASE
        )
        
        # Parse JSON
        try:
            result = json.loads(repaired_text)
        except json.JSONDecodeError as exc:
            # If repair didn't work, try to extract JSON from markdown code blocks
            json_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', repaired_text, re.DOTALL)
            if json_match:
                try:
                    result = json.loads(json_match.group(1))
                except json.JSONDecodeError:
                    raise ValueError(f"LLM response is not valid JSON even after repair: {raw_text[:500]}") from exc
            else:
                raise ValueError(f"LLM response is not valid JSON: {raw_text[:500]}") from exc
        
        # Validate against schema
        try:
            jsonschema.validate(result, response_schema)
        except ValidationError as exc:
            # Try to prune extra keys
            if isinstance(result, dict) and response_schema.get("properties"):
                allowed_keys = set(response_schema["properties"].keys())
                pruned = {k: v for k, v in result.items() if k in allowed_keys}
                if pruned != result:
                    try:
                        jsonschema.validate(pruned, response_schema)
                        result = pruned
                    except ValidationError:
                        pass
                else:
                    log.warning(f"[KNOWING_VOLUNTEER] Schema validation failed: {exc.message}, using result anyway")
            else:
                log.warning(f"[KNOWING_VOLUNTEER] Schema validation failed: {exc.message}, using result anyway")
        
    except Exception as e:
        log.error(f"[KNOWING_VOLUNTEER] LLM call failed: {e}", exc_info=True)
        # Fallback: return ambiguous result
        result = {
            "intent": "AMBIGUOUS",
            "confidence": 0.0,
            "tone_reply": "I see. Could you tell me a bit more about yourself?",
            "signals": {}
        }
    
    # Extract values
    intent = result.get("intent", "AMBIGUOUS")
    confidence = float(result.get("confidence", 0.0))
    tone_reply = result.get("tone_reply", "")
    
    # Strip emojis from tone_reply (safety measure for encoding issues)
    if tone_reply:
        tone_reply = _strip_emojis(tone_reply)
    
    signals = result.get("signals", {})
    if isinstance(signals, dict):
        comfort = signals.get("language_comfort")
        if isinstance(comfort, str):
            comfort_norm = comfort.strip().lower()
            comfort_map = {
                "read": "Read",
                "write": "Write",
                "speak": "Speak",
                "all": "All",
            }
            if comfort_norm in comfort_map:
                signals["language_comfort"] = comfort_map[comfort_norm]
        # Backward-compat: map old keys to new ones if present
        if "teaching_experience" not in signals and "has_teaching_experience" in signals:
            signals["teaching_experience"] = signals.get("has_teaching_experience")
        if "teaching_readiness" not in signals and "teaching_interest" in signals:
            interest = signals.get("teaching_interest")
            if isinstance(interest, str):
                interest_norm = interest.strip().lower()
                readiness_map = {
                    "yes": "excited",
                    "maybe": "unsure_but_open",
                    "no": "no",
                }
                if interest_norm in readiness_map:
                    signals["teaching_readiness"] = readiness_map[interest_norm]
            elif interest is None:
                signals["teaching_readiness"] = None
    
    # Get expected target (rubric we were aiming for this turn)
    expected_target = session["tool_state"]["selection"].get("expected_target")
    
    # Initialize / read low confidence streak
    low_conf_streak = session["tool_state"]["selection"].get("low_conf_streak", 0)
    
    clarification_for_target = False
    
    # Rule-based fallback for common replies (helps avoid loops on simple responses)
    force_commit_target = False
    if expected_target:
        text_lower = (user_text or "").lower().strip()
        rule_signals = {}
        if expected_target == "teaching_readiness":
            if re.search(r"\b(very\s+)?comfortable|confident|ready|okay|ok|sure\b", text_lower):
                rule_signals["teaching_readiness"] = "comfortable_with_guidance"
            elif re.search(r"\b(not comfortable|uncomfortable|not confident)\b", text_lower):
                rule_signals["teaching_readiness"] = "no"
            elif re.search(r"\b(unsure|not sure|maybe|nervous)\b", text_lower):
                rule_signals["teaching_readiness"] = "unsure_but_open"
        elif expected_target == "motivation":
            if re.search(r"\b(help|teach|support|give back|contribute|volunteer|kids|children|students|education)\b", text_lower):
                rule_signals["motivation"] = "help"
        elif expected_target == "teaching_experience":
            if re.search(r"\b(yes|have|taught|teaching|mentor|mentored|trained|experience)\b", text_lower):
                rule_signals["teaching_experience"] = True
            elif re.search(r"\b(no|not really|never|haven't|have not|didn't|did not)\b", text_lower):
                rule_signals["teaching_experience"] = False
        elif expected_target == "commitment_horizon":
            if re.search(r"\b(yes|ok|okay|sure|can|will|possible|fine)\b", text_lower):
                rule_signals["commitment_horizon"] = "yes"
            elif re.search(r"\b(not sure|maybe|unsure)\b", text_lower):
                rule_signals["commitment_horizon"] = "unsure"
            elif re.search(r"\b(no|cannot|can't|cant)\b", text_lower):
                rule_signals["commitment_horizon"] = "no"
        elif expected_target == "language" and preferred_language:
            if re.search(r"\b(very\s+)?comfortable|confident|good|ok|okay|fine\b", text_lower):
                rule_signals["language_comfort"] = "All"
        if rule_signals:
            signals.update(rule_signals)
            force_commit_target = True
    
    # Guard: if confidence is low and LLM claims to extract a new signal for expected_target,
    # do NOT commit that field yet. Instead, ask a simple clarification question and
    # increment low_conf_streak.
    if expected_target and confidence < LOW_CONF_THRESHOLD and not force_commit_target:
        new_signal_for_target = False
        
        if expected_target == "language":
            # Language rubric: treat either language or language_comfort as a claimed signal
            new_lang = signals.get("language")
            new_comfort = signals.get("language_comfort")
            if ((new_lang is not None and session["tool_state"]["selection"]["profile"].get("language") is None) or
                (new_comfort is not None and session["tool_state"]["selection"]["profile"].get("language_comfort") is None)):
                new_signal_for_target = True
        else:
            # Simple scalar fields
            profile_val = session["tool_state"]["selection"]["profile"].get(expected_target)
            if signals.get(expected_target) is not None and profile_val is None:
                new_signal_for_target = True
        
        if new_signal_for_target:
            clarification_for_target = True
            
            # Strip out the low-confidence field(s) so they are not merged into profile
            if expected_target == "language":
                if signals.get("language") is not None:
                    signals["language"] = None
                if signals.get("language_comfort") is not None:
                    signals["language_comfort"] = None
            else:
                signals[expected_target] = None
            
            # Keep the LLM's tone_reply (no hardcoded follow-up)
            if not tone_reply:
                tone_reply = "I see. Could you tell me a bit more?"
            
            # Increment low confidence streak
            low_conf_streak += 1
            session["tool_state"]["selection"]["low_conf_streak"] = low_conf_streak
    
    # Get current profile (already initialized above)
    profile = session["tool_state"]["selection"]["profile"]
    
    # Merge signals into profile (only set non-null values, don't overwrite existing)
    signals_extracted = {}
    
    if signals.get("motivation") is not None and profile.get("motivation") is None:
        profile["motivation"] = signals.get("motivation")
        signals_extracted["motivation"] = profile["motivation"]
        log.info(f"[KNOWING_VOLUNTEER] Extracted motivation: {profile['motivation']}")
    
    if signals.get("teaching_experience") is not None and profile.get("teaching_experience") is None:
        profile["teaching_experience"] = signals.get("teaching_experience")
        signals_extracted["teaching_experience"] = profile["teaching_experience"]
        log.info(f"[KNOWING_VOLUNTEER] Extracted teaching_experience: {profile['teaching_experience']}")
    
    if signals.get("teaching_readiness") is not None and profile.get("teaching_readiness") is None:
        profile["teaching_readiness"] = signals.get("teaching_readiness")
        signals_extracted["teaching_readiness"] = profile["teaching_readiness"]
        log.info(f"[KNOWING_VOLUNTEER] Extracted teaching_readiness: {profile['teaching_readiness']}")
    
    if signals.get("commitment_horizon") is not None and profile.get("commitment_horizon") is None:
        profile["commitment_horizon"] = signals.get("commitment_horizon")
        signals_extracted["commitment_horizon"] = profile["commitment_horizon"]
        log.info(f"[KNOWING_VOLUNTEER] Extracted commitment_horizon: {profile['commitment_horizon']}")
    
    if signals.get("language") is not None and profile.get("language") is None:
        profile["language"] = signals.get("language")
        signals_extracted["language"] = profile["language"]
        log.info(f"[KNOWING_VOLUNTEER] Extracted language: {profile['language']}")
    
    if signals.get("language_comfort") is not None and profile.get("language_comfort") is None:
        profile["language_comfort"] = signals.get("language_comfort")
        signals_extracted["language_comfort"] = profile["language_comfort"]
        log.info(f"[KNOWING_VOLUNTEER] Extracted language_comfort: {profile['language_comfort']}")

        # If language is already known from preferences, persist it when comfort is captured
        if preferred_language and profile.get("language") is None:
            profile["language"] = preferred_language
            signals_extracted["language"] = profile["language"]
            log.info(f"[KNOWING_VOLUNTEER] Set language from preferences: {profile['language']}")
    
    # If we successfully extracted the expected_target with high confidence, reset low_conf_streak
    high_conf_success = False
    if expected_target and confidence >= LOW_CONF_THRESHOLD:
        if expected_target in signals_extracted:
            high_conf_success = True
        elif expected_target == "language" and "language_comfort" in signals_extracted:
            high_conf_success = True
    if force_commit_target and expected_target:
        if expected_target in signals_extracted or (expected_target == "language" and "language_comfort" in signals_extracted):
            high_conf_success = True
    
    if high_conf_success:
        if low_conf_streak != 0:
            log.info(f"[KNOWING_VOLUNTEER] High-confidence extraction for {expected_target}, resetting low_conf_streak")
        low_conf_streak = 0
        session["tool_state"]["selection"]["low_conf_streak"] = 0
    
    # Mark fields as discussed based on intent (even if signal extraction failed)
    discussed_fields = session["tool_state"]["selection"].get("discussed_fields", set())
    if not isinstance(discussed_fields, set):
        discussed_fields = set(discussed_fields) if discussed_fields else set()
    
    # Map intent to field and mark as discussed
    field_from_intent = INTENT_TO_FIELD_MAP.get(intent)
    if field_from_intent:
        # If we are in clarification_for_target mode for this same rubric, do NOT mark it
        # as discussed yet. We want the follow-up confirmation before treating it as covered.
        if not (clarification_for_target and field_from_intent == expected_target):
            discussed_fields.add(field_from_intent)
            log.info(f"[KNOWING_VOLUNTEER] Marked '{field_from_intent}' as discussed based on intent '{intent}'")
    
    # Also mark fields as discussed if signal was extracted (even if null/negative)
    for field_name in signals_extracted.keys():
        # Special case: language_comfort extraction should mark "language" rubric as discussed
        if field_name == "language_comfort":
            discussed_fields.add("language")
            log.info(f"[KNOWING_VOLUNTEER] Marked 'language' as discussed (language_comfort signal extracted)")
        else:
            discussed_fields.add(field_name)
            log.info(f"[KNOWING_VOLUNTEER] Marked '{field_name}' as discussed (signal extracted)")
    
    # Log if expected signal was not extracted
    if "expected_target" in session.get("tool_state", {}).get("selection", {}):
        expected_target = session["tool_state"]["selection"]["expected_target"]
        if expected_target and expected_target not in signals_extracted:
            # Still mark as discussed if intent matches
            if field_from_intent == expected_target:
                log.info(f"[KNOWING_VOLUNTEER] Expected signal '{expected_target}' not extracted, but intent matches - marked as discussed")
            else:
                log.warning(f"[KNOWING_VOLUNTEER] Expected signal '{expected_target}' was not extracted. Intent: {intent}, LLM signals: {signals}")
    
    # Update profile and discussed_fields in session
    session["tool_state"]["selection"]["profile"] = profile
    session["tool_state"]["selection"]["discussed_fields"] = discussed_fields
    
    # Recalculate next_target AFTER merging signals and updating discussed_fields
    next_target_after_merge = _get_next_missing_rubric(profile, discussed_fields)
    collected_after = {k: v for k, v in profile.items() if v is not None}
    log.info(f"[KNOWING_VOLUNTEER] After merging: next_target={next_target_after_merge}, collected={list(collected_after.keys())}, discussed={list(discussed_fields)}, low_conf_streak={low_conf_streak}")
    
    # Increment question_index
    if "question_index" not in session["tool_state"]["selection"]:
        session["tool_state"]["selection"]["question_index"] = 0
    session["tool_state"]["selection"]["question_index"] += 1
    question_index = session["tool_state"]["selection"]["question_index"]
    
    # Hard cap on number of questions to prevent long loops
    MAX_KNOWING_VOLUNTEER_QUESTIONS = 6
    if question_index >= MAX_KNOWING_VOLUNTEER_QUESTIONS:
        log.info(f"[KNOWING_VOLUNTEER] Max questions reached ({question_index}), stopping")
        decision = KnowingVolunteerResult.COMPLETE_INSUFFICIENT_INFO
        tone_reply = "Thanks for sharing — I have enough to suggest a next step."
    # Low-confidence escape hatch: if we have repeatedly low confidence, stop probing
    elif low_conf_streak >= 2:
        log.info("[KNOWING_VOLUNTEER] Low confidence streak >= 2, stopping with COMPLETE_INSUFFICIENT_INFO")
        decision = KnowingVolunteerResult.COMPLETE_INSUFFICIENT_INFO
        # Graceful closing (this will be sent instead of another probing question)
        tone_reply = "That’s perfectly okay — I have enough to suggest a next step 😊"
    else:
        # Compute decision (pass user_text for deferral detection)
        decision = evaluate_knowing_volunteer(intent, question_index, profile, user_text=user_text)
    
    # Ensure tone_reply has a question when CONTINUE (minimal safety check)
    if decision == KnowingVolunteerResult.CONTINUE and tone_reply:
        # Use the recalculated next_target (after signal merging) for logging only
        next_target_for_question = next_target_after_merge
        
        # Minimal check: if no question mark, log warning but trust LLM
        if "?" not in tone_reply:
            log.warning(f"[KNOWING_VOLUNTEER] LLM tone_reply has no question mark. Expected target: {next_target_for_question}. tone_reply: {tone_reply[:100]}")
            # Trust LLM - don't force a question, but log for monitoring
        else:
            log.info(f"[KNOWING_VOLUNTEER] LLM generated question for next_target={next_target_for_question}: {tone_reply[:100]}")
    
    return {
        "intent": intent,
        "confidence": confidence,
        "assistant_text": tone_reply,
        "signals": profile.copy(),  # Return merged profile
        "decision": decision.value
    }

