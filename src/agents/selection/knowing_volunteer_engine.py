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
KNOWING_VOLUNTEER_PROMPT = """
You are SIA, the Sunbird SERVE volunteer onboarding guide.

Current state: KNOWING_VOLUNTEER.

Context:
- The volunteer has already completed eligibility, identity, and preference collection.
- Basic onboarding steps are complete.
- This step is to understand the volunteer as a person in a light, respectful way.
- You are NOT evaluating or filtering the volunteer at this stage.
- You are only understanding:
  1) their background (in a general, non-personal way),
  2) their motivation to volunteer, why they want to volunteer, what drew them to SERVE
  3) any prior teaching / mentoring experience (formal or informal),
  4) their comfort interacting with children or learners.
  5) the subjects or topics they are comfortable teaching
  6) the age group of the children they are comfortable interacting with
  7) interest in teaching
  
- The orchestrator controls which question was last asked in this state via `last_agent_prompt`.

Your goal:
Classify the user's latest message and produce:
- a single intent label,
- a confidence score (0.0–1.0),
- a short, warm WhatsApp-style acknowledgement ("tone_reply").

Allowed intents:
- MOTIVATION_SHARED   -> explains why they want to volunteer / help / give back /
- EXPERIENCE_SHARED   -> mentions teaching, tutoring, mentoring, training, or helping others learn
- NO_EXPERIENCE       -> explicitly states no teaching or mentoring experience
- COMFORT_SHARED      -> expresses comfort or hesitation working with children or learners
- QUERY               -> asks a question instead of answering
- AMBIGUOUS           -> vague, off-topic, or unclear response
- STOP                -> stop / unsubscribe / leave

Classification rules:
- Do NOT judge or filter based on experience; beginners are welcome.
- If the user explicitly says they have no experience -> NO_EXPERIENCE.
- Use `last_agent_prompt` to infer whether the response relates to motivation, experience, or comfort.
- If the message does not clearly map to any category -> AMBIGUOUS.
- Do NOT infer or invent information not explicitly stated.

Conversation boundaries:
- Do NOT ask personal questions (email, phone number, family, marital status, children, health, finances, etc.).
- Ask questions only around their work experience, teaching or mentoring experience, experience working with children, age group of the children they are comfortable with working , subjects they are comfortable with teaching

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
- If a signal is not mentioned, return null (or empty list for subjects).
- Allowed values:
  - has_teaching_experience: true / false / null
  - subjects: list of subjects explicitly mentioned (lowercase) or empty list
  - teaching_interest: yes / no / maybe / null
  - children_age_comfort:
      "primary"   -> ages ~5–10
      "middle"    -> ages ~11–14
      "secondary" -> ages ~15–18
      "unsure"    -> expresses uncertainty or discomfort
      null
  - motivation: null / help / serve others / empower / uplift / bring joy / happiness / give

Output ONLY valid JSON (all string values must be in double quotes):
  {
  "intent": "EXPERIENCE_SHARED",
  "confidence": 0.7,
  "tone_reply": "Great to hear about your background! What inspired you to consider teaching?",

  "signals": {
    "has_teaching_experience": true,
    "teaching_interest": "maybe",
    "motivation": "help",
    "subjects": ["math", "science"],
    "children_age_comfort": "primary"
  }
}

IMPORTANT JSON rules:
- All string values MUST be in double quotes: "yes", "no", "maybe", "help", etc.
- Boolean values: true or false (no quotes)
- null values: null (no quotes, lowercase)
- Array values: ["item1", "item2"] (strings in array must be quoted)
- If a signal is not mentioned, use null (not "null" as a string)
"""


# Volunteer profile structure
def init_volunteer_profile() -> Dict:
    """Initialize empty volunteer profile structure"""
    return {
        "motivation": None,
        "has_teaching_experience": None,
        "children_age_comfort": None,
        "teaching_interest": None,
        "subjects": []
    }


def knowing_volunteer_complete(profile: Dict) -> bool:
    """
    Check if knowing volunteer is complete.
    
    Returns True if >=4 out of 5 signals are present (subjects counts if non-empty).
    
    Args:
        profile: Volunteer profile dict
    
    Returns:
        bool: True if complete, False otherwise
    """
    signals = [
        profile.get("motivation"),
        profile.get("has_teaching_experience"),
        profile.get("children_age_comfort"),
        profile.get("subjects"),  # List - counts if non-empty
        profile.get("teaching_interest")
    ]
    
    # Count non-null/non-empty signals
    count = 0
    for signal in signals:
        if signal is not None:
            if isinstance(signal, list):
                if len(signal) > 0:  # Non-empty list counts
                    count += 1
            else:
                count += 1
    
    # Complete if >= 4 out of 5 signals present
    return count >= 4


def evaluate_knowing_volunteer(
    intent: str,
    question_index: int,
    profile: Dict,
    max_questions: int = 20,
    min_questions: int = 5
) -> KnowingVolunteerResult:
    """
    Decide flow outcome for KNOWING_VOLUNTEER.
    
    Args:
        intent: Intent from LLM classification
        question_index: Current question index (0-based)
        profile: Volunteer profile dict
        max_questions: Maximum questions to ask
        min_questions: Minimum questions before allowing completion
    
    Returns:
        KnowingVolunteerResult enum value
    """
    # 1️⃣ Explicit stop
    if intent == "STOP":
        return KnowingVolunteerResult.STOP
    
    # 2️⃣ If profile is sufficiently filled
    if knowing_volunteer_complete(profile) and question_index >= min_questions - 1:
        return KnowingVolunteerResult.COMPLETE
    
    # 3️⃣ If we've explored enough, move forward gracefully
    if question_index >= max_questions - 1:
        return KnowingVolunteerResult.COMPLETE_INSUFFICIENT_INFO
    
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
    
    # Build messages
    messages = [
        {"role": "system", "content": MASTER_SYSTEM_PROMPT},
        {"role": "system", "content": KNOWING_VOLUNTEER_PROMPT}
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
                    "has_teaching_experience": {"type": ["boolean", "null"]},
                    "teaching_interest": {"type": ["string", "null"]},
                    "motivation": {"type": ["string", "null"]},
                    "subjects": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "children_age_comfort": {"type": ["string", "null"]}
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
        # Replace unquoted maybe/yes/no in teaching_interest field
        repaired_text = re.sub(
            r'"teaching_interest"\s*:\s*(maybe|yes|no)(?=\s*[,}])',
            r'"teaching_interest": "\1"',
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
        # Replace unquoted string values in children_age_comfort field
        repaired_text = re.sub(
            r'"children_age_comfort"\s*:\s*(primary|middle|secondary|unsure)(?=\s*[,}])',
            r'"children_age_comfort": "\1"',
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
    
    # Initialize tool_state.selection if not exists
    if "tool_state" not in session:
        session["tool_state"] = {}
    if "selection" not in session["tool_state"]:
        session["tool_state"]["selection"] = {}
    if "profile" not in session["tool_state"]["selection"]:
        session["tool_state"]["selection"]["profile"] = init_volunteer_profile()
    
    # Get current profile
    profile = session["tool_state"]["selection"]["profile"]
    
    # Merge signals into profile (only set non-null values)
    if signals.get("motivation") is not None:
        profile["motivation"] = signals.get("motivation")
    
    if signals.get("has_teaching_experience") is not None and profile.get("has_teaching_experience") is None:
        profile["has_teaching_experience"] = signals.get("has_teaching_experience")
    
    if signals.get("subjects"):
        # Append subjects and dedupe
        existing_subjects = profile.get("subjects", [])
        new_subjects = [s.lower().strip() for s in signals.get("subjects", [])]
        combined = list(set(existing_subjects + new_subjects))
        profile["subjects"] = combined
    
    if signals.get("teaching_interest") is not None and profile.get("teaching_interest") is None:
        profile["teaching_interest"] = signals.get("teaching_interest")
    
    if signals.get("children_age_comfort") is not None and profile.get("children_age_comfort") is None:
        profile["children_age_comfort"] = signals.get("children_age_comfort")
    
    # Update profile in session
    session["tool_state"]["selection"]["profile"] = profile
    
    # Increment question_index
    if "question_index" not in session["tool_state"]["selection"]:
        session["tool_state"]["selection"]["question_index"] = 0
    session["tool_state"]["selection"]["question_index"] += 1
    question_index = session["tool_state"]["selection"]["question_index"]
    
    # Compute decision
    decision = evaluate_knowing_volunteer(intent, question_index, profile)
    
    # Ensure tone_reply has a question when CONTINUE (no dead air)
    if decision == KnowingVolunteerResult.CONTINUE and tone_reply:
        # Check if tone_reply contains a question
        if "?" not in tone_reply:
            # Generate fallback question based on missing signals
            missing_questions = []
            if not profile.get("motivation"):
                missing_questions.append("What inspired you to consider volunteering?")
            elif profile.get("has_teaching_experience") is None:
                missing_questions.append("Do you have any experience teaching or mentoring?")
            elif not profile.get("teaching_interest"):
                missing_questions.append("How do you feel about teaching?")
            elif not profile.get("children_age_comfort"):
                missing_questions.append("What age group of children are you comfortable working with?")
            elif not profile.get("subjects") or len(profile.get("subjects", [])) == 0:
                missing_questions.append("What subjects or topics are you comfortable teaching?")
            
            # Append first missing question if available
            if missing_questions:
                tone_reply = f"{tone_reply} {missing_questions[0]}"
            else:
                # Generic fallback
                tone_reply = f"{tone_reply} Could you tell me a bit more about yourself?"
    
    return {
        "intent": intent,
        "confidence": confidence,
        "assistant_text": tone_reply,
        "signals": profile.copy(),  # Return merged profile
        "decision": decision.value
    }

