"""
ELIGIBILITY State Handler (State 3: Eligibility Check)
Assume-Confirm-Interrupt pattern: single compact prompt, interrupt-aware handling
Strict: if any requirement not met → community exit
"""
import logging
import time
import re
from typing import Dict, Any, Optional, Tuple
from ..messages import (
    ELIGIBILITY_PROMPT, ELIGIBILITY_EXIT,
    ELIGIBILITY_BUTTONS,
    ELIGIBILITY_TELL_ME_MORE_MSG,
    ELIGIBILITY_ISSUE_SELECTION_MSG, ELIGIBILITY_ISSUE_SELECTION_BUTTONS,
    ELIGIBILITY_ISSUE_AGE_PROMPT, ELIGIBILITY_ISSUE_AGE_BUTTONS,
    ELIGIBILITY_ISSUE_DEVICE_PROMPT, ELIGIBILITY_ISSUE_DEVICE_BUTTONS,
    ELIGIBILITY_ISSUE_TIME_PROMPT, ELIGIBILITY_ISSUE_TIME_BUTTONS,
    ELIGIBILITY_ISSUE_UNPAID_PROMPT, ELIGIBILITY_ISSUE_UNPAID_BUTTONS,
    ELIGIBILITY_ISSUE_OTHER_PROMPT,
    # Legacy (kept for backward compatibility)
    ELIGIBILITY_CLARIFY_AGE_PROMPT, ELIGIBILITY_CLARIFY_AGE_BUTTONS,
    ELIGIBILITY_CLARIFY_DEVICE_PROMPT, ELIGIBILITY_CLARIFY_DEVICE_BUTTONS,
    ELIGIBILITY_CLARIFY_UNPAID_PROMPT, ELIGIBILITY_CLARIFY_UNPAID_BUTTONS
)
from ..validators import is_yes_response, is_no_response

log = logging.getLogger(__name__)

# Legacy clarification messages (kept for backward compatibility if needed)
ELIGIBILITY_CLARIFY_AGE = """Just to confirm — are you 18 or above? 🙂"""
ELIGIBILITY_CLARIFY_DEVICE = """Do you have a tablet or laptop with a reasonably stable internet connection? 🙂"""
ELIGIBILITY_CLARIFY_UNPAID = """And are you okay with this being a voluntary (unpaid) role? 🙂"""

# FAQ answers for common questions
ELIGIBILITY_FAQ_AGE = """Yes, 18+ is required for classroom volunteering. This is a policy requirement.

So just to confirm — are all three okay for you? (18+, tablet/laptop+internet, unpaid role)
You can simply reply with Yes or No 🙂"""
ELIGIBILITY_FAQ_DEVICE = """A tablet or laptop with stable internet is needed for live online classes. (Smartphones/phones are not suitable for this program.)

So just to confirm — are all three okay for you? (18+, tablet/laptop+internet, unpaid role)
You can simply reply with Yes or No 🙂"""
ELIGIBILITY_FAQ_UNPAID = """Yes, this is a volunteer role with no payment.

So just to confirm — are all three okay for you? (18+, tablet/laptop+internet, unpaid role)
You can simply reply with Yes or No 🙂"""


def detect_button_click(text: str) -> Optional[str]:
    """
    Detect if user clicked a button by matching button label text.
    
    Returns:
        Button ID string (e.g., "YES_WORKS", "TELL_ME_MORE", "ISSUE_AGE", etc.)
        None if not a button click
    """
    text_lower = text.lower().strip()
    
    # Main eligibility buttons
    if text_lower in ["yes, this works", "yes this works", "yes", "y", "works", "this works"]:
        return "YES_WORKS"
    if text_lower in ["tell me more", "tell me", "more", "info", "information"]:
        return "TELL_ME_MORE"
    if text_lower in ["something won't work", "something wont work", "something won't", "wont work", "something wrong"]:
        return "SOMETHING_WONT_WORK"
    
    # Issue selection buttons
    if text_lower in ["age", "18", "eighteen"]:
        return "ISSUE_AGE"
    if text_lower in ["device", "laptop", "tablet", "phone", "smartphone"]:
        return "ISSUE_DEVICE"
    if text_lower in ["time", "hours", "2 hours", "commitment", "weekly"]:
        return "ISSUE_TIME"
    if text_lower in ["unpaid", "payment", "paid", "volunteer", "voluntary"]:
        return "ISSUE_UNPAID"
    if text_lower in ["other", "something else", "different"]:
        return "ISSUE_OTHER"
    
    # Issue-specific yes/no buttons
    if text_lower in ["yes", "y", "ok", "okay", "sure", "fine", "works", "good"]:
        return "YES"
    if text_lower in ["no", "n", "not", "can't", "cannot", "won't", "wont"]:
        return "NO"
    
    return None


def classify_eligibility_rule_based(text: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Rule-based classification for ELIGIBILITY state (NO LLM).
    Interrupt-aware: detects partial responses that mention specific constraints.
    
    Returns:
        (intent: Optional[str], missing_requirement: Optional[str])
        intent can be: ELIGIBLE_YES, ELIGIBLE_NO, QUERY, ELIGIBLE_UNCLEAR, or None (ambiguous)
        missing_requirement can be: "age", "device", "unpaid", "commitment", or None
    """
    text_lower = text.lower().strip()
    words = text_lower.split()
    
    # Check for QUERY first (questions)
    if "?" in text or re.search(r"^(what|how|when|why|where|who|which|can|could|do|does|is|are)\b", text, re.I):
        # Check if it's a question about eligibility requirements
        if any(term in text_lower for term in ["18", "age", "laptop", "phone", "internet", "device", "paid", "payment", "unpaid", "volunteer", "compulsory", "required", "need", "hour", "time", "commitment"]):
            return ("QUERY", None)
    
    # Check for simple "no" response - generic decline (exit immediately)
    if is_no_response(text):
        return ("ELIGIBLE_NO", None)
    
    # Check for ELIGIBLE_NO - Age violations
    age_violations = [
        r"\b(i'?m|i am|im)\s*(17|sixteen|15|fifteen|14|fourteen|13|thirteen|12|twelve)\b",
        r"\b(17|sixteen|15|fifteen|14|fourteen|13|thirteen|12|twelve)\s*(years? old|yr|yrs)\b",
        r"\b(under|below|less than)\s*(18|eighteen)\b",
        r"\b(not 18|not eighteen|not yet 18)\b",
        r"\b(17|sixteen|15|fifteen|14|fourteen|13|thirteen|12|twelve)\b",  # Just a number < 18
    ]
    for pattern in age_violations:
        if re.search(pattern, text_lower):
            return ("ELIGIBLE_NO", "age")
    
    # Check for ELIGIBLE_NO - Phone/Smartphone only (not acceptable)
    phone_only_patterns = [
        r"\b(only|just)\s+(phone|smartphone|mobile)\b",
        r"\b(phone|smartphone|mobile)\s+only\b",
        r"\b(have|got|own)\s+(phone|smartphone|mobile)\s+(but|,)\s*(no|don'?t have|dont have)\s*(laptop|tablet)\b",
        r"\b(have|got|own)\s+(phone|smartphone|mobile)\s+(only|just)\b",
        r"\b(no|don'?t have|dont have)\s*(laptop|tablet)\s*(but|,)\s*(have|got|own)\s*(phone|smartphone|mobile)\b",
    ]
    for pattern in phone_only_patterns:
        if re.search(pattern, text_lower):
            return ("ELIGIBLE_NO", "device")
    
    # Check for ELIGIBLE_NO - Device/Internet violations
    device_violations = [
        r"\b(no (laptop|tablet|device|internet|wifi|internet connection))\b",
        r"\b(don'?t have|dont have|don't have|do not have)\s*(laptop|tablet|device|internet|wifi)\b",
        r"\b(no access to|without)\s*(internet|wifi|laptop|tablet|device)\b",
        r"\b(laptop|tablet|device|internet|wifi)\s*(is|are)?\s*(broken|not working|not available|unavailable)\b",
    ]
    for pattern in device_violations:
        if re.search(pattern, text_lower):
            return ("ELIGIBLE_NO", "device")
    
    # Check for ELIGIBLE_NO - Commitment violations (insufficient time)
    commitment_violations = [
        r"\b(cannot|can't|cant|not possible|unable|won't be able|wont be able)\s*(give|do|commit|spend)\s*(2|two)\s*(hour|hr|hours)\b",
        r"\b(only|just)\s*\d+\s*(hour|hr|hours?)\s*(a|per|every)\s*(week|month)\b",  # "only 1 hour a week"
        r"\b(less than|below|under)\s*(2|two)\s*(hour|hr|hours)\b",
        r"\b(can't|cannot|cant)\s*(give|do|commit)\s*(2|two)\s*(hour|hr|hours)\b",
    ]
    for pattern in commitment_violations:
        if re.search(pattern, text_lower):
            return ("ELIGIBLE_NO", "commitment")
    
    # Check for numeric commitment violations
    hour_match = re.search(r"(\d+)\s*(hour|hr|hours)", text_lower)
    if hour_match:
        hours = int(hour_match.group(1))
        if hours < 2 and any(term in text_lower for term in ["only", "just", "can", "give", "do"]):
            return ("ELIGIBLE_NO", "commitment")
    
    # Check for ELIGIBLE_NO - Unpaid requirement violations
    unpaid_violations = [
        r"\b(need|want|require|expect|hoping for|looking for)\s*(payment|paid|money|stipend|salary|compensation|remuneration)\b",
        r"\b(is|is it|is this)\s*(paid|a paid|compensated|remunerated)\b",
        r"\b(i thought|i think|i assumed)\s*(it'?s|its|this is|it is)\s*(paid|a paid)\b",
        r"\b(not comfortable|not okay|not ok|not fine)\s*(with|about)\s*(unpaid|volunteer|no payment|no pay)\b",
        r"\b(can'?t|cannot|cant)\s*(do|volunteer|teach)\s*(unpaid|without payment|for free)\b",
        r"\b(only if|only when|if|when)\s*(paid|there'?s payment|i get paid|i'?m paid)\b",
    ]
    for pattern in unpaid_violations:
        if re.search(pattern, text_lower):
            return ("ELIGIBLE_NO", "unpaid")
    
    # Check for ELIGIBLE_YES - Clear confirmation
    if is_yes_response(text):
        # Check if it mentions all or confirms all
        if any(phrase in text_lower for phrase in ["all", "all ok", "all okay", "all good", "all fine", "all confirmed", "works for me", "that's fine", "thats fine"]):
            return ("ELIGIBLE_YES", None)
        # Simple "yes" after the prompt - assume all requirements met
        return ("ELIGIBLE_YES", None)
    
    # Positive keywords that suggest all requirements are okay
    yes_patterns = [
        r"\b(yes|ok|okay|sure|definitely|absolutely|of course|sounds good|fine|works|good)\b",
        r"\b(all|everything|each|both)\s*(ok|okay|good|fine|works|confirmed)\b",
        r"\b(i'?m|i am|im)\s*\d+\s*(and|&)\s*(have|got)\s*(internet|laptop|tablet|device)\b",  # "I'm 25 and have internet/laptop"
        r"\b(yes|ok)\s*(i|i'?m|i am|im)\s*(understand|understood|get it|got it)\b",  # "Yes I understand"
    ]
    for pattern in yes_patterns:
        if re.search(pattern, text_lower):
            # Make sure it's not in a negative context
            if not re.search(r"\b(but|however|although|though|except|not)\b", text_lower):
                return ("ELIGIBLE_YES", None)
    
    # Check for ELIGIBLE_UNCLEAR - Partial interrupts (interrupt-aware detection)
    # Patterns like "Everything ok except device", "I can do 1 hour", "Laptop yes, unpaid not sure"
    interrupt_patterns = [
        r"\b(everything|all|all of them|most)\s*(ok|okay|good|fine|works|fine)\s*(except|but|however|except for|other than)\b",
        r"\b(except|but|however|except for|other than)\s*(device|laptop|tablet|internet|age|18|unpaid|payment|paid|commitment|hours?|time)\b",
        r"\b(can do|can give|can commit)\s*\d+\s*(hour|hr|hours?)\b",  # "I can do 1 hour"
        r"\b(only|just)\s*\d+\s*(hour|hr|hours?)\b",  # "only 1 hour" (but not already caught as COMMIT_NO)
    ]
    for pattern in interrupt_patterns:
        if re.search(pattern, text_lower):
            # Determine which requirement is the issue
            if any(term in text_lower for term in ["device", "laptop", "tablet", "internet", "wifi", "phone", "smartphone"]):
                return ("ELIGIBLE_UNCLEAR", "device")
            elif any(term in text_lower for term in ["age", "18", "17", "sixteen", "under 18"]):
                return ("ELIGIBLE_UNCLEAR", "age")
            elif any(term in text_lower for term in ["unpaid", "paid", "payment", "volunteer", "voluntary"]):
                return ("ELIGIBLE_UNCLEAR", "unpaid")
            elif any(term in text_lower for term in ["hour", "hours", "time", "commitment", "week"]):
                return ("ELIGIBLE_UNCLEAR", "commitment")
            else:
                return ("ELIGIBLE_UNCLEAR", None)
    
    # Check for mixed positive/negative signals (interrupt pattern)
    # "Laptop yes, unpaid not sure" or "Everything ok except..."
    mixed_interrupt = re.search(r"\b(yes|ok|okay|good|fine|works|have|got)\s*(,|but|however|except|except for)\s*(no|not|don't|dont|can't|cant|cannot|unpaid|device|age|18|hours?|time)\b", text_lower)
    if mixed_interrupt:
        # Determine which requirement is the issue
        if any(term in text_lower for term in ["device", "laptop", "tablet", "internet", "wifi", "phone", "smartphone"]):
            return ("ELIGIBLE_UNCLEAR", "device")
        elif any(term in text_lower for term in ["age", "18", "17", "sixteen"]):
            return ("ELIGIBLE_UNCLEAR", "age")
        elif any(term in text_lower for term in ["unpaid", "paid", "payment", "volunteer"]):
            return ("ELIGIBLE_UNCLEAR", "unpaid")
        elif any(term in text_lower for term in ["hour", "hours", "time", "commitment"]):
            return ("ELIGIBLE_UNCLEAR", "commitment")
    
    # Check for uncertainty indicators
    age_mentioned = re.search(r"\b(18|eighteen|\d+)\s*(years?|yr|yrs|or above|or older)\b", text_lower)
    device_mentioned = any(term in text_lower for term in ["laptop", "tablet", "device", "internet", "wifi"])
    unpaid_mentioned = any(term in text_lower for term in ["unpaid", "volunteer", "voluntary", "no payment", "no pay", "free"])
    commitment_mentioned = any(term in text_lower for term in ["hour", "hours", "time", "week", "commitment", "2 hours", "two hours"])
    
    uncertainty_patterns = [
        r"\b(18 soon|turning 18|almost 18|will be 18)\b",
        r"\b(sometimes|occasionally|not always|not stable|patchy|unstable)\s*(internet|wifi|connection)\b",
        r"\b(not sure|unsure|not certain|maybe|perhaps|might be)\s*(about|with|regarding)\s*(unpaid|payment|volunteer|device|age|hours?|time)\b",
        r"\b(depends|depends on)\s*(payment|paid|stipend|device|internet|hours?|time)\b",
    ]
    for pattern in uncertainty_patterns:
        if re.search(pattern, text_lower):
            # Determine which requirement is unclear
            if "18" in text_lower or "age" in text_lower:
                return ("ELIGIBLE_UNCLEAR", "age")
            elif "internet" in text_lower or "wifi" in text_lower or "device" in text_lower or "laptop" in text_lower or "tablet" in text_lower:
                return ("ELIGIBLE_UNCLEAR", "device")
            elif "paid" in text_lower or "payment" in text_lower or "unpaid" in text_lower or "volunteer" in text_lower:
                return ("ELIGIBLE_UNCLEAR", "unpaid")
            elif "hour" in text_lower or "time" in text_lower or "commitment" in text_lower:
                return ("ELIGIBLE_UNCLEAR", "commitment")
            else:
                return ("ELIGIBLE_UNCLEAR", None)
    
    # Partial information - only some requirements mentioned (interrupt pattern)
    requirements_mentioned = sum([bool(age_mentioned), bool(device_mentioned), bool(unpaid_mentioned), bool(commitment_mentioned)])
    if requirements_mentioned > 0 and requirements_mentioned < 4:
        # Determine which is missing or unclear
        if not age_mentioned and any(term in text_lower for term in ["device", "laptop", "unpaid", "hour", "time"]):
            return ("ELIGIBLE_UNCLEAR", "age")
        elif not device_mentioned and any(term in text_lower for term in ["age", "18", "unpaid", "hour", "time"]):
            return ("ELIGIBLE_UNCLEAR", "device")
        elif not unpaid_mentioned and any(term in text_lower for term in ["age", "18", "device", "laptop", "hour", "time"]):
            return ("ELIGIBLE_UNCLEAR", "unpaid")
        elif not commitment_mentioned and any(term in text_lower for term in ["age", "18", "device", "laptop", "unpaid"]):
            return ("ELIGIBLE_UNCLEAR", "commitment")
    
    # Vague positive response without clear confirmation
    if any(word in text_lower for word in ["yes", "ok", "okay", "sure", "fine", "good"]):
        if len(words) <= 3:  # Very short response
            return ("ELIGIBLE_UNCLEAR", None)
    
    # Default: ambiguous (will need LLM)
    return (None, None)


def should_use_llm(text: str, rule_intent: Optional[str], missing_req: Optional[str]) -> bool:
    """
    Determine if LLM should be invoked based on message characteristics.
    
    Returns True if:
    - Rule-based classifier returned None (ambiguous)
    - Message is long and contains mixed signals
    - User message mixes question + partial answer
    - Uncertainty detected that rules couldn't handle
    """
    words = text.split()
    word_count = len(words)
    text_lower = text.lower()
    
    # Don't use LLM for simple questions - use FAQ instead
    if rule_intent == "QUERY":
        return False
    
    # If rule-based returned nothing, use LLM
    if rule_intent is None:
        return True
    
    # If ELIGIBLE_UNCLEAR but missing_req is None, might need LLM for complex cases
    if rule_intent == "ELIGIBLE_UNCLEAR" and missing_req is None and word_count > 10:
        return True
    
    # Long messages (>12-15 words) with mixed signals
    if word_count > 12:
        # Check for mixed signals: "yes but", "have but", etc.
        mixed_signals = [
            r"\b(yes|ok|sure|have|got)\s+but\b",
            r"\b(however|although|though|but)\b",
            r"\b(can|will|able)\s+but\s+(not|only|just|need)\b",
        ]
        for pattern in mixed_signals:
            if re.search(pattern, text_lower):
                return True
    
    # Message contains both question and partial answer
    has_question = "?" in text
    has_answer = any(word in text_lower for word in ["yes", "no", "ok", "sure", "have", "18", "laptop", "tablet"])
    if has_question and has_answer and word_count > 8:
        return True
    
    # Complex uncertainty cases
    complex_uncertainty = [
        r"\b(17|sixteen)\s+but\s+will\s+turn\s+18\b",
        r"\b(tablet|laptop)\s+but\s+internet\s+(is|'?s)\s+(patchy|unstable|not stable)\b",
        r"\b(can|will)\s+volunteer\s+but\s+(was|am)\s+hoping\s+(for|there'?s)\s+(stipend|payment)\b",
    ]
    for pattern in complex_uncertainty:
        if re.search(pattern, text_lower):
            return True
    
    # Don't use LLM for simple responses
    if word_count <= 5 and rule_intent in ["ELIGIBLE_YES", "ELIGIBLE_NO", "QUERY"]:
        return False
    
    return False


def get_clarification_message(missing_requirement: Optional[str]) -> Tuple[str, list[str]]:
    """
    Get the appropriate clarification message and buttons based on missing requirement.
    Interrupt-aware: asks ONE targeted clarification.
    
    Returns:
        (message: str, buttons: list[str])
    """
    if missing_requirement == "age":
        return (ELIGIBILITY_CLARIFY_AGE_PROMPT, ELIGIBILITY_CLARIFY_AGE_BUTTONS)
    elif missing_requirement == "device":
        # Interrupt-aware: acknowledge and ask specifically
        msg = "Thanks for telling me 🙂 Just to confirm — do you have access to a laptop or tablet for sessions?"
        return (msg, ELIGIBILITY_CLARIFY_DEVICE_BUTTONS)
    elif missing_requirement == "unpaid":
        return (ELIGIBILITY_CLARIFY_UNPAID_PROMPT, ELIGIBILITY_CLARIFY_UNPAID_BUTTONS)
    elif missing_requirement == "commitment":
        # Interrupt-aware: acknowledge and ask specifically about time
        msg = "Thanks for telling me 🙂 Just to confirm — would you be comfortable teaching around 2 hours a week?"
        return (msg, ["Yes", "No"])
    else:
        # Generic clarification - re-ask the main prompt
        return (ELIGIBILITY_PROMPT, ELIGIBILITY_BUTTONS)


def get_faq_answer(text: str) -> Optional[str]:
    """
    Get FAQ answer for common questions about eligibility requirements.
    """
    text_lower = text.lower()
    
    # Age-related questions
    if any(term in text_lower for term in ["18", "age", "older", "above", "compulsory", "required"]):
        return ELIGIBILITY_FAQ_AGE
    
    # Device/internet questions
    if any(term in text_lower for term in ["laptop", "phone", "tablet", "device", "internet", "wifi", "mobile", "smartphone"]):
        return ELIGIBILITY_FAQ_DEVICE
    
    # Unpaid/volunteer questions
    if any(term in text_lower for term in ["paid", "payment", "unpaid", "volunteer", "voluntary", "stipend", "money", "compensation"]):
        return ELIGIBILITY_FAQ_UNPAID
    
    return None


async def handle_eligibility(phone: str, text: str, sess: Dict[str, Any], profile: Dict[str, Any]) -> None:
    """
    Handle ELIGIBILITY state - single compact prompt with interrupt-aware handling.
    
    Flow:
    1. Single eligibility prompt (assumes readiness, lists all requirements including commitment)
    2. Clear YES → proceed to IDENTITY
    3. Explicit NO/Constraint → immediate exit
    4. Partial interrupt → ONE targeted clarification
    5. Query → answer briefly, re-ask
    6. Ambiguous → rule-based + LLM fallback
    7. Re-entry handling from REJECTED state
    """
    # Late import to avoid circular dependency
    from ..wa_loop import (
        mcp_wa_send, _add_to_history, _handle, SESSIONS,
        mcp_llm_classify_intent, build_llm_context
    )
    
    # Handle re-entry from REJECTED state
    if sess.get("state") == "REJECTED":
        log.info(f"[ELIGIBILITY] User re-entering from REJECTED state, resetting eligibility")
        sess["state"] = "ELIGIBILITY"
        sess["_eligibility_prompted"] = False
        sess["_eligibility_clarification_sent"] = False
        sess["_eligibility_missing_req"] = None
        sess["_eligibility_clarification_step"] = None
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
    
    # ========== SINGLE ELIGIBILITY PROMPT WITH BUTTONS ==========
    # Initial prompt - single compact message with interactive buttons
    if text == "__kick__" or not sess.get("_eligibility_prompted"):
        log.info(f"[ELIGIBILITY] Sending single eligibility prompt with buttons to {phone}")
        message_id = await mcp_wa_send(phone, ELIGIBILITY_PROMPT, buttons=ELIGIBILITY_BUTTONS)
        _add_to_history(phone, bot_msg=ELIGIBILITY_PROMPT)
        
        # Persistence: Update state and log event
        try:
            from datetime import datetime, timezone
            from storage.db import get_db_session
            from storage.session_store import update_session_state_and_tool_state
            from storage.event_logger import log_event
            from ..config import settings
            
            now_iso = datetime.now(timezone.utc).isoformat()
            with get_db_session() as db:
                session_id = sess.get("_db_session_id")
                update_session_state_and_tool_state(
                    db=db,
                    wa_phone=phone,
                    state="ONBOARDING",
                    sub_state="ELIGIBILITY",
                    last_outbound_msg_id=message_id,
                    tool_state_updates={
                        "eligibility": {
                            "prompted_at": now_iso,
                            "mode": "ALIGN"
                        }
                    }
                )
                log_event(
                    db=db,
                    wa_phone=phone,
                    agent_name=settings.AGENT_NAME,
                    event_type="ELIGIBILITY_PROMPT_SENT",
                    event_source="agent",
                    state="ONBOARDING",
                    sub_state="ELIGIBILITY",
                    status="SUCCESS",
                    details={"buttons": ELIGIBILITY_BUTTONS},
                    session_id=session_id
                )
        except Exception as e:
            log.warning(f"[ELIGIBILITY] Failed to persist prompt: {e}", exc_info=True)
        
        sess["_eligibility_prompted"] = True
        sess["_eligibility_mode"] = "ALIGN"  # Track current mode
        sess["_eligibility_clarification_sent"] = False
        sess["_eligibility_missing_req"] = None
        sess["_eligibility_clarification_step"] = None
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
    
    # Step 1: Check for button click
    button_click = detect_button_click(text)
    eligibility_mode = sess.get("_eligibility_mode", "ALIGN")
    
    if button_click:
        log.info(f"[ELIGIBILITY] Button clicked: {button_click}, mode: {eligibility_mode}")
        
        # ========== MAIN ALIGNMENT BUTTONS ==========
        if button_click == "YES_WORKS":
            # User confirmed all requirements - proceed to IDENTITY
            log.info(f"[ELIGIBILITY] User confirmed all requirements, proceeding to IDENTITY")
            
            # Persistence: Store eligibility response
            try:
                from datetime import datetime, timezone
                from storage.db import get_db_session
                from storage.session_store import update_session_state_and_tool_state
                from storage.event_logger import log_event
                from ..config import settings
                
                now_iso = datetime.now(timezone.utc).isoformat()
                with get_db_session() as db:
                    session_id = sess.get("_db_session_id")
                    # Read existing eligibility from tool_state
                    from sqlalchemy import select
                    from storage.tables import serve_agent_sessions
                    stmt = select(serve_agent_sessions.c.tool_state).where(
                        serve_agent_sessions.c.wa_phone == phone
                    )
                    result = db.execute(stmt).first()
                    existing_eligibility = {}
                    if result and result[0] and isinstance(result[0], dict):
                        existing_eligibility = result[0].get("eligibility", {})
                    
                    eligibility_update = existing_eligibility.copy()
                    eligibility_update.update({
                        "response": "yes",
                        "passed": True,
                        "q1_commitment": True,
                        "q2_age": True,
                        "q3_device": True,
                        "responded_at": now_iso
                    })
                    
                    update_session_state_and_tool_state(
                        db=db,
                        wa_phone=phone,
                        state="ONBOARDING",
                        sub_state="IDENTITY",
                        tool_state_updates={"eligibility": eligibility_update}
                    )
                    log_event(
                        db=db,
                        wa_phone=phone,
                        agent_name=settings.AGENT_NAME,
                        event_type="ELIGIBILITY_RESPONSE",
                        event_source="user",
                        state="ONBOARDING",
                        sub_state="ELIGIBILITY",
                        status="SUCCESS",
                        details={"response": "yes", "passed": True, "raw_text": text},
                        session_id=session_id
                    )
            except Exception as e:
                log.warning(f"[ELIGIBILITY] Failed to persist response: {e}", exc_info=True)
            
            profile.setdefault("eligibility", {})["passed"] = True
            sess["profile"] = profile
            sess["state"] = "IDENTITY"
            sess["_eligibility_mode"] = None
            sess["_eligibility_clarification_sent"] = False
            sess["_eligibility_missing_req"] = None
            sess["_eligibility_clarification_step"] = None
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            await _handle(phone, "__kick__")
            return
        
        elif button_click == "TELL_ME_MORE":
            # Send explanation and re-show main prompt
            log.info(f"[ELIGIBILITY] User asked for more info")
            await mcp_wa_send(phone, ELIGIBILITY_TELL_ME_MORE_MSG)
            _add_to_history(phone, bot_msg=ELIGIBILITY_TELL_ME_MORE_MSG)
            # Re-show main prompt with buttons
            await mcp_wa_send(phone, ELIGIBILITY_PROMPT, buttons=ELIGIBILITY_BUTTONS)
            _add_to_history(phone, bot_msg=ELIGIBILITY_PROMPT)
            sess["_eligibility_mode"] = "ALIGN"
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        
        elif button_click == "SOMETHING_WONT_WORK":
            # Show issue selection
            log.info(f"[ELIGIBILITY] User indicated something won't work, showing issue selection")
            await mcp_wa_send(phone, ELIGIBILITY_ISSUE_SELECTION_MSG, buttons=ELIGIBILITY_ISSUE_SELECTION_BUTTONS)
            _add_to_history(phone, bot_msg=ELIGIBILITY_ISSUE_SELECTION_MSG)
            sess["_eligibility_mode"] = "ISSUE_PICK"
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        
        # ========== ISSUE SELECTION BUTTONS ==========
        elif eligibility_mode == "ISSUE_PICK":
            if button_click == "ISSUE_AGE":
                await mcp_wa_send(phone, ELIGIBILITY_ISSUE_AGE_PROMPT, buttons=ELIGIBILITY_ISSUE_AGE_BUTTONS)
                _add_to_history(phone, bot_msg=ELIGIBILITY_ISSUE_AGE_PROMPT)
                sess["_eligibility_mode"] = "ISSUE_AGE"
                sess["_eligibility_clarification_step"] = "age"
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
            elif button_click == "ISSUE_DEVICE":
                await mcp_wa_send(phone, ELIGIBILITY_ISSUE_DEVICE_PROMPT, buttons=ELIGIBILITY_ISSUE_DEVICE_BUTTONS)
                _add_to_history(phone, bot_msg=ELIGIBILITY_ISSUE_DEVICE_PROMPT)
                sess["_eligibility_mode"] = "ISSUE_DEVICE"
                sess["_eligibility_clarification_step"] = "device"
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
            elif button_click == "ISSUE_TIME":
                await mcp_wa_send(phone, ELIGIBILITY_ISSUE_TIME_PROMPT, buttons=ELIGIBILITY_ISSUE_TIME_BUTTONS)
                _add_to_history(phone, bot_msg=ELIGIBILITY_ISSUE_TIME_PROMPT)
                sess["_eligibility_mode"] = "ISSUE_TIME"
                sess["_eligibility_clarification_step"] = "commitment"
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
            elif button_click == "ISSUE_UNPAID":
                await mcp_wa_send(phone, ELIGIBILITY_ISSUE_UNPAID_PROMPT, buttons=ELIGIBILITY_ISSUE_UNPAID_BUTTONS)
                _add_to_history(phone, bot_msg=ELIGIBILITY_ISSUE_UNPAID_PROMPT)
                sess["_eligibility_mode"] = "ISSUE_UNPAID"
                sess["_eligibility_clarification_step"] = "unpaid"
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
            elif button_click == "ISSUE_OTHER":
                await mcp_wa_send(phone, ELIGIBILITY_ISSUE_OTHER_PROMPT)
                _add_to_history(phone, bot_msg=ELIGIBILITY_ISSUE_OTHER_PROMPT)
                sess["_eligibility_mode"] = "ISSUE_OTHER"
                sess["_eligibility_clarification_step"] = "other"
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
        
        # ========== ISSUE-SPECIFIC YES/NO BUTTONS ==========
        elif eligibility_mode in ["ISSUE_AGE", "ISSUE_DEVICE", "ISSUE_TIME", "ISSUE_UNPAID"]:
            if button_click == "YES":
                # Issue resolved - re-show main prompt
                log.info(f"[ELIGIBILITY] Issue {eligibility_mode} resolved, re-showing main prompt")
                await mcp_wa_send(phone, ELIGIBILITY_PROMPT, buttons=ELIGIBILITY_BUTTONS)
                _add_to_history(phone, bot_msg=ELIGIBILITY_PROMPT)
                sess["_eligibility_mode"] = "ALIGN"
                sess["_eligibility_clarification_step"] = None
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
            elif button_click == "NO":
                # Requirement not met - exit immediately
                log.info(f"[ELIGIBILITY] Requirement {eligibility_mode} not met, exiting")
                await mcp_wa_send(phone, ELIGIBILITY_EXIT)
                _add_to_history(phone, bot_msg=ELIGIBILITY_EXIT)
                
                # Persistence: Store rejection response
                try:
                    from datetime import datetime, timezone
                    from storage.db import get_db_session
                    from storage.session_store import update_session_state_and_tool_state, finalize_onboarding
                    from storage.event_logger import log_event
                    from agents.onboarding.config import settings
                    
                    now_iso = datetime.now(timezone.utc).isoformat()
                    with get_db_session() as db:
                        session_id = sess.get("_db_session_id")
                        # Read existing eligibility from tool_state
                        from sqlalchemy import select
                        from storage.tables import serve_agent_sessions
                        stmt = select(serve_agent_sessions.c.tool_state).where(
                            serve_agent_sessions.c.wa_phone == phone
                        )
                        result = db.execute(stmt).first()
                        existing_eligibility = {}
                        if result and result[0] and isinstance(result[0], dict):
                            existing_eligibility = result[0].get("eligibility", {})
                        
                        # Map eligibility_mode to requirement
                        req_map = {
                            "ISSUE_AGE": "age",
                            "ISSUE_DEVICE": "device",
                            "ISSUE_TIME": "commitment",
                            "ISSUE_UNPAID": "unpaid"
                        }
                        failed_req = req_map.get(eligibility_mode, "unknown")
                        
                        eligibility_update = existing_eligibility.copy()
                        eligibility_update.update({
                            "response": "no",
                            "passed": False,
                            "rejection_reason": failed_req,
                            "responded_at": now_iso
                        })
                        
                        update_session_state_and_tool_state(
                            db=db,
                            wa_phone=phone,
                            state="REJECTED",
                            tool_state_updates={"eligibility": eligibility_update}
                        )
                        log_event(
                            db=db,
                            wa_phone=phone,
                            agent_name=settings.AGENT_NAME,
                            event_type="ELIGIBILITY_RESPONSE",
                            event_source="user",
                            state="ONBOARDING",
                            sub_state="ELIGIBILITY",
                            status="SUCCESS",
                            details={"response": "no", "passed": False, "failed_requirement": failed_req, "raw_text": text},
                            session_id=session_id
                        )
                        
                        finalize_onboarding(
                            db,
                            wa_phone=phone,
                            eligibility_status="REJECTED",
                            available_days=None,
                            available_time_bands=None,
                            end_reason=f"eligibility_failed_{failed_req}"
                        )
                        log_event(
                            db=db,
                            wa_phone=phone,
                            agent_name=settings.AGENT_NAME,
                            event_type="SESSION_ENDED",
                            event_source="onboarding_agent",
                            state="REJECTED",
                            status="rejected",
                            details={"reason": f"eligibility_failed_{failed_req}"},
                            session_id=session_id
                        )
                        log.info(f"[PERSISTENCE] Finalized rejected session for {phone}")
                except Exception as e:
                    log.warning(f"[PERSISTENCE] Failed to finalize rejected session for {phone}: {e}", exc_info=True)
                
                sess["state"] = "REJECTED"
                sess["_eligibility_mode"] = None
                sess["_eligibility_clarification_sent"] = False
                sess["_eligibility_missing_req"] = None
                sess["_eligibility_clarification_step"] = None
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
        
        # Handle ISSUE_OTHER: user typed free text about their issue
        elif eligibility_mode == "ISSUE_OTHER":
            # Use rule-based + LLM to classify which issue they're describing
            log.info(f"[ELIGIBILITY] Processing ISSUE_OTHER free text: {text[:50]}")
            rule_intent, missing_req = classify_eligibility_rule_based(text)
            
            # If rule-based found a specific issue, route to that handler
            if missing_req in ["age", "device", "commitment", "unpaid"]:
                # Map to issue mode and show appropriate prompt
                issue_map = {
                    "age": ("ISSUE_AGE", ELIGIBILITY_ISSUE_AGE_PROMPT, ELIGIBILITY_ISSUE_AGE_BUTTONS),
                    "device": ("ISSUE_DEVICE", ELIGIBILITY_ISSUE_DEVICE_PROMPT, ELIGIBILITY_ISSUE_DEVICE_BUTTONS),
                    "commitment": ("ISSUE_TIME", ELIGIBILITY_ISSUE_TIME_PROMPT, ELIGIBILITY_ISSUE_TIME_BUTTONS),
                    "unpaid": ("ISSUE_UNPAID", ELIGIBILITY_ISSUE_UNPAID_PROMPT, ELIGIBILITY_ISSUE_UNPAID_BUTTONS),
                }
                mode, prompt, buttons = issue_map[missing_req]
                await mcp_wa_send(phone, prompt, buttons=buttons)
                _add_to_history(phone, bot_msg=prompt)
                sess["_eligibility_mode"] = mode
                sess["_eligibility_clarification_step"] = missing_req
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
            else:
                # Use LLM to classify if rule-based didn't find a specific issue
                try:
                    llm_context = build_llm_context("ELIGIBILITY", sess, last_prompt=ELIGIBILITY_ISSUE_OTHER_PROMPT)
                    llm_result = await mcp_llm_classify_intent(text, "ELIGIBILITY", llm_context)
                    llm_intent = (llm_result.get("intent") or "").upper()
                    
                    # Map LLM intent to issue type
                    if "AGE" in llm_intent or "18" in text.lower():
                        missing_req = "age"
                    elif "DEVICE" in llm_intent or any(term in text.lower() for term in ["phone", "laptop", "tablet", "device"]):
                        missing_req = "device"
                    elif "TIME" in llm_intent or "COMMITMENT" in llm_intent or any(term in text.lower() for term in ["hour", "time", "week"]):
                        missing_req = "commitment"
                    elif "UNPAID" in llm_intent or "PAID" in llm_intent or any(term in text.lower() for term in ["payment", "paid", "unpaid", "money"]):
                        missing_req = "unpaid"
                    
                    if missing_req:
                        issue_map = {
                            "age": ("ISSUE_AGE", ELIGIBILITY_ISSUE_AGE_PROMPT, ELIGIBILITY_ISSUE_AGE_BUTTONS),
                            "device": ("ISSUE_DEVICE", ELIGIBILITY_ISSUE_DEVICE_PROMPT, ELIGIBILITY_ISSUE_DEVICE_BUTTONS),
                            "commitment": ("ISSUE_TIME", ELIGIBILITY_ISSUE_TIME_PROMPT, ELIGIBILITY_ISSUE_TIME_BUTTONS),
                            "unpaid": ("ISSUE_UNPAID", ELIGIBILITY_ISSUE_UNPAID_PROMPT, ELIGIBILITY_ISSUE_UNPAID_BUTTONS),
                        }
                        mode, prompt, buttons = issue_map[missing_req]
                        await mcp_wa_send(phone, prompt, buttons=buttons)
                        _add_to_history(phone, bot_msg=prompt)
                        sess["_eligibility_mode"] = mode
                        sess["_eligibility_clarification_step"] = missing_req
                        sess["ts"] = time.time()
                        SESSIONS[phone] = sess
                        return
                except Exception as e:
                    log.warning(f"[ELIGIBILITY] LLM classification failed for ISSUE_OTHER: {e}")
                
                # If we still can't classify, re-ask issue selection
                await mcp_wa_send(phone, ELIGIBILITY_ISSUE_SELECTION_MSG, buttons=ELIGIBILITY_ISSUE_SELECTION_BUTTONS)
                _add_to_history(phone, bot_msg=ELIGIBILITY_ISSUE_SELECTION_MSG)
                sess["_eligibility_mode"] = "ISSUE_PICK"
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
    
    # Step 2: Free-text fallback (for users who type instead of clicking buttons)
    # Use existing rule-based classification
    rule_intent, missing_req = classify_eligibility_rule_based(text)
    log.info(f"[ELIGIBILITY] Rule-based classification: intent={rule_intent}, missing_req={missing_req}")
    
    # Determine if LLM is needed
    use_llm = should_use_llm(text, rule_intent, missing_req)
    final_intent = rule_intent
    final_missing_req = missing_req
    
    if use_llm:
        # LLM fallback for ambiguous/mixed signals
        try:
            log.info(f"[ELIGIBILITY] Calling LLM fallback for ambiguous/mixed response")
            llm_context = build_llm_context("ELIGIBILITY", sess, last_prompt=ELIGIBILITY_PROMPT)
            llm_result = await mcp_llm_classify_intent(text, "ELIGIBILITY", llm_context)
            llm_intent = (llm_result.get("intent") or "").upper()
            llm_conf = float(llm_result.get("confidence") or 0.0)
            
            log.info(f"[ELIGIBILITY] LLM classification: intent={llm_intent}, confidence={llm_conf}")
            
            # Map LLM intents to our intent names
            if llm_intent in ["ELIGIBLE_YES", "ELIGIBLE_OK"]:
                # Check if user only has phone/smartphone (not acceptable) - strict enforcement
                text_lower = text.lower()
                phone_only_patterns = [
                    r"\b(only|just)\s+(phone|smartphone|mobile)\b",
                    r"\b(phone|smartphone|mobile)\s+only\b",
                    r"\b(have|got|own)\s+(phone|smartphone|mobile)\s+(but|,)\s*(no|don'?t have|dont have)\s*(laptop|tablet)\b",
                ]
                has_phone_only = any(re.search(pattern, text_lower) for pattern in phone_only_patterns)
                has_laptop_tablet = any(term in text_lower for term in ["laptop", "tablet"])
                
                if has_phone_only and not has_laptop_tablet:
                    # User only has phone - reject (strict enforcement)
                    final_intent = "ELIGIBLE_NO"
                    final_missing_req = "device"
                else:
                    final_intent = "ELIGIBLE_YES"
                    final_missing_req = None
            elif llm_intent in ["ELIGIBLE_NO", "ELIGIBLE_NOT_OK"]:
                final_intent = "ELIGIBLE_NO"
                # Try to determine which requirement from LLM response or text
                text_lower = text.lower()
                if any(term in text_lower for term in ["18", "age", "17", "sixteen", "under 18"]):
                    final_missing_req = "age"
                elif any(term in text_lower for term in ["laptop", "tablet", "device", "internet", "wifi", "phone", "smartphone"]):
                    final_missing_req = "device"
                elif any(term in text_lower for term in ["paid", "payment", "unpaid", "volunteer"]):
                    final_missing_req = "unpaid"
                elif any(term in text_lower for term in ["hour", "hours", "time", "commitment"]):
                    final_missing_req = "commitment"
            elif llm_intent in ["ELIGIBLE_UNCLEAR", "ELIGIBLE_UNCERTAIN"]:
                final_intent = "ELIGIBLE_UNCLEAR"
                # Use rule-based missing_req if available, or try to infer
                if not final_missing_req:
                    text_lower = text.lower()
                    if any(term in text_lower for term in ["18", "age", "17", "turning 18"]):
                        final_missing_req = "age"
                    elif any(term in text_lower for term in ["internet", "wifi", "connection", "stable", "device", "laptop", "tablet"]):
                        final_missing_req = "device"
                    elif any(term in text_lower for term in ["paid", "payment", "unpaid"]):
                        final_missing_req = "unpaid"
                    elif any(term in text_lower for term in ["hour", "hours", "time", "commitment"]):
                        final_missing_req = "commitment"
            elif llm_intent == "QUERY":
                final_intent = "QUERY"
            else:
                # LLM returned something else - use rule-based if available
                if rule_intent:
                    final_intent = rule_intent
                    final_missing_req = missing_req
                else:
                    # Default to UNCLEAR
                    final_intent = "ELIGIBLE_UNCLEAR"
                    log.info(f"[ELIGIBILITY] LLM returned unknown intent, defaulting to ELIGIBLE_UNCLEAR")
        except Exception as e:
            log.warning(f"[ELIGIBILITY] LLM classification failed: {e}, using rule-based result")
            if not rule_intent:
                final_intent = "ELIGIBLE_UNCLEAR"
    
    # Step 3: Handle based on final intent (free-text fallback for typed responses)
    # Map typed responses to button actions based on current mode
    eligibility_mode = sess.get("_eligibility_mode", "ALIGN")
    
    # If in issue-specific mode, handle yes/no typed responses
    if eligibility_mode in ["ISSUE_AGE", "ISSUE_DEVICE", "ISSUE_TIME", "ISSUE_UNPAID"]:
        if final_intent == "ELIGIBLE_YES":
            # Issue resolved - re-show main prompt
            log.info(f"[ELIGIBILITY] Issue {eligibility_mode} resolved via typed response, re-showing main prompt")
            await mcp_wa_send(phone, ELIGIBILITY_PROMPT, buttons=ELIGIBILITY_BUTTONS)
            _add_to_history(phone, bot_msg=ELIGIBILITY_PROMPT)
            sess["_eligibility_mode"] = "ALIGN"
            sess["_eligibility_clarification_step"] = None
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        elif final_intent == "ELIGIBLE_NO":
            # Requirement not met - exit immediately
            log.info(f"[ELIGIBILITY] Requirement {eligibility_mode} not met via typed response, exiting")
            await mcp_wa_send(phone, ELIGIBILITY_EXIT)
            _add_to_history(phone, bot_msg=ELIGIBILITY_EXIT)
            
            # Persistence: Store rejection response
            try:
                from datetime import datetime, timezone
                from storage.db import get_db_session
                from storage.session_store import update_session_state_and_tool_state, finalize_onboarding
                from storage.event_logger import log_event
                from agents.onboarding.config import settings
                
                now_iso = datetime.now(timezone.utc).isoformat()
                with get_db_session() as db:
                    session_id = sess.get("_db_session_id")
                    # Read existing eligibility from tool_state
                    from sqlalchemy import select
                    from storage.tables import serve_agent_sessions
                    stmt = select(serve_agent_sessions.c.tool_state).where(
                        serve_agent_sessions.c.wa_phone == phone
                    )
                    result = db.execute(stmt).first()
                    existing_eligibility = {}
                    if result and result[0] and isinstance(result[0], dict):
                        existing_eligibility = result[0].get("eligibility", {})
                    
                    # Map eligibility_mode to requirement or use final_missing_req
                    req_map = {
                        "ISSUE_AGE": "age",
                        "ISSUE_DEVICE": "device",
                        "ISSUE_TIME": "commitment",
                        "ISSUE_UNPAID": "unpaid"
                    }
                    failed_req = req_map.get(eligibility_mode) or final_missing_req or "unknown"
                    
                    eligibility_update = existing_eligibility.copy()
                    eligibility_update.update({
                        "response": "no",
                        "passed": False,
                        "rejection_reason": failed_req,
                        "responded_at": now_iso
                    })
                    
                    update_session_state_and_tool_state(
                        db=db,
                        wa_phone=phone,
                        state="REJECTED",
                        tool_state_updates={"eligibility": eligibility_update}
                    )
                    log_event(
                        db=db,
                        wa_phone=phone,
                        agent_name=settings.AGENT_NAME,
                        event_type="ELIGIBILITY_RESPONSE",
                        event_source="user",
                        state="ONBOARDING",
                        sub_state="ELIGIBILITY",
                        status="SUCCESS",
                        details={"response": "no", "passed": False, "failed_requirement": failed_req, "raw_text": text},
                        session_id=session_id
                    )
                    
                    finalize_onboarding(
                        db,
                        wa_phone=phone,
                        eligibility_status="REJECTED",
                        available_days=None,
                        available_time_bands=None,
                        end_reason=f"eligibility_failed_{failed_req}"
                    )
                    log_event(
                        db=db,
                        wa_phone=phone,
                        agent_name=settings.AGENT_NAME,
                        event_type="SESSION_ENDED",
                        event_source="onboarding_agent",
                        state="REJECTED",
                        status="rejected",
                        details={"reason": f"eligibility_failed_{failed_req}"},
                        session_id=session_id
                    )
                    log.info(f"[PERSISTENCE] Finalized rejected session for {phone}")
            except Exception as e:
                log.warning(f"[PERSISTENCE] Failed to finalize rejected session for {phone}: {e}", exc_info=True)
            
            sess["state"] = "REJECTED"
            sess["_eligibility_mode"] = None
            sess["_eligibility_clarification_sent"] = False
            sess["_eligibility_missing_req"] = None
            sess["_eligibility_clarification_step"] = None
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        else:
            # Unclear response - re-ask the issue-specific question
            issue_prompts = {
                "ISSUE_AGE": (ELIGIBILITY_ISSUE_AGE_PROMPT, ELIGIBILITY_ISSUE_AGE_BUTTONS),
                "ISSUE_DEVICE": (ELIGIBILITY_ISSUE_DEVICE_PROMPT, ELIGIBILITY_ISSUE_DEVICE_BUTTONS),
                "ISSUE_TIME": (ELIGIBILITY_ISSUE_TIME_PROMPT, ELIGIBILITY_ISSUE_TIME_BUTTONS),
                "ISSUE_UNPAID": (ELIGIBILITY_ISSUE_UNPAID_PROMPT, ELIGIBILITY_ISSUE_UNPAID_BUTTONS),
            }
            prompt, buttons = issue_prompts.get(eligibility_mode, (ELIGIBILITY_PROMPT, ELIGIBILITY_BUTTONS))
            await mcp_wa_send(phone, prompt, buttons=buttons)
            _add_to_history(phone, bot_msg=prompt)
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
    
    # Handle typed responses in ALIGN mode (map to button actions)
    if eligibility_mode == "ALIGN" or eligibility_mode is None:
        if final_intent == "ELIGIBLE_YES":
            # Clear YES - proceed to IDENTITY (same as YES_WORKS button)
            log.info(f"[ELIGIBILITY] User confirmed all requirements via typed response, proceeding to IDENTITY")
            
            # Persistence: Store eligibility response
            try:
                from datetime import datetime, timezone
                from storage.db import get_db_session
                from storage.session_store import update_session_state_and_tool_state
                from storage.event_logger import log_event
                from ..config import settings
                
                now_iso = datetime.now(timezone.utc).isoformat()
                with get_db_session() as db:
                    session_id = sess.get("_db_session_id")
                    # Read existing eligibility from tool_state
                    from sqlalchemy import select
                    from storage.tables import serve_agent_sessions
                    stmt = select(serve_agent_sessions.c.tool_state).where(
                        serve_agent_sessions.c.wa_phone == phone
                    )
                    result = db.execute(stmt).first()
                    existing_eligibility = {}
                    if result and result[0] and isinstance(result[0], dict):
                        existing_eligibility = result[0].get("eligibility", {})
                    
                    eligibility_update = existing_eligibility.copy()
                    eligibility_update.update({
                        "response": "yes",
                        "passed": True,
                        "q1_commitment": True,
                        "q2_age": True,
                        "q3_device": True,
                        "responded_at": now_iso
                    })
                    
                    update_session_state_and_tool_state(
                        db=db,
                        wa_phone=phone,
                        state="ONBOARDING",
                        sub_state="IDENTITY",
                        tool_state_updates={"eligibility": eligibility_update}
                    )
                    log_event(
                        db=db,
                        wa_phone=phone,
                        agent_name=settings.AGENT_NAME,
                        event_type="ELIGIBILITY_RESPONSE",
                        event_source="user",
                        state="ONBOARDING",
                        sub_state="ELIGIBILITY",
                        status="SUCCESS",
                        details={"response": "yes", "passed": True, "raw_text": text},
                        session_id=session_id
                    )
            except Exception as e:
                log.warning(f"[ELIGIBILITY] Failed to persist response: {e}", exc_info=True)
            
            profile.setdefault("eligibility", {})["passed"] = True
            sess["profile"] = profile
            sess["state"] = "IDENTITY"
            sess["_eligibility_mode"] = None
            sess["_eligibility_clarification_sent"] = False
            sess["_eligibility_missing_req"] = None
            sess["_eligibility_clarification_step"] = None
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            await _handle(phone, "__kick__")
            return
        elif final_intent == "QUERY":
            # Query - send "Tell me more" explanation and re-show main prompt
            log.info(f"[ELIGIBILITY] User asked a question, sending explanation")
            await mcp_wa_send(phone, ELIGIBILITY_TELL_ME_MORE_MSG)
            _add_to_history(phone, bot_msg=ELIGIBILITY_TELL_ME_MORE_MSG)
            await mcp_wa_send(phone, ELIGIBILITY_PROMPT, buttons=ELIGIBILITY_BUTTONS)
            _add_to_history(phone, bot_msg=ELIGIBILITY_PROMPT)
            sess["_eligibility_mode"] = "ALIGN"
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        elif final_intent == "ELIGIBLE_NO" or final_intent == "ELIGIBLE_UNCLEAR":
            # Something won't work - show issue selection
            log.info(f"[ELIGIBILITY] User indicated problem via typed response, showing issue selection")
            await mcp_wa_send(phone, ELIGIBILITY_ISSUE_SELECTION_MSG, buttons=ELIGIBILITY_ISSUE_SELECTION_BUTTONS)
            _add_to_history(phone, bot_msg=ELIGIBILITY_ISSUE_SELECTION_MSG)
            sess["_eligibility_mode"] = "ISSUE_PICK"
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
    
    # Fallback for unexpected modes or edge cases
    # If we reach here, something unexpected happened - re-show main prompt with buttons
    log.warning(f"[ELIGIBILITY] Unexpected state: mode={eligibility_mode}, intent={final_intent}, re-showing main prompt")
    await mcp_wa_send(phone, ELIGIBILITY_PROMPT, buttons=ELIGIBILITY_BUTTONS)
    _add_to_history(phone, bot_msg=ELIGIBILITY_PROMPT)
    sess["_eligibility_mode"] = "ALIGN"
    sess["ts"] = time.time()
    SESSIONS[phone] = sess
    return
