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
        "yes" if Yes button clicked
        "no" if No button clicked
        "tell_me_more" if Tell me more button clicked
        None if not a button click
    """
    text_lower = text.lower().strip()
    
    # Check for exact button label matches (plain text, no emojis)
    if text_lower in ["yes", "y"]:
        return "yes"
    if text_lower in ["no", "n"]:
        return "no"
    if text_lower in ["tell me more", "tell me", "more", "info", "information"]:
        return "tell_me_more"
    
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
    
    # ========== SINGLE ELIGIBILITY PROMPT (no separate commitment check) ==========
    # Initial prompt - single compact message (no buttons for now)
    if text == "__kick__" or not sess.get("_eligibility_prompted"):
        log.info(f"[ELIGIBILITY] Sending single eligibility prompt to {phone}")
        await mcp_wa_send(phone, ELIGIBILITY_PROMPT)
        _add_to_history(phone, bot_msg=ELIGIBILITY_PROMPT)
        sess["_eligibility_prompted"] = True
        sess["_eligibility_clarification_sent"] = False
        sess["_eligibility_missing_req"] = None
        sess["_eligibility_clarification_step"] = None
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
    
    # Step 1: Check for button click (if buttons were used in future)
    button_click = detect_button_click(text)
    
    if button_click:
        log.info(f"[ELIGIBILITY] Button clicked: {button_click}")
        
        # Handle button clicks based on current state
        clarification_step = sess.get("_eligibility_clarification_step")
        
        if button_click == "yes":
            if clarification_step is None:
                # Yes on initial prompt - proceed to IDENTITY
                log.info(f"[ELIGIBILITY] User clicked Yes on initial prompt, proceeding to IDENTITY")
                # Mark eligibility as passed
                profile.setdefault("eligibility", {})["passed"] = True
                sess["profile"] = profile
                sess["state"] = "IDENTITY"
                sess["_eligibility_clarification_sent"] = False
                sess["_eligibility_missing_req"] = None
                sess["_eligibility_clarification_step"] = None
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                await _handle(phone, "__kick__")
                return
            elif clarification_step in ["age", "device", "unpaid", "commitment"]:
                # Yes on clarification - all confirmed, proceed to IDENTITY
                log.info(f"[ELIGIBILITY] Requirement confirmed via clarification, proceeding to IDENTITY")
                profile.setdefault("eligibility", {})["passed"] = True
                sess["profile"] = profile
                sess["state"] = "IDENTITY"
                sess["_eligibility_clarification_sent"] = False
                sess["_eligibility_missing_req"] = None
                sess["_eligibility_clarification_step"] = None
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                await _handle(phone, "__kick__")
                return
        
        elif button_click == "no":
            # No at any point - exit immediately
            log.info(f"[ELIGIBILITY] User clicked No, sending exit message immediately")
            await mcp_wa_send(phone, ELIGIBILITY_EXIT)
            _add_to_history(phone, bot_msg=ELIGIBILITY_EXIT)
            sess["state"] = "REJECTED"
            sess["_eligibility_clarification_sent"] = False
            sess["_eligibility_missing_req"] = None
            sess["_eligibility_clarification_step"] = None
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            
            # Persistence: Finalize with REJECTED status
            try:
                from storage.db import get_db_session
                from storage.session_store import finalize_onboarding
                from storage.event_logger import log_event
                from agents.onboarding.config import settings
                
                with get_db_session() as db:
                    finalize_onboarding(
                        db,
                        wa_phone=phone,
                        eligibility_status="REJECTED",
                        available_days=None,
                        available_time_bands=None,
                        end_reason="eligibility_failed"
                    )
                    session_id = sess.get("_db_session_id")
                    log_event(
                        db=db,
                        wa_phone=phone,
                        agent_name=settings.AGENT_NAME,
                        event_type="SESSION_ENDED",
                        event_source="onboarding_agent",
                        state="REJECTED",
                        status="rejected",
                        details={"reason": "eligibility_failed"},
                        session_id=session_id
                    )
                    log.info(f"[PERSISTENCE] Finalized rejected session for {phone}")
            except Exception as e:
                log.warning(f"[PERSISTENCE] Failed to finalize rejected session for {phone}: {e}", exc_info=True)
            
            return
    
    # Step 2: Free-text fallback (PRIMARY PATH for interrupt-aware handling)
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
    
    # Step 3: Handle based on final intent (interrupt-aware)
    # If in clarification flow, handle response
    clarification_step = sess.get("_eligibility_clarification_step")
    
    if clarification_step:
        # User is in clarification flow (interrupt handling)
        if final_intent == "ELIGIBLE_YES":
            # Confirmed requirement - proceed to IDENTITY
            log.info(f"[ELIGIBILITY] Requirement confirmed via clarification, proceeding to IDENTITY")
            profile.setdefault("eligibility", {})["passed"] = True
            sess["profile"] = profile
            sess["state"] = "IDENTITY"
            sess["_eligibility_clarification_sent"] = False
            sess["_eligibility_missing_req"] = None
            sess["_eligibility_clarification_step"] = None
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            await _handle(phone, "__kick__")
            return
        elif final_intent == "ELIGIBLE_NO":
            # Still NO after clarification - exit
            log.info(f"[ELIGIBILITY] User still declined after clarification, sending exit")
            await mcp_wa_send(phone, ELIGIBILITY_EXIT)
            _add_to_history(phone, bot_msg=ELIGIBILITY_EXIT)
            sess["state"] = "REJECTED"
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        else:
            # Still unclear - re-ask clarification
            clarify_msg, clarify_buttons = get_clarification_message(clarification_step)
            await mcp_wa_send(phone, clarify_msg, buttons=clarify_buttons)
            _add_to_history(phone, bot_msg=clarify_msg)
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
    
    # Handle first response (interrupt-aware)
    if final_intent == "ELIGIBLE_YES":
        # Clear YES - proceed to IDENTITY
        log.info(f"[ELIGIBILITY] User confirmed all requirements, proceeding to IDENTITY")
        profile.setdefault("eligibility", {})["passed"] = True
        sess["profile"] = profile
        sess["state"] = "IDENTITY"
        sess["_eligibility_clarification_sent"] = False
        sess["_eligibility_missing_req"] = None
        sess["_eligibility_clarification_step"] = None
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        await _handle(phone, "__kick__")
        return
    
    elif final_intent == "ELIGIBLE_NO":
        # Explicit NO or constraint - immediate exit (no persuasion)
        log.info(f"[ELIGIBILITY] User declined or constraint not met, sending exit immediately")
        await mcp_wa_send(phone, ELIGIBILITY_EXIT)
        _add_to_history(phone, bot_msg=ELIGIBILITY_EXIT)
        sess["state"] = "REJECTED"
        sess["_eligibility_clarification_sent"] = False
        sess["_eligibility_missing_req"] = None
        sess["_eligibility_clarification_step"] = None
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        
        # Persistence: Finalize with REJECTED status
        try:
            from storage.db import get_db_session
            from storage.session_store import finalize_onboarding
            from storage.event_logger import log_event
            from agents.onboarding.config import settings
            
            with get_db_session() as db:
                finalize_onboarding(
                    db,
                    wa_phone=phone,
                    eligibility_status="REJECTED",
                    available_days=None,
                    available_time_bands=None,
                    end_reason="eligibility_failed"
                )
                session_id = sess.get("_db_session_id")
                log_event(
                    db=db,
                    wa_phone=phone,
                    agent_name=settings.AGENT_NAME,
                    event_type="SESSION_ENDED",
                    event_source="onboarding_agent",
                    state="REJECTED",
                    status="rejected",
                    details={"reason": "eligibility_failed"},
                    session_id=session_id
                )
                log.info(f"[PERSISTENCE] Finalized rejected session for {phone}")
        except Exception as e:
            log.warning(f"[PERSISTENCE] Failed to finalize rejected session for {phone}: {e}", exc_info=True)
        
        return
    
    elif final_intent == "ELIGIBLE_UNCLEAR":
        # Partial interrupt - ask ONE targeted clarification
        log.info(f"[ELIGIBILITY] Partial interrupt detected, asking clarification for: {final_missing_req}")
        clarify_msg, clarify_buttons = get_clarification_message(final_missing_req)
        await mcp_wa_send(phone, clarify_msg, buttons=clarify_buttons)
        _add_to_history(phone, bot_msg=clarify_msg)
        sess["_eligibility_clarification_sent"] = True
        sess["_eligibility_clarification_step"] = final_missing_req
        sess["_eligibility_missing_req"] = final_missing_req
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
    
    elif final_intent == "QUERY":
        # Query - answer briefly and re-ask
        log.info(f"[ELIGIBILITY] User asked question, answering and re-asking")
        faq_answer = get_faq_answer(text)
        if faq_answer:
            await mcp_wa_send(phone, faq_answer)
            _add_to_history(phone, bot_msg=faq_answer)
        else:
            # Generic answer
            await mcp_wa_send(phone, ELIGIBILITY_PROMPT)
            _add_to_history(phone, bot_msg=ELIGIBILITY_PROMPT)
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
    
    else:
        # Ambiguous/unknown - default to UNCLEAR and ask for clarification
        log.info(f"[ELIGIBILITY] Ambiguous response, asking for clarification")
        await mcp_wa_send(phone, ELIGIBILITY_PROMPT)
        _add_to_history(phone, bot_msg=ELIGIBILITY_PROMPT)
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
