"""
ELIGIBILITY State Handler (State 3: Eligibility Check)
Button-first approach with free-text fallback
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
    
    Returns:
        (intent: Optional[str], missing_requirement: Optional[str])
        intent can be: ELIGIBLE_YES, ELIGIBLE_NO, QUERY, ELIGIBLE_UNCLEAR, or None (ambiguous)
        missing_requirement can be: "age", "device", "unpaid", or None
    """
    text_lower = text.lower().strip()
    words = text_lower.split()
    
    # Check for QUERY first (questions)
    if "?" in text or re.search(r"^(what|how|when|why|where|who|which|can|could|do|does|is|are)\b", text, re.I):
        # Check if it's a question about eligibility requirements
        if any(term in text_lower for term in ["18", "age", "laptop", "phone", "internet", "device", "paid", "payment", "unpaid", "volunteer", "compulsory", "required", "need"]):
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
    
    # Check for ELIGIBLE_YES - Clear confirmation of all three
    if is_yes_response(text):
        # Check if it mentions all three or confirms all
        if any(phrase in text_lower for phrase in ["all three", "all ok", "all okay", "all good", "all fine", "all confirmed"]):
            return ("ELIGIBLE_YES", None)
        # Simple "yes" after the prompt - assume all three
        return ("ELIGIBLE_YES", None)
    
    # Positive keywords that suggest all three are okay
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
    
    # Check for ELIGIBLE_UNCLEAR - Partial/vague responses
    # Mentions only age but not device/unpaid
    age_mentioned = re.search(r"\b(18|eighteen|\d+)\s*(years?|yr|yrs|or above|or older)\b", text_lower)
    device_mentioned = any(term in text_lower for term in ["laptop", "tablet", "device", "internet", "wifi"])
    unpaid_mentioned = any(term in text_lower for term in ["unpaid", "volunteer", "voluntary", "no payment", "no pay", "free"])
    
    # Check for uncertainty indicators
    uncertainty_patterns = [
        r"\b(18 soon|turning 18|almost 18|will be 18)\b",
        r"\b(sometimes|occasionally|not always|not stable|patchy|unstable)\s*(internet|wifi|connection)\b",
        r"\b(not sure|unsure|not certain|maybe|perhaps|might be)\s*(about|with|regarding)\s*(unpaid|payment|volunteer)\b",
        r"\b(depends|depends on)\s*(payment|paid|stipend)\b",
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
            else:
                return ("ELIGIBLE_UNCLEAR", None)
    
    # Partial information - only one or two requirements mentioned
    requirements_mentioned = sum([bool(age_mentioned), bool(device_mentioned), bool(unpaid_mentioned)])
    if requirements_mentioned > 0 and requirements_mentioned < 3:
        # Determine which is missing
        if not age_mentioned:
            return ("ELIGIBLE_UNCLEAR", "age")
        elif not device_mentioned:
            return ("ELIGIBLE_UNCLEAR", "device")
        elif not unpaid_mentioned:
            return ("ELIGIBLE_UNCLEAR", "unpaid")
    
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
    
    Returns:
        (message: str, buttons: list[str])
    """
    if missing_requirement == "age":
        return (ELIGIBILITY_CLARIFY_AGE_PROMPT, ELIGIBILITY_CLARIFY_AGE_BUTTONS)
    elif missing_requirement == "device":
        return (ELIGIBILITY_CLARIFY_DEVICE_PROMPT, ELIGIBILITY_CLARIFY_DEVICE_BUTTONS)
    elif missing_requirement == "unpaid":
        return (ELIGIBILITY_CLARIFY_UNPAID_PROMPT, ELIGIBILITY_CLARIFY_UNPAID_BUTTONS)
    else:
        # Generic clarification - re-ask all three with buttons
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
    Handle ELIGIBILITY state - button-first approach with free-text fallback.
    
    Flow:
    1. Initial prompt with buttons (Yes/No/Tell me more)
    2. Button click handling (primary path)
    3. Progressive clarification flow (Tell me more → one requirement at a time)
    4. Free-text fallback (rule-based + LLM if needed)
    5. Immediate exit for ELIGIBLE_NO (no persuasion, no exceptions)
    6. Re-entry handling from REJECTED state
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
    
    # Initial prompt with buttons
    if text == "__kick__" or not sess.get("_eligibility_prompted"):
        log.info(f"[ELIGIBILITY] Sending eligibility check with buttons to {phone}")
        await mcp_wa_send(phone, ELIGIBILITY_PROMPT, buttons=ELIGIBILITY_BUTTONS)
        _add_to_history(phone, bot_msg=ELIGIBILITY_PROMPT)
        sess["_eligibility_prompted"] = True
        sess["_eligibility_clarification_sent"] = False
        sess["_eligibility_missing_req"] = None
        sess["_eligibility_clarification_step"] = None  # Track which step in progressive flow
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
    
    # Step 1: Check for button click (PRIMARY PATH)
    button_click = detect_button_click(text)
    
    if button_click:
        log.info(f"[ELIGIBILITY] Button clicked: {button_click}")
        
        # Handle button clicks based on current state
        clarification_step = sess.get("_eligibility_clarification_step")
        
        if button_click == "yes":
            if clarification_step is None:
                # Yes on initial prompt - proceed to IDENTITY
                log.info(f"[ELIGIBILITY] User clicked Yes on initial prompt, proceeding to IDENTITY")
                sess["state"] = "IDENTITY"
                sess["_eligibility_clarification_sent"] = False
                sess["_eligibility_missing_req"] = None
                sess["_eligibility_clarification_step"] = None
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                await _handle(phone, "__kick__")
                return
            elif clarification_step == "age":
                # Yes on age clarification - move to device
                log.info(f"[ELIGIBILITY] Age confirmed, asking device")
                clarify_msg, clarify_buttons = get_clarification_message("device")
                await mcp_wa_send(phone, clarify_msg, buttons=clarify_buttons)
                _add_to_history(phone, bot_msg=clarify_msg)
                sess["_eligibility_clarification_step"] = "device"
                sess["_eligibility_missing_req"] = "device"
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
            elif clarification_step == "device":
                # Yes on device clarification - move to unpaid
                log.info(f"[ELIGIBILITY] Device confirmed, asking unpaid")
                clarify_msg, clarify_buttons = get_clarification_message("unpaid")
                await mcp_wa_send(phone, clarify_msg, buttons=clarify_buttons)
                _add_to_history(phone, bot_msg=clarify_msg)
                sess["_eligibility_clarification_step"] = "unpaid"
                sess["_eligibility_missing_req"] = "unpaid"
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
            elif clarification_step == "unpaid":
                # Yes on unpaid clarification - all confirmed, proceed to IDENTITY
                log.info(f"[ELIGIBILITY] All requirements confirmed via progressive flow, proceeding to IDENTITY")
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
            return
        
        elif button_click == "tell_me_more":
            # Enter progressive clarification flow - start with age
            log.info(f"[ELIGIBILITY] User clicked Tell me more, starting progressive clarification")
            clarify_msg, clarify_buttons = get_clarification_message("age")
            await mcp_wa_send(phone, clarify_msg, buttons=clarify_buttons)
            _add_to_history(phone, bot_msg=clarify_msg)
            sess["_eligibility_clarification_sent"] = True
            sess["_eligibility_clarification_step"] = "age"
            sess["_eligibility_missing_req"] = "age"
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
    
    # Step 2: Free-text fallback (SECONDARY PATH)
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
                # Check if user only has phone/smartphone (not acceptable)
                text_lower = text.lower()
                phone_only_patterns = [
                    r"\b(only|just)\s+(phone|smartphone|mobile)\b",
                    r"\b(phone|smartphone|mobile)\s+only\b",
                    r"\b(have|got|own)\s+(phone|smartphone|mobile)\s+(but|,)\s*(no|don'?t have|dont have)\s*(laptop|tablet)\b",
                ]
                has_phone_only = any(re.search(pattern, text_lower) for pattern in phone_only_patterns)
                has_laptop_tablet = any(term in text_lower for term in ["laptop", "tablet"])
                
                if has_phone_only and not has_laptop_tablet:
                    # User only has phone - reject
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
            elif llm_intent in ["ELIGIBLE_UNCLEAR", "ELIGIBLE_UNCERTAIN"]:
                final_intent = "ELIGIBLE_UNCLEAR"
                # Use rule-based missing_req if available, or try to infer
                if not final_missing_req:
                    text_lower = text.lower()
                    if any(term in text_lower for term in ["18", "age", "17", "turning 18"]):
                        final_missing_req = "age"
                    elif any(term in text_lower for term in ["internet", "wifi", "connection", "stable"]):
                        final_missing_req = "device"
                    elif any(term in text_lower for term in ["paid", "payment", "unpaid"]):
                        final_missing_req = "unpaid"
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
    
    # Step 3: Handle based on final intent
    # If in progressive clarification flow, handle response
    clarification_step = sess.get("_eligibility_clarification_step")
    
    if clarification_step:
        # User is in progressive clarification flow
        if final_intent == "ELIGIBLE_YES":
            # Confirmed current requirement - move to next
            if clarification_step == "age":
                # Age confirmed, ask device
                clarify_msg, clarify_buttons = get_clarification_message("device")
                await mcp_wa_send(phone, clarify_msg, buttons=clarify_buttons)
                _add_to_history(phone, bot_msg=clarify_msg)
                sess["_eligibility_clarification_step"] = "device"
                sess["_eligibility_missing_req"] = "device"
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
            elif clarification_step == "device":
                # Device confirmed, ask unpaid
                clarify_msg, clarify_buttons = get_clarification_message("unpaid")
                await mcp_wa_send(phone, clarify_msg, buttons=clarify_buttons)
                _add_to_history(phone, bot_msg=clarify_msg)
                sess["_eligibility_clarification_step"] = "unpaid"
                sess["_eligibility_missing_req"] = "unpaid"
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
            elif clarification_step == "unpaid":
                # All confirmed - proceed to IDENTITY
                log.info(f"[ELIGIBILITY] All requirements confirmed, proceeding to IDENTITY")
                sess["state"] = "IDENTITY"
                sess["_eligibility_clarification_sent"] = False
                sess["_eligibility_missing_req"] = None
                sess["_eligibility_clarification_step"] = None
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                await _handle(phone, "__kick__")
                return
        elif final_intent == "ELIGIBLE_NO":
            # Declined at any step - exit immediately
            log.info(f"[ELIGIBILITY] User declined requirement, sending exit message")
            await mcp_wa_send(phone, ELIGIBILITY_EXIT)
            _add_to_history(phone, bot_msg=ELIGIBILITY_EXIT)
            sess["state"] = "REJECTED"
            sess["_eligibility_clarification_sent"] = False
            sess["_eligibility_missing_req"] = None
            sess["_eligibility_clarification_step"] = None
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        else:
            # Still unclear - re-ask current step with buttons
            clarify_msg, clarify_buttons = get_clarification_message(clarification_step)
            await mcp_wa_send(phone, clarify_msg, buttons=clarify_buttons)
            _add_to_history(phone, bot_msg=clarify_msg)
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
    
    # Handle first response (not in progressive flow)
    if final_intent == "ELIGIBLE_YES":
        # YES - proceed directly to next state
        log.info(f"[ELIGIBILITY] User confirmed all requirements (ELIGIBLE_YES), proceeding to IDENTITY")
        sess["state"] = "IDENTITY"
        sess["_eligibility_clarification_sent"] = False
        sess["_eligibility_missing_req"] = None
        sess["_eligibility_clarification_step"] = None
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        await _handle(phone, "__kick__")
        return
    
    elif final_intent == "ELIGIBLE_NO":
        # NO - exit immediately (no persuasion, no exceptions)
        log.info(f"[ELIGIBILITY] User declined requirements (ELIGIBLE_NO), sending exit message immediately")
        await mcp_wa_send(phone, ELIGIBILITY_EXIT)
        _add_to_history(phone, bot_msg=ELIGIBILITY_EXIT)
        sess["state"] = "REJECTED"
        sess["_eligibility_clarification_sent"] = False
        sess["_eligibility_missing_req"] = None
        sess["_eligibility_clarification_step"] = None
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
    
    elif final_intent == "QUERY":
        # QUERY - answer briefly and re-ask with buttons
        faq_answer = get_faq_answer(text)
        if faq_answer:
            log.info(f"[ELIGIBILITY] User asked question, answering with FAQ")
            await mcp_wa_send(phone, faq_answer)
            _add_to_history(phone, bot_msg=faq_answer)
        else:
            # Generic answer
            log.info(f"[ELIGIBILITY] User asked question, answering generically")
            answer = """These are the three requirements we need:
• 18 or above
• Tablet or laptop + internet (smartphones/phones are not suitable)
• Voluntary (unpaid) role

Are all three okay for you?"""
            await mcp_wa_send(phone, answer, buttons=ELIGIBILITY_BUTTONS)
            _add_to_history(phone, bot_msg=answer)
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
    
    elif final_intent == "ELIGIBLE_UNCLEAR":
        # UNCLEAR - ask one targeted clarification with buttons
        clarify_msg, clarify_buttons = get_clarification_message(final_missing_req)
        log.info(f"[ELIGIBILITY] Response unclear, asking clarification for {final_missing_req}")
        await mcp_wa_send(phone, clarify_msg, buttons=clarify_buttons)
        _add_to_history(phone, bot_msg=clarify_msg)
        sess["_eligibility_clarification_sent"] = True
        sess["_eligibility_clarification_step"] = final_missing_req or "age"  # Start progressive flow
        sess["_eligibility_missing_req"] = final_missing_req
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
    
    else:
        # Ambiguous/unknown - default to UNCLEAR and ask clarification with buttons
        log.info(f"[ELIGIBILITY] Ambiguous response, defaulting to ELIGIBLE_UNCLEAR")
        clarify_msg, clarify_buttons = get_clarification_message(None)  # Generic re-ask
        await mcp_wa_send(phone, clarify_msg, buttons=clarify_buttons)
        _add_to_history(phone, bot_msg=clarify_msg)
        sess["_eligibility_clarification_sent"] = True
        sess["_eligibility_clarification_step"] = "age"  # Start progressive flow
        sess["_eligibility_missing_req"] = None
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
