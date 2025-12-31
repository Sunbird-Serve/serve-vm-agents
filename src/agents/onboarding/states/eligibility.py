"""
ELIGIBILITY State Handler (State 3: Eligibility Check)
Rule-first approach with LLM fallback only when needed
Strict: if any requirement not met → community exit
"""
import logging
import time
import re
from typing import Dict, Any, Optional, Tuple
from ..messages import ELIGIBILITY_PROMPT, ELIGIBILITY_EXIT
from ..validators import is_yes_response, is_no_response

log = logging.getLogger(__name__)

# Clarification messages for each requirement
ELIGIBILITY_CLARIFY_AGE = """Just to confirm — are you 18 or above? 🙂"""
ELIGIBILITY_CLARIFY_DEVICE = """Do you have a phone/laptop with a reasonably stable internet connection? 🙂"""
ELIGIBILITY_CLARIFY_UNPAID = """And are you okay with this being a voluntary (unpaid) role? 🙂"""

# FAQ answers for common questions
ELIGIBILITY_FAQ_AGE = """Yes, 18+ is required for classroom volunteering. This is a policy requirement. Are all three okay for you?"""
ELIGIBILITY_FAQ_DEVICE = """A phone or laptop with stable internet is needed for live online classes. Are all three okay for you?"""
ELIGIBILITY_FAQ_UNPAID = """Yes, this is a volunteer role with no payment. Are all three okay for you?"""


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
    
    # Check for ELIGIBLE_NO - Device/Internet violations
    device_violations = [
        r"\b(no (laptop|phone|tablet|device|smartphone|internet|wifi|internet connection))\b",
        r"\b(don'?t have|dont have|don't have|do not have)\s*(laptop|phone|tablet|device|internet|wifi)\b",
        r"\b(no access to|without)\s*(internet|wifi|laptop|phone|device)\b",
        r"\b(laptop|phone|device|internet|wifi)\s*(is|are)?\s*(broken|not working|not available|unavailable)\b",
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
        r"\b(i'?m|i am|im)\s*\d+\s*(and|&)\s*(have|got)\s*(internet|laptop|phone|device)\b",  # "I'm 25 and have internet"
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
    device_mentioned = any(term in text_lower for term in ["laptop", "phone", "tablet", "device", "internet", "wifi"])
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
            elif "internet" in text_lower or "wifi" in text_lower or "device" in text_lower or "laptop" in text_lower or "phone" in text_lower:
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
    has_answer = any(word in text_lower for word in ["yes", "no", "ok", "sure", "have", "18", "laptop", "phone"])
    if has_question and has_answer and word_count > 8:
        return True
    
    # Complex uncertainty cases
    complex_uncertainty = [
        r"\b(17|sixteen)\s+but\s+will\s+turn\s+18\b",
        r"\b(phone|laptop)\s+but\s+internet\s+(is|'?s)\s+(patchy|unstable|not stable)\b",
        r"\b(can|will)\s+volunteer\s+but\s+(was|am)\s+hoping\s+(for|there'?s)\s+(stipend|payment)\b",
    ]
    for pattern in complex_uncertainty:
        if re.search(pattern, text_lower):
            return True
    
    # Don't use LLM for simple responses
    if word_count <= 5 and rule_intent in ["ELIGIBLE_YES", "ELIGIBLE_NO", "QUERY"]:
        return False
    
    return False


def get_clarification_message(missing_requirement: Optional[str]) -> str:
    """
    Get the appropriate clarification message based on missing requirement.
    """
    if missing_requirement == "age":
        return ELIGIBILITY_CLARIFY_AGE
    elif missing_requirement == "device":
        return ELIGIBILITY_CLARIFY_DEVICE
    elif missing_requirement == "unpaid":
        return ELIGIBILITY_CLARIFY_UNPAID
    else:
        # Generic clarification - re-ask all three
        return ELIGIBILITY_PROMPT


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
    Handle ELIGIBILITY state - strict 3-point eligibility check.
    
    Flow:
    1. Rule-based classification (NO LLM)
    2. LLM fallback only when needed (ambiguous/mixed signals)
    3. Clarification for ELIGIBLE_UNCLEAR (one targeted question)
    4. Immediate exit for ELIGIBLE_NO (no persuasion, no exceptions)
    """
    # Late import to avoid circular dependency
    from ..wa_loop import (
        mcp_wa_send, _add_to_history, _handle, SESSIONS,
        mcp_llm_classify_intent, build_llm_context
    )
    
    if text == "__kick__" or not sess.get("_eligibility_prompted"):
        # First time: send the eligibility check question
        log.info(f"[ELIGIBILITY] Sending eligibility check to {phone}")
        await mcp_wa_send(phone, ELIGIBILITY_PROMPT)
        _add_to_history(phone, bot_msg=ELIGIBILITY_PROMPT)
        sess["_eligibility_prompted"] = True
        sess["_eligibility_clarification_sent"] = False
        sess["_eligibility_missing_req"] = None
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
    
    # Step 1: Rule-based classification (NO LLM)
    rule_intent, missing_req = classify_eligibility_rule_based(text)
    log.info(f"[ELIGIBILITY] Rule-based classification: intent={rule_intent}, missing_req={missing_req}")
    
    # Step 2: Determine if LLM is needed
    use_llm = should_use_llm(text, rule_intent, missing_req)
    final_intent = rule_intent
    final_missing_req = missing_req
    
    if use_llm:
        # Step 3: LLM fallback
        try:
            log.info(f"[ELIGIBILITY] Calling LLM fallback for ambiguous/mixed response")
            llm_context = build_llm_context("ELIGIBILITY", sess, last_prompt=ELIGIBILITY_PROMPT)
            llm_result = await mcp_llm_classify_intent(text, "ELIGIBILITY", llm_context)
            llm_intent = (llm_result.get("intent") or "").upper()
            llm_conf = float(llm_result.get("confidence") or 0.0)
            
            log.info(f"[ELIGIBILITY] LLM classification: intent={llm_intent}, confidence={llm_conf}")
            
            # Map LLM intents to our intent names
            if llm_intent in ["ELIGIBLE_YES", "ELIGIBLE_OK"]:
                final_intent = "ELIGIBLE_YES"
                final_missing_req = None
            elif llm_intent in ["ELIGIBLE_NO", "ELIGIBLE_NOT_OK"]:
                final_intent = "ELIGIBLE_NO"
                # Try to determine which requirement from LLM response or text
                text_lower = text.lower()
                if any(term in text_lower for term in ["18", "age", "17", "sixteen", "under 18"]):
                    final_missing_req = "age"
                elif any(term in text_lower for term in ["laptop", "phone", "device", "internet", "wifi"]):
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
    
    # Step 4: Handle based on final intent
    # If clarification was already sent, check response again
    if sess.get("_eligibility_clarification_sent"):
        prev_missing_req = sess.get("_eligibility_missing_req")
        
        if final_intent == "ELIGIBLE_YES":
            # Confirmed after clarification - proceed
            log.info(f"[ELIGIBILITY] User confirmed after clarification, proceeding to IDENTITY")
            sess["state"] = "IDENTITY"
            sess["_eligibility_clarification_sent"] = False
            sess["_eligibility_missing_req"] = None
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            await _handle(phone, "__kick__")
            return
        elif final_intent == "ELIGIBLE_NO":
            # Still NO after clarification - exit immediately
            log.info(f"[ELIGIBILITY] User still declined after clarification, sending exit message")
            await mcp_wa_send(phone, ELIGIBILITY_EXIT)
            _add_to_history(phone, bot_msg=ELIGIBILITY_EXIT)
            sess["state"] = "REJECTED"
            sess["_eligibility_clarification_sent"] = False
            sess["_eligibility_missing_req"] = None
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        elif final_intent == "ELIGIBLE_UNCLEAR":
            # Still unclear - ask one more clarification (different requirement if possible)
            if final_missing_req and final_missing_req != prev_missing_req:
                # Different requirement unclear - clarify that one
                clarify_msg = get_clarification_message(final_missing_req)
                log.info(f"[ELIGIBILITY] Still unclear, clarifying {final_missing_req}")
                await mcp_wa_send(phone, clarify_msg)
                _add_to_history(phone, bot_msg=clarify_msg)
                sess["_eligibility_missing_req"] = final_missing_req
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
            else:
                # Same or generic unclear - re-ask all three once more, then exit if still unclear
                log.info(f"[ELIGIBILITY] Still unclear after clarification, re-asking all three")
                await mcp_wa_send(phone, ELIGIBILITY_PROMPT)
                _add_to_history(phone, bot_msg=ELIGIBILITY_PROMPT)
                sess["_eligibility_clarification_sent"] = False  # Reset to allow one more try
                sess["_eligibility_missing_req"] = None
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
        else:
            # QUERY or other - handle normally
            pass
    
    # Handle first response (or response after clarification reset)
    if final_intent == "ELIGIBLE_YES":
        # YES - proceed directly to next state
        log.info(f"[ELIGIBILITY] User confirmed all requirements (ELIGIBLE_YES), proceeding to IDENTITY")
        sess["state"] = "IDENTITY"
        sess["_eligibility_clarification_sent"] = False
        sess["_eligibility_missing_req"] = None
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
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
    
    elif final_intent == "QUERY":
        # QUERY - answer briefly and re-ask
        faq_answer = get_faq_answer(text)
        if faq_answer:
            log.info(f"[ELIGIBILITY] User asked question, answering with FAQ")
            await mcp_wa_send(phone, faq_answer)
            _add_to_history(phone, bot_msg=faq_answer)
        else:
            # Generic answer
            log.info(f"[ELIGIBILITY] User asked question, answering generically")
            answer = "These are the three requirements we need. Are all three okay for you?"
            await mcp_wa_send(phone, answer)
            _add_to_history(phone, bot_msg=answer)
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
    
    elif final_intent == "ELIGIBLE_UNCLEAR":
        # UNCLEAR - ask one targeted clarification
        clarify_msg = get_clarification_message(final_missing_req)
        log.info(f"[ELIGIBILITY] Response unclear, asking clarification for {final_missing_req}")
        await mcp_wa_send(phone, clarify_msg)
        _add_to_history(phone, bot_msg=clarify_msg)
        sess["_eligibility_clarification_sent"] = True
        sess["_eligibility_missing_req"] = final_missing_req
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
    
    else:
        # Ambiguous/unknown - default to UNCLEAR and ask clarification
        log.info(f"[ELIGIBILITY] Ambiguous response, defaulting to ELIGIBLE_UNCLEAR")
        clarify_msg = get_clarification_message(None)  # Generic re-ask
        await mcp_wa_send(phone, clarify_msg)
        _add_to_history(phone, bot_msg=clarify_msg)
        sess["_eligibility_clarification_sent"] = True
        sess["_eligibility_missing_req"] = None
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
