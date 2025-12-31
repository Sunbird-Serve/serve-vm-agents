"""
INTENT State Handler (State 2: Commitment Check)
Rule-first approach with LLM fallback only when needed
"""
import logging
import time
import re
from typing import Dict, Any, Optional, Tuple
from ..messages import INTENT_PROMPT, INTENT_PERSUASION, INTENT_EXIT
from ..validators import is_yes_response, is_no_response

log = logging.getLogger(__name__)

# FAQ answer for QUERY intent
INTENT_QUERY_ANSWER = """You'll teach live online while students sit in their school smart classroom. Usually ~2 hours/week, and we work around your schedule 😊

Would you be comfortable teaching around 2 hours a week?"""


def classify_intent_rule_based(text: str) -> Tuple[Optional[str], float]:
    """
    Rule-based classification for INTENT state (NO LLM).
    
    Returns:
        (intent: Optional[str], confidence: float)
        intent can be: TIME_YES, TIME_NO, TIME_MAYBE, DEFERRAL, QUERY, or None (ambiguous)
    """
    text_lower = text.lower().strip()
    words = text_lower.split()
    word_count = len(words)
    
    # Check for QUERY first (questions)
    if "?" in text or re.search(r"^(what|how|when|why|where|who|which|can|could|do|does|is|are)\b", text, re.I):
        # Check if it's a question about the 2-hour commitment
        if any(term in text_lower for term in ["2 hour", "two hour", "hours", "time", "week", "teach", "session", "do", "need"]):
            return ("QUERY", 0.9)
    
    # Check for DEFERRAL
    deferral_patterns = [
        r"\b(not now|not right now|later|after|next month|next week|next year)\b",
        r"\b(after exams?|after my|when i|once i|after this)\b",
        r"\b(not today|tomorrow|someday|some other time)\b"
    ]
    for pattern in deferral_patterns:
        if re.search(pattern, text_lower):
            return ("DEFERRAL", 0.85)
    
    # Check for TIME_NO (negative responses)
    # Clear negatives
    if is_no_response(text):
        return ("TIME_NO", 0.9)
    
    # Negative keywords/phrases
    no_patterns = [
        r"\b(cannot|can't|cant|not possible|impossible|unable|won't be able|wont be able)\b",
        r"\b(too busy|no time|don't have time|dont have time|not able|can't commit|cant commit)\b",
        r"\b(only \d+\s*(minute|min|hour|hr))\b",  # "only 1 hour", "only 30 minutes"
        r"\b(\d+\s*(minute|min))\b",  # "30 minutes", "1 minute" (likely insufficient)
        r"\b(less than|below|under)\s*\d+\s*(hour|hr)\b",  # "less than 2 hours"
    ]
    for pattern in no_patterns:
        if re.search(pattern, text_lower):
            return ("TIME_NO", 0.85)
    
    # Check for numeric responses indicating insufficient time
    # Match patterns like "1 hour", "30 mins", "half hour" (but not "2 hours" or more)
    hour_match = re.search(r"(\d+)\s*(hour|hr|hours)", text_lower)
    if hour_match:
        hours = int(hour_match.group(1))
        if hours < 2:
            return ("TIME_NO", 0.8)
    
    minute_match = re.search(r"(\d+)\s*(minute|min|mins)", text_lower)
    if minute_match:
        minutes = int(minute_match.group(1))
        if minutes < 120:  # Less than 2 hours
            return ("TIME_NO", 0.8)
    
    # Check for TIME_YES (positive responses)
    if is_yes_response(text):
        return ("TIME_YES", 0.9)
    
    # Positive keywords with 2+ hours mentioned
    yes_patterns = [
        r"\b(yes|ok|okay|sure|definitely|absolutely|of course|sounds good)\b",
        r"\b(i can|i'm able|i am able|comfortable|fine|works|good)\b",
        r"\b(2|two|3|three|4|four|\d+)\s*(hour|hr|hours).*week\b",  # "2 hours a week", "3 hours weekly"
        r"\b(available|ready|willing|happy|glad)\b",
        r"\b(2-3|2 to 3|3-4|3 to 4)\s*(hour|hr|hours)\b",  # "2-3 hours"
    ]
    for pattern in yes_patterns:
        if re.search(pattern, text_lower):
            return ("TIME_YES", 0.85)
    
    # Check for TIME_MAYBE (uncertain but not negative)
    maybe_patterns = [
        r"\b(maybe|perhaps|probably|might|could|can try|will try|should be fine|think so)\b",
        r"\b(depends|not sure|unsure|might be|could be)\b",
        r"\b(some weeks|most weeks|usually|often)\b",
    ]
    for pattern in maybe_patterns:
        if re.search(pattern, text_lower):
            return ("TIME_MAYBE", 0.75)
    
    # If we have 2+ hours mentioned without negative context, treat as YES
    if re.search(r"\b(2|two|3|three|4|four|\d+)\s*(hour|hr|hours)\b", text_lower):
        # Check if it's not in a negative context
        if not re.search(r"\b(only|just|less|below|under|can't|cannot)\b", text_lower):
            return ("TIME_YES", 0.7)
    
    # Default: ambiguous (will need LLM)
    return (None, 0.0)


def should_use_llm(text: str, rule_intent: Optional[str], rule_confidence: float) -> bool:
    """
    Determine if LLM should be invoked based on message characteristics.
    
    Returns True if:
    - Message is long (>12-15 words) AND contains mixed signals
    - Rule-based classifier returned None (ambiguous)
    - Message contains both answer and question
    """
    words = text.split()
    word_count = len(words)
    text_lower = text.lower()
    
    # If rule-based returned nothing, use LLM
    if rule_intent is None:
        return True
    
    # If confidence is very low, use LLM
    if rule_confidence < 0.5:
        return True
    
    # Long messages (>12-15 words) with mixed signals
    if word_count > 12:
        # Check for mixed signals: "yes but", "can do but", "however", etc.
        mixed_signals = [
            r"\b(yes|ok|sure|can do|comfortable)\s+but\b",
            r"\b(however|although|though|but)\b",
            r"\b(can|will|able)\s+but\s+(not|only|just)\b",
        ]
        for pattern in mixed_signals:
            if re.search(pattern, text_lower):
                return True
    
    # Message contains both answer and question
    has_question = "?" in text or re.search(r"^(what|how|when|why|where|who|which|can|could|do|does|is|are)\b", text, re.I)
    has_answer = any(word in text_lower for word in ["yes", "no", "ok", "sure", "can", "comfortable", "fine"])
    if has_question and has_answer:
        return True
    
    # Don't use LLM for simple responses
    if word_count <= 3 and rule_intent in ["TIME_YES", "TIME_NO", "TIME_MAYBE", "DEFERRAL"]:
        return False
    
    return False


async def handle_intent(phone: str, text: str, sess: Dict[str, Any], profile: Dict[str, Any]) -> None:
    """
    Handle INTENT state - commitment check with rule-first approach and LLM fallback.
    
    Flow:
    1. Rule-based classification (NO LLM)
    2. LLM fallback only when needed (ambiguous/mixed signals)
    3. Gentle persuasion for TIME_NO (once)
    4. Exit for DEFERRAL or confirmed TIME_NO
    """
    # Late import to avoid circular dependency
    from ..wa_loop import (
        mcp_wa_send, _add_to_history, _handle, SESSIONS,
        mcp_llm_classify_intent, build_llm_context
    )
    
    if text == "__kick__" or not sess.get("_intent_prompted"):
        # First time: send the 2-hour commitment question
        log.info(f"[INTENT] Sending commitment question to {phone}")
        await mcp_wa_send(phone, INTENT_PROMPT)
        _add_to_history(phone, bot_msg=INTENT_PROMPT)
        sess["_intent_prompted"] = True
        sess["_intent_persuasion_sent"] = False
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
    
    # Step 1: Rule-based classification (NO LLM)
    rule_intent, rule_confidence = classify_intent_rule_based(text)
    log.info(f"[INTENT] Rule-based classification: intent={rule_intent}, confidence={rule_confidence}")
    
    # Step 2: Determine if LLM is needed
    use_llm = should_use_llm(text, rule_intent, rule_confidence)
    final_intent = rule_intent
    llm_called = False
    
    if use_llm:
        # Step 3: LLM fallback
        try:
            log.info(f"[INTENT] Calling LLM fallback for ambiguous/mixed response")
            llm_context = build_llm_context("INTENT", sess, last_prompt=INTENT_PROMPT)
            llm_result = await mcp_llm_classify_intent(text, "INTENT", llm_context)
            llm_intent = (llm_result.get("intent") or "").upper()
            llm_conf = float(llm_result.get("confidence") or 0.0)
            llm_called = True
            
            log.info(f"[INTENT] LLM classification: intent={llm_intent}, confidence={llm_conf}")
            
            # Map LLM intents to our intent names
            if llm_intent in ["TIME_YES", "TIME_OK"]:
                final_intent = "TIME_YES"
            elif llm_intent in ["TIME_NO", "TIME_NOT_OK"]:
                final_intent = "TIME_NO"
            elif llm_intent in ["TIME_MAYBE", "TIME_UNCLEAR"]:
                final_intent = "TIME_MAYBE"
            elif llm_intent == "DEFERRAL":
                final_intent = "DEFERRAL"
            elif llm_intent == "QUERY":
                final_intent = "QUERY"
            else:
                # LLM returned something else - use rule-based if available, else treat as MAYBE
                if rule_intent:
                    final_intent = rule_intent
                else:
                    final_intent = "TIME_MAYBE"  # Default to proceeding
                    log.info(f"[INTENT] LLM returned unknown intent, defaulting to TIME_MAYBE")
        except Exception as e:
            log.warning(f"[INTENT] LLM classification failed: {e}, using rule-based result")
            if not rule_intent:
                final_intent = "TIME_MAYBE"  # Default to proceeding if both fail
    
    # Step 4: Handle based on final intent
    # If persuasion was already sent, check response again
    if sess.get("_intent_persuasion_sent"):
        if final_intent == "TIME_NO":
            # Still NO after persuasion - send exit message
            log.info(f"[INTENT] User still declined after persuasion, sending exit message")
            await mcp_wa_send(phone, INTENT_EXIT)
            _add_to_history(phone, bot_msg=INTENT_EXIT)
            sess["state"] = "REJECTED"
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        else:
            # Changed mind or neutral/positive - proceed to next state
            log.info(f"[INTENT] User agreed after persuasion, proceeding to ELIGIBILITY")
            sess["state"] = "ELIGIBILITY"
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            await _handle(phone, "__kick__")
            return
    
    # Handle first response
    if final_intent == "TIME_YES":
        # YES - proceed directly to next state
        log.info(f"[INTENT] User confirmed (TIME_YES), proceeding to ELIGIBILITY")
        sess["state"] = "ELIGIBILITY"
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        await _handle(phone, "__kick__")
        return
    
    elif final_intent == "TIME_MAYBE":
        # MAYBE - proceed to next state (do NOT block)
        log.info(f"[INTENT] User uncertain (TIME_MAYBE), proceeding to ELIGIBILITY")
        sess["state"] = "ELIGIBILITY"
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        await _handle(phone, "__kick__")
        return
    
    elif final_intent == "TIME_NO":
        # NO - send gentle persuasion message
        log.info(f"[INTENT] User declined (TIME_NO), sending persuasion message")
        await mcp_wa_send(phone, INTENT_PERSUASION)
        _add_to_history(phone, bot_msg=INTENT_PERSUASION)
        sess["_intent_persuasion_sent"] = True
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
    
    elif final_intent == "DEFERRAL":
        # DEFERRAL - exit with community link
        log.info(f"[INTENT] User deferred, sending exit message")
        await mcp_wa_send(phone, INTENT_EXIT)
        _add_to_history(phone, bot_msg=INTENT_EXIT)
        sess["state"] = "REJECTED"
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
    
    elif final_intent == "QUERY":
        # QUERY - answer briefly and re-ask
        log.info(f"[INTENT] User asked question, answering and re-asking")
        await mcp_wa_send(phone, INTENT_QUERY_ANSWER)
        _add_to_history(phone, bot_msg=INTENT_QUERY_ANSWER)
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
    
    else:
        # Ambiguous/unknown - default to proceeding (TIME_MAYBE behavior)
        log.info(f"[INTENT] Ambiguous response, defaulting to proceed (TIME_MAYBE)")
        sess["state"] = "ELIGIBILITY"
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        await _handle(phone, "__kick__")
        return
