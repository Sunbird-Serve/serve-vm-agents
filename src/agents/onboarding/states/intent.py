"""
INTENT State Handler (State 2: Purpose Acknowledgement)
Lightweight: acknowledges purpose, offers to guide, transitions to ELIGIBILITY
"""
import logging
import time
import re
from typing import Dict, Any, Optional

from ..messages import INTENT_PROMPT, INTENT_FOLLOWUP, INTENT_EXIT
from ..validators import is_no_response

log = logging.getLogger(__name__)


def classify_intent_simple(text: str) -> Optional[str]:
    """
    Simple classification for INTENT state - only checks for STOP/DEFERRAL.
    
    Returns:
        "DEFERRAL" if user wants to defer/stop
        None otherwise (proceed to ELIGIBILITY)
    """
    text_lower = text.lower().strip()
    
    # Check for DEFERRAL/STOP patterns
    deferral_patterns = [
        r"\b(not now|not right now|later|after|next month|next week|next year)\b",
        r"\b(after exams?|after my|when i|once i|after this)\b",
        r"\b(not today|tomorrow|someday|some other time)\b",
        r"\b(stop|exit|quit|no thanks|not interested|don't want|dont want)\b",
    ]
    for pattern in deferral_patterns:
        if re.search(pattern, text_lower):
            return "DEFERRAL"
    
    # Check for simple "no" response
    if is_no_response(text):
        return "DEFERRAL"
    
    # Anything else - proceed to ELIGIBILITY
    return None


async def handle_intent(phone: str, text: str, sess: Dict[str, Any], profile: Dict[str, Any]) -> None:
    """
    Handle INTENT state - lightweight purpose acknowledgement.
    
    Flow:
    1. Send purpose acknowledgement message
    2. Wait for any volunteer reply
    3. Check for DEFERRAL/STOP only
    4. Transition to ELIGIBILITY on any non-stop reply
    """
    # Late import to avoid circular dependency
    from ..wa_loop import (
        mcp_wa_send, _add_to_history, _handle, SESSIONS, _llm_call_messages, _extract_llm_text
    )
    
    if text == "__kick__" or not sess.get("_intent_prompted"):
        # First time: send curiosity question
        log.info(f"[INTENT] Sending curiosity question to {phone}")
        await mcp_wa_send(phone, INTENT_PROMPT)
        _add_to_history(phone, bot_msg=INTENT_PROMPT)
        sess["_intent_prompted"] = True
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
    
    # Check if follow-up was already sent - if so, this is the response to the follow-up
    if sess.get("_intent_followup_sent"):
        # User is responding to the follow-up message - check for DEFERRAL/STOP
        log.info(f"[INTENT] User responding to follow-up message from {phone}")
        intent = classify_intent_simple(text)
        log.info(f"[INTENT] Classification: intent={intent}")
        
        if intent == "DEFERRAL":
            # DEFERRAL/STOP - exit with community link
            log.info(f"[INTENT] User deferred/stopped, sending exit message")
            await mcp_wa_send(phone, INTENT_EXIT)
            _add_to_history(phone, bot_msg=INTENT_EXIT)
            sess["state"] = "REJECTED"
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        else:
            # User wants to continue - transition to ELIGIBILITY
            log.info(f"[INTENT] User wants to continue, proceeding to ELIGIBILITY for {phone}")
            sess["state"] = "ELIGIBILITY"
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            await _handle(phone, "__kick__")
            return
    
    # This is the first user response (their reason for clicking SERVE link)
    # Generate reflective response, then send follow-up and wait
    log.info(f"[INTENT] User shared their reason, generating reflective response for {phone}")
    
    # Classify response - check for DEFERRAL/STOP first
    intent = classify_intent_simple(text)
    log.info(f"[INTENT] Classification: intent={intent}")
    
    if intent == "DEFERRAL":
        # DEFERRAL/STOP - exit with community link
        log.info(f"[INTENT] User deferred/stopped, sending exit message")
        await mcp_wa_send(phone, INTENT_EXIT)
        _add_to_history(phone, bot_msg=INTENT_EXIT)
        sess["state"] = "REJECTED"
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
    
    # Generate a single-line reflective response using LLM
    try:
        reflective_prompt = """You are SIA, a warm and respectful conversational agent for Sunbird SERVE.

The volunteer just shared what made them click the SERVE link. Generate a single-line reflective response (1 line max, warm, no promises, no emojis).

Examples:
- "That's wonderful to hear."
- "I appreciate you sharing that."
- "That's really meaningful."
- "Thank you for sharing that with me."

Keep it brief, warm, and genuine. Do not make promises or commitments."""

        messages = [
            {"role": "system", "content": reflective_prompt},
            {"role": "user", "content": text}
        ]
        
        llm_result = await _llm_call_messages(messages, temperature=0.4, max_tokens=50, timeout=10)
        
        # Extract text from LLM response using existing helper
        reflective_response = _extract_llm_text(llm_result).strip()
        
        # Fallback if LLM didn't return a good response
        if not reflective_response or len(reflective_response) > 100:
            reflective_response = "That's wonderful to hear."
        
        log.info(f"[INTENT] Generated reflective response: {reflective_response}")
        
        # Send reflective response
        await mcp_wa_send(phone, reflective_response)
        _add_to_history(phone, bot_msg=reflective_response)
        
    except Exception as e:
        log.warning(f"[INTENT] Failed to generate reflective response: {e}, using fallback")
        # Fallback: send a simple acknowledgement
        fallback_response = "That's wonderful to hear."
        await mcp_wa_send(phone, fallback_response)
        _add_to_history(phone, bot_msg=fallback_response)
    
    # Send follow-up message and wait for response
    log.info(f"[INTENT] Sending follow-up message to {phone} and waiting for response")
    await mcp_wa_send(phone, INTENT_FOLLOWUP)
    _add_to_history(phone, bot_msg=INTENT_FOLLOWUP)
    
    # Set flag to indicate follow-up was sent - next message will be the response to this
    sess["_intent_followup_sent"] = True
    sess["ts"] = time.time()
    SESSIONS[phone] = sess
    return
