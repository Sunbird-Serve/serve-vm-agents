"""
INTENT State Handler (State 2: Purpose Acknowledgement)
Lightweight: acknowledges purpose, offers to guide, transitions to ELIGIBILITY
"""
import logging
import time
import re
from typing import Dict, Any, Optional

from ..messages import INTENT_PROMPT, INTENT_EXIT
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
        mcp_wa_send, _add_to_history, _handle, SESSIONS
    )
    
    if text == "__kick__" or not sess.get("_intent_prompted"):
        # First time: send purpose acknowledgement message
        log.info(f"[INTENT] Sending purpose acknowledgement to {phone}")
        await mcp_wa_send(phone, INTENT_PROMPT)
        _add_to_history(phone, bot_msg=INTENT_PROMPT)
        sess["_intent_prompted"] = True
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
    
    # Classify response - only check for DEFERRAL/STOP
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
        # Any other response - proceed to ELIGIBILITY
        log.info(f"[INTENT] User responded, proceeding to ELIGIBILITY")
        sess["state"] = "ELIGIBILITY"
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        await _handle(phone, "__kick__")
        return
