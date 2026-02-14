"""
INTENT State Handler (State 2: Purpose Acknowledgement)
Lightweight: acknowledges purpose, offers to guide, transitions to ELIGIBILITY
"""
import logging
import time
import re
from datetime import datetime, timezone
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
        message_id = await mcp_wa_send(phone, INTENT_PROMPT)
        _add_to_history(phone, bot_msg=INTENT_PROMPT)
        
        # Persistence: Update state and log event
        try:
            from storage.db import get_db_session
            from storage.session_store import update_session_state_and_tool_state
            from storage.event_logger import log_event
            from ..config import settings
            
            with get_db_session() as db:
                session_id = sess.get("_db_session_id")
                update_session_state_and_tool_state(
                    db=db,
                    wa_phone=phone,
                    state="ONBOARDING",
                    sub_state="INTENT",
                    last_outbound_msg_id=message_id
                )
                log_event(
                    db=db,
                    wa_phone=phone,
                    agent_name=settings.AGENT_NAME,
                    event_type="MOTIVATION_ASKED",
                    event_source="agent",
                    state="ONBOARDING",
                    sub_state="INTENT",
                    status="SUCCESS",
                    details={},
                    session_id=session_id
                )
        except Exception as e:
            log.warning(f"[INTENT] Failed to persist: {e}", exc_info=True)
        
        sess["_intent_prompted"] = True
        sess["state"] = "INTENT"
        sess["sub_state"] = "INTENT"
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
            sess["_feedback_next_state"] = "REJECTED"
            sess["state"] = "FEEDBACK"
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            await _handle(phone, "__kick__")
            return
        else:
            # User wants to continue - transition to PEEK_CHOICE
            log.info(f"[INTENT] User wants to continue, proceeding to PEEK_CHOICE for {phone}")
            sess["state"] = "PEEK_CHOICE"
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            await _handle(phone, "__kick__")
            return
    
    # This is the first user response (their reason for clicking SERVE link)
    # Generate reflective response using LLM, then transition directly to VIDEO
    log.info(f"[INTENT] User shared their reason, generating reflective response for {phone}")
    
    # Classify response - check for DEFERRAL/STOP first
    intent = classify_intent_simple(text)
    log.info(f"[INTENT] Classification: intent={intent}")
    
    if intent == "DEFERRAL":
        # DEFERRAL/STOP - exit with community link
        log.info(f"[INTENT] User deferred/stopped, sending exit message")
        await mcp_wa_send(phone, INTENT_EXIT)
        _add_to_history(phone, bot_msg=INTENT_EXIT)
        
        # Persistence: Log event for deferral
        try:
            from storage.db import get_db_session
            from storage.event_logger import log_event
            from ..config import settings
            
            with get_db_session() as db:
                session_id = sess.get("_db_session_id")
                log_event(
                    db=db,
                    wa_phone=phone,
                    agent_name=settings.AGENT_NAME,
                    event_type="MOTIVATION_RECEIVED",
                    event_source="user",
                    state="ONBOARDING",
                    sub_state="INTENT",
                    status="SUCCESS",
                    details={"text": text, "deferred": True},
                    session_id=session_id
                )
        except Exception as e:
            log.warning(f"[INTENT] Failed to persist: {e}", exc_info=True)
        
        sess["_feedback_next_state"] = "REJECTED"
        sess["state"] = "FEEDBACK"
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        await _handle(phone, "__kick__")
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
        
        # Send reflective response, then prompt for next step
        await mcp_wa_send(phone, reflective_response)
        _add_to_history(phone, bot_msg=reflective_response)
        
        # Mark that VIDEO_INTRO was already sent (so VIDEO state won't send it again)
        sess["_video_intro_sent"] = True
        
    except Exception as e:
        log.warning(f"[INTENT] Failed to generate reflective response: {e}, using fallback")
        # Fallback: send reflection separately
        fallback_response = "That's wonderful to hear."
        await mcp_wa_send(phone, fallback_response)
        _add_to_history(phone, bot_msg=fallback_response)
        
        # Mark that VIDEO_INTRO was already sent
        sess["_video_intro_sent"] = True
    
    # Persistence: Store motivation text and log event
    now_iso = datetime.now(timezone.utc).isoformat()
    try:
        from storage.db import get_db_session
        from storage.session_store import update_session_state_and_tool_state
        from storage.event_logger import log_event
        from ..config import settings
        
        with get_db_session() as db:
            session_id = sess.get("_db_session_id")
            # Set class_video.offered=true and choice=yes since we're going directly to video
            tool_state_updates = {
                "motivation": {"text": text, "at": now_iso},
                "class_video": {
                    "offered": True,
                    "choice": "yes",  # Auto-selected since we're going directly
                    "at": now_iso
                }
            }
            update_session_state_and_tool_state(
                db=db,
                wa_phone=phone,
                state="ONBOARDING",
                sub_state="VIDEO",
                tool_state_updates=tool_state_updates
            )
            log_event(
                db=db,
                wa_phone=phone,
                agent_name=settings.AGENT_NAME,
                event_type="MOTIVATION_RECEIVED",
                event_source="user",
                state="ONBOARDING",
                sub_state="INTENT",
                status="SUCCESS",
                details={"text": text},
                session_id=session_id
            )
    except Exception as e:
        log.warning(f"[INTENT] Failed to persist: {e}", exc_info=True)
    
    # Transition to PEEK_CHOICE
    log.info(f"[INTENT] Transitioning to PEEK_CHOICE for {phone}")
    sess["state"] = "PEEK_CHOICE"
    sess["sub_state"] = "PEEK_CHOICE"
    sess["ts"] = time.time()
    SESSIONS[phone] = sess
    await _handle(phone, "__kick__")
    return
