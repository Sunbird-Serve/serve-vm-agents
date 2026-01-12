"""
READINESS_CHECK State Handler
Check if user is ready for a chat now or wants to come back later.
"""
import logging
import time
import re
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from ..messages import (
    READINESS_CHECK_PROMPT,
    READINESS_CHECK_BUTTONS,
    READINESS_DEFERRED_MSG,
)
from ..validators import is_yes_response, is_no_response

log = logging.getLogger(__name__)


def detect_readiness_button(text: str, evt: Optional[Dict] = None) -> Optional[str]:
    """
    Detect button payload from message.
    
    Returns:
        Payload ID string ("ready_now", "ready_later") or None
    """
    # Priority 1: Check for payload in event data
    if evt:
        data = evt.get("data") or {}
        payload = data.get("payload") or data.get("button_id") or data.get("button_payload")
        if payload:
            if payload == "ready_now":
                return "ready_now"
            elif payload == "ready_later":
                return "ready_later"
    
    # Priority 2: Match button text
    text_lower = text.lower().strip()
    
    # Ready now button
    if text_lower in ["let's chat now", "lets chat now", "chat now", "ready now", 
                      "yes", "y", "ok", "okay", "sure", "go ahead", "let's go", 
                      "lets go", "proceed", "start", "continue"]:
        return "ready_now"
    
    # Later works better button
    if text_lower in ["later works better", "later", "not now", "not right now",
                      "some other time", "another time", "busy", "come back later"]:
        return "ready_later"
    
    return None


def classify_readiness_intent(text: str) -> str:
    """
    Rule-based classification for READINESS_CHECK state.
    
    Returns:
        Intent: READY_CONTINUE, DEFERRAL, STOP, QUERY, or AMBIGUOUS
    """
    text_lower = text.lower().strip()
    
    # Check for STOP
    stop_patterns = [
        r"\b(stop|unsubscribe|leave|quit|exit|end)\b",
        r"\b(don'?t message|dont message|no more messages)\b",
    ]
    for pattern in stop_patterns:
        if re.search(pattern, text_lower):
            return "STOP"
    
    # Check for QUERY
    if "?" in text or re.search(r"^(what|how|when|why|where|who|which|can|could|do|does|is|are)\b", text, re.I):
        return "QUERY"
    
    # Check for DEFERRAL / later
    deferral_patterns = [
        r"\b(later|not now|not right now|another time|some other time|maybe later|after|busy)\b",
        r"\b(can'?t now|cant now|cannot now|will do later|do it later|come back)\b",
    ]
    for pattern in deferral_patterns:
        if re.search(pattern, text_lower):
            return "DEFERRAL"
    
    # Check for READY_CONTINUE
    if is_yes_response(text) or any(word in text_lower for word in [
        "chat", "ready", "go ahead", "proceed", "start", "continue", "let's go", "lets go",
        "sure", "ok", "okay", "yes", "y", "alright", "sounds good", "now"
    ]):
        return "READY_CONTINUE"
    
    # Default to DEFERRAL (respectful of time)
    return "DEFERRAL"


async def handle_readiness_check(
    phone: str, 
    text: str, 
    sess: Dict[str, Any], 
    profile: Dict[str, Any],
    evt: Optional[Dict] = None
) -> None:
    """
    Handle READINESS_CHECK state - check if ready for chat now or later.
    """
    from ..wa_loop import (
        mcp_wa_send, _add_to_history, _handle, SESSIONS,
        mcp_llm_classify_intent, build_llm_context,
        mcp_deferral_create
    )
    
    # Entry: Send button message
    if text == "__kick__" or not sess.get("_readiness_check_prompted"):
        log.info(f"[READINESS_CHECK] Sending prompt with buttons to {phone}")
        message_id = await mcp_wa_send(phone, READINESS_CHECK_PROMPT, buttons=READINESS_CHECK_BUTTONS)
        _add_to_history(phone, bot_msg=READINESS_CHECK_PROMPT)
        
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
                    sub_state="READINESS_CHECK",
                    last_outbound_msg_id=message_id
                )
                log_event(
                    db=db,
                    wa_phone=phone,
                    agent_name=settings.AGENT_NAME,
                    event_type="READINESS_PROMPT_SENT",
                    event_source="agent",
                    state="ONBOARDING",
                    sub_state="READINESS_CHECK",
                    status="SUCCESS",
                    details={"buttons": READINESS_CHECK_BUTTONS},
                    session_id=session_id
                )
        except Exception as e:
            log.warning(f"[READINESS_CHECK] Failed to persist: {e}", exc_info=True)
        
        sess["_readiness_check_prompted"] = True
        sess["state"] = "READINESS_CHECK"
        sess["sub_state"] = "READINESS_CHECK"
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
    
    # Handle user response
    button_payload = detect_readiness_button(text, evt)
    
    if button_payload == "ready_now":
        # Continue -> INTENT
        log.info(f"[READINESS_CHECK] User clicked 'Let's chat now', proceeding to INTENT")
        
        # Persistence: Store readiness choice and log event
        now_iso = datetime.now(timezone.utc).isoformat()
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
                    tool_state_updates={"readiness": {"choice": "now", "at": now_iso}}
                )
                log_event(
                    db=db,
                    wa_phone=phone,
                    agent_name=settings.AGENT_NAME,
                    event_type="READINESS_RESPONSE",
                    event_source="user",
                    state="ONBOARDING",
                    sub_state="READINESS_CHECK",
                    status="SUCCESS",
                    details={"choice": "now", "raw_text": text},
                    session_id=session_id
                )
        except Exception as e:
            log.warning(f"[READINESS_CHECK] Failed to persist: {e}", exc_info=True)
        
        sess["state"] = "INTENT"
        sess["sub_state"] = "INTENT"
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        await _handle(phone, "__kick__")
        return
    
    elif button_payload == "ready_later":
        # Later -> DEFERRED
        log.info(f"[READINESS_CHECK] User clicked 'Later works better', creating deferral")
        await mcp_wa_send(phone, READINESS_DEFERRED_MSG)
        _add_to_history(phone, bot_msg=READINESS_DEFERRED_MSG)
        
        volunteer_id = profile.get("uuid") or phone
        try:
            await mcp_deferral_create(
                volunteer_id=volunteer_id,
                reason="user_requested_later",
                until_iso=None,
                idempotency_key=None
            )
        except Exception as e:
            log.warning(f"[READINESS_CHECK] Failed to create deferral: {e}")
        
        # Persistence: Store readiness choice and log events
        now_iso = datetime.now(timezone.utc).isoformat()
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
                    state="DEFERRED",
                    sub_state=None,
                    tool_state_updates={"readiness": {"choice": "later", "at": now_iso}},
                    end_reason="user_requested_later"
                )
                log_event(
                    db=db,
                    wa_phone=phone,
                    agent_name=settings.AGENT_NAME,
                    event_type="READINESS_RESPONSE",
                    event_source="user",
                    state="ONBOARDING",
                    sub_state="READINESS_CHECK",
                    status="SUCCESS",
                    details={"choice": "later", "raw_text": text},
                    session_id=session_id
                )
                log_event(
                    db=db,
                    wa_phone=phone,
                    agent_name=settings.AGENT_NAME,
                    event_type="SESSION_DEFERRED",
                    event_source="agent",
                    state="DEFERRED",
                    status="SUCCESS",
                    details={"reason": "user_requested_later"},
                    session_id=session_id
                )
        except Exception as e:
            log.warning(f"[READINESS_CHECK] Failed to persist: {e}", exc_info=True)
        
        sess["state"] = "DEFERRED"
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
    
    # Free text fallback
    intent = classify_readiness_intent(text)
    log.info(f"[READINESS_CHECK] Free text classified as: {intent}")
    
    if intent == "STOP":
        # Persistence: Log optout event
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
                    event_type="SESSION_OPTOUT",
                    event_source="user",
                    state="ONBOARDING",
                    sub_state="READINESS_CHECK",
                    status="SUCCESS",
                    details={"raw_text": text},
                    session_id=session_id
                )
        except Exception as e:
            log.warning(f"[READINESS_CHECK] Failed to persist: {e}", exc_info=True)
        
        sess["state"] = "OPTOUT"
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        stop_msg = "Understood. I'll stop messages. If you change your mind, just say 'Hi' here anytime. 💛"
        await mcp_wa_send(phone, stop_msg)
        _add_to_history(phone, bot_msg=stop_msg)
        return
    
    elif intent == "QUERY":
        # Answer briefly, re-send buttons
        try:
            llm_context = build_llm_context("READINESS_CHECK", sess)
            llm_result = await mcp_llm_classify_intent(text, "READINESS_CHECK", llm_context)
            tone_reply = llm_result.get("tone_reply", "")
            if tone_reply and len(tone_reply) < 200:
                await mcp_wa_send(phone, tone_reply)
                _add_to_history(phone, bot_msg=tone_reply)
            else:
                await mcp_wa_send(phone, "I'm here whenever you're ready. What would you like to do?")
                _add_to_history(phone, bot_msg="I'm here whenever you're ready. What would you like to do?")
        except Exception as e:
            log.warning(f"[READINESS_CHECK] LLM fallback failed: {e}")
            await mcp_wa_send(phone, "I'm here whenever you're ready. What would you like to do?")
            _add_to_history(phone, bot_msg="I'm here whenever you're ready. What would you like to do?")
        
        await mcp_wa_send(phone, READINESS_CHECK_PROMPT, buttons=READINESS_CHECK_BUTTONS)
        _add_to_history(phone, bot_msg=READINESS_CHECK_PROMPT)
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
    
    elif intent == "READY_CONTINUE":
        # Continue -> INTENT
        log.info(f"[READINESS_CHECK] User wants to continue, proceeding to INTENT")
        
        # Persistence: Store readiness choice and log event
        now_iso = datetime.now(timezone.utc).isoformat()
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
                    tool_state_updates={"readiness": {"choice": "now", "at": now_iso}}
                )
                log_event(
                    db=db,
                    wa_phone=phone,
                    agent_name=settings.AGENT_NAME,
                    event_type="READINESS_RESPONSE",
                    event_source="user",
                    state="ONBOARDING",
                    sub_state="READINESS_CHECK",
                    status="SUCCESS",
                    details={"choice": "now", "raw_text": text},
                    session_id=session_id
                )
        except Exception as e:
            log.warning(f"[READINESS_CHECK] Failed to persist: {e}", exc_info=True)
        
        sess["state"] = "INTENT"
        sess["sub_state"] = "INTENT"
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        await _handle(phone, "__kick__")
        return
    
    elif intent == "DEFERRAL":
        # DEFERRAL -> DEFERRED
        log.info(f"[READINESS_CHECK] User wants to defer, creating deferral")
        await mcp_wa_send(phone, READINESS_DEFERRED_MSG)
        _add_to_history(phone, bot_msg=READINESS_DEFERRED_MSG)
        
        volunteer_id = profile.get("uuid") or phone
        try:
            await mcp_deferral_create(
                volunteer_id=volunteer_id,
                reason="user_requested_later",
                until_iso=None,
                idempotency_key=None
            )
        except Exception as e:
            log.warning(f"[READINESS_CHECK] Failed to create deferral: {e}")
        
        # Persistence: Store readiness choice and log events
        now_iso = datetime.now(timezone.utc).isoformat()
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
                    state="DEFERRED",
                    sub_state=None,
                    tool_state_updates={"readiness": {"choice": "later", "at": now_iso}}
                )
                log_event(
                    db=db,
                    wa_phone=phone,
                    agent_name=settings.AGENT_NAME,
                    event_type="READINESS_RESPONSE",
                    event_source="user",
                    state="ONBOARDING",
                    sub_state="READINESS_CHECK",
                    status="SUCCESS",
                    details={"choice": "later", "raw_text": text},
                    session_id=session_id
                )
                log_event(
                    db=db,
                    wa_phone=phone,
                    agent_name=settings.AGENT_NAME,
                    event_type="SESSION_DEFERRED",
                    event_source="agent",
                    state="DEFERRED",
                    status="SUCCESS",
                    details={"reason": "user_requested_later"},
                    session_id=session_id
                )
        except Exception as e:
            log.warning(f"[READINESS_CHECK] Failed to persist: {e}", exc_info=True)
        
        sess["state"] = "DEFERRED"
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return

