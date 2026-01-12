"""
CONTINUE_CONFIRM State Handler
Confirm continuation with time expectation (10 minutes).
"""
import logging
import time
import re
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from ..messages import (
    CONTINUE_CONFIRM_PROMPT,
    CONTINUE_CONFIRM_BUTTONS,
    CONTINUE_CONFIRM_DEFERRED_MSG,
)
from ..validators import is_yes_response, is_no_response

log = logging.getLogger(__name__)


def detect_continue_confirm_button(text: str, evt: Optional[Dict] = None) -> Optional[str]:
    """Detect button payload from message."""
    if evt:
        data = evt.get("data") or {}
        payload = data.get("payload") or data.get("button_id") or data.get("button_payload")
        if payload:
            if payload == "confirm_continue":
                return "confirm_continue"
            elif payload == "confirm_later":
                return "confirm_later"
    
    text_lower = text.lower().strip()
    
    # Continue button
    if text_lower in ["yes, continue", "yes continue", "continue", "yes", "y", "ok", "okay", 
                      "sure", "go ahead", "let's go", "lets go", "proceed", "start"]:
        return "confirm_continue"
    
    # Later button
    if text_lower in ["i'll come back later", "ill come back later", "come back later", "later",
                      "not now", "not right now", "some other time", "another time", "busy"]:
        return "confirm_later"
    
    return None


def classify_continue_confirm_intent(text: str) -> str:
    """Rule-based classification for CONTINUE_CONFIRM state."""
    text_lower = text.lower().strip()
    
    # Check for STOP
    if re.search(r"\b(stop|unsubscribe|leave|quit|exit|end)\b", text_lower):
        return "STOP"
    
    # Check for DEFERRAL / later
    deferral_patterns = [
        r"\b(later|not now|not right now|another time|some other time|maybe later|after|busy|come back)\b",
        r"\b(can'?t now|cant now|cannot now|will do later|do it later)\b",
    ]
    for pattern in deferral_patterns:
        if re.search(pattern, text_lower):
            return "DEFERRAL"
    
    # Check for CONTINUE
    if is_yes_response(text) or any(word in text_lower for word in [
        "continue", "go ahead", "proceed", "start", "ready", "let's go", "lets go",
        "sure", "ok", "okay", "yes", "y", "alright", "sounds good", "10 minutes", "10 mins"
    ]):
        return "CONTINUE"
    
    # Default to DEFERRAL (respectful of time)
    return "DEFERRAL"


async def handle_continue_confirm(
    phone: str, 
    text: str, 
    sess: Dict[str, Any], 
    profile: Dict[str, Any],
    evt: Optional[Dict] = None
) -> None:
    """Handle CONTINUE_CONFIRM state - confirm continuation with time expectation."""
    from ..wa_loop import (
        mcp_wa_send, _add_to_history, _handle, SESSIONS,
        mcp_deferral_create
    )
    
    # Entry: Send button message
    if text == "__kick__" or not sess.get("_continue_confirm_prompted"):
        log.info(f"[CONTINUE_CONFIRM] Sending prompt with buttons to {phone}")
        message_id = await mcp_wa_send(phone, CONTINUE_CONFIRM_PROMPT, buttons=CONTINUE_CONFIRM_BUTTONS)
        _add_to_history(phone, bot_msg=CONTINUE_CONFIRM_PROMPT)
        
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
                    sub_state="CONTINUE_CONFIRM",
                    last_outbound_msg_id=message_id
                )
                log_event(
                    db=db,
                    wa_phone=phone,
                    agent_name=settings.AGENT_NAME,
                    event_type="CONTINUE_CONFIRM_SENT",
                    event_source="agent",
                    state="ONBOARDING",
                    sub_state="CONTINUE_CONFIRM",
                    status="SUCCESS",
                    details={"time_hint": "10 minutes", "buttons": CONTINUE_CONFIRM_BUTTONS},
                    session_id=session_id
                )
        except Exception as e:
            log.warning(f"[CONTINUE_CONFIRM] Failed to persist: {e}", exc_info=True)
        
        sess["_continue_confirm_prompted"] = True
        sess["state"] = "CONTINUE_CONFIRM"
        sess["sub_state"] = "CONTINUE_CONFIRM"
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
    
    # Handle user response
    button_payload = detect_continue_confirm_button(text, evt)
    
    if button_payload == "confirm_continue":
        # Continue -> ELIGIBILITY
        log.info(f"[CONTINUE_CONFIRM] User clicked 'Yes, continue', proceeding to ELIGIBILITY")
        
        # Persistence: Store choice and log event
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
                    sub_state="ELIGIBILITY",
                    tool_state_updates={"continue_confirm": {"choice": "yes", "at": now_iso}}
                )
                log_event(
                    db=db,
                    wa_phone=phone,
                    agent_name=settings.AGENT_NAME,
                    event_type="CONTINUE_CONFIRM_RESPONSE",
                    event_source="user",
                    state="ONBOARDING",
                    sub_state="CONTINUE_CONFIRM",
                    status="SUCCESS",
                    details={"choice": "yes", "raw_text": text},
                    session_id=session_id
                )
        except Exception as e:
            log.warning(f"[CONTINUE_CONFIRM] Failed to persist: {e}", exc_info=True)
        
        sess["state"] = "ELIGIBILITY"
        sess["sub_state"] = "ELIGIBILITY"
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        await _handle(phone, "__kick__")
        return
    
    elif button_payload == "confirm_later":
        # Later -> DEFERRED
        log.info(f"[CONTINUE_CONFIRM] User clicked 'I'll come back later', creating deferral")
        await mcp_wa_send(phone, CONTINUE_CONFIRM_DEFERRED_MSG)
        _add_to_history(phone, bot_msg=CONTINUE_CONFIRM_DEFERRED_MSG)
        
        volunteer_id = profile.get("uuid") or phone
        try:
            await mcp_deferral_create(
                volunteer_id=volunteer_id,
                reason="user_requested_later",
                until_iso=None,
                idempotency_key=None
            )
        except Exception as e:
            log.warning(f"[CONTINUE_CONFIRM] Failed to create deferral: {e}")
        
        # Persistence: Store choice and log events
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
                    tool_state_updates={"continue_confirm": {"choice": "later", "at": now_iso}}
                )
                log_event(
                    db=db,
                    wa_phone=phone,
                    agent_name=settings.AGENT_NAME,
                    event_type="CONTINUE_CONFIRM_RESPONSE",
                    event_source="user",
                    state="ONBOARDING",
                    sub_state="CONTINUE_CONFIRM",
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
            log.warning(f"[CONTINUE_CONFIRM] Failed to persist: {e}", exc_info=True)
        
        sess["state"] = "DEFERRED"
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
    
    # Free text fallback
    intent = classify_continue_confirm_intent(text)
    log.info(f"[CONTINUE_CONFIRM] Free text classified as: {intent}")
    
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
                    sub_state="CONTINUE_CONFIRM",
                    status="SUCCESS",
                    details={"raw_text": text},
                    session_id=session_id
                )
        except Exception as e:
            log.warning(f"[CONTINUE_CONFIRM] Failed to persist: {e}", exc_info=True)
        
        sess["state"] = "OPTOUT"
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        stop_msg = "Understood. I'll stop messages. If you change your mind, just say 'Hi' here anytime. 💛"
        await mcp_wa_send(phone, stop_msg)
        _add_to_history(phone, bot_msg=stop_msg)
        return
    
    elif intent == "CONTINUE":
        # Continue -> ELIGIBILITY
        log.info(f"[CONTINUE_CONFIRM] User wants to continue, proceeding to ELIGIBILITY")
        
        # Persistence: Store choice and log event
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
                    sub_state="ELIGIBILITY",
                    tool_state_updates={"continue_confirm": {"choice": "yes", "at": now_iso}}
                )
                log_event(
                    db=db,
                    wa_phone=phone,
                    agent_name=settings.AGENT_NAME,
                    event_type="CONTINUE_CONFIRM_RESPONSE",
                    event_source="user",
                    state="ONBOARDING",
                    sub_state="CONTINUE_CONFIRM",
                    status="SUCCESS",
                    details={"choice": "yes", "raw_text": text},
                    session_id=session_id
                )
        except Exception as e:
            log.warning(f"[CONTINUE_CONFIRM] Failed to persist: {e}", exc_info=True)
        
        sess["state"] = "ELIGIBILITY"
        sess["sub_state"] = "ELIGIBILITY"
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        await _handle(phone, "__kick__")
        return
    
    elif intent == "DEFERRAL":
        # DEFERRAL -> DEFERRED
        log.info(f"[CONTINUE_CONFIRM] User wants to defer, creating deferral")
        await mcp_wa_send(phone, CONTINUE_CONFIRM_DEFERRED_MSG)
        _add_to_history(phone, bot_msg=CONTINUE_CONFIRM_DEFERRED_MSG)
        
        volunteer_id = profile.get("uuid") or phone
        try:
            await mcp_deferral_create(
                volunteer_id=volunteer_id,
                reason="user_requested_later",
                until_iso=None,
                idempotency_key=None
            )
        except Exception as e:
            log.warning(f"[CONTINUE_CONFIRM] Failed to create deferral: {e}")
        
        # Persistence: Store choice and log events
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
                    tool_state_updates={"continue_confirm": {"choice": "later", "at": now_iso}}
                )
                log_event(
                    db=db,
                    wa_phone=phone,
                    agent_name=settings.AGENT_NAME,
                    event_type="CONTINUE_CONFIRM_RESPONSE",
                    event_source="user",
                    state="ONBOARDING",
                    sub_state="CONTINUE_CONFIRM",
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
            log.warning(f"[CONTINUE_CONFIRM] Failed to persist: {e}", exc_info=True)
        
        sess["state"] = "DEFERRED"
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return

