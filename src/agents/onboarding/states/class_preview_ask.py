"""
CLASS_PREVIEW_ASK State Handler
Ask if user wants to see a class preview video.
"""
import logging
import time
import re
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional

from ..messages import (
    CLASS_PREVIEW_ASK_PROMPT,
    CLASS_PREVIEW_ASK_BUTTONS,
)
from ..validators import is_yes_response, is_no_response

log = logging.getLogger(__name__)


def detect_class_preview_button(text: str, evt: Optional[Dict] = None) -> Optional[str]:
    """Detect button payload from message."""
    if evt:
        data = evt.get("data") or {}
        payload = data.get("payload") or data.get("button_id") or data.get("button_payload")
        if payload:
            if payload == "class_yes":
                return "class_yes"
            elif payload == "class_skip":
                return "class_skip"
    
    text_lower = text.lower().strip()
    
    if text_lower in ["yes, show me", "yes show me", "show me", "yes", "y", "ok", "okay", "sure"]:
        return "class_yes"
    
    if text_lower in ["skip for now", "skip", "not now", "later", "no", "n", "nope"]:
        return "class_skip"
    
    return None


def classify_class_preview_intent(text: str) -> str:
    """Rule-based classification for CLASS_PREVIEW_ASK state."""
    text_lower = text.lower().strip()
    
    # Check for STOP
    if re.search(r"\b(stop|unsubscribe|leave|quit|exit|end)\b", text_lower):
        return "STOP"
    
    # Check for QUERY
    if "?" in text or re.search(r"^(what|how|when|why|where|who|which|can|could|do|does|is|are)\b", text, re.I):
        return "QUERY"
    
    # Check for DEFERRAL
    if re.search(r"\b(later|not now|not right now|another time|some other time|maybe later|after|busy)\b", text_lower):
        return "DEFERRAL"
    
    # Check for CLASS_SKIP
    if is_no_response(text) or any(word in text_lower for word in ["skip", "not now", "later", "no thanks"]):
        return "CLASS_SKIP"
    
    # Check for CLASS_YES
    if is_yes_response(text) or any(word in text_lower for word in ["yes", "show", "see", "watch", "ok", "sure"]):
        return "CLASS_YES"
    
    # Default to CLASS_SKIP
    return "CLASS_SKIP"


async def handle_class_preview_ask(
    phone: str, 
    text: str, 
    sess: Dict[str, Any], 
    profile: Dict[str, Any],
    evt: Optional[Dict] = None
) -> None:
    """Handle CLASS_PREVIEW_ASK state."""
    from ..wa_loop import (
        mcp_wa_send, _add_to_history, _handle, SESSIONS,
        mcp_llm_classify_intent, build_llm_context,
        mcp_deferral_create, _peek_planner_llm
    )
    
    # Entry: Send button message
    if text == "__kick__" or not sess.get("_class_preview_ask_prompted"):
        log.info(f"[CLASS_PREVIEW_ASK] Sending prompt with buttons to {phone}")
        message_id = await mcp_wa_send(phone, CLASS_PREVIEW_ASK_PROMPT, buttons=CLASS_PREVIEW_ASK_BUTTONS)
        _add_to_history(phone, bot_msg=CLASS_PREVIEW_ASK_PROMPT)
        
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
                    sub_state="CLASS_PREVIEW_ASK",
                    last_outbound_msg_id=message_id
                )
                log_event(
                    db=db,
                    wa_phone=phone,
                    agent_name=settings.AGENT_NAME,
                    event_type="CLASS_PREVIEW_ASKED",
                    event_source="agent",
                    state="ONBOARDING",
                    sub_state="CLASS_PREVIEW_ASK",
                    status="SUCCESS",
                    details={"buttons": CLASS_PREVIEW_ASK_BUTTONS},
                    session_id=session_id
                )
        except Exception as e:
            log.warning(f"[CLASS_PREVIEW_ASK] Failed to persist: {e}", exc_info=True)
        
        sess["_class_preview_ask_prompted"] = True
        sess["state"] = "CLASS_PREVIEW_ASK"
        sess["sub_state"] = "CLASS_PREVIEW_ASK"
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
    
    # Handle user response
    button_payload = detect_class_preview_button(text, evt)
    
    if button_payload == "class_yes":
        # Yes -> VIDEO
        log.info(f"[CLASS_PREVIEW_ASK] User clicked 'Yes, show me', proceeding to VIDEO")
        
        # Persistence: Store choice and log event
        now_iso = datetime.now(timezone.utc).isoformat()
        try:
            from storage.db import get_db_session
            from storage.session_store import update_session_state_and_tool_state
            from storage.event_logger import log_event
            from ..config import settings
            
            with get_db_session() as db:
                session_id = sess.get("_db_session_id")
                # Initialize class_video in tool_state
                tool_state_updates = {
                    "class_video": {
                        "offered": True,
                        "choice": "yes",
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
                    event_type="CLASS_PREVIEW_RESPONSE",
                    event_source="user",
                    state="ONBOARDING",
                    sub_state="CLASS_PREVIEW_ASK",
                    status="SUCCESS",
                    details={"choice": "yes", "raw_text": text},
                    session_id=session_id
                )
        except Exception as e:
            log.warning(f"[CLASS_PREVIEW_ASK] Failed to persist: {e}", exc_info=True)
        
        sess["state"] = "VIDEO"
        sess["sub_state"] = "VIDEO"
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        await _handle(phone, "__kick__")
        return
    
    elif button_payload == "class_skip":
        # Skip -> NEEDS_PREVIEW
        log.info(f"[CLASS_PREVIEW_ASK] User clicked 'Skip for now', proceeding to NEEDS_PREVIEW")
        
        # Persistence: Store choice and log event
        now_iso = datetime.now(timezone.utc).isoformat()
        try:
            from storage.db import get_db_session
            from storage.session_store import update_session_state_and_tool_state
            from storage.event_logger import log_event
            from ..config import settings
            
            with get_db_session() as db:
                session_id = sess.get("_db_session_id")
                tool_state_updates = {
                    "class_video": {
                        "offered": True,
                        "choice": "skip",
                        "at": now_iso
                    }
                }
                update_session_state_and_tool_state(
                    db=db,
                    wa_phone=phone,
                    state="ONBOARDING",
                    sub_state="NEEDS_PREVIEW",
                    tool_state_updates=tool_state_updates
                )
                log_event(
                    db=db,
                    wa_phone=phone,
                    agent_name=settings.AGENT_NAME,
                    event_type="CLASS_PREVIEW_RESPONSE",
                    event_source="user",
                    state="ONBOARDING",
                    sub_state="CLASS_PREVIEW_ASK",
                    status="SUCCESS",
                    details={"choice": "skip", "raw_text": text},
                    session_id=session_id
                )
        except Exception as e:
            log.warning(f"[CLASS_PREVIEW_ASK] Failed to persist: {e}", exc_info=True)
        
        sess["state"] = "NEEDS_PREVIEW"
        sess["sub_state"] = "NEEDS_PREVIEW"
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        await _handle(phone, "__kick__")
        return
    
    # Free text fallback
    text_lower = text.lower().strip()
    if re.search(r"\b(stop|unsubscribe|leave|quit|exit|end)\b", text_lower):
        sess["_feedback_next_state"] = "OPTOUT"
        sess["state"] = "FEEDBACK"
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        stop_msg = "Understood. I'll stop messages. If you change your mind, just say 'Hi' here anytime. 💛"
        await mcp_wa_send(phone, stop_msg)
        _add_to_history(phone, bot_msg=stop_msg)
        await _handle(phone, "__kick__")
        return
    
    if re.search(r"\b(later|not now|not right now|another time|some other time|maybe later|after|busy)\b", text_lower):
        volunteer_id = profile.get("uuid") or phone
        try:
            await mcp_deferral_create(
                volunteer_id=volunteer_id,
                reason="user_requested_later",
                until_iso=None,
                idempotency_key=None
            )
        except Exception as e:
            log.warning(f"[CLASS_PREVIEW_ASK] Failed to create deferral: {e}")
        # Local reminder (DB) so we can nudge within 24h
        try:
            from storage.db import get_db_session
            from storage.reminders import add_reminder
            from storage.event_logger import log_event
            from ..config import settings
            from ..messages import INACTIVITY_FOLLOWUP_PROMPT, INACTIVITY_FOLLOWUP_BUTTONS
            when_iso = (datetime.now(timezone.utc) + timedelta(hours=23)).isoformat()
            with get_db_session() as db:
                reminder = add_reminder(
                    db,
                    wa_phone=phone,
                    when_iso=when_iso,
                    reason="user_requested_later",
                    payload={"send_mode": "text", "text": INACTIVITY_FOLLOWUP_PROMPT, "buttons": INACTIVITY_FOLLOWUP_BUTTONS, "feedback_after_send": True},
                )
                log_event(
                    db=db,
                    wa_phone=phone,
                    agent_name=settings.AGENT_NAME,
                    event_type="REMINDER_SCHEDULED",
                    event_source="agent",
                    state="ONBOARDING",
                    sub_state="CLASS_PREVIEW_ASK",
                    status="SUCCESS",
                    details={"reason": "user_requested_later", "when_iso": when_iso, "reminder_id": reminder.get("id")},
                    session_id=sess.get("_db_session_id"),
                )
        except Exception as e:
            log.warning(f"[CLASS_PREVIEW_ASK] Failed to schedule local reminder: {e}", exc_info=True)
        sess["state"] = "DEFERRED"
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
    
    if "?" in text or re.search(r"^(what|how|when|why|where|who|which|can|could|do|does|is|are)\b", text, re.I):
        try:
            llm_context = build_llm_context("CLASS_PREVIEW_ASK", sess)
            llm_result = await mcp_llm_classify_intent(text, "CLASS_PREVIEW_ASK", llm_context)
            tone_reply = llm_result.get("tone_reply", "")
            if tone_reply and len(tone_reply) < 200:
                await mcp_wa_send(phone, tone_reply)
                _add_to_history(phone, bot_msg=tone_reply)
            else:
                await mcp_wa_send(phone, "It's a short video showing how classes work. Would you like to see it?")
                _add_to_history(phone, bot_msg="It's a short video showing how classes work. Would you like to see it?")
        except Exception as e:
            log.warning(f"[CLASS_PREVIEW_ASK] LLM fallback failed: {e}")
            await mcp_wa_send(phone, "It's a short video showing how classes work. Would you like to see it?")
            _add_to_history(phone, bot_msg="It's a short video showing how classes work. Would you like to see it?")
        
        await mcp_wa_send(phone, CLASS_PREVIEW_ASK_PROMPT, buttons=CLASS_PREVIEW_ASK_BUTTONS)
        _add_to_history(phone, bot_msg=CLASS_PREVIEW_ASK_PROMPT)
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
    
    # Use LLM planner for SHOW_VIDEO / SKIP / CLARIFY
    try:
        plan = await _peek_planner_llm(text, stage="VIDEO")
        action = (plan.get("action") or "").upper()
        tone_reply = (plan.get("tone_reply") or "").strip()
    except Exception as e:
        log.warning(f"[CLASS_PREVIEW_ASK] Planner failed: {e}")
        action = ""
        tone_reply = ""
    
    if action == "CLARIFY":
        if tone_reply:
            await mcp_wa_send(phone, tone_reply)
            _add_to_history(phone, bot_msg=tone_reply)
        await mcp_wa_send(phone, CLASS_PREVIEW_ASK_PROMPT, buttons=CLASS_PREVIEW_ASK_BUTTONS)
        _add_to_history(phone, bot_msg=CLASS_PREVIEW_ASK_PROMPT)
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
    
    if action == "SHOW_VIDEO":
        log.info(f"[CLASS_PREVIEW_ASK] Planner chose SHOW_VIDEO, proceeding to VIDEO")
        
        now_iso = datetime.now(timezone.utc).isoformat()
        try:
            from storage.db import get_db_session
            from storage.session_store import update_session_state_and_tool_state
            from storage.event_logger import log_event
            from ..config import settings
            
            with get_db_session() as db:
                session_id = sess.get("_db_session_id")
                tool_state_updates = {
                    "class_video": {
                        "offered": True,
                        "choice": "yes",
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
                    event_type="CLASS_PREVIEW_RESPONSE",
                    event_source="user",
                    state="ONBOARDING",
                    sub_state="CLASS_PREVIEW_ASK",
                    status="SUCCESS",
                    details={"choice": "yes", "raw_text": text},
                    session_id=session_id
                )
        except Exception as e:
            log.warning(f"[CLASS_PREVIEW_ASK] Failed to persist: {e}", exc_info=True)
        
        sess["state"] = "VIDEO"
        sess["sub_state"] = "VIDEO"
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        await _handle(phone, "__kick__")
        return
    
    if action == "SKIP":
        log.info(f"[CLASS_PREVIEW_ASK] Planner chose SKIP, proceeding to NEEDS_PREVIEW")
        
        now_iso = datetime.now(timezone.utc).isoformat()
        try:
            from storage.db import get_db_session
            from storage.session_store import update_session_state_and_tool_state
            from storage.event_logger import log_event
            from ..config import settings
            
            with get_db_session() as db:
                session_id = sess.get("_db_session_id")
                tool_state_updates = {
                    "class_video": {
                        "offered": True,
                        "choice": "skip",
                        "at": now_iso
                    }
                }
                update_session_state_and_tool_state(
                    db=db,
                    wa_phone=phone,
                    state="ONBOARDING",
                    sub_state="NEEDS_PREVIEW",
                    tool_state_updates=tool_state_updates
                )
                log_event(
                    db=db,
                    wa_phone=phone,
                    agent_name=settings.AGENT_NAME,
                    event_type="CLASS_PREVIEW_RESPONSE",
                    event_source="user",
                    state="ONBOARDING",
                    sub_state="CLASS_PREVIEW_ASK",
                    status="SUCCESS",
                    details={"choice": "skip", "raw_text": text},
                    session_id=session_id
                )
        except Exception as e:
            log.warning(f"[CLASS_PREVIEW_ASK] Failed to persist: {e}", exc_info=True)
        
        sess["state"] = "NEEDS_PREVIEW"
        sess["sub_state"] = "NEEDS_PREVIEW"
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        await _handle(phone, "__kick__")
        return
