"""
VIDEO State Handler
Show class preview video and wait for done/skip.
Do NOT block if user doesn't watch - proceed on any response.
If no response after 40 seconds, auto-proceed to NEEDS_PREVIEW.
"""
import logging
import time
import re
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from ..messages import (
    VIDEO_INTRO,
    VIDEO_DONE_PROMPT,
)

log = logging.getLogger(__name__)


def classify_video_intent(text: str) -> str:
    """Rule-based classification for VIDEO state."""
    text_lower = text.lower().strip()
    
    # Check for STOP
    if re.search(r"\b(stop|unsubscribe|leave|quit|exit|end)\b", text_lower):
        return "STOP"
    
    # Any response -> VIDEO_DONE (don't block)
    # Includes: done, watched, okay, sure, anything, etc.
    done_keywords = ["done", "watched", "viewed", "finished", "completed", "ok", "okay", 
                     "yes", "y", "sure", "anything", "skip", "next", "continue", "proceed", "ready",
                     "alright", "fine", "good", "great", "nice"]
    
    if any(keyword in text_lower for keyword in done_keywords):
        return "VIDEO_DONE"
    
    # Default: proceed anyway (don't block) - treat any text as valid response
    return "VIDEO_DONE"


async def handle_video(
    phone: str, 
    text: str, 
    sess: Dict[str, Any], 
    profile: Dict[str, Any],
    evt: Optional[Dict] = None
) -> None:
    """Handle VIDEO state - send in-app video and proceed on any response."""
    from ..wa_loop import (
        mcp_wa_send, mcp_wa_send_class_video,
        _add_to_history, _handle, SESSIONS,
        mcp_deferral_create
    )
    from ..messages import VIDEO_ERROR_MSG
    
    # Entry: Send video intro, upload and send video, then prompt
    if text == "__kick__" or not sess.get("_video_sent"):
        log.info(f"[VIDEO] Sending in-app video to {phone}")
        
        # Check if VIDEO_INTRO was already sent in INTENT state
        if not sess.get("_video_intro_sent"):
            # Send intro (if not already sent)
            intro_msg_id = await mcp_wa_send(phone, VIDEO_INTRO)
            _add_to_history(phone, bot_msg=VIDEO_INTRO)
        else:
            log.info(f"[VIDEO] VIDEO_INTRO already sent in INTENT state, skipping")
            intro_msg_id = None
        
        # Add a 5 second delay before sending video to avoid message dumping
        await asyncio.sleep(5.0)
        
        # Send class video (server handles loading the file internally)
        video_message_id = None
        video_sent_success = False
        try:
            video_message_id = await mcp_wa_send_class_video(phone)
            
            if video_message_id:
                log.info(f"[VIDEO] Video sent successfully, message_id: {video_message_id}")
                _add_to_history(phone, bot_msg="[VIDEO]")
                video_sent_success = True
                
                # Persistence: Log video sent event
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
                            event_type="CLASS_VIDEO_SENT",
                            event_source="tool",
                            state="ONBOARDING",
                            sub_state="VIDEO",
                            status="SUCCESS",
                            details={"media_id": video_message_id, "tool": "serve.whatsapp.send_class_video"},
                            session_id=session_id
                        )
                except Exception as e:
                    log.warning(f"[VIDEO] Failed to log video sent event: {e}", exc_info=True)
            else:
                # Send failed - send error message but continue flow
                log.warning(f"[VIDEO] Failed to send video, sending error message")
                await mcp_wa_send(phone, VIDEO_ERROR_MSG)
                _add_to_history(phone, bot_msg=VIDEO_ERROR_MSG)
                
                # Persistence: Log video send failure
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
                            event_type="CLASS_VIDEO_SENT",
                            event_source="tool",
                            state="ONBOARDING",
                            sub_state="VIDEO",
                            status="FAILURE",
                            details={"error": "Video send returned no message_id", "tool": "serve.whatsapp.send_class_video"},
                            session_id=session_id
                        )
                except Exception as e:
                    log.warning(f"[VIDEO] Failed to log video failure: {e}", exc_info=True)
        except Exception as e:
            log.error(f"[VIDEO] Exception during video send: {e}", exc_info=True)
            # Send error message but continue flow
            await mcp_wa_send(phone, VIDEO_ERROR_MSG)
            _add_to_history(phone, bot_msg=VIDEO_ERROR_MSG)
            
            # Persistence: Log video send exception
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
                        event_type="CLASS_VIDEO_SENT",
                        event_source="tool",
                        state="ONBOARDING",
                        sub_state="VIDEO",
                        status="FAILURE",
                        details={"error": str(e), "tool": "serve.whatsapp.send_class_video"},
                        session_id=session_id
                    )
            except:
                pass
        
        # Persistence: Update state and tool_state
        now_iso = datetime.now(timezone.utc).isoformat()
        try:
            from storage.db import get_db_session
            from storage.session_store import update_session_state_and_tool_state
            from ..config import settings
            
            with get_db_session() as db:
                # Get existing class_video from tool_state (may already be set from INTENT)
                # Read current tool_state first to preserve existing data
                from sqlalchemy import select
                from storage.tables import serve_agent_sessions
                
                stmt = select(serve_agent_sessions.c.tool_state).where(
                    serve_agent_sessions.c.wa_phone == phone
                )
                result = db.execute(stmt).first()
                existing_class_video = {}
                if result and result[0] and isinstance(result[0], dict):
                    existing_class_video = result[0].get("class_video", {})
                
                # Merge with video send data
                class_video_update = existing_class_video.copy() if existing_class_video else {}
                class_video_update.update({
                    "offered": True,
                    "choice": "yes",
                    "media_id": video_message_id if video_sent_success else None,
                    "at": now_iso
                })
                
                tool_state_updates = {
                    "class_video": class_video_update
                }
                update_session_state_and_tool_state(
                    db=db,
                    wa_phone=phone,
                    state="ONBOARDING",
                    sub_state="VIDEO",
                    last_outbound_msg_id=intro_msg_id,
                    tool_state_updates=tool_state_updates
                )
        except Exception as e:
            log.warning(f"[VIDEO] Failed to persist: {e}", exc_info=True)
        
        sess["_video_sent"] = True
        sess["_video_response_received"] = False
        sess["state"] = "VIDEO"
        sess["sub_state"] = "VIDEO"
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        
        # Schedule 40-second timeout to auto-proceed if no response
        async def video_timeout_handler():
            await asyncio.sleep(40.0)
            
            # Check if still in VIDEO state and no response received
            current_sess = SESSIONS.get(phone)
            if (current_sess and
                current_sess.get("state") == "VIDEO" and
                not current_sess.get("_video_response_received")):
                log.info(f"[VIDEO] 40-second timeout reached, auto-proceeding to NEEDS_PREVIEW for {phone}")
                current_sess["state"] = "NEEDS_PREVIEW"
                current_sess["sub_state"] = "NEEDS_PREVIEW"
                current_sess["ts"] = time.time()
                SESSIONS[phone] = current_sess
                # Trigger NEEDS_PREVIEW state handler
                await _handle(phone, "__kick__")
        
        # Create background task for timeout (non-blocking)
        asyncio.create_task(video_timeout_handler())
        return
    
    # Handle user response - send VIDEO_DONE_PROMPT, then proceed to NEEDS_PREVIEW
    intent = classify_video_intent(text)
    log.info(f"[VIDEO] User response classified as: {intent}")
    
    # Mark response as received to prevent timeout
    sess["_video_response_received"] = True
    sess["ts"] = time.time()
    SESSIONS[phone] = sess
    
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
                    sub_state="VIDEO",
                    status="SUCCESS",
                    details={"raw_text": text},
                    session_id=session_id
                )
        except Exception as e:
            log.warning(f"[VIDEO] Failed to persist: {e}", exc_info=True)
        
        sess["state"] = "OPTOUT"
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        stop_msg = "Understood. I'll stop messages. If you change your mind, just say 'Hi' here anytime. 💛"
        await mcp_wa_send(phone, stop_msg)
        _add_to_history(phone, bot_msg=stop_msg)
        return
    
    # Send VIDEO_DONE_PROMPT before proceeding
    log.info(f"[VIDEO] Sending done prompt to {phone}")
    done_msg_id = None
    try:
        done_msg_id = await mcp_wa_send(phone, VIDEO_DONE_PROMPT)
        _add_to_history(phone, bot_msg=VIDEO_DONE_PROMPT)
        log.info(f"[VIDEO] Done prompt sent successfully")
    except Exception as e:
        log.error(f"[VIDEO] Failed to send done prompt: {e}", exc_info=True)
    
    # Persistence: Store video ack and log event
    now_iso = datetime.now(timezone.utc).isoformat()
    try:
        from storage.db import get_db_session
        from storage.session_store import update_session_state_and_tool_state
        from storage.event_logger import log_event
        from ..config import settings
        
        with get_db_session() as db:
            session_id = sess.get("_db_session_id")
            # Get existing class_video from tool_state to preserve media_id
            # Read current tool_state first
            from sqlalchemy import select
            from storage.tables import serve_agent_sessions
            
            stmt = select(serve_agent_sessions.c.tool_state).where(
                serve_agent_sessions.c.wa_phone == phone
            )
            result = db.execute(stmt).first()
            existing_class_video = {}
            if result and result[0] and isinstance(result[0], dict):
                existing_class_video = result[0].get("class_video", {})
            
            # Merge ack into existing class_video
            class_video_update = existing_class_video.copy() if existing_class_video else {}
            class_video_update.update({
                "offered": True,
                "choice": "yes",
                "ack": text[:200],  # Store raw text, max 200 chars
                "at": now_iso
            })
            
            tool_state_updates = {
                "class_video": class_video_update
            }
            update_session_state_and_tool_state(
                db=db,
                wa_phone=phone,
                state="ONBOARDING",
                sub_state="NEEDS_PREVIEW",
                last_outbound_msg_id=done_msg_id,
                tool_state_updates=tool_state_updates
            )
            log_event(
                db=db,
                wa_phone=phone,
                agent_name=settings.AGENT_NAME,
                event_type="CLASS_VIDEO_ACK",
                event_source="user",
                state="ONBOARDING",
                sub_state="VIDEO",
                status="SUCCESS",
                details={"ack": text[:200]},
                session_id=session_id
            )
    except Exception as e:
        log.warning(f"[VIDEO] Failed to persist: {e}", exc_info=True)
    
    # Any other response -> proceed to NEEDS_PREVIEW (don't block)
    log.info(f"[VIDEO] Proceeding to NEEDS_PREVIEW after user response")
    sess["state"] = "NEEDS_PREVIEW"
    sess["sub_state"] = "NEEDS_PREVIEW"
    sess["ts"] = time.time()
    SESSIONS[phone] = sess
    await _handle(phone, "__kick__")
    return

