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
    VIDEO_FOOTER,
    VIDEO_DONE_PROMPT,
    PEEK_NEEDS_PROMPT,
    PEEK_SKIP_MESSAGE,
    PEEK_MAYBE_MESSAGE,
)

log = logging.getLogger(__name__)
VIDEO_ABOUT_MSG = (
    "This is a real class demo led by a volunteer, shared to give you a feel for how a SERVE session looks."
)


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


def is_video_about_question(text: str) -> bool:
    """Detect questions about what the video/class is about."""
    if not text:
        return False
    text_lower = text.lower()
    if "video" not in text_lower and "class" not in text_lower:
        return False
    if "?" in text_lower:
        return True
    return bool(re.search(r"\b(what|how|is|this|about|shown|demo)\b", text_lower))


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
        mcp_deferral_create, _peek_planner_llm
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
        
        # Footer + needs preview question in the same message
        await asyncio.sleep(2.0)
        combined_footer = f"{VIDEO_FOOTER}\n\n{PEEK_NEEDS_PROMPT}"
        await mcp_wa_send(phone, combined_footer)
        _add_to_history(phone, bot_msg=combined_footer)
        sess["_video_needs_prompted"] = True
        
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
    
    # If user asked about the video, answer briefly before proceeding
    if is_video_about_question(text):
        await mcp_wa_send(phone, VIDEO_ABOUT_MSG)
        _add_to_history(phone, bot_msg=VIDEO_ABOUT_MSG)
        sess["_state_handled_question"] = True
    
    # Send VIDEO_DONE_PROMPT before proceeding (skip if we already asked needs preview)
    done_msg_id = None
    if not sess.get("_video_needs_prompted"):
        log.info(f"[VIDEO] Sending done prompt to {phone}")
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
    
    # Decide needs preview action based on user's reply
    text_lower = (text or "").lower().strip()
    needs_action = None
    tone_reply = ""
    if text_lower in {"maybe", "maybe later"}:
        needs_action = "SKIP"
        sess["_peek_soft_deferral"] = True
    elif text_lower in {"no", "nope", "nah", "no thanks", "not now", "skip"}:
        needs_action = "SKIP"
    elif text_lower in {"yes", "y", "sure", "ok", "okay", "show", "see", "view", "yes please"}:
        needs_action = "SHOW_NEEDS"
    else:
        try:
            plan = await _peek_planner_llm(text, stage="NEEDS")
            needs_action = (plan.get("action") or "").upper()
            tone_reply = (plan.get("tone_reply") or "").strip()
        except Exception as e:
            log.warning(f"[VIDEO] Needs planner failed: {e}")
            needs_action = ""
            tone_reply = ""
    
    if needs_action == "CLARIFY":
        if tone_reply:
            await mcp_wa_send(phone, tone_reply)
            _add_to_history(phone, bot_msg=tone_reply)
        else:
            await mcp_wa_send(phone, PEEK_NEEDS_PROMPT)
            _add_to_history(phone, bot_msg=PEEK_NEEDS_PROMPT)
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
    if needs_action == "SKIP":
        skip_msg = PEEK_MAYBE_MESSAGE if sess.get("_peek_soft_deferral") else PEEK_SKIP_MESSAGE
        await mcp_wa_send(phone, skip_msg)
        _add_to_history(phone, bot_msg=skip_msg)
        sess.pop("_peek_soft_deferral", None)
        next_state = "CONTINUE_CONFIRM"
    else:
        next_state = "NEEDS_PREVIEW"

    # Route to next state (ignore _video_next_state if we already prompted for needs)
    if not sess.get("_video_needs_prompted"):
        next_state = sess.pop("_video_next_state", next_state)
    log.info(f"[VIDEO] Proceeding to {next_state} after user response")
    sess["state"] = next_state
    sess["sub_state"] = next_state
    sess["ts"] = time.time()
    SESSIONS[phone] = sess
    await _handle(phone, "__kick__")
    return

