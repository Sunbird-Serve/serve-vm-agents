"""
FEEDBACK State Handler (Post-QA feedback collection)
"""
import logging
import time
from datetime import datetime, timezone
from typing import Dict, Any

from ..messages import QA_FEEDBACK_PROMPT, QA_FEEDBACK_BUTTONS, QA_FEEDBACK_CLOSING, format_message

log = logging.getLogger(__name__)


def _normalize_feedback(text: str) -> str | None:
    text_lower = (text or "").lower().strip()
    if "helpful" in text_lower or "great" in text_lower or "good" in text_lower:
        return "great"
    if "not helpful" in text_lower:
        return "needs_improvement"
    if "okay" in text_lower or "ok" in text_lower:
        return "okay"
    if "improve" in text_lower or "needs" in text_lower:
        return "needs_improvement"
    return None


async def handle_feedback(phone: str, text: str, sess: Dict[str, Any], profile: Dict[str, Any]) -> None:
    """
    Handle FEEDBACK state - collect feedback and then close.
    """
    from ..wa_loop import (
        mcp_wa_send, _add_to_history, _handle, SESSIONS
    )

    if text == "__kick__" or not sess.get("_feedback_prompted"):
        msg_id = await mcp_wa_send(phone, QA_FEEDBACK_PROMPT, buttons=QA_FEEDBACK_BUTTONS)
        _add_to_history(phone, bot_msg=QA_FEEDBACK_PROMPT)
        sess["_feedback_prompted"] = True
        sess["_feedback_last_msg_id"] = msg_id
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return

    choice = _normalize_feedback(text)
    if not choice:
        # Re-ask if unclear
        await mcp_wa_send(phone, QA_FEEDBACK_PROMPT, buttons=QA_FEEDBACK_BUTTONS)
        _add_to_history(phone, bot_msg=QA_FEEDBACK_PROMPT)
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return

    # Persist feedback
    try:
        from storage.db import get_db_session
        from storage.session_store import update_session_state_and_tool_state
        from storage.event_logger import log_event
        from ..config import settings

        now_iso = datetime.now(timezone.utc).isoformat()
        with get_db_session() as db:
            session_id = sess.get("_db_session_id")
            update_session_state_and_tool_state(
                db=db,
                wa_phone=phone,
                state="ONBOARDING",
                sub_state="FEEDBACK",
                tool_state_updates={"feedback": {"choice": choice, "at": now_iso}}
            )
            log_event(
                db=db,
                wa_phone=phone,
                agent_name=settings.AGENT_NAME,
                event_type="FEEDBACK_COLLECTED",
                event_source="user",
                state="ONBOARDING",
                sub_state="FEEDBACK",
                status="SUCCESS",
                details={"choice": choice, "raw_text": text},
                session_id=session_id
            )
    except Exception as e:
        log.warning(f"[FEEDBACK] Failed to persist feedback: {e}", exc_info=True)

    # Send closing message and transition to COMPLETE
    name = profile.get("name") or "there"
    closing = format_message(QA_FEEDBACK_CLOSING, name=name)
    await mcp_wa_send(phone, closing)
    _add_to_history(phone, bot_msg=closing)

    sess["state"] = "COMPLETE"
    sess["ts"] = time.time()
    SESSIONS[phone] = sess
    await _handle(phone, "__kick__")

