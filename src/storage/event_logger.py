"""
Event logger for serve_agent_events (best-effort, non-blocking)
"""
import uuid
import logging
from datetime import datetime, timezone
from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy import select, insert

from .tables import serve_agent_sessions, serve_agent_events

log = logging.getLogger(__name__)


def log_event(
    db: Session,
    wa_phone: str,
    agent_name: str,
    event_type: str,
    event_source: str,
    state: str | None = None,
    sub_state: str | None = None,
    status: str | None = None,
    details: dict | None = None,
    session_id: Optional[uuid.UUID] = None
) -> None:
    """
    Log an event to serve_agent_events (best-effort, never raises).
    
    If session_id is not provided, looks up current session_id by wa_phone.
    
    Args:
        db: Database session
        wa_phone: WhatsApp phone number
        agent_name: Agent name (e.g., "onboarding")
        event_type: Event type (e.g., "SESSION_STARTED", "ELIGIBILITY_RESULT")
        event_source: Event source (e.g., "onboarding_agent")
        state: Current state (optional)
        sub_state: Sub-state (optional)
        status: Event status (optional)
        details: Additional details as dict (optional)
        session_id: Session ID (optional, will lookup if not provided)
    """
    try:
        # Lookup session_id if not provided
        if session_id is None:
            stmt = select(serve_agent_sessions.c.session_id).where(
                serve_agent_sessions.c.wa_phone == wa_phone
            ).order_by(serve_agent_sessions.c.updated_at.desc()).limit(1)
            result = db.execute(stmt).first()
            if result:
                session_id = result[0]
            else:
                log.warning(f"[EVENT_LOGGER] Could not find session_id for wa_phone={wa_phone}, skipping event log for {event_type}")
                return
        
        # Insert event
        event_id = uuid.uuid4()
        occurred_at = datetime.now(timezone.utc)
        
        insert_stmt = insert(serve_agent_events).values(
            event_id=event_id,
            session_id=session_id,
            wa_phone=wa_phone,
            agent_name=agent_name,
            state=state,
            sub_state=sub_state,
            event_type=event_type,
            event_source=event_source,
            event_status=status,
            details=details,
            occurred_at=occurred_at,
        )
        result = db.execute(insert_stmt)
        log.debug(f"[EVENT_LOGGER] Logged event {event_type} for {wa_phone} (event_id: {event_id})")
        # Note: commit handled by context manager - DO NOT rollback here!
        
    except Exception as e:
        # Best-effort: log error but never raise
        # DO NOT rollback - let the context manager handle transaction management
        log.warning(f"[EVENT_LOGGER] Failed to log event {event_type} for {wa_phone}: {e}", exc_info=True)
        # Do not call db.rollback() - it will rollback the entire transaction including session writes!

