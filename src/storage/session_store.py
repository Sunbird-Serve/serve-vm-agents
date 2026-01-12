"""
Session store functions for managing serve_agent_sessions
"""
import uuid
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy import select, update, and_

from .tables import serve_agent_sessions

log = logging.getLogger(__name__)


def get_or_create_session(
    db: Session,
    wa_phone: str,
    agent_name: str = "onboarding",
    expires_days: int = 14
) -> dict:
    """
    Get existing active session or create a new one.
    
    Behavior:
    - If row exists for wa_phone and ended=false and expires_at > now: return it
    - Else if row exists but ended=true OR expired: update existing row (new session_id, reset fields)
    - Else: insert a new row
    
    Args:
        db: Database session
        wa_phone: WhatsApp phone number
        agent_name: Agent name (default: "onboarding")
        expires_days: Days until session expires (default: 14)
    
    Returns:
        dict: Session data as dictionary
    """
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(days=expires_days)
    
    # Check for existing session
    stmt = select(serve_agent_sessions).where(
        serve_agent_sessions.c.wa_phone == wa_phone
    )
    result = db.execute(stmt).first()
    
    if result:
        row = result._mapping
        # Check if session is active (not ended and not expired)
        # Ensure expires_at is timezone-aware for comparison
        expires_at = row["expires_at"]
        if expires_at is not None:
            # If timezone-naive, assume UTC and make it timezone-aware
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)
            
            if not row["ended"] and expires_at > now:
                # Return existing active session
                return dict(row)
        
        # Session exists but ended or expired - update it
        new_session_id = uuid.uuid4()
        update_stmt = (
            update(serve_agent_sessions)
            .where(serve_agent_sessions.c.wa_phone == wa_phone)
            .values(
                session_id=new_session_id,
                state="WELCOME",
                sub_state=None,
                last_agent_prompt_id=None,
                last_outbound_msg_id=None,
                temp_name=None,
                temp_email=None,
                temp_phone=None,
                eligibility_status=None,
                eligibility_fail_reason=None,
                eligibility_checked_at=None,
                available_days=None,
                available_time_bands=None,
                retries=None,
                tool_state=None,
                ended=False,
                end_reason=None,
                updated_at=now,
                expires_at=expires_at,
            )
        )
        result = db.execute(update_stmt)
        # Note: commit handled by context manager
        
        # Fetch updated row
        stmt = select(serve_agent_sessions).where(
            serve_agent_sessions.c.session_id == new_session_id
        )
        result = db.execute(stmt).first()
        if result:
            return dict(result._mapping)
        # Fall through to create new if update didn't work
    
    # No existing session - create new one
    new_session_id = uuid.uuid4()
    insert_stmt = serve_agent_sessions.insert().values(
        session_id=new_session_id,
        wa_phone=wa_phone,
        conversation_id=None,
        state="WELCOME",
        sub_state=None,
        last_agent_prompt_id=None,
        last_outbound_msg_id=None,
        temp_name=None,
        temp_email=None,
        temp_phone=None,
        eligibility_status=None,
        eligibility_fail_reason=None,
        eligibility_checked_at=None,
        device_policy="laptop_or_tablet_only",
        available_days=None,
        available_time_bands=None,
        retries=None,
        tool_state=None,
        ended=False,
        end_reason=None,
        created_at=now,
        updated_at=now,
        expires_at=expires_at,
    )
    db.execute(insert_stmt)
    # Note: commit handled by context manager
    
    # Fetch newly created row
    stmt = select(serve_agent_sessions).where(
        serve_agent_sessions.c.session_id == new_session_id
    )
    result = db.execute(stmt).first()
    return dict(result._mapping)


def update_identity_temp(
    db: Session,
    wa_phone: str,
    temp_name: Optional[str] = None,
    temp_email: Optional[str] = None,
    temp_phone: Optional[str] = None
) -> None:
    """
    Update temporary identity fields (name, email, phone).
    
    Only updates non-None fields. Creates session row if it doesn't exist.
    
    Args:
        db: Database session
        wa_phone: WhatsApp phone number
        temp_name: Temporary name (optional)
        temp_email: Temporary email (optional)
        temp_phone: Temporary phone (optional)
    """
    # First, ensure session exists in DB
    stmt = select(serve_agent_sessions.c.session_id).where(
        serve_agent_sessions.c.wa_phone == wa_phone
    )
    result = db.execute(stmt).first()
    
    if not result:
        # Session doesn't exist - create it first
        log.warning(f"[SESSION_STORE] Session not found for {wa_phone}, creating it")
        get_or_create_session(db, wa_phone=wa_phone, agent_name="onboarding")
    
    now = datetime.now(timezone.utc)
    values = {"updated_at": now}
    
    if temp_name is not None:
        values["temp_name"] = temp_name
    if temp_email is not None:
        values["temp_email"] = temp_email
    if temp_phone is not None:
        values["temp_phone"] = temp_phone
    
    if len(values) > 1:  # More than just updated_at
        update_stmt = (
            update(serve_agent_sessions)
            .where(serve_agent_sessions.c.wa_phone == wa_phone)
            .values(**values)
        )
        result = db.execute(update_stmt)
        # Verify that update affected at least one row
        if result.rowcount == 0:
            log.error(f"[SESSION_STORE] Update identity failed: no rows affected for {wa_phone}")
            raise RuntimeError(f"Failed to update identity for {wa_phone}: no matching session found")
        log.info(f"[SESSION_STORE] Updated identity for {wa_phone}: {result.rowcount} row(s) affected")
    # Note: commit handled by context manager


def finalize_onboarding(
    db: Session,
    wa_phone: str,
    eligibility_status: str,
    available_days: Optional[list[str]] = None,
    available_time_bands: Optional[list[str]] = None,
    end_reason: str = "completed"
) -> None:
    """
    Finalize onboarding session by updating eligibility, preferences, and marking as ended.
    
    Args:
        db: Database session
        wa_phone: WhatsApp phone number
        eligibility_status: Eligibility status (e.g., "ELIGIBLE", "REJECTED")
        available_days: List of available days (optional)
        available_time_bands: List of available time bands (optional)
        end_reason: Reason for ending (default: "completed")
    """
    now = datetime.now(timezone.utc)
    
    update_stmt = (
        update(serve_agent_sessions)
        .where(serve_agent_sessions.c.wa_phone == wa_phone)
        .values(
            eligibility_status=eligibility_status,
            eligibility_checked_at=now,
            available_days=available_days,
            available_time_bands=available_time_bands,
            ended=True,
            end_reason=end_reason,
            state="CLOSE",
            updated_at=now,
        )
    )
    result = db.execute(update_stmt)
    # Verify that update affected at least one row
    if result.rowcount == 0:
        log.error(f"[SESSION_STORE] Finalize onboarding failed: no rows affected for {wa_phone}")
        raise RuntimeError(f"Failed to finalize onboarding for {wa_phone}: no matching session found")
    log.info(f"[SESSION_STORE] Finalized onboarding for {wa_phone}: {result.rowcount} row(s) affected")
    # Note: commit handled by context manager


def set_state(
    db: Session,
    wa_phone: str,
    state: str,
    sub_state: Optional[str] = None,
    last_agent_prompt_id: Optional[str] = None,
    last_outbound_msg_id: Optional[str] = None
) -> None:
    """
    Update session state and related fields.
    
    Args:
        db: Database session
        wa_phone: WhatsApp phone number
        state: New state
        sub_state: Sub-state (optional)
        last_agent_prompt_id: Last agent prompt ID (optional)
        last_outbound_msg_id: Last outbound message ID (optional)
    """
    now = datetime.now(timezone.utc)
    values = {
        "state": state,
        "updated_at": now,
    }
    
    if sub_state is not None:
        values["sub_state"] = sub_state
    if last_agent_prompt_id is not None:
        values["last_agent_prompt_id"] = last_agent_prompt_id
    if last_outbound_msg_id is not None:
        values["last_outbound_msg_id"] = last_outbound_msg_id
    
    update_stmt = (
        update(serve_agent_sessions)
        .where(serve_agent_sessions.c.wa_phone == wa_phone)
        .values(**values)
    )
    result = db.execute(update_stmt)
    if result.rowcount == 0:
        log.warning(f"[SESSION_STORE] Set state failed: no rows affected for {wa_phone}")
    # Note: commit handled by context manager


def update_session_state_and_tool_state(
    db: Session,
    wa_phone: str,
    state: str,
    sub_state: Optional[str] = None,
    last_outbound_msg_id: Optional[str] = None,
    last_agent_prompt_id: Optional[str] = None,
    tool_state_updates: Optional[dict] = None,
    retries: Optional[dict] = None
) -> None:
    """
    Update session state, sub_state, and merge tool_state updates atomically.
    
    Args:
        db: Database session
        wa_phone: WhatsApp phone number
        state: New state (e.g., "ONBOARDING")
        sub_state: Sub-state (e.g., "READINESS_CHECK", "INTENT", etc.)
        last_outbound_msg_id: Last outbound message ID (optional)
        last_agent_prompt_id: Last agent prompt ID (optional)
        tool_state_updates: Dict of tool_state fields to merge (e.g., {"readiness": {...}})
        retries: Retries dict to update (optional)
    """
    now = datetime.now(timezone.utc)
    
    # Get current tool_state
    stmt = select(serve_agent_sessions.c.tool_state).where(
        serve_agent_sessions.c.wa_phone == wa_phone
    )
    result = db.execute(stmt).first()
    
    tool_state = {}
    if result and result[0] and isinstance(result[0], dict):
        tool_state = result[0].copy()
    
    # Merge tool_state updates
    if tool_state_updates:
        tool_state.update(tool_state_updates)
    
    # Build update values
    values = {
        "state": state,
        "updated_at": now,
    }
    
    if sub_state is not None:
        values["sub_state"] = sub_state
    if last_outbound_msg_id is not None:
        values["last_outbound_msg_id"] = last_outbound_msg_id
    if last_agent_prompt_id is not None:
        values["last_agent_prompt_id"] = last_agent_prompt_id
    if tool_state_updates:
        values["tool_state"] = tool_state
    if retries is not None:
        values["retries"] = retries
    
    update_stmt = (
        update(serve_agent_sessions)
        .where(serve_agent_sessions.c.wa_phone == wa_phone)
        .values(**values)
    )
    result = db.execute(update_stmt)
    if result.rowcount == 0:
        log.warning(f"[SESSION_STORE] Update session state failed: no rows affected for {wa_phone}")
    # Note: commit handled by context manager


def get_last_inbound_id(db: Session, wa_phone: str) -> Optional[str]:
    """
    Get the last processed inbound message ID from tool_state.
    
    Args:
        db: Database session
        wa_phone: WhatsApp phone number
        
    Returns:
        Optional[str]: Last inbound message ID, or None if not found
    """
    stmt = select(serve_agent_sessions.c.tool_state).where(
        serve_agent_sessions.c.wa_phone == wa_phone
    )
    result = db.execute(stmt).first()
    
    if not result:
        return None
    
    tool_state = result[0]
    if not tool_state or not isinstance(tool_state, dict):
        return None
    
    idempotency = tool_state.get("idempotency", {})
    return idempotency.get("last_inbound_msg_id")


def set_last_inbound_id(
    db: Session,
    wa_phone: str,
    inbound_msg_id: str,
    outbound_msg_id: Optional[str] = None
) -> None:
    """
    Update idempotency info in tool_state JSONB.
    
    Stores:
    - last_inbound_msg_id
    - last_inbound_at (timestamp)
    - last_outbound_msg_id (if provided)
    
    Args:
        db: Database session
        wa_phone: WhatsApp phone number
        inbound_msg_id: Inbound message ID
        outbound_msg_id: Outbound message ID (optional)
    """
    now = datetime.now(timezone.utc)
    
    # Get current tool_state
    stmt = select(serve_agent_sessions.c.tool_state).where(
        serve_agent_sessions.c.wa_phone == wa_phone
    )
    result = db.execute(stmt).first()
    
    tool_state = {}
    if result and result[0] and isinstance(result[0], dict):
        tool_state = result[0].copy()
    
    # Update idempotency section
    if "idempotency" not in tool_state:
        tool_state["idempotency"] = {}
    
    tool_state["idempotency"]["last_inbound_msg_id"] = inbound_msg_id
    tool_state["idempotency"]["last_inbound_at"] = now.isoformat()
    if outbound_msg_id is not None:
        tool_state["idempotency"]["last_outbound_msg_id"] = outbound_msg_id
    
    # Update database
    update_stmt = (
        update(serve_agent_sessions)
        .where(serve_agent_sessions.c.wa_phone == wa_phone)
        .values(
            tool_state=tool_state,
            updated_at=now
        )
    )
    result = db.execute(update_stmt)
    if result.rowcount == 0:
        log.warning(f"[SESSION_STORE] Set last_inbound_id failed: no rows affected for {wa_phone}")
    # Note: commit handled by context manager

