"""
Fulfillment Agent - Main Handler

Handles the Fulfillment Agent state machine for opportunity discovery and nomination.
"""
import logging
import time
import re
from typing import Optional, List, Dict

from .types import FulfillmentState, NeedCard
from .prompts import (
    FULFILL_INTRO_MSG,
    FULFILL_LIST_HEADER,
    FULFILL_INVALID_PICK_MSG,
    FULFILL_CONFIRM_SUCCESS_MSG,
    FULFILL_CONFIRM_FAILED_MSG,
    FULFILL_EXIT_MSG,
    format_need_list,
)
from .config import settings

log = logging.getLogger(__name__)

# These will be imported from the main routing/dispatcher
# For now, we'll use late imports to avoid circular dependencies
_mcp_wa_send = None
_log_event = None


def _get_mcp_wa_send():
    """Lazy import of mcp_wa_send to avoid circular dependencies"""
    global _mcp_wa_send
    if _mcp_wa_send is None:
        from agents.onboarding.wa_loop import mcp_wa_send
        _mcp_wa_send = mcp_wa_send
    return _mcp_wa_send


def _get_log_event():
    """Lazy import of log_event to avoid circular dependencies"""
    global _log_event
    if _log_event is None:
        from storage.event_logger import log_event
        _log_event = log_event
    return _log_event


# ========== Stub Functions (to be replaced with real API calls) ==========

async def fetch_open_needs() -> List[NeedCard]:
    """
    Fetch open needs/opportunities (STUB - returns fixed list).
    
    TODO: Replace with real Serve API call to fetch open needs.
    
    Returns:
        List of NeedCard objects
    """
    # Stub: return fixed list of 3 needs
    return [
        NeedCard(
            need_id="need_001",
            title="Grade 11 – Computer Fundamentals & MS Office",
            org_name="Women's Degree College",
            location="Begumpet — Hyderabad, Telangana",
            days_text="Mon & Wed",
            time_text="3:30–4:30 PM IST"
        ),
        NeedCard(
            need_id="need_002",
            title="Grade 8 – Mathematics Basics",
            org_name="Government High School",
            location="Secunderabad, Telangana",
            days_text="Tue & Thu",
            time_text="4:00–5:00 PM IST"
        ),
        NeedCard(
            need_id="need_003",
            title="Grade 10 – English Communication",
            org_name="Community Learning Center",
            location="Hitech City, Hyderabad, Telangana",
            days_text="Sat",
            time_text="10:00 AM–12:00 PM IST"
        ),
    ]


async def nominate_selected_need(need_id: str, nominated_user_id: str) -> Dict[str, bool]:
    """
    Nominate volunteer for selected need (STUB - always returns success).
    
    TODO: Replace with real Serve API call to create nomination.
    
    Args:
        need_id: ID of the selected need
        nominated_user_id: User/volunteer identifier (phone or user_id)
    
    Returns:
        Dict with "success" key (boolean)
    """
    # Stub: always return success
    log.info(f"[FULFILLMENT] Stub: Nominating {nominated_user_id} for need {need_id}")
    return {"success": True}


# ========== State Handlers ==========

async def handle_fulfillment(phone: str, text: str, session: dict):
    """
    Main handler for Fulfillment Agent state machine.
    
    Args:
        phone: WhatsApp phone number
        text: Inbound message text
        session: Current session data (state, profile, etc.) - modified in place
    """
    state = session.get("state", FulfillmentState.INTRO)
    
    log.info(f"[FULFILLMENT] Handling state={state} for {phone}, text='{text[:30]}...'")
    
    # Route to appropriate state handler
    if state == FulfillmentState.INTRO:
        await handle_fulfill_intro(phone, text, session)
    elif state == FulfillmentState.LIST:
        await handle_fulfill_list(phone, text, session)
    elif state == FulfillmentState.WAIT_PICK:
        await handle_fulfill_wait_pick(phone, text, session)
    elif state == FulfillmentState.NOMINATE:
        await handle_fulfill_nominate(phone, text, session)
    elif state == FulfillmentState.DONE:
        await handle_fulfill_done(phone, text, session)
    elif state == FulfillmentState.EXIT:
        await handle_fulfill_exit(phone, session)
    else:
        log.warning(f"[FULFILLMENT] Unknown state: {state}, defaulting to INTRO")
        session["state"] = FulfillmentState.INTRO
        await handle_fulfill_intro(phone, "__kick__", session)


async def handle_fulfill_intro(phone: str, text: str, session: dict):
    """
    Handle FULFILL_INTRO state: ask consent to see open opportunities.
    
    On __kick__: send FULFILL_INTRO_MSG and set state=FULFILL_INTRO.
    If user says Yes -> go to FULFILL_LIST.
    If user says No -> send exit msg and set state=FULFILL_EXIT.
    """
    if text == "__kick__" or not session.get("_fulfill_intro_sent"):
        log.info(f"[FULFILLMENT] Starting fulfillment for {phone}")
        
        # Log FULFILL_STARTED event
        try:
            from storage.db import get_db_session
            log_event = _get_log_event()
            with get_db_session() as db:
                log_event(
                    db=db,
                    wa_phone=phone,
                    agent_name=settings.AGENT_NAME,
                    event_type="FULFILL_STARTED",
                    event_source="fulfillment_agent",
                    state=FulfillmentState.INTRO,
                    status="started"
                )
        except Exception as e:
            log.warning(f"[FULFILLMENT] Failed to log FULFILL_STARTED event: {e}")
        
        # Send intro message
        mcp_wa_send = _get_mcp_wa_send()
        await mcp_wa_send(phone, FULFILL_INTRO_MSG)
        
        session["_fulfill_intro_sent"] = True
        session["state"] = FulfillmentState.INTRO
        session["ts"] = time.time()
        
        from agents.onboarding.wa_loop import SESSIONS
        SESSIONS[phone] = session
    else:
        # User replied: check Yes/No
        text_lower = text.lower().strip()
        
        # Check for Yes
        if text_lower in ["yes", "y", "yeah", "yep", "sure", "ok", "okay"]:
            log.info(f"[FULFILLMENT] User {phone} said Yes, proceeding to LIST")
            session["state"] = FulfillmentState.LIST
            session["ts"] = time.time()
            
            from agents.onboarding.wa_loop import SESSIONS
            SESSIONS[phone] = session
            
            # Move to list state
            await handle_fulfill_list(phone, "__kick__", session)
        # Check for No
        elif text_lower in ["no", "n", "nope", "not now", "later"]:
            log.info(f"[FULFILLMENT] User {phone} said No, exiting")
            session["state"] = FulfillmentState.EXIT
            session["ts"] = time.time()
            
            from agents.onboarding.wa_loop import SESSIONS
            SESSIONS[phone] = session
            
            # Handle exit
            await handle_fulfill_exit(phone, session)
        else:
            # Ambiguous response - re-ask
            log.info(f"[FULFILLMENT] Ambiguous response from {phone}, re-asking")
            mcp_wa_send = _get_mcp_wa_send()
            await mcp_wa_send(phone, FULFILL_INTRO_MSG)


async def handle_fulfill_list(phone: str, text: str, session: dict):
    """
    Handle FULFILL_LIST state: fetch needs (stub), render list, prompt to pick.
    
    Sends list + prompt and sets state=FULFILL_WAIT_PICK.
    """
    if text == "__kick__" or not session.get("_fulfill_list_sent"):
        log.info(f"[FULFILLMENT] Fetching and displaying needs list for {phone}")
        
        # Fetch needs (stub)
        needs = await fetch_open_needs()
        
        # Format list
        list_message = format_need_list(needs, max_items=5)
        
        # Send list
        mcp_wa_send = _get_mcp_wa_send()
        await mcp_wa_send(phone, list_message)
        
        # Store needs in session for validation
        session["fulfillment"] = session.get("fulfillment", {})
        session["fulfillment"]["needs"] = [need.to_dict() for need in needs]
        
        # Create choice mapping: {"1": need_id1, "2": need_id2, ...}
        choice_map = {}
        for idx, need in enumerate(needs[:5], start=1):
            choice_map[str(idx)] = need.need_id
        session["fulfillment"]["choice_map"] = choice_map
        
        # Log NEEDS_LIST_SHOWN event
        try:
            from storage.db import get_db_session
            log_event = _get_log_event()
            with get_db_session() as db:
                log_event(
                    db=db,
                    wa_phone=phone,
                    agent_name=settings.AGENT_NAME,
                    event_type="NEEDS_LIST_SHOWN",
                    event_source="fulfillment_agent",
                    state=FulfillmentState.LIST,
                    status="shown",
                    details={"count": len(needs)}
                )
        except Exception as e:
            log.warning(f"[FULFILLMENT] Failed to log NEEDS_LIST_SHOWN event: {e}")
        
        # Move to wait_pick state
        session["_fulfill_list_sent"] = True
        session["state"] = FulfillmentState.WAIT_PICK
        session["ts"] = time.time()
        
        from agents.onboarding.wa_loop import SESSIONS
        SESSIONS[phone] = session


async def handle_fulfill_wait_pick(phone: str, text: str, session: dict):
    """
    Handle FULFILL_WAIT_PICK state: parse reply, validate selection.
    
    If numeric selection valid -> go to FULFILL_NOMINATE.
    Else reply invalid pick and remain in FULFILL_WAIT_PICK.
    """
    # Extract number from text
    text_stripped = text.strip()
    
    # Try to extract a number (1, 2, 3, etc.)
    match = re.search(r'^(\d+)', text_stripped)
    if match:
        selection_str = match.group(1)
        
        # Check if selection is valid
        choice_map = session.get("fulfillment", {}).get("choice_map", {})
        
        if selection_str in choice_map:
            # Valid selection
            need_id = choice_map[selection_str]
            selected_need = None
            
            # Find the full need details
            needs = session.get("fulfillment", {}).get("needs", [])
            for need in needs:
                if need.get("need_id") == need_id:
                    selected_need = need
                    break
            
            log.info(f"[FULFILLMENT] User {phone} selected option {selection_str} (need_id: {need_id})")
            
            # Store selection
            session["fulfillment"]["selected_choice"] = selection_str
            session["fulfillment"]["selected_need_id"] = need_id
            session["fulfillment"]["selected_need"] = selected_need
            
            # Log NEED_SELECTED event
            try:
                from storage.db import get_db_session
                log_event = _get_log_event()
                with get_db_session() as db:
                    log_event(
                        db=db,
                        wa_phone=phone,
                        agent_name=settings.AGENT_NAME,
                        event_type="NEED_SELECTED",
                        event_source="fulfillment_agent",
                        state=FulfillmentState.WAIT_PICK,
                        status="selected",
                        details={
                            "selection_index": selection_str,
                            "need_id": need_id
                        }
                    )
            except Exception as e:
                log.warning(f"[FULFILLMENT] Failed to log NEED_SELECTED event: {e}")
            
            # Move to nominate state
            session["state"] = FulfillmentState.NOMINATE
            session["ts"] = time.time()
            
            from agents.onboarding.wa_loop import SESSIONS
            SESSIONS[phone] = session
            
            # Handle nomination
            await handle_fulfill_nominate(phone, text, session)
        else:
            # Invalid selection number
            log.warning(f"[FULFILLMENT] Invalid selection '{selection_str}' from {phone}")
            mcp_wa_send = _get_mcp_wa_send()
            await mcp_wa_send(phone, FULFILL_INVALID_PICK_MSG)
            
            # Stay in WAIT_PICK state
            session["ts"] = time.time()
            from agents.onboarding.wa_loop import SESSIONS
            SESSIONS[phone] = session
    else:
        # No number found
        log.warning(f"[FULFILLMENT] No number found in reply from {phone}: '{text[:30]}...'")
        mcp_wa_send = _get_mcp_wa_send()
        await mcp_wa_send(phone, FULFILL_INVALID_PICK_MSG)
        
        # Stay in WAIT_PICK state
        session["ts"] = time.time()
        from agents.onboarding.wa_loop import SESSIONS
        SESSIONS[phone] = session


async def handle_fulfill_nominate(phone: str, text: str, session: dict):
    """
    Handle FULFILL_NOMINATE state: call nominate stub.
    
    If success -> send success msg and set state=FULFILL_DONE.
    Else -> send failed msg and remain in FULFILL_WAIT_PICK (or retry).
    """
    log.info(f"[FULFILLMENT] Processing nomination for {phone}")
    
    # Get selected need
    fulfillment_data = session.get("fulfillment", {})
    need_id = fulfillment_data.get("selected_need_id")
    
    if not need_id:
        log.error(f"[FULFILLMENT] No need_id found in session for {phone}")
        # Fallback: go back to list
        session["state"] = FulfillmentState.LIST
        session["ts"] = time.time()
        from agents.onboarding.wa_loop import SESSIONS
        SESSIONS[phone] = session
        await handle_fulfill_list(phone, "__kick__", session)
        return
    
    # Get user identifier (use phone for now, or user_id if available)
    user_id = phone  # TODO: Use actual user_id from profile if available
    
    # Call nominate stub
    result = await nominate_selected_need(need_id, user_id)
    
    mcp_wa_send = _get_mcp_wa_send()
    
    if result.get("success"):
        # Success
        log.info(f"[FULFILLMENT] Nomination successful for {phone}, need_id: {need_id}")
        
        # Log NOMINATION_SUCCESS event
        try:
            from storage.db import get_db_session
            log_event = _get_log_event()
            with get_db_session() as db:
                log_event(
                    db=db,
                    wa_phone=phone,
                    agent_name=settings.AGENT_NAME,
                    event_type="NOMINATION_SUCCESS",
                    event_source="fulfillment_agent",
                    state=FulfillmentState.NOMINATE,
                    status="success",
                    details={"need_id": need_id}
                )
        except Exception as e:
            log.warning(f"[FULFILLMENT] Failed to log NOMINATION_SUCCESS event: {e}")
        
        # Send success message
        await mcp_wa_send(phone, FULFILL_CONFIRM_SUCCESS_MSG)
        
        # Move to done state
        session["state"] = FulfillmentState.DONE
        session["ts"] = time.time()
        
        from agents.onboarding.wa_loop import SESSIONS
        SESSIONS[phone] = session
    else:
        # Failed
        log.warning(f"[FULFILLMENT] Nomination failed for {phone}, need_id: {need_id}")
        
        # Log NOMINATION_FAILED event
        try:
            from storage.db import get_db_session
            log_event = _get_log_event()
            with get_db_session() as db:
                log_event(
                    db=db,
                    wa_phone=phone,
                    agent_name=settings.AGENT_NAME,
                    event_type="NOMINATION_FAILED",
                    event_source="fulfillment_agent",
                    state=FulfillmentState.NOMINATE,
                    status="failed",
                    details={"need_id": need_id}
                )
        except Exception as e:
            log.warning(f"[FULFILLMENT] Failed to log NOMINATION_FAILED event: {e}")
        
        # Send failed message
        await mcp_wa_send(phone, FULFILL_CONFIRM_FAILED_MSG)
        
        # Go back to wait_pick state (allow retry)
        session["state"] = FulfillmentState.WAIT_PICK
        session["ts"] = time.time()
        
        from agents.onboarding.wa_loop import SESSIONS
        SESSIONS[phone] = session


async def handle_fulfill_done(phone: str, text: str, session: dict):
    """
    Handle FULFILL_DONE state: confirmation complete.
    
    Just acknowledge any further messages (journey complete).
    """
    log.info(f"[FULFILLMENT] Journey complete for {phone}")
    # Journey is complete - just acknowledge if needed
    session["ts"] = time.time()
    from agents.onboarding.wa_loop import SESSIONS
    SESSIONS[phone] = session


async def handle_fulfill_exit(phone: str, session: dict):
    """
    Handle FULFILL_EXIT state: user said No, send exit message.
    """
    log.info(f"[FULFILLMENT] User {phone} exited")
    
    # Send exit message
    mcp_wa_send = _get_mcp_wa_send()
    await mcp_wa_send(phone, FULFILL_EXIT_MSG)
    
    # Mark session as ended (or keep it open for potential return)
    session["state"] = FulfillmentState.EXIT
    session["ts"] = time.time()
    
    from agents.onboarding.wa_loop import SESSIONS
    SESSIONS[phone] = session
