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
_mcp_call = None


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


def _get_mcp_call():
    """Lazy import of _mcp_call to avoid circular dependencies"""
    global _mcp_call
    if _mcp_call is None:
        from agents.onboarding.wa_loop import _mcp_call
        _mcp_call = _mcp_call
    return _mcp_call


# ========== API Functions ==========

def _format_days(days: list) -> str:
    """
    Format days array from API format to readable string.
    
    Args:
        days: List of weekday strings (e.g., ["MONDAY", "TUESDAY"])
    
    Returns:
        Formatted string (e.g., "Mon & Tue")
    """
    if not days or not isinstance(days, list):
        return ""
    
    # Map uppercase weekday names to short readable format
    day_map = {
        "MONDAY": "Mon",
        "TUESDAY": "Tue",
        "WEDNESDAY": "Wed",
        "THURSDAY": "Thu",
        "FRIDAY": "Fri",
        "SATURDAY": "Sat",
        "SUNDAY": "Sun"
    }
    
    formatted_days = []
    for day in days:
        day_upper = str(day).upper()
        if day_upper in day_map:
            formatted_days.append(day_map[day_upper])
        else:
            # Fallback: capitalize first letter
            formatted_days.append(str(day).capitalize()[:3])
    
    return " & ".join(formatted_days)


def _format_time_slots(time_slots: list) -> str:
    """
    Format timeSlots array from API format to readable string.
    
    Args:
        time_slots: List of time slot objects with {day, startTime, endTime}
                   Times are in 24-hour format (HH:MM)
    
    Returns:
        Formatted string (e.g., "9:00–11:00 AM IST")
    """
    if not time_slots or not isinstance(time_slots, list):
        return ""
    
    # Group time slots by time range (same start/end times)
    time_groups = {}
    for slot in time_slots:
        if not isinstance(slot, dict):
            continue
        
        start_time = slot.get("startTime", "")
        end_time = slot.get("endTime", "")
        
        if not start_time or not end_time:
            continue
        
        # Convert 24-hour to 12-hour format
        try:
            start_hour, start_min = map(int, start_time.split(":"))
            end_hour, end_min = map(int, end_time.split(":"))
            
            # Convert to 12-hour format
            start_period = "AM" if start_hour < 12 else "PM"
            end_period = "AM" if end_hour < 12 else "PM"
            
            if start_hour == 0:
                start_hour = 12
            elif start_hour > 12:
                start_hour -= 12
            
            if end_hour == 0:
                end_hour = 12
            elif end_hour > 12:
                end_hour -= 12
            
            time_range = f"{start_hour}:{start_min:02d}–{end_hour}:{end_min:02d} {start_period}"
            
            # Group by time range
            if time_range not in time_groups:
                time_groups[time_range] = []
            time_groups[time_range].append(slot.get("day", ""))
            
        except (ValueError, AttributeError):
            # Fallback: use original format
            time_range = f"{start_time}–{end_time}"
            if time_range not in time_groups:
                time_groups[time_range] = []
            time_groups[time_range].append(slot.get("day", ""))
    
    # Format grouped time ranges
    if not time_groups:
        return ""
    
    # For simplicity, use the first time range (most common case)
    # If multiple ranges exist, we could format them all
    first_range = list(time_groups.keys())[0]
    return f"{first_range} IST"


async def fetch_open_needs(limit: int = 5) -> List[NeedCard]:
    """
    Fetch open needs/opportunities using serve.needs.list MCP tool.
    
    Args:
        limit: Maximum number of needs to return (default: 5, max: 20)
    
    Returns:
        List of NeedCard objects
    """
    try:
        # Ensure limit doesn't exceed API maximum
        size = min(limit, 20)
        
        log.info(f"[FULFILLMENT] Fetching open needs via MCP (size={size})")
        
        # Prepare MCP tool arguments (all optional, defaults provided by server)
        arguments = {
            "page": 0,  # 0-indexed page number
            "size": size,  # Items per page (max 20)
            "status": "Approved"  # Filter by status
        }
        
        # Call MCP tool
        mcp_call = _get_mcp_call()
        result = await mcp_call("serve.needs.list", arguments, timeout=15)
        
        # Parse response - expect paginated structure with items array
        needs_data = []
        if isinstance(result, dict):
            # Look for items array in paginated response
            if "items" in result:
                needs_data = result["items"]
            elif isinstance(result.get("result"), dict) and "items" in result.get("result", {}):
                needs_data = result["result"]["items"]
            # Fallback: try other possible structures
            elif "needs" in result:
                needs_data = result["needs"]
            elif "data" in result:
                needs_data = result["data"]
            elif isinstance(result.get("result"), list):
                needs_data = result["result"]
        
        if not needs_data:
            log.warning(f"[FULFILLMENT] No needs returned from serve.needs.list")
            return []
        
        log.info(f"[FULFILLMENT] Received {len(needs_data)} needs from API")
        
        # Map response to NeedCard objects
        need_cards = []
        for need_data in needs_data:
            try:
                # Handle both dict and object-like structures
                if isinstance(need_data, dict):
                    need_dict = need_data
                else:
                    # If it's an object, try to convert to dict
                    need_dict = need_data if hasattr(need_data, '__dict__') else {}
                
                # Map fields from actual API response
                need_id = need_dict.get("needId") or str(need_dict.get("id", ""))
                title = need_dict.get("title") or ""
                org_name = need_dict.get("schoolName") or ""
                
                # Construct location from district and state
                district = need_dict.get("district", "")
                state = need_dict.get("state", "")
                location_parts = []
                if district:
                    location_parts.append(district)
                if state:
                    location_parts.append(state)
                location = ", ".join(location_parts) if location_parts else ""
                
                # Format days array
                days = need_dict.get("days", [])
                days_text = _format_days(days) if days else None
                
                # Format timeSlots array
                time_slots = need_dict.get("timeSlots", [])
                time_text = _format_time_slots(time_slots) if time_slots else None
                
                # Only create NeedCard if we have required fields
                if need_id and title:
                    need_card = NeedCard(
                        need_id=str(need_id),
                        title=str(title),
                        org_name=str(org_name) if org_name else "",
                        location=location,
                        days_text=days_text,
                        time_text=time_text
                    )
                    need_cards.append(need_card)
                    log.debug(f"[FULFILLMENT] Mapped need: {need_id} - {title}")
                else:
                    log.warning(f"[FULFILLMENT] Skipping need with missing required fields: needId={need_id}, title={title}")
                    
            except Exception as e:
                log.warning(f"[FULFILLMENT] Error mapping need data to NeedCard: {e}, data: {need_data}")
                continue
        
        log.info(f"[FULFILLMENT] Successfully fetched {len(need_cards)} needs from serve.needs.list")
        return need_cards[:limit]  # Ensure we don't exceed requested limit
        
    except Exception as e:
        log.error(f"[FULFILLMENT] Failed to fetch needs from serve.needs.list: {e}")
        # Return empty list on error (graceful degradation)
        return []


async def nominate_selected_need(need_id: str, nominated_user_id: str) -> Dict[str, bool]:
    """
    Nominate volunteer for selected need using serve.fulfill.nominate MCP tool.
    
    Args:
        need_id: UUID of the selected need
        nominated_user_id: User/volunteer identifier (currently ignored, using hardcoded value)
    
    Returns:
        Dict with "success" key (boolean)
    """
    # Hardcoded nominatedUserId as per requirements
    HARDCODED_USER_ID = "1-93c6dd23-599a-4191-82c9-af6d2fc5a1f9"
    
    log.info(f"[FULFILLMENT] Nominating user {HARDCODED_USER_ID} for need {need_id}")
    
    try:
        # Prepare MCP tool arguments
        arguments = {
            "needId": need_id,
            "nominatedUserId": HARDCODED_USER_ID,
            "source": "whatsapp"  # Optional but good to include
        }
        
        # Call MCP tool
        mcp_call = _get_mcp_call()
        result = await mcp_call("serve.fulfill.nominate", arguments, timeout=15)
        
        # Parse response - MCP tools typically return success on HTTP 200/201
        # Check for explicit success indicators or assume success if no error
        success = False
        
        if isinstance(result, dict):
            # Check for explicit success field
            if "success" in result:
                success = bool(result["success"])
            elif "status" in result:
                # Check status field
                status = result.get("status", "").lower()
                success = status in ["success", "created", "ok", "completed"]
            elif "error" not in result:
                # No error field suggests success
                success = True
        elif result is not None:
            # Non-dict response (unlikely but handle it)
            success = True
        
        if success:
            log.info(f"[FULFILLMENT] Nomination successful for need {need_id}, user {HARDCODED_USER_ID}")
        else:
            log.warning(f"[FULFILLMENT] Nomination may have failed for need {need_id}, user {HARDCODED_USER_ID}. Response: {result}")
        
        return {"success": success}
        
    except Exception as e:
        log.error(f"[FULFILLMENT] Failed to nominate user {HARDCODED_USER_ID} for need {need_id}: {e}", exc_info=True)
        return {"success": False}


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
    Handle FULFILL_LIST state: fetch needs via MCP tool, render list, prompt to pick.
    
    Sends list + prompt and sets state=FULFILL_WAIT_PICK.
    """
    if text == "__kick__" or not session.get("_fulfill_list_sent"):
        log.info(f"[FULFILLMENT] Fetching and displaying needs list for {phone}")
        
        # Fetch needs via MCP tool (volunteer_id not supported by API)
        needs = await fetch_open_needs(limit=5)
        
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
