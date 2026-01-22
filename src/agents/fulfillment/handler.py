"""
Fulfillment Agent - Main Handler

Handles the Fulfillment Agent state machine for opportunity discovery and nomination.
"""
import logging
import time
import re
from datetime import datetime, timezone
from typing import Optional, List, Dict

from .types import FulfillmentState, NeedCard
from .prompts import (
    FULFILL_INTRO_MSG,
    FULFILL_LIST_HEADER,
    FULFILL_LIST_FOOTER,
    FULFILL_INVALID_PICK_MSG,
    FULFILL_CONFIRM_SUCCESS_MSG,
    FULFILL_CONFIRM_FAILED_MSG,
    FULFILL_EXIT_MSG,
    FULFILL_DEFERRED_MSG,
    format_need_list,
)
from .config import settings
from agents.onboarding.validators import is_defer_response, is_resume_response

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


async def nominate_selected_need(need_id: str, volunteer_id: str, wa_phone: str) -> Dict[str, bool]:
    """
    Nominate volunteer for selected need using serve.fulfill.nominate MCP tool.
    
    Args:
        need_id: UUID of the selected need
        volunteer_id: SERVE volunteer osid
        wa_phone: WhatsApp phone (for idempotency key)
    
    Returns:
        Dict with "success" key (boolean)
    """
    idempotency_key = f"{wa_phone}:{need_id}:nominate"
    log.info(f"[FULFILLMENT] Nominating volunteer {volunteer_id} for need {need_id}")
    
    try:
        # Prepare MCP tool arguments as per serve.fulfill.nominate contract
        arguments = {
            "needId": need_id,
            "nominatedUserId": volunteer_id,
            "source": "whatsapp",
            "idempotency_key": idempotency_key,
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
            log.info(f"[FULFILLMENT] Nomination successful for need {need_id}, volunteer {volunteer_id}")
        else:
            log.warning(f"[FULFILLMENT] Nomination may have failed for need {need_id}, volunteer {volunteer_id}. Response: {result}")
        
        return {"success": success}
        
    except Exception as e:
        log.error(f"[FULFILLMENT] Failed to nominate volunteer {volunteer_id} for need {need_id}: {e}", exc_info=True)
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

    # Resume/pause handling
    mcp_wa_send = _get_mcp_wa_send()
    if text != "__kick__":
        if session.get("_paused"):
            if is_resume_response(text):
                session["_paused"] = False
                session.pop("_pause_reason", None)
                session.pop("_paused_state", None)
                last_prompt = session.get("_last_agent_prompt")
                if last_prompt:
                    await mcp_wa_send(phone, last_prompt)
                session["ts"] = time.time()
                from agents.onboarding.wa_loop import SESSIONS
                SESSIONS[phone] = session
                return

            # If user asks a question while paused, move to QA_WINDOW to answer
            if "?" in (text or ""):
                from agents.onboarding.wa_loop import SESSIONS, _handle as onboarding_handle
                session["agent"] = "onboarding"
                session["state"] = "QA_WINDOW"
                session.setdefault("_qa_count", 0)
                session.setdefault("_qa_topics", [])
                session.setdefault("_qa_summary_sent", False)
                session["ts"] = time.time()
                SESSIONS[phone] = session
                await onboarding_handle(phone, text)
                return

            # Stay paused on any other input
            await mcp_wa_send(phone, FULFILL_DEFERRED_MSG)
            session["ts"] = time.time()
            from agents.onboarding.wa_loop import SESSIONS
            SESSIONS[phone] = session
            return

        if is_defer_response(text):
            await mcp_wa_send(phone, FULFILL_DEFERRED_MSG)
            from agents.onboarding.wa_loop import SESSIONS, _handle as onboarding_handle
            session["agent"] = "onboarding"
            session["state"] = "QA_WINDOW"
            session.setdefault("_qa_count", 0)
            session.setdefault("_qa_topics", [])
            session.setdefault("_qa_summary_sent", False)
            session["ts"] = time.time()
            SESSIONS[phone] = session
            await onboarding_handle(phone, "__kick__")
            return
    
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
    Handle FULFILL_START: send intro and immediately show list (no extra Yes/No step).
    """
    log.info(f"[FULFILLMENT] Starting fulfillment for {phone}")
    
    now_iso = datetime.now(timezone.utc).isoformat()
    
    # Send intro message
    mcp_wa_send = _get_mcp_wa_send()
    intro_msg_id = await mcp_wa_send(phone, FULFILL_INTRO_MSG)
    
    # Persistence: mark fulfillment started and intro sent
    try:
        from storage.db import get_db_session
        from storage.session_store import update_session_state_and_tool_state
        log_event = _get_log_event()
        
        with get_db_session() as db:
            session_id = session.get("_db_session_id")
            
            # Merge into tool_state.fulfillment
            tool_state_updates = {
                "fulfillment": {
                    "started_at": now_iso,
                }
            }
            
            update_session_state_and_tool_state(
                db=db,
                wa_phone=phone,
                state="FULFILLMENT",
                sub_state=FulfillmentState.INTRO,
                last_outbound_msg_id=intro_msg_id,
                tool_state_updates=tool_state_updates,
            )
            
            # Log FULFILL_STARTED event
            log_event(
                db=db,
                wa_phone=phone,
                agent_name=settings.AGENT_NAME,
                event_type="FULFILL_STARTED",
                event_source="fulfillment_agent",
                state="FULFILLMENT",
                sub_state=FulfillmentState.INTRO,
                status="started",
                details={},
                session_id=session_id,
            )
    except Exception as e:
        log.warning(f"[FULFILLMENT] Failed to persist FULFILL_STARTED: {e}", exc_info=True)
    
    # Immediately move to list state
    session["_fulfill_intro_sent"] = True
    session["state"] = FulfillmentState.LIST
    session["ts"] = time.time()
    
    from agents.onboarding.wa_loop import SESSIONS
    SESSIONS[phone] = session
    
    await handle_fulfill_list(phone, "__kick__", session)


async def handle_fulfill_list(phone: str, text: str, session: dict):
    """
    Handle FULFILL_LIST state: fetch needs via MCP tool, render list, prompt to pick.
    
    Sends list + prompt and sets state=FULFILL_WAIT_PICK.
    """
    if text == "__kick__" or not session.get("_fulfill_list_sent"):
        log.info(f"[FULFILLMENT] Fetching and displaying needs list for {phone}")
        
        # Fetch needs via MCP tool (volunteer_id not supported by API)
        needs = await fetch_open_needs(limit=5)

        if not needs:
            # No needs available - notify and move to QA window
            no_needs_msg = format_need_list([], max_items=0)
            mcp_wa_send = _get_mcp_wa_send()
            await mcp_wa_send(phone, no_needs_msg)
            session["_fulfill_list_sent"] = True
            session["ts"] = time.time()
            from agents.onboarding.wa_loop import SESSIONS, _handle as onboarding_handle
            session["agent"] = "onboarding"
            session["state"] = "QA_WINDOW"
            session.setdefault("_qa_count", 0)
            session.setdefault("_qa_topics", [])
            session.setdefault("_qa_summary_sent", False)
            SESSIONS[phone] = session
            await onboarding_handle(phone, "__kick__")
            return
        
        # Format list
        list_message = format_need_list(needs, max_items=5)
        
        # Send list
        mcp_wa_send = _get_mcp_wa_send()
        list_msg_id = await mcp_wa_send(phone, list_message)
        session["_last_agent_prompt"] = list_message
        
        # Store needs in session for validation
        session["fulfillment"] = session.get("fulfillment", {})
        session["fulfillment"]["needs"] = [need.to_dict() for need in needs]
        
        # Create choice mapping: {"1": need_id1, "2": need_id2, ...}
        choice_map: Dict[str, str] = {}
        for idx, need in enumerate(needs[:5], start=1):
            choice_map[str(idx)] = need.need_id
        session["fulfillment"]["choice_map"] = choice_map
        
        # Persistence: tool_state.fulfillment.needs + NEEDS_LIST_SHOWN event
        try:
            from storage.db import get_db_session
            from storage.session_store import update_session_state_and_tool_state
            log_event = _get_log_event()
            
            now_iso = datetime.now(timezone.utc).isoformat()
            needs_payload = [need.to_dict() for need in needs]
            
            with get_db_session() as db:
                session_id = session.get("_db_session_id")
                
                tool_state_updates = {
                    "fulfillment": {
                        "need_list_cached_at": now_iso,
                        "needs": needs_payload,
                        "need_map": choice_map,
                    }
                }
                
                update_session_state_and_tool_state(
                    db=db,
                    wa_phone=phone,
                    state="FULFILLMENT",
                    sub_state=FulfillmentState.WAIT_PICK,
                    last_outbound_msg_id=list_msg_id,
                    tool_state_updates=tool_state_updates,
                )
                
                log_event(
                    db=db,
                    wa_phone=phone,
                    agent_name=settings.AGENT_NAME,
                    event_type="NEEDS_LIST_SHOWN",
                    event_source="fulfillment_agent",
                    state="FULFILLMENT",
                    sub_state=FulfillmentState.LIST,
                    status="shown",
                    details={"count": len(needs)},
                    session_id=session_id,
                )
        except Exception as e:
            log.warning(f"[FULFILLMENT] Failed to persist NEEDS_LIST_SHOWN: {e}", exc_info=True)
        
        # Move to wait_pick state
        session["_fulfill_list_sent"] = True
        session["state"] = FulfillmentState.WAIT_PICK
        session["ts"] = time.time()
        
        from agents.onboarding.wa_loop import SESSIONS
        SESSIONS[phone] = session


async def handle_fulfill_wait_pick(phone: str, text: str, session: dict):
    """
    Handle FULFILL_WAIT_PICK state: parse reply, validate selection or defer.
    
    - If numeric selection valid -> store selection and go to FULFILL_NOMINATE.
    - If defer keywords ("not now", "later", "maybe", etc.) -> graceful exit/deferred.
    - If question -> reassure and re-show list.
    - Else -> invalid input, re-prompt.
    """
    text_stripped = text.strip()
    text_lower = text_stripped.lower()
    
    mcp_wa_send = _get_mcp_wa_send()
    
    # Helper: detect defer
    defer_keywords = ["not now", "later", "maybe", "think", "decide later"]
    if any(k in text_lower for k in defer_keywords):
        log.info(f"[FULFILLMENT] User {phone} chose to defer nomination")
        # Persistence: mark fulfillment deferred
        try:
            from storage.db import get_db_session
            from storage.session_store import update_session_state_and_tool_state
            log_event = _get_log_event()
            
            now_iso = datetime.now(timezone.utc).isoformat()
            with get_db_session() as db:
                session_id = session.get("_db_session_id")
                
                tool_state_updates = {
                    "fulfillment": {
                        "status": "deferred",
                        "deferred_at": now_iso,
                    }
                }
                
                update_session_state_and_tool_state(
                    db=db,
                    wa_phone=phone,
                    state="FULFILLMENT",
                    sub_state=FulfillmentState.WAIT_PICK,
                    tool_state_updates=tool_state_updates,
                )
                
                # Log FULFILL_DEFERRED event
                log_event(
                    db=db,
                    wa_phone=phone,
                    agent_name=settings.AGENT_NAME,
                    event_type="FULFILL_DEFERRED",
                    event_source="fulfillment_agent",
                    state="FULFILLMENT",
                    sub_state=FulfillmentState.WAIT_PICK,
                    status="deferred",
                    details={},
                    session_id=session_id,
                )
        except Exception as e:
            log.warning(f"[FULFILLMENT] Failed to persist FULFILL_DEFERRED: {e}", exc_info=True)
        
        # Pause flow and allow resume later
        await mcp_wa_send(phone, FULFILL_DEFERRED_MSG)
        session["_paused"] = True
        session["_pause_reason"] = "user_deferred"
        session["ts"] = time.time()
        from agents.onboarding.wa_loop import SESSIONS
        SESSIONS[phone] = session
        return
    
    # Helper: detect question
    if "?" in text_stripped or re.search(r"^(what|how|when|why|where|who|which|can|could|do|does|is|are)\b", text_stripped, re.I):
        log.info(f"[FULFILLMENT] Question detected from {phone}, reassuring and re-showing list")
        reassurance = "No pressure at all — you can always decide later. For now, here are some options you can pick from:"
        await mcp_wa_send(phone, reassurance)
        # Re-show list
        session["ts"] = time.time()
        from agents.onboarding.wa_loop import SESSIONS
        SESSIONS[phone] = session
        await handle_fulfill_list(phone, "__kick__", session)
        return
    
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
            
            # Persistence: store selection in tool_state.fulfillment
            try:
                from storage.db import get_db_session
                from storage.session_store import update_session_state_and_tool_state
                log_event = _get_log_event()
                
                now_iso = datetime.now(timezone.utc).isoformat()
                with get_db_session() as db:
                    session_id = session.get("_db_session_id")
                    
                    selection_payload = {
                        "selected_choice": selection_str,
                        "selected_need_id": need_id,
                        "selected_need_title": (selected_need or {}).get("title") if isinstance(selected_need, dict) else None,
                        "selected_at": now_iso,
                    }
                    
                    tool_state_updates = {
                        "fulfillment": {
                            "selection": selection_payload,
                        }
                    }
                    
                    update_session_state_and_tool_state(
                        db=db,
                        wa_phone=phone,
                        state="FULFILLMENT",
                        sub_state=FulfillmentState.NOMINATE,
                        tool_state_updates=tool_state_updates,
                    )
                    
                    # Log NEED_SELECTED event
                    log_event(
                        db=db,
                        wa_phone=phone,
                        agent_name=settings.AGENT_NAME,
                        event_type="FULFILL_NEED_SELECTED",
                        event_source="fulfillment_agent",
                        state="FULFILLMENT",
                        sub_state=FulfillmentState.WAIT_PICK,
                        status="selected",
                        details={
                            "selection_index": selection_str,
                            "need_id": need_id,
                        },
                        session_id=session_id,
                    )
            except Exception as e:
                log.warning(f"[FULFILLMENT] Failed to persist FULFILL_NEED_SELECTED: {e}", exc_info=True)
            
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
            await mcp_wa_send(phone, FULFILL_INVALID_PICK_MSG)
            
            # Stay in WAIT_PICK state
            session["ts"] = time.time()
            from agents.onboarding.wa_loop import SESSIONS
            SESSIONS[phone] = session
    else:
        # No number found
        log.warning(f"[FULFILLMENT] No number found in reply from {phone}: '{text[:30]}...'")
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
    
    # Get volunteer_id (SERVE osid) from session/profile first, then registration tool_state (DB)
    volunteer_id = None
    profile = session.get("profile", {})
    if isinstance(profile, dict):
        volunteer_id = profile.get("volunteer_id") or profile.get("serve_volunteer_id")
    try:
        from storage.db import get_db_session
        from sqlalchemy import select
        from storage.tables import serve_agent_sessions
        
        with get_db_session() as db:
            stmt = select(serve_agent_sessions.c.tool_state).where(
                serve_agent_sessions.c.wa_phone == phone
            )
            result = db.execute(stmt).first()
            if result and result[0] and isinstance(result[0], dict):
                tool_state = result[0]
                reg = tool_state.get("registration", {})
                serve_block = reg.get("serve", {}) if isinstance(reg, dict) else {}
                volunteer_id = volunteer_id or serve_block.get("volunteer_id")
    except Exception as e:
        log.warning(f"[FULFILLMENT] Failed to load volunteer_id from tool_state for {phone}: {e}", exc_info=True)
    
    if not volunteer_id:
        log.error(f"[FULFILLMENT] Missing volunteer_id for {phone}, cannot nominate")
        mcp_wa_send = _get_mcp_wa_send()
        await mcp_wa_send(
            phone,
            "Sorry — I couldn't find your registration details to complete the nomination. "
            "A coordinator will reach out to help you with the next steps."
        )
        return
    
    # Call nominate tool via MCP
    result = await nominate_selected_need(need_id, volunteer_id, phone)
    
    mcp_wa_send = _get_mcp_wa_send()
    
    now_iso = datetime.now(timezone.utc).isoformat()
    
    if result.get("success"):
        # Success
        log.info(f"[FULFILLMENT] Nomination successful for {phone}, need_id: {need_id}")
        
        # Send success message with need title
        need_title = None
        if isinstance(fulfillment_data, dict):
            selected_need = fulfillment_data.get("selected_need")
            if isinstance(selected_need, dict):
                need_title = selected_need.get("title")
        if not need_title:
            need_title = "the selected opportunity"
        success_msg = FULFILL_CONFIRM_SUCCESS_MSG.format(need_title=need_title)
        success_msg_id = await mcp_wa_send(phone, success_msg)
        
        # Persistence: store successful nomination
        try:
            from storage.db import get_db_session
            from storage.session_store import update_session_state_and_tool_state
            log_event = _get_log_event()
            
            with get_db_session() as db:
                session_id = session.get("_db_session_id")
                
                # Read existing fulfillment from tool_state
                from sqlalchemy import select
                from storage.tables import serve_agent_sessions
                stmt = select(serve_agent_sessions.c.tool_state).where(
                    serve_agent_sessions.c.wa_phone == phone
                )
                db_result = db.execute(stmt).first()
                existing_fulfillment = {}
                if db_result and db_result[0] and isinstance(db_result[0], dict):
                    existing_fulfillment = db_result[0].get("fulfillment", {})
                
                nomination_payload = {
                    "status": "success",
                    "need_id": need_id,
                    "volunteer_id": volunteer_id,
                    "nominated_at": now_iso,
                    "error": None,
                }
                
                fulfillment_update = existing_fulfillment.copy()
                fulfillment_update["nomination"] = nomination_payload
                fulfillment_update["completed_at"] = now_iso
                
                update_session_state_and_tool_state(
                    db=db,
                    wa_phone=phone,
                    state="FULFILLMENT",
                    sub_state=FulfillmentState.DONE,
                    last_outbound_msg_id=success_msg_id,
                    tool_state_updates={"fulfillment": fulfillment_update},
                )
                
                # Log NOMINATION_SUCCESS + FULFILL_COMPLETED
                log_event(
                    db=db,
                    wa_phone=phone,
                    agent_name=settings.AGENT_NAME,
                    event_type="NOMINATION_SUCCESS",
                    event_source="fulfillment_agent",
                    state="FULFILLMENT",
                    sub_state=FulfillmentState.NOMINATE,
                    status="success",
                    details={"need_id": need_id, "volunteer_id": volunteer_id},
                    session_id=session_id,
                )
                log_event(
                    db=db,
                    wa_phone=phone,
                    agent_name=settings.AGENT_NAME,
                    event_type="FULFILL_COMPLETED",
                    event_source="fulfillment_agent",
                    state="FULFILLMENT",
                    sub_state=FulfillmentState.DONE,
                    status="success",
                    details={"need_id": need_id, "volunteer_id": volunteer_id},
                    session_id=session_id,
                )
        except Exception as e:
            log.warning(f"[FULFILLMENT] Failed to persist successful nomination: {e}", exc_info=True)
        
        # Move to done state
        session["state"] = FulfillmentState.DONE
        session["ts"] = time.time()
        
        from agents.onboarding.wa_loop import SESSIONS
        SESSIONS[phone] = session
    else:
        # Failed
        log.warning(f"[FULFILLMENT] Nomination failed for {phone}, need_id: {need_id}")
        
        # Send failed message
        failed_msg_id = await mcp_wa_send(phone, FULFILL_CONFIRM_FAILED_MSG)
        
        # Persistence: store failed nomination attempt
        try:
            from storage.db import get_db_session
            from storage.session_store import update_session_state_and_tool_state
            log_event = _get_log_event()
            
            with get_db_session() as db:
                session_id = session.get("_db_session_id")
                
                from sqlalchemy import select
                from storage.tables import serve_agent_sessions
                stmt = select(serve_agent_sessions.c.tool_state).where(
                    serve_agent_sessions.c.wa_phone == phone
                )
                db_result = db.execute(stmt).first()
                existing_fulfillment = {}
                if db_result and db_result[0] and isinstance(db_result[0], dict):
                    existing_fulfillment = db_result[0].get("fulfillment", {})
                
                nomination_payload = {
                    "status": "failed",
                    "need_id": need_id,
                    "volunteer_id": volunteer_id,
                    "nominated_at": now_iso,
                    "error": "nomination_failed",
                }
                
                fulfillment_update = existing_fulfillment.copy()
                fulfillment_update["nomination"] = nomination_payload
                
                update_session_state_and_tool_state(
                    db=db,
                    wa_phone=phone,
                    state="FULFILLMENT",
                    sub_state=FulfillmentState.WAIT_PICK,
                    last_outbound_msg_id=failed_msg_id,
                    tool_state_updates={"fulfillment": fulfillment_update},
                )
                
                # Log NOMINATION_FAILED event
                log_event(
                    db=db,
                    wa_phone=phone,
                    agent_name=settings.AGENT_NAME,
                    event_type="NOMINATION_FAILED",
                    event_source="fulfillment_agent",
                    state="FULFILLMENT",
                    sub_state=FulfillmentState.NOMINATE,
                    status="failed",
                    details={"need_id": need_id, "volunteer_id": volunteer_id},
                    session_id=session_id,
                )
        except Exception as e:
            log.warning(f"[FULFILLMENT] Failed to persist failed nomination: {e}", exc_info=True)
        
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
    # Journey is complete - move back to onboarding QA window for any final questions.
    session["ts"] = time.time()
    try:
        from agents.onboarding.wa_loop import SESSIONS, _handle as onboarding_handle
        # Switch agent back to onboarding and enter QA_WINDOW ("Do you have any questions for me?")
        session["agent"] = "onboarding"
        session["state"] = "QA_WINDOW"
        session.setdefault("_qa_count", 0)
        session.setdefault("_qa_topics", [])
        session.setdefault("_qa_summary_sent", False)
        SESSIONS[phone] = session
        # Kick off QA_WINDOW state
        await onboarding_handle(phone, "__kick__")
    except Exception as e:
        # If anything goes wrong, just persist the fulfillment session and continue.
        from agents.onboarding.wa_loop import SESSIONS
        log.warning(
            f"[FULFILLMENT] Failed to transition to QA_WINDOW after done for {phone}: {e}",
            exc_info=True,
        )
        SESSIONS[phone] = session


async def handle_fulfill_exit(phone: str, session: dict):
    """
    Handle FULFILL_EXIT state: user said No, send exit message.
    """
    log.info(f"[FULFILLMENT] User {phone} exited")
    
    # Send exit message
    mcp_wa_send = _get_mcp_wa_send()
    await mcp_wa_send(phone, FULFILL_EXIT_MSG)
    
    # After exit, route back to onboarding QA window for any remaining questions.
    session["ts"] = time.time()
    try:
        from agents.onboarding.wa_loop import SESSIONS, _handle as onboarding_handle
        session["agent"] = "onboarding"
        session["state"] = "QA_WINDOW"
        session.setdefault("_qa_count", 0)
        session.setdefault("_qa_topics", [])
        session.setdefault("_qa_summary_sent", False)
        SESSIONS[phone] = session
        await onboarding_handle(phone, "__kick__")
    except Exception as e:
        from agents.onboarding.wa_loop import SESSIONS
        log.warning(
            f"[FULFILLMENT] Failed to transition to QA_WINDOW after exit for {phone}: {e}",
            exc_info=True,
        )
        # Fallback: just persist the fulfillment EXIT state
        session["state"] = FulfillmentState.EXIT
        SESSIONS[phone] = session
