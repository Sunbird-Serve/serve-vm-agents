"""
NEEDS_PREVIEW State Handler
Fetch and display needs preview using MCP serve.needs.list.
"""
import logging
import time
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

from ..messages import (
    NEEDS_PREVIEW_HEADER,
    NEEDS_PREVIEW_DISCLAIMER,
    NEEDS_PREVIEW_ERROR_MSG,
)

log = logging.getLogger(__name__)


def format_needs_list(needs_data: List[Dict], include_disclaimer: bool = True) -> str:
    """
    Format needs list for WhatsApp display with book emoji and disclaimer appended.
    
    Args:
        needs_data: List of need dicts from API
        
    Returns:
        Formatted string with emoji-prefixed list and disclaimer at the end
    """
    if not needs_data:
        return ""
    
    # Limit to 4 items max
    display_needs = needs_data[:4]
    
    lines = []
    
    for need in display_needs:
        # Extract fields
        title = need.get("title", "")
        org_name = need.get("schoolName", "") or need.get("orgName", "")
        
        # Format location
        location_parts = []
        district = need.get("district", "")
        state = need.get("state", "")
        if district:
            location_parts.append(district)
        if state:
            location_parts.append(state)
        location = ", ".join(location_parts) if location_parts else ""
        
        # Format days
        days = need.get("days", [])
        days_text = ""
        if days:
            day_map = {
                "MONDAY": "Mon", "TUESDAY": "Tue", "WEDNESDAY": "Wed",
                "THURSDAY": "Thu", "FRIDAY": "Fri", "SATURDAY": "Sat", "SUNDAY": "Sun"
            }
            formatted_days = [day_map.get(str(d).upper(), str(d)[:3]) for d in days if d]
            if formatted_days:
                days_text = " & ".join(formatted_days)
        
        # Format time slots (use same logic as Fulfillment)
        time_slots = need.get("timeSlots", [])
        time_text = ""
        if time_slots and isinstance(time_slots, list) and len(time_slots) > 0:
            # Format similar to Fulfillment _format_time_slots
            # Take first time slot for preview
            first_slot = time_slots[0]
            if isinstance(first_slot, dict):
                start = first_slot.get("startTime", "")
                end = first_slot.get("endTime", "")
                if start and end:
                    # Format as "HH:MM-HH:MM" or "HH:MM AM - HH:MM PM"
                    time_text = f"{start}-{end}"
                elif start:
                    time_text = start
        
        # Line 1: Book emoji + title (bold/italics using asterisks)
        if title:
            lines.append(f"📚 *{title}*")
        else:
            lines.append(f"📚 *Need*")
        
        # Line 2: Organization + location
        if org_name and location:
            lines.append(f"   {org_name}, {location}")
        elif org_name:
            lines.append(f"   {org_name}")
        elif location:
            lines.append(f"   {location}")
        
        # Line 3: Days + time window (if available)
        schedule_parts = []
        if days_text:
            schedule_parts.append(days_text)
        if time_text:
            schedule_parts.append(time_text)
        
        if schedule_parts:
            lines.append(f"   {' | '.join(schedule_parts)}")
        
        lines.append("")  # Empty line between items
    
    # Append disclaimer at the end
    if include_disclaimer:
        lines.append("")
        lines.append(NEEDS_PREVIEW_DISCLAIMER)
    
    return "\n".join(lines)


def format_needs_carousel(needs_data: List[Dict]) -> Dict[str, Any]:
    """
    Format needs list as WhatsApp interactive list (carousel) structure.
    
    Args:
        needs_data: List of need dicts from API
        
    Returns:
        Dict with carousel structure: {
            "header": str,
            "body": Optional[str],
            "footer": str,
            "sections": [{
                "title": Optional[str],
                "rows": [{
                    "id": str,
                    "title": str,
                    "description": str
                }]
            }]
        }
    """
    if not needs_data:
        return {}
    
    # Limit to 10 items max (WhatsApp limit)
    display_needs = needs_data[:10]
    
    rows = []
    row_id_to_need_id = {}  # Map row IDs to actual need IDs
    
    for idx, need in enumerate(display_needs, start=1):
        # Extract fields
        need_id = need.get("id") or need.get("needId") or need.get("_id", "")
        title = need.get("title", "") or f"Need {idx}"
        org_name = need.get("schoolName", "") or need.get("orgName", "")
        
        # Format location
        location_parts = []
        district = need.get("district", "")
        state = need.get("state", "")
        if district:
            location_parts.append(district)
        if state:
            location_parts.append(state)
        location = ", ".join(location_parts) if location_parts else ""
        
        # Format days
        days = need.get("days", [])
        days_text = ""
        if days:
            day_map = {
                "MONDAY": "Mon", "TUESDAY": "Tue", "WEDNESDAY": "Wed",
                "THURSDAY": "Thu", "FRIDAY": "Fri", "SATURDAY": "Sat", "SUNDAY": "Sun"
            }
            formatted_days = [day_map.get(str(d).upper(), str(d)[:3]) for d in days if d]
            if formatted_days:
                days_text = " & ".join(formatted_days)
        
        # Format time slots
        time_slots = need.get("timeSlots", [])
        time_text = ""
        if time_slots and isinstance(time_slots, list) and len(time_slots) > 0:
            first_slot = time_slots[0]
            if isinstance(first_slot, dict):
                start = first_slot.get("startTime", "")
                end = first_slot.get("endTime", "")
                if start and end:
                    time_text = f"{start}-{end}"
                elif start:
                    time_text = start
        
        # Build description (max 72 chars for WhatsApp)
        desc_parts = []
        if org_name:
            desc_parts.append(org_name)
        if location:
            desc_parts.append(location)
        if days_text:
            desc_parts.append(days_text)
        if time_text:
            desc_parts.append(time_text)
        
        description = " | ".join(desc_parts)[:72]  # WhatsApp limit
        
        # Create row
        row_id = f"need_{idx}"
        rows.append({
            "id": row_id,
            "title": title[:24],  # WhatsApp limit
            "description": description
        })
        
        # Store mapping
        row_id_to_need_id[row_id] = str(need_id)
    
    # Build carousel structure
    carousel = {
        "header": NEEDS_PREVIEW_HEADER[:60],  # WhatsApp limit
        "footer": NEEDS_PREVIEW_DISCLAIMER[:60],  # WhatsApp limit
        "sections": [{
            "rows": rows
        }]
    }
    
    # Store row_id mapping in carousel for later use
    carousel["_row_id_to_need_id"] = row_id_to_need_id
    
    return carousel


async def fetch_needs_preview(limit: int = 4) -> List[Dict]:
    """
    Fetch needs using MCP serve.needs.list.
    
    Returns:
        List of need dicts
    """
    try:
        from ..wa_loop import _mcp_call
        
        log.info(f"[NEEDS_PREVIEW] Fetching needs via MCP (limit={limit})")
        
        arguments = {
            "page": 0,
            "size": min(limit, 20),
            "status": "Approved"
        }
        
        result = await _mcp_call("serve.needs.list", arguments, timeout=15)
        
        # Parse response
        needs_data = []
        if isinstance(result, dict):
            if "items" in result:
                needs_data = result["items"]
            elif isinstance(result.get("result"), dict) and "items" in result.get("result", {}):
                needs_data = result["result"]["items"]
            elif "needs" in result:
                needs_data = result["needs"]
            elif "data" in result:
                needs_data = result["data"]
            elif isinstance(result.get("result"), list):
                needs_data = result["result"]
        
        log.info(f"[NEEDS_PREVIEW] Received {len(needs_data)} needs from API")
        return needs_data[:limit]
        
    except Exception as e:
        log.error(f"[NEEDS_PREVIEW] Failed to fetch needs: {e}")
        return []


async def handle_needs_preview(
    phone: str, 
    text: str, 
    sess: Dict[str, Any], 
    profile: Dict[str, Any],
    evt: Optional[Dict] = None
) -> None:
    """Handle NEEDS_PREVIEW state - fetch and display needs, then proceed to ELIGIBILITY."""
    from ..wa_loop import (
        mcp_wa_send, _add_to_history, _handle, SESSIONS,
    )
    
    # Entry: Fetch needs and display
    if text == "__kick__" or not sess.get("_needs_preview_sent"):
        log.info(f"[NEEDS_PREVIEW] Fetching and displaying needs preview for {phone}")
        
        # Send header
        header_msg_id = await mcp_wa_send(phone, NEEDS_PREVIEW_HEADER)
        _add_to_history(phone, bot_msg=NEEDS_PREVIEW_HEADER)
        
        # Fetch needs
        needs_data = []
        needs_success = False
        try:
            needs_data = await fetch_needs_preview(limit=4)
            needs_success = len(needs_data) > 0
        except Exception as e:
            log.error(f"[NEEDS_PREVIEW] Exception fetching needs: {e}", exc_info=True)
        
        # Prepare items for persistence
        needs_items = []
        if needs_data:
            for need in needs_data[:4]:
                need_id = need.get("id") or need.get("needId") or need.get("_id", "")
                # Create short label (same format as displayed)
                title = need.get("title", "")
                org_name = need.get("schoolName", "") or need.get("orgName", "")
                label_parts = []
                if title:
                    label_parts.append(title)
                if org_name:
                    label_parts.append(org_name)
                label = " - ".join(label_parts) if label_parts else f"Need {need_id[:8]}"
                needs_items.append({"need_id": str(need_id), "label": label[:100]})
        
        if needs_data and needs_success:
            # Format and send needs list (includes disclaimer at the end)
            needs_list = format_needs_list(
                needs_data, include_disclaimer=not sess.get("_needs_preview_note")
            )
            if needs_list:
                list_msg_id = await mcp_wa_send(phone, needs_list)
                _add_to_history(phone, bot_msg=needs_list)
                message_id = list_msg_id or header_msg_id
            else:
                message_id = header_msg_id
        else:
            # Error fetching needs - send graceful message and continue
            log.warning(f"[NEEDS_PREVIEW] Failed to fetch needs, sending error message")
            error_msg_id = await mcp_wa_send(phone, NEEDS_PREVIEW_ERROR_MSG)
            _add_to_history(phone, bot_msg=NEEDS_PREVIEW_ERROR_MSG)
            message_id = error_msg_id or header_msg_id
        
        # Optional note for requirements preview (only for peek flow)
        if sess.get("_needs_preview_note"):
            await mcp_wa_send(phone, sess["_needs_preview_note"])
            _add_to_history(phone, bot_msg=sess["_needs_preview_note"])
            sess.pop("_needs_preview_note", None)

        # Add 10-second delay before transitioning
        log.info(f"[NEEDS_PREVIEW] Waiting 10 seconds before continuing to {phone}")
        await asyncio.sleep(10)
        
        # Persistence: Store needs preview and log event
        now_iso = datetime.now(timezone.utc).isoformat()
        try:
            from storage.db import get_db_session
            from storage.session_store import update_session_state_and_tool_state
            from storage.event_logger import log_event
            from ..config import settings
            
            with get_db_session() as db:
                session_id = sess.get("_db_session_id")
                tool_state_updates = {
                    "needs_preview": {
                        "shown": needs_success,
                        "items": needs_items,
                        "at": now_iso
                    }
                }
                update_session_state_and_tool_state(
                    db=db,
                    wa_phone=phone,
                    state="ONBOARDING",
                    sub_state="NEEDS_PREVIEW",
                    last_outbound_msg_id=message_id,
                    tool_state_updates=tool_state_updates
                )
                log_event(
                    db=db,
                    wa_phone=phone,
                    agent_name=settings.AGENT_NAME,
                    event_type="NEEDS_PREVIEW_SENT",
                    event_source="tool",
                    state="ONBOARDING",
                    sub_state="NEEDS_PREVIEW",
                    status="SUCCESS" if needs_success else "FAILURE",
                    details={
                        "items": needs_items,
                        "source": "serve.needs.list",
                        "error": None if needs_success else "Failed to fetch needs"
                    },
                    session_id=session_id
                )
        except Exception as e:
            log.warning(f"[NEEDS_PREVIEW] Failed to persist: {e}", exc_info=True)
        
        sess["_needs_preview_sent"] = True
        sess["state"] = "NEEDS_PREVIEW"
        sess["sub_state"] = "NEEDS_PREVIEW"
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        
        # Transition to next state (default ELIGIBILITY)
        next_state = sess.pop("_needs_preview_next_state", "ELIGIBILITY")
        log.info(f"[NEEDS_PREVIEW] Transitioning to {next_state}")
        sess["state"] = next_state
        sess["sub_state"] = next_state
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        await _handle(phone, "__kick__")
        return
    
    # Should not reach here (auto-transitions on entry)
    # But handle gracefully if user sends a message
    next_state = sess.pop("_needs_preview_next_state", "CONTINUE_CONFIRM")
    log.warning(f"[NEEDS_PREVIEW] Received message in NEEDS_PREVIEW after preview sent, transitioning to {next_state}")
    sess["state"] = next_state
    sess["ts"] = time.time()
    SESSIONS[phone] = sess
    await _handle(phone, "__kick__")
    return

