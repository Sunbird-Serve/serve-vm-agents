"""
NEEDS_PREVIEW State Handler
Fetch and display needs preview using MCP serve.needs.list.
"""
import logging
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

from ..messages import (
    NEEDS_PREVIEW_HEADER,
    NEEDS_PREVIEW_DISCLAIMER,
    NEEDS_PREVIEW_ERROR_MSG,
)

log = logging.getLogger(__name__)


def format_needs_list(needs_data: List[Dict]) -> str:
    """
    Format needs list for WhatsApp display (same format as Fulfillment agent, no CTA).
    
    Args:
        needs_data: List of need dicts from API
        
    Returns:
        Formatted string with numbered list (no header, no CTA)
    """
    if not needs_data:
        return ""
    
    # Limit to 4 items max
    display_needs = needs_data[:4]
    
    lines = []
    
    for idx, need in enumerate(display_needs, start=1):
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
        
        # Line 1: Numbered title (bold/italics using asterisks)
        if title:
            lines.append(f"{idx}) *{title}*")
        else:
            lines.append(f"{idx}) *Need {idx}*")
        
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
    
    return "\n".join(lines)


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
            # Format and send needs list
            needs_list = format_needs_list(needs_data)
            if needs_list:
                await mcp_wa_send(phone, needs_list)
                _add_to_history(phone, bot_msg=needs_list)
        else:
            # Error fetching needs - send graceful message and continue
            log.warning(f"[NEEDS_PREVIEW] Failed to fetch needs, sending error message")
            await mcp_wa_send(phone, NEEDS_PREVIEW_ERROR_MSG)
            _add_to_history(phone, bot_msg=NEEDS_PREVIEW_ERROR_MSG)
        
        # Send disclaimer
        disclaimer_msg_id = await mcp_wa_send(phone, NEEDS_PREVIEW_DISCLAIMER)
        _add_to_history(phone, bot_msg=NEEDS_PREVIEW_DISCLAIMER)
        
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
                    last_outbound_msg_id=disclaimer_msg_id or header_msg_id,
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
        
        # Immediately transition to CONTINUE_CONFIRM (don't wait for user response)
        log.info(f"[NEEDS_PREVIEW] Transitioning to CONTINUE_CONFIRM")
        sess["state"] = "CONTINUE_CONFIRM"
        sess["sub_state"] = "CONTINUE_CONFIRM"
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        await _handle(phone, "__kick__")
        return
    
    # Should not reach here (auto-transitions on entry)
    # But handle gracefully if user sends a message
    log.warning(f"[NEEDS_PREVIEW] Received message in NEEDS_PREVIEW after preview sent, transitioning to CONTINUE_CONFIRM")
    sess["state"] = "CONTINUE_CONFIRM"
    sess["ts"] = time.time()
    SESSIONS[phone] = sess
    await _handle(phone, "__kick__")
    return

