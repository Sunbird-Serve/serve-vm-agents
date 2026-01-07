"""
Fulfillment Agent - Prompts and Message Templates
"""
from .types import NeedCard

# FULFILL_INTRO message
FULFILL_INTRO_MSG = """Ready to see open opportunities you can pick from? 😊

Reply Yes to continue."""

# FULFILL_LIST header
FULFILL_LIST_HEADER = "Open opportunities (reply 1/2/3):"

# FULFILL_INVALID_PICK message
FULFILL_INVALID_PICK_MSG = "Please reply with a number like 1 or 2 😊"

# FULFILL_CONFIRM_SUCCESS message
FULFILL_CONFIRM_SUCCESS_MSG = """Done ✅ I've placed your nomination for this opportunity.

You'll receive the session details once it's confirmed."""

# FULFILL_CONFIRM_FAILED message
FULFILL_CONFIRM_FAILED_MSG = """Hmm, I couldn't nominate right now.

Please try again in a moment."""

# FULFILL_EXIT message
FULFILL_EXIT_MSG = """No problem 🌿 If you'd like, you can come back anytime and type 'needs' to see open opportunities."""


def format_need_list(needs: list[NeedCard], max_items: int = 5) -> str:
    """
    Format a list of needs as a numbered WhatsApp message.
    
    Args:
        needs: List of NeedCard objects
        max_items: Maximum number of items to show (default: 5)
    
    Returns:
        Formatted string with numbered list
    """
    if not needs:
        return "No open opportunities at the moment. Check back later! 😊"
    
    # Limit to max_items
    display_needs = needs[:max_items]
    
    lines = [FULFILL_LIST_HEADER, ""]
    
    for idx, need in enumerate(display_needs, start=1):
        # Line 1: Title (bold/italics using asterisks)
        lines.append(f"{idx}) *{need.title}*")
        
        # Line 2: Organization + location
        location_line = f"   {need.org_name}, {need.location}"
        lines.append(location_line)
        
        # Line 3: Days + time window (always show if available)
        schedule_parts = []
        if need.days_text:
            schedule_parts.append(need.days_text)
        if need.time_text:
            schedule_parts.append(need.time_text)
        
        if schedule_parts:
            # Use ASCII-safe separator instead of Unicode bullet
            lines.append(f"   {' | '.join(schedule_parts)}")
        elif need.days_text or need.time_text:
            # If only one is available, show it
            single_item = need.days_text or need.time_text
            if single_item:
                lines.append(f"   {single_item}")
        
        lines.append("")  # Empty line between items
    
    return "\n".join(lines)
