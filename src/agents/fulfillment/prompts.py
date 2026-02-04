"""
Fulfillment Agent - Prompts and Message Templates
"""
from .types import NeedCard

# FULFILL_INTRO message (shown at FULFILL_START)
FULFILL_INTRO_MSG = """Here are some open teaching opportunities you can choose from 😊

Reply with the number to nominate yourself."""

# FULFILL_LIST header and footer copy
FULFILL_LIST_HEADER = "Open opportunities you can consider:"

FULFILL_LIST_FOOTER = """This is just to understand your preference — we can always adjust or discuss later.
Reply 1/2/3... or if it is not matching your preferences type 'Not now' to decide later."""

# FULFILL_INVALID_PICK message
FULFILL_INVALID_PICK_MSG = "Please reply with a number like 1 or 2 😊 (or type 'Not now' to decide later)"

# FULFILL_CONFIRM_SUCCESS message (nomination success)
FULFILL_CONFIRM_SUCCESS_MSG = """Done! 🎉

I’ve nominated you for *{need_title}*.
A SERVE coordinator will review this and get in touch with you soon with next steps.
Thank you for stepping forward to make a difference 🌱"""

# FULFILL_CONFIRM_FAILED message (nomination error)
FULFILL_CONFIRM_FAILED_MSG = """Sorry — I hit a small issue while nominating you.
A coordinator will reach out soon.
You can also try again later by messaging me here."""

# FULFILL_EXIT / DEFERRED message
FULFILL_EXIT_MSG = """That’s completely okay 😊

There’s no rush.
You can come back anytime to explore opportunities.
Thank you for taking the time to go through SERVE today 🌱"""

# Generic pause message (resume later)
FULFILL_DEFERRED_MSG = """No worries — take your time.
Whenever you're ready, just reply here and we’ll continue from where you left off."""


def format_need_list(needs: list[NeedCard], max_items: int = 5, start_index: int = 1) -> str:
    """
    Format a list of needs as a numbered WhatsApp message.
    
    Args:
        needs: List of NeedCard objects
        max_items: Maximum number of items to show (default: 5)
        start_index: Starting number for list items (default: 1)
    
    Returns:
        Formatted string with numbered list
    """
    if not needs:
        return "No open opportunities right now. A coordinator will reach out soon."
    
    # Limit to max_items
    display_needs = needs[:max_items]
    
    lines = [FULFILL_LIST_HEADER, ""]
    
    for idx, need in enumerate(display_needs, start=start_index):
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
    
    # Append footer with instructions
    lines.append(FULFILL_LIST_FOOTER)
    
    return "\n".join(lines)
