"""
Formal Onboarding Handler Class
Organized state-to-handler mapping with MCP tool interactions.

This class provides a structured interface while delegating to the existing
functional implementation in wa_loop.py. Both can coexist.
"""
import asyncio
import logging
from typing import Dict, Callable, Optional, Any
from .wa_loop import (
    _handle,  # Existing functional handler
    SESSIONS,
    CONVERSATION_HISTORIES,
)

log = logging.getLogger(__name__)


class OnboardingHandler:
    """
    Formal handler class for onboarding state machine.
    Maps states to handler methods and manages MCP tool interactions.
    """
    
    def __init__(self):
        """Initialize the handler with state registry."""
        self.state_handlers: Dict[str, Callable] = {
            "WELCOME": self.handle_welcome,
            "ELIGIBILITY_PART1": self.handle_eligibility_part1,
            "ELIGIBILITY_PART2": self.handle_eligibility_part2,
            "PREFS_DAYTIME": self.handle_prefs_daytime,
            "QA_WINDOW": self.handle_qa_window,
            "ORIENTATION_SLOT": self.handle_orientation_slot,
            "ORIENTATION_SCHEDULING": self.handle_orientation_scheduling,
            "COMPLETE": self.handle_complete,
            "DEFERRED": self.handle_deferred,
            "OPTOUT": self.handle_optout,
            "REJECTED": self.handle_rejected,
        }
    
    async def process_message(
        self,
        phone: str,
        text: str,
        profile: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Main entry point for processing a message.
        
        Args:
            phone: Volunteer phone number (session key)
            text: Message text from user
            profile: Optional profile data (will fetch from session if not provided)
        
        Returns:
            Dict with processing result info
        """
        # Get or initialize session
        sess = SESSIONS.get(phone)
        if not sess:
            log.warning(f"[Handler] No session found for {phone}, initializing...")
            # Delegate to existing initialization logic
            await _handle(phone, "__kick__")
            sess = SESSIONS.get(phone)
            if not sess:
                return {"error": "Failed to initialize session"}
        
        state = sess.get("state", "WELCOME")
        
        # Get handler for current state
        handler = self.state_handlers.get(state)
        if not handler:
            log.error(f"[Handler] No handler found for state: {state}")
            # Fallback to existing functional handler
            await _handle(phone, text)
            return {"state": state, "handled": "fallback"}
        
        # Call state-specific handler
        try:
            result = await handler(phone, text, sess, profile or sess.get("profile", {}))
            return {
                "state": state,
                "next_state": sess.get("state"),
                "handled": True,
                "result": result
            }
        except Exception as e:
            log.error(f"[Handler] Error in state {state}: {e}", exc_info=True)
            # Fallback to existing handler on error
            await _handle(phone, text)
            return {"state": state, "handled": "fallback_on_error", "error": str(e)}
    
    # ========== State Handlers ==========
    # Each handler can either:
    # 1. Delegate to existing _handle (for now)
    # 2. Implement custom logic using MCP tools directly
    # 3. Hybrid: use MCP tools but follow existing patterns
    
    async def handle_welcome(
        self,
        phone: str,
        text: str,
        sess: Dict,
        profile: Dict
    ) -> Dict[str, Any]:
        """Handle WELCOME state: greeting and consent."""
        # For now, delegate to existing implementation
        # Can be refactored to use MCP tools directly later
        await _handle(phone, text)
        return {"action": "welcome_flow"}
    
    async def handle_eligibility_part1(
        self,
        phone: str,
        text: str,
        sess: Dict,
        profile: Dict
    ) -> Dict[str, Any]:
        """Handle ELIGIBILITY_PART1: age and device checks."""
        await _handle(phone, text)
        return {"action": "eligibility_part1"}
    
    async def handle_eligibility_part2(
        self,
        phone: str,
        text: str,
        sess: Dict,
        profile: Dict
    ) -> Dict[str, Any]:
        """Handle ELIGIBILITY_PART2: commitment check with persuasion."""
        await _handle(phone, text)
        return {"action": "eligibility_part2"}
    
    async def handle_prefs_daytime(
        self,
        phone: str,
        text: str,
        sess: Dict,
        profile: Dict
    ) -> Dict[str, Any]:
        """Handle PREFS_DAYTIME: collect teaching day/time preferences."""
        await _handle(phone, text)
        return {"action": "prefs_daytime"}
    
    async def handle_qa_window(
        self,
        phone: str,
        text: str,
        sess: Dict,
        profile: Dict
    ) -> Dict[str, Any]:
        """Handle QA_WINDOW: short Q&A before orientation scheduling."""
        await _handle(phone, text)
        return {"action": "qa_window"}
    
    async def handle_orientation_slot(
        self,
        phone: str,
        text: str,
        sess: Dict,
        profile: Dict
    ) -> Dict[str, Any]:
        """Handle ORIENTATION_SLOT: collect orientation availability and share options."""
        await _handle(phone, text)
        return {"action": "orientation_slot"}
    
    async def handle_orientation_scheduling(
        self,
        phone: str,
        text: str,
        sess: Dict,
        profile: Dict
    ) -> Dict[str, Any]:
        """Handle ORIENTATION_SCHEDULING: slot selection and booking."""
        await _handle(phone, text)
        return {"action": "orientation_scheduling"}
    
    async def handle_complete(
        self,
        phone: str,
        text: str,
        sess: Dict,
        profile: Dict
    ) -> Dict[str, Any]:
        """Handle COMPLETE: final confirmation state."""
        await _handle(phone, text)
        return {"action": "complete"}
    
    async def handle_deferred(
        self,
        phone: str,
        text: str,
        sess: Dict,
        profile: Dict
    ) -> Dict[str, Any]:
        """Handle DEFERRED: user deferred onboarding."""
        await _handle(phone, text)
        return {"action": "deferred"}
    
    async def handle_optout(
        self,
        phone: str,
        text: str,
        sess: Dict,
        profile: Dict
    ) -> Dict[str, Any]:
        """Handle OPTOUT: user opted out."""
        await _handle(phone, text)
        return {"action": "optout"}
    
    async def handle_rejected(
        self,
        phone: str,
        text: str,
        sess: Dict,
        profile: Dict
    ) -> Dict[str, Any]:
        """Handle REJECTED: user ineligible."""
        await _handle(phone, text)
        return {"action": "rejected"}
    
    # ========== Utility Methods ==========
    
    def get_current_state(self, phone: str) -> Optional[str]:
        """Get current state for a phone number."""
        sess = SESSIONS.get(phone)
        return sess.get("state") if sess else None
    
    def get_session(self, phone: str) -> Optional[Dict]:
        """Get session data for a phone number."""
        return SESSIONS.get(phone)
    
    def list_states(self) -> list[str]:
        """List all registered states."""
        return list(self.state_handlers.keys())
    
    def register_handler(self, state: str, handler: Callable):
        """Register a custom handler for a state (for extensibility)."""
        self.state_handlers[state] = handler
        log.info(f"[Handler] Registered custom handler for state: {state}")


# ========== Convenience Functions ==========
# These can be used as drop-in replacements or alongside existing code

_handler_instance: Optional[OnboardingHandler] = None


def get_handler() -> OnboardingHandler:
    """Get or create singleton handler instance."""
    global _handler_instance
    if _handler_instance is None:
        _handler_instance = OnboardingHandler()
    return _handler_instance


async def handle_with_class(phone: str, text: str) -> Dict[str, Any]:
    """
    Process message using the formal handler class.
    Can be used alongside or instead of _handle().
    """
    handler = get_handler()
    return await handler.process_message(phone, text)

