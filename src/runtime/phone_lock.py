"""
Per-phone asyncio.Lock manager with automatic cleanup
"""
import asyncio
import time
import logging
from typing import Dict

log = logging.getLogger(__name__)

# Global lock storage: {wa_phone: asyncio.Lock}
_phone_locks: Dict[str, asyncio.Lock] = {}

# Track last usage: {wa_phone: timestamp}
_last_used: Dict[str, float] = {}

# Counter for cleanup trigger
_cleanup_counter = 0
CLEANUP_INTERVAL = 100  # Cleanup every N requests
LOCK_TIMEOUT_SECONDS = 30 * 60  # 30 minutes


def get_phone_lock(wa_phone: str) -> asyncio.Lock:
    """
    Get or create an asyncio.Lock for the given phone number.
    
    Implements automatic cleanup of unused locks every N requests.
    
    Args:
        wa_phone: WhatsApp phone number
        
    Returns:
        asyncio.Lock: Lock instance for this phone
    """
    global _cleanup_counter
    
    # Periodic cleanup
    _cleanup_counter += 1
    if _cleanup_counter >= CLEANUP_INTERVAL:
        _cleanup_locks()
        _cleanup_counter = 0
    
    # Get or create lock
    if wa_phone not in _phone_locks:
        _phone_locks[wa_phone] = asyncio.Lock()
        log.debug(f"[PHONE_LOCK] Created new lock for {wa_phone}")
    
    # Update last used timestamp
    _last_used[wa_phone] = time.time()
    
    return _phone_locks[wa_phone]


def _cleanup_locks() -> None:
    """
    Remove locks that haven't been used in the last 30 minutes.
    """
    now = time.time()
    to_remove = []
    
    for wa_phone, last_used_ts in _last_used.items():
        if now - last_used_ts > LOCK_TIMEOUT_SECONDS:
            to_remove.append(wa_phone)
    
    for wa_phone in to_remove:
        _phone_locks.pop(wa_phone, None)
        _last_used.pop(wa_phone, None)
        log.debug(f"[PHONE_LOCK] Cleaned up unused lock for {wa_phone}")
    
    if to_remove:
        log.info(f"[PHONE_LOCK] Cleaned up {len(to_remove)} unused lock(s)")

