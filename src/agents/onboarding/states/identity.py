"""
IDENTITY State Handler (State 4: Name, Phone, Email Collection)
Rule-first approach with LLM fallback only when needed
Phone + email are required - one nudge, then exit if refused
"""
import logging
import time
import re
import asyncio
from typing import Dict, Any, Optional, Tuple
from ..messages import (
    IDENTITY_NAME_PROMPT, IDENTITY_CONTACT_PROMPT,
    IDENTITY_NUDGE, IDENTITY_BOUNDARY, IDENTITY_EXIT, format_message,
    IDENTITY_CONFIRM_CONTACT, IDENTITY_EMAIL_CORRECTION, IDENTITY_CONTACT_RETRY
)
from ..validators import is_yes_response, is_no_response
from ..config import settings
import httpx
import uuid

log = logging.getLogger(__name__)

# Missing field prompts
IDENTITY_MISSING_EMAIL = """Could you also share your email? 🙂"""
IDENTITY_MISSING_PHONE = """Could you also share your phone number? 🙂"""
IDENTITY_SAVE_RETRY = """Hmm, I couldn't save that just now — could you share your phone number and email once again? 🙂"""
IDENTITY_NAME_RETRY = """Could you share your name? 🙂"""
IDENTITY_NAME_INVALID = """No worries, What name should I call you?"""
IDENTITY_INVALID_EMAIL = """Please provide a valid email address (e.g., name@example.com) 🙂"""
IDENTITY_INVALID_PHONE = """Please provide a valid 10-digit phone number 🙂"""


def is_valid_phone(phone: str) -> bool:
    """
    Validate phone number format.
    
    Args:
        phone: Phone number string
        
    Returns:
        True if valid (exactly 10 digits), False otherwise
    """
    if not phone:
        return False
    
    # Remove any non-digit characters
    digits_only = re.sub(r'\D', '', phone)
    
    # Must be exactly 10 digits
    if len(digits_only) == 10 and digits_only.isdigit():
        return True
    
    return False


def is_valid_email(email: str) -> bool:
    """
    Validate email format.
    
    Args:
        email: Email string
        
    Returns:
        True if valid email format, False otherwise
    """
    if not email:
        return False
    
    # Basic email validation pattern
    # More strict than extraction: requires proper domain and TLD
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    
    if not re.match(email_pattern, email):
        return False
    
    # Additional checks
    parts = email.split('@')
    if len(parts) != 2:
        return False
    
    local, domain = parts
    
    # Local part should not be empty
    if not local or len(local) > 64:
        return False
    
    # Domain should have at least one dot and valid TLD
    if '.' not in domain:
        return False
    
    domain_parts = domain.split('.')
    if len(domain_parts) < 2:
        return False
    
    # TLD should be at least 2 characters
    tld = domain_parts[-1]
    if len(tld) < 2 or not tld.isalpha():
        return False
    
    return True


def extract_phone(text: str) -> Optional[str]:
    """
    Extract phone number from text (flexible format, handles obfuscated).
    
    Supports:
    - 10-digit Indian numbers
    - +91 prefix (optional)
    - Spaces, dashes, parentheses
    - Obfuscated formats like "nine eight seven..."
    
    Args:
        text: User's message
        
    Returns:
        Phone number string (10 digits) or None
    """
    text_lower = text.lower()
    
    # Handle obfuscated formats (e.g., "nine eight seven six five...")
    number_words = {
        "zero": "0", "one": "1", "two": "2", "three": "3", "four": "4",
        "five": "5", "six": "6", "seven": "7", "eight": "8", "nine": "9"
    }
    for word, digit in number_words.items():
        if word in text_lower:
            # Try to extract number words sequence
            words = text_lower.split()
            digits = []
            for w in words:
                if w in number_words:
                    digits.append(number_words[w])
            if len(digits) >= 10:
                return ''.join(digits[-10:])  # Take last 10 digits
    
    # Remove common separators and spaces
    cleaned = re.sub(r'[\s\-\(\)\+]', '', text)
    
    # Look for 10-digit numbers (Indian format)
    phone_match = re.search(r'\b(\d{10})\b', cleaned)
    if phone_match:
        return phone_match.group(1)
    
    # Look for numbers with country code (+91 optional)
    phone_match = re.search(r'(\+?91)?\s*(\d{10})', cleaned)
    if phone_match:
        return phone_match.group(2)
    
    # Look for phone patterns with separators
    phone_match = re.search(r'(\d{3})[\s\-]?(\d{3})[\s\-]?(\d{4})', cleaned)
    if phone_match:
        return phone_match.group(1) + phone_match.group(2) + phone_match.group(3)
    
    return None


def extract_email(text: str) -> Optional[str]:
    """
    Extract email from text (handles obfuscated formats).
    
    Supports:
    - Standard email pattern
    - Obfuscated formats like "name at gmail dot com"
    
    Args:
        text: User's message
        
    Returns:
        Email string or None (only if it matches the pattern, validation happens separately)
    """
    text_lower = text.lower()
    
    # Handle obfuscated formats (e.g., "name at gmail dot com")
    if " at " in text_lower or " @ " in text_lower:
        # Replace "at" with "@" and "dot" with "."
        obfuscated = text_lower.replace(" at ", "@").replace(" dot ", ".").replace(" dot", ".")
        # Try to extract email from modified text
        # Pattern requires: local@domain.tld (with proper TLD)
        email_match = re.search(r'\b([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\b', obfuscated)
        if email_match:
            extracted = email_match.group(1).lower()
            # Basic sanity check: must have @ and at least one dot after @
            if "@" in extracted and "." in extracted.split("@")[1]:
                return extracted
    
    # Standard email pattern
    # Pattern requires: local@domain.tld (with proper TLD)
    email_match = re.search(r'\b([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\b', text)
    if email_match:
        extracted = email_match.group(1).lower()
        # Basic sanity check: must have @ and at least one dot after @
        if "@" in extracted and "." in extracted.split("@")[1]:
            return extracted
    
    return None


def validate_name(text: str) -> bool:
    """
    Validate name input - check for common non-name phrases and invalid patterns.
    
    Args:
        text: User's message
    
    Returns:
        True if valid name, False if invalid (non-name phrase, digits, @, too short/long)
    """
    text_lower = text.lower().strip()
    
    # Check for common non-name phrases
    non_name_phrases = [
        "i don't know", "i dont know", "idk", "don't know", "dont know",
        "not sure", "unsure", "skip", "later", "no", "none", "nothing",
        "n/a", "na", "not applicable", "prefer not", "rather not"
    ]
    
    for phrase in non_name_phrases:
        if phrase in text_lower:
            return False
    
    # Check for digits (names shouldn't contain digits)
    if re.search(r'\d', text):
        return False
    
    # Check for @ symbol (email-like input)
    if '@' in text:
        return False
    
    # Check length (too short or too long)
    cleaned = re.sub(r'^[^\w]+|[^\w]+$', '', text.strip())
    if len(cleaned) < 2 or len(cleaned) > 100:
        return False
    
    # Check if it has alphabetic characters
    if not re.search(r'[a-zA-Z]', cleaned):
        return False
    
    return True


def extract_name(text: str) -> Tuple[Optional[str], bool]:
    """
    Extract and validate name from text.
    
    Args:
        text: User's message
    
    Returns:
        (name: Optional[str], is_valid: bool)
        name is cleaned and capitalized if valid
    """
    # First validate using validate_name helper
    if not validate_name(text):
        return (None, False)
    
    text = text.strip()
    
    # Remove common prefixes/suffixes
    text = re.sub(r'^(my name is|i am|i\'m|im|name is|this is|i\'m called|called)\s+', '', text, flags=re.IGNORECASE)
    text = text.strip()
    
    # Remove leading/trailing punctuation but keep internal punctuation
    text = re.sub(r'^[^\w]+|[^\w]+$', '', text)
    
    # Check if it's just emojis or numbers
    if re.match(r'^[\U0001F300-\U0001F9FF\s]+$', text):  # Emoji-only
        return (None, False)
    
    if re.match(r'^\d+$', text):  # Numbers only
        return (None, False)
    
    if text == "??" or text == "???" or len(text) < 2:
        return (None, False)
    
    # Check if it has alphabetic characters
    if not re.search(r'[a-zA-Z]', text):
        return (None, False)
    
    # Basic validation: should have at least 2 characters, max 100
    if len(text) >= 2 and len(text) <= 100:
        # Capitalize first letter of each word, preserve internal punctuation
        words = re.split(r'(\s+)', text)
        capitalized = ''.join(word.capitalize() if word.strip() else word for word in words)
        return (capitalized.strip(), True)
    
    return (None, False)


def classify_name_response(text: str) -> Tuple[str, Optional[str]]:
    """
    Classify name response using rule-based approach.
    
    Returns:
        (intent: str, name: Optional[str])
        intent can be: NAME_OK, NAME_UNCLEAR, QUERY
    """
    text_lower = text.lower().strip()
    
    # Check for QUERY
    if "?" in text or re.search(r"^(what|how|when|why|where|who|which|can|could|do|does|is|are)\b", text, re.I):
        return ("QUERY", None)
    
    # Extract and validate name
    name, is_valid = extract_name(text)
    
    if is_valid and name:
        return ("NAME_OK", name)
    else:
        return ("NAME_UNCLEAR", None)


def classify_contacts_response(text: str) -> Tuple[str, Optional[str], Optional[str], Optional[str]]:
    """
    Classify contacts response using rule-based approach.
    
    Returns:
        (intent: str, phone: Optional[str], email: Optional[str], invalid_field: Optional[str])
        intent can be: CONTACTS_OK, CONTACTS_PARTIAL, REFUSE_CONTACTS, QUERY, INVALID_EMAIL, INVALID_PHONE, AMBIGUOUS
        invalid_field can be: "email", "phone", or None
    """
    text_lower = text.lower().strip()
    
    # Check for QUERY
    if "?" in text or re.search(r"^(what|how|when|why|where|who|which|can|could|do|does|is|are)\b", text, re.I):
        return ("QUERY", None, None, None)
    
    # Check for REFUSE_CONTACTS
    refusal_patterns = [
        r"\b(don'?t want|dont want|not sharing|won'?t share|wont share|prefer not|rather not|no thanks)\b",
        r"\b(not comfortable|privacy|don'?t want to share|dont want to share)\b",
        r"\b(can'?t share|cannot share|cant share|unable to share)\b",
    ]
    for pattern in refusal_patterns:
        if re.search(pattern, text_lower):
            return ("REFUSE_CONTACTS", None, None, None)
    
    # Extract phone and email
    extracted_phone = extract_phone(text)
    extracted_email = extract_email(text)
    
    # Validate extracted data FIRST (before checking for failed extractions)
    if extracted_phone:
        if not is_valid_phone(extracted_phone):
            return ("INVALID_PHONE", None, None, "phone")
    
    if extracted_email:
        if not is_valid_email(extracted_email):
            return ("INVALID_EMAIL", None, None, "email")
    
    # Detect invalid format attempts (user tried but extraction failed)
    # Check if user tried to provide email but it's invalid
    # More specific check: look for @ symbol (most reliable indicator)
    has_email_indicators = "@" in text or " at " in text_lower
    # Also check if text contains email-like patterns that didn't extract
    # This pattern matches emails without TLD (which extract_email rejects)
    has_email_like_pattern = bool(re.search(r'[a-zA-Z0-9._%+-]+\s*@\s*[a-zA-Z0-9.-]+', text))
    has_phone_indicators = bool(re.search(r'\d', text)) and (len(re.findall(r'\d', text)) >= 5)
    
    # Check for invalid format attempts (user tried but extraction failed)
    # Priority: Check for email attempts first
    if has_email_indicators or has_email_like_pattern:
        if not extracted_email:
            # User tried to provide email but extraction failed (invalid format - likely missing TLD or malformed)
            # Double-check: make sure it's not just @ in random text
            # If @ is present and we have email-like pattern but no valid extraction, it's invalid
            if "@" in text:
                # Additional check: make sure there's text before and after @
                parts = text.split("@")
                if len(parts) >= 2 and len(parts[0].strip()) > 0 and len(parts[1].strip()) > 0:
                    return ("INVALID_EMAIL", None, None, "email")
    
    if has_phone_indicators and not extracted_phone:
        # User tried to provide phone but format is invalid
        # But be careful - might just be part of other text
        # Only flag if it looks like a phone attempt (has 5+ digits in sequence or common phone patterns)
        if re.search(r'\d{5,}', text) or re.search(r'(\+91|91)?\s*\d{6,}', text):
            return ("INVALID_PHONE", None, None, "phone")
    
    # Both valid
    if extracted_phone and extracted_email:
        if is_valid_phone(extracted_phone) and is_valid_email(extracted_email):
            return ("CONTACTS_OK", extracted_phone, extracted_email, None)
    
    # Partial - one valid
    if extracted_phone and is_valid_phone(extracted_phone):
        return ("CONTACTS_PARTIAL", extracted_phone, None, None)
    elif extracted_email and is_valid_email(extracted_email):
        return ("CONTACTS_PARTIAL", None, extracted_email, None)
    
    # Ambiguous
    if len(text.strip()) > 3:
        return ("AMBIGUOUS", None, None, None)
    else:
        return ("AMBIGUOUS", None, None, None)


def should_use_llm_for_name(text: str, intent: str) -> bool:
    """
    Determine if LLM should be invoked for name extraction.
    """
    # Only use LLM if unclear and message is long/complex
    if intent == "NAME_UNCLEAR" and len(text.split()) > 5:
        return True
    return False


def should_use_llm_for_contacts(text: str, intent: str) -> bool:
    """
    Determine if LLM should be invoked for contacts extraction.
    """
    words = text.split()
    word_count = len(words)
    text_lower = text.lower()
    
    # Use LLM if:
    # 1. Ambiguous and long message
    if intent == "AMBIGUOUS" and word_count > 10:
        return True
    
    # 2. Long message with mixed content
    if word_count > 12:
        # Check for mixed signals
        has_question = "?" in text
        has_answer = any(term in text_lower for term in ["phone", "email", "@", "gmail", "yahoo", "number"])
        if has_question and has_answer:
            return True
    
    # 3. Obfuscated formats detected but extraction failed
    obfuscated_indicators = [
        " at ", " dot ", "nine", "eight", "seven", "six", "five"
    ]
    if any(indicator in text_lower for indicator in obfuscated_indicators):
        if intent in ["AMBIGUOUS", "CONTACTS_PARTIAL"]:
            return True
    
    return False


async def save_profile(name: str, phone: str, email: str) -> Tuple[bool, Optional[str]]:
    """
    Call saveProfile MCP tool to save volunteer profile.
    
    Args:
        name: Volunteer name
        phone: Phone number
        email: Email address
        
    Returns:
        (success: bool, error_message: Optional[str])
    """
    MCP_BASE = settings.MCP_BASE
    MCP_JSONRPC_ENDPOINT = f"{MCP_BASE}/mcp/v1/jsonrpc"
    
    req_id = str(uuid.uuid4())
    payload = {
        "jsonrpc": "2.0",
        "id": req_id,
        "method": "tools/call",
        "params": {
            "name": "saveProfile",
            "arguments": {
                "name": name,
                "phone": phone,
                "email": email
            }
        }
    }
    
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(MCP_JSONRPC_ENDPOINT, json=payload)
            r.raise_for_status()
            response = r.json()
            
            if "error" in response:
                error = response["error"]
                log.error(f"[IDENTITY] saveProfile MCP error: {error}")
                return False, error.get("message", "Unknown error")
            
            log.info(f"[IDENTITY] saveProfile succeeded for {phone}")
            return True, None
    except Exception as e:
        error_msg = str(e)
        log.error(f"[IDENTITY] saveProfile failed: {error_msg}")
        return False, error_msg


async def handle_identity(phone: str, text: str, sess: Dict[str, Any], profile: Dict[str, Any]) -> None:
    """
    Handle IDENTITY state - collect name, phone, and email.
    
    Flow:
    1. Step A: Ask for name (rule-first, LLM fallback if needed)
    2. Step B: Ask for phone + email (rule-first, LLM fallback if needed)
    3. Refusal handling: one nudge, then exit
    4. saveProfile call and error handling
    """
    # Late import to avoid circular dependency
    from ..wa_loop import (
        mcp_wa_send, _add_to_history, _handle, SESSIONS,
        mcp_llm_classify_intent, build_llm_context
    )
    
    log.info(f"[IDENTITY] DEBUG: Handler called - text='{text[:50]}...', name_collected={sess.get('_identity_name_collected')}, contact_collected={sess.get('_identity_contact_collected')}, contact_asked={sess.get('_identity_contact_asked')}")
    
    # Step A: Ask for name if not collected
    if not sess.get("_identity_name_collected"):
        if text == "__kick__" or not sess.get("_identity_name_asked"):
            # First time: ask for name
            log.info(f"[IDENTITY] Asking for name from {phone}")
            await mcp_wa_send(phone, IDENTITY_NAME_PROMPT)
            _add_to_history(phone, bot_msg=IDENTITY_NAME_PROMPT)
            sess["_identity_name_asked"] = True
            sess["_identity_name_retry_count"] = 0
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        else:
            # User responded to name question
            # Step 1: Rule-based classification
            intent, extracted_name = classify_name_response(text)
            log.info(f"[IDENTITY] Name classification: intent={intent}, name={extracted_name}")
            
            # Step 2: LLM fallback if needed
            if intent == "NAME_UNCLEAR" and should_use_llm_for_name(text, intent):
                try:
                    log.info(f"[IDENTITY] Calling LLM for name extraction")
                    llm_context = build_llm_context("IDENTITY", sess, last_prompt=IDENTITY_NAME_PROMPT)
                    llm_result = await mcp_llm_classify_intent(text, "IDENTITY", llm_context)
                    llm_intent = (llm_result.get("intent") or "").upper()
                    
                    if llm_intent == "NAME_PROVIDED":
                        # Try to extract name from LLM response or original text
                        name, is_valid = extract_name(text)
                        if is_valid and name:
                            extracted_name = name
                            intent = "NAME_OK"
                            log.info(f"[IDENTITY] LLM helped extract name: {extracted_name}")
                except Exception as e:
                    log.warning(f"[IDENTITY] LLM classification failed: {e}")
            
            # Step 3: Handle based on intent
            if intent == "NAME_OK" and extracted_name:
                # Valid name received
                log.info(f"[IDENTITY] Name collected: {extracted_name}")
                profile["name"] = extracted_name
                sess["profile"] = profile
                sess["_identity_name_collected"] = True
                sess["_identity_name_asked"] = False
                sess["_identity_name_retry_count"] = 0
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                
                # Ask for email only (phone is already available from WhatsApp)
                # Format phone for display (add + prefix if not present, format as +91XXXXXXXXXX)
                display_phone = phone
                if not display_phone.startswith("+"):
                    # Add country code if not present (assuming India +91)
                    if len(display_phone) == 10:
                        display_phone = f"+91{display_phone}"
                    else:
                        display_phone = f"+{display_phone}"
                
                contact_msg = format_message(IDENTITY_CONTACT_PROMPT, name=extracted_name, phone=display_phone)
                await mcp_wa_send(phone, contact_msg)
                _add_to_history(phone, bot_msg=contact_msg)
                sess["_identity_contact_asked"] = True
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
            
            elif intent == "QUERY":
                # Answer question briefly and re-ask name
                log.info(f"[IDENTITY] User asked question about name, re-asking")
                await mcp_wa_send(phone, IDENTITY_NAME_PROMPT)
                _add_to_history(phone, bot_msg=IDENTITY_NAME_PROMPT)
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
            
            else:
                # NAME_UNCLEAR - check if it's invalid (non-name phrase, digits, @, etc.)
                is_invalid = not validate_name(text)
                
                if is_invalid:
                    # Invalid input (non-name phrase, digits, @, etc.) - use polite re-ask message
                    log.info(f"[IDENTITY] Invalid name input detected (non-name phrase/digits/@), re-asking politely")
                    await mcp_wa_send(phone, IDENTITY_NAME_INVALID)
                    _add_to_history(phone, bot_msg=IDENTITY_NAME_INVALID)
                    sess["ts"] = time.time()
                    SESSIONS[phone] = sess
                    return
                else:
                    # NAME_UNCLEAR but not obviously invalid - retry once
                    retry_count = sess.get("_identity_name_retry_count", 0)
                    if retry_count < 1:
                        log.info(f"[IDENTITY] Invalid name received, retrying ({retry_count + 1})")
                        sess["_identity_name_retry_count"] = retry_count + 1
                        await mcp_wa_send(phone, IDENTITY_NAME_RETRY)
                        _add_to_history(phone, bot_msg=IDENTITY_NAME_RETRY)
                        sess["ts"] = time.time()
                        SESSIONS[phone] = sess
                        return
                    else:
                        # Already retried once - ask again briefly
                        log.info(f"[IDENTITY] Name still unclear after retry, asking once more")
                        await mcp_wa_send(phone, IDENTITY_NAME_RETRY)
                        _add_to_history(phone, bot_msg=IDENTITY_NAME_RETRY)
                        sess["ts"] = time.time()
                        SESSIONS[phone] = sess
                        return
    
    # Step B: Collect email only (phone is already available from WhatsApp)
    if sess.get("_identity_name_collected") and not sess.get("_identity_contact_collected"):
        log.info(f"[IDENTITY] DEBUG: Entering Step B (email collection, phone={phone})")
        
        # Check if we're waiting for confirmation
        if sess.get("_identity_waiting_confirmation"):
            # User is responding to confirmation prompt
            log.info(f"[IDENTITY] User responding to confirmation prompt")
            stored_email = sess.get("_identity_pending_email")
            
            if is_yes_response(text):
                # Confirmed - proceed to save
                log.info(f"[IDENTITY] User confirmed contact info")
                extracted_email = stored_email
                extracted_phone = phone  # Use WhatsApp phone directly
                
                # Both are valid - save profile (bypassed for now)
                name = profile.get("name", "")
                log.info(f"[IDENTITY] Phone and email confirmed: {extracted_phone}, {extracted_email}")
                
                # Persistence: Update identity temp fields (checkpoint 2)
                try:
                    from storage.db import get_db_session
                    from storage.session_store import update_identity_temp
                    
                    with get_db_session() as db:
                        update_identity_temp(
                            db,
                            wa_phone=phone,
                            temp_name=name if name else None,
                            temp_email=extracted_email,
                            temp_phone=extracted_phone
                        )
                        log.info(f"[PERSISTENCE] Updated identity temp fields for {phone}")
                except Exception as e:
                    log.warning(f"[PERSISTENCE] Failed to update identity for {phone}: {e}", exc_info=True)
                    # Continue without DB - don't block flow
                
                # Clear confirmation flags
                sess.pop("_identity_waiting_confirmation", None)
                sess.pop("_identity_pending_email", None)
                
                # TODO: Uncomment when saveProfile MCP tool is implemented
                # Try to save profile
                # success, error = await save_profile(name, extracted_phone, extracted_email)
                
                # For now, bypass saveProfile and proceed
                log.info(f"[IDENTITY] saveProfile bypassed (not implemented yet), proceeding anyway")
                profile["phone"] = extracted_phone
                profile["email"] = extracted_email
                sess["profile"] = profile
                sess["_identity_contact_collected"] = True
                sess["_identity_nudge_sent"] = False  # Reset for next time
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                
                # Transition to next state (Preferences)
                log.info(f"[IDENTITY] Proceeding to PREFERENCES (saveProfile bypassed)")
                sess["state"] = "PREFERENCES"
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                await _handle(phone, "__kick__")
                return
            elif is_no_response(text):
                # Not confirmed - ask for email again
                log.info(f"[IDENTITY] User said no to confirmation, asking for email again")
                sess.pop("_identity_waiting_confirmation", None)
                sess.pop("_identity_pending_email", None)
                await mcp_wa_send(phone, IDENTITY_EMAIL_CORRECTION)
                _add_to_history(phone, bot_msg=IDENTITY_EMAIL_CORRECTION)
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
            else:
                # Ambiguous response - re-ask confirmation
                log.info(f"[IDENTITY] Ambiguous confirmation response, re-asking")
                stored_email = sess.get("_identity_pending_email")
                display_phone = phone
                if not display_phone.startswith("+"):
                    if len(display_phone) == 10:
                        display_phone = f"+91{display_phone}"
                    else:
                        display_phone = f"+{display_phone}"
                confirm_msg = format_message(IDENTITY_CONFIRM_CONTACT, phone=display_phone, email=stored_email)
                await mcp_wa_send(phone, confirm_msg)
                _add_to_history(phone, bot_msg=confirm_msg)
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
        
        # Step 1: Extract email only (ignore phone in user response)
        extracted_email = extract_email(text)
        log.info(f"[IDENTITY] Email extraction: {extracted_email}")
        
        # Step 1.5: Handle invalid email format IMMEDIATELY
        if extracted_email and not is_valid_email(extracted_email):
            log.info(f"[IDENTITY] Invalid email format provided")
            await mcp_wa_send(phone, IDENTITY_INVALID_EMAIL)
            _add_to_history(phone, bot_msg=IDENTITY_INVALID_EMAIL)
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        
        # Step 2: LLM fallback if email not found
        if not extracted_email:
            # Check if it's a refusal or query first
            text_lower = text.lower().strip()
            refusal_patterns = [
                r"\b(don'?t want|dont want|not sharing|won'?t share|wont share|prefer not|rather not|no thanks)\b",
                r"\b(not comfortable|privacy|don'?t want to share|dont want to share)\b",
                r"\b(can'?t share|cannot share|cant share|unable to share)\b",
            ]
            is_refusal = any(re.search(pattern, text_lower) for pattern in refusal_patterns)
            is_query = "?" in text or re.search(r"^(what|how|when|why|where|who|which|can|could|do|does|is|are)\b", text, re.I)
            
            if is_refusal:
                # Handle refusal flow
                if not sess.get("_identity_nudge_sent"):
                    # First nudge
                    log.info(f"[IDENTITY] User refused email, sending nudge")
                    await mcp_wa_send(phone, IDENTITY_NUDGE)
                    _add_to_history(phone, bot_msg=IDENTITY_NUDGE)
                    sess["_identity_nudge_sent"] = True
                    sess["ts"] = time.time()
                    SESSIONS[phone] = sess
                    return
                else:
                    # Still refusing after nudge - send boundary and exit
                    log.info(f"[IDENTITY] User still refusing after nudge, sending exit")
                    name = profile.get("name", "there")
                    
                    # Send boundary message
                    await mcp_wa_send(phone, IDENTITY_BOUNDARY)
                    _add_to_history(phone, bot_msg=IDENTITY_BOUNDARY)
                    await asyncio.sleep(1)  # Small pause
                    
                    # Send exit message
                    exit_msg = format_message(IDENTITY_EXIT, name=name)
                    await mcp_wa_send(phone, exit_msg)
                    _add_to_history(phone, bot_msg=exit_msg)
                    
                    sess["state"] = "REJECTED"
                    sess["ts"] = time.time()
                    SESSIONS[phone] = sess
                    return
            elif is_query:
                # Answer question briefly and re-ask email
                log.info(f"[IDENTITY] User asked question about email, re-asking")
                name = profile.get("name", "there")
                display_phone = phone
                if not display_phone.startswith("+"):
                    if len(display_phone) == 10:
                        display_phone = f"+91{display_phone}"
                    else:
                        display_phone = f"+{display_phone}"
                contact_msg = format_message(IDENTITY_CONTACT_PROMPT, name=name, phone=display_phone)
                await mcp_wa_send(phone, contact_msg)
                _add_to_history(phone, bot_msg=contact_msg)
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
            elif should_use_llm_for_contacts(text, "AMBIGUOUS"):
                # Try LLM to extract email
                try:
                    log.info(f"[IDENTITY] Calling LLM for email extraction")
                    llm_context = build_llm_context("IDENTITY", sess, last_prompt=IDENTITY_CONTACT_PROMPT)
                    llm_result = await mcp_llm_classify_intent(text, "IDENTITY", llm_context)
                    llm_intent = (llm_result.get("intent") or "").upper()
                    
                    if llm_intent == "CONTACTS_PROVIDED" or llm_intent == "EMAIL_PROVIDED":
                        # Re-extract email with LLM context
                        email = extract_email(text)
                        if email and is_valid_email(email):
                            extracted_email = email
                            log.info(f"[IDENTITY] LLM helped extract email")
                except Exception as e:
                    log.warning(f"[IDENTITY] LLM classification failed: {e}")
        
        # Step 3: Handle based on email extraction result
        if extracted_email and is_valid_email(extracted_email):
            # Valid email provided - show confirmation
            log.info(f"[IDENTITY] Valid email provided: {extracted_email}")
            display_phone = phone
            if not display_phone.startswith("+"):
                if len(display_phone) == 10:
                    display_phone = f"+91{display_phone}"
                else:
                    display_phone = f"+{display_phone}"
            
            confirm_msg = format_message(IDENTITY_CONFIRM_CONTACT, phone=display_phone, email=extracted_email)
            await mcp_wa_send(phone, confirm_msg)
            _add_to_history(phone, bot_msg=confirm_msg)
            
            # Store email and set confirmation flag
            sess["_identity_pending_email"] = extracted_email
            sess["_identity_waiting_confirmation"] = True
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        elif extracted_email:
            # Invalid email format (already handled in Step 1.5, but double-check)
            log.info(f"[IDENTITY] Invalid email format: {extracted_email}")
            await mcp_wa_send(phone, IDENTITY_INVALID_EMAIL)
            _add_to_history(phone, bot_msg=IDENTITY_INVALID_EMAIL)
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        else:
            # No email found - re-ask
            log.info(f"[IDENTITY] No email found in response, re-asking")
            await mcp_wa_send(phone, IDENTITY_CONTACT_RETRY)
            _add_to_history(phone, bot_msg=IDENTITY_CONTACT_RETRY)
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        
