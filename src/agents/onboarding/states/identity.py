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
    IDENTITY_NUDGE, IDENTITY_BOUNDARY, IDENTITY_EXIT, format_message
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
        Email string or None
    """
    text_lower = text.lower()
    
    # Handle obfuscated formats (e.g., "name at gmail dot com")
    if " at " in text_lower or " @ " in text_lower:
        # Replace "at" with "@" and "dot" with "."
        obfuscated = text_lower.replace(" at ", "@").replace(" dot ", ".").replace(" dot", ".")
        # Try to extract email from modified text
        email_match = re.search(r'\b([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\b', obfuscated)
        if email_match:
            return email_match.group(1).lower()
    
    # Standard email pattern
    email_match = re.search(r'\b([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\b', text)
    if email_match:
        return email_match.group(1).lower()
    
    return None


def extract_name(text: str) -> Tuple[Optional[str], bool]:
    """
    Extract and validate name from text.
    
    Args:
        text: User's message
        
    Returns:
        (name: Optional[str], is_valid: bool)
        name is cleaned and capitalized if valid
    """
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


def classify_contacts_response(text: str) -> Tuple[str, Optional[str], Optional[str]]:
    """
    Classify contacts response using rule-based approach.
    
    Returns:
        (intent: str, phone: Optional[str], email: Optional[str])
        intent can be: CONTACTS_OK, CONTACTS_PARTIAL, REFUSE_CONTACTS, QUERY, AMBIGUOUS
    """
    text_lower = text.lower().strip()
    
    # Check for QUERY
    if "?" in text or re.search(r"^(what|how|when|why|where|who|which|can|could|do|does|is|are)\b", text, re.I):
        return ("QUERY", None, None)
    
    # Check for REFUSE_CONTACTS
    refusal_patterns = [
        r"\b(don'?t want|dont want|not sharing|won'?t share|wont share|prefer not|rather not|no thanks)\b",
        r"\b(not comfortable|privacy|don'?t want to share|dont want to share)\b",
        r"\b(can'?t share|cannot share|cant share|unable to share)\b",
    ]
    for pattern in refusal_patterns:
        if re.search(pattern, text_lower):
            return ("REFUSE_CONTACTS", None, None)
    
    # Extract phone and email
    extracted_phone = extract_phone(text)
    extracted_email = extract_email(text)
    
    if extracted_phone and extracted_email:
        return ("CONTACTS_OK", extracted_phone, extracted_email)
    elif extracted_phone:
        return ("CONTACTS_PARTIAL", extracted_phone, None)
    elif extracted_email:
        return ("CONTACTS_PARTIAL", None, extracted_email)
    else:
        # Check if it's clearly ambiguous (has some text but no extractable data)
        if len(text.strip()) > 3:
            return ("AMBIGUOUS", None, None)
        else:
            return ("AMBIGUOUS", None, None)


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
                
                # Ask for phone + email
                contact_msg = format_message(IDENTITY_CONTACT_PROMPT, name=extracted_name)
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
                # NAME_UNCLEAR - retry once
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
    
    # Step B: Collect phone + email
    if sess.get("_identity_name_collected") and not sess.get("_identity_contact_collected"):
        # Step 1: Rule-based classification
        intent, extracted_phone, extracted_email = classify_contacts_response(text)
        log.info(f"[IDENTITY] Contacts classification: intent={intent}, phone={extracted_phone}, email={extracted_email}")
        
        # Step 2: LLM fallback if needed
        if intent in ["AMBIGUOUS", "CONTACTS_PARTIAL"] and should_use_llm_for_contacts(text, intent):
            try:
                log.info(f"[IDENTITY] Calling LLM for contacts extraction")
                llm_context = build_llm_context("IDENTITY", sess, last_prompt=IDENTITY_CONTACT_PROMPT)
                llm_result = await mcp_llm_classify_intent(text, "IDENTITY", llm_context)
                llm_intent = (llm_result.get("intent") or "").upper()
                
                if llm_intent == "CONTACTS_PROVIDED":
                    # Re-extract with LLM context
                    phone = extract_phone(text)
                    email = extract_email(text)
                    if phone and email:
                        extracted_phone = phone
                        extracted_email = email
                        intent = "CONTACTS_OK"
                        log.info(f"[IDENTITY] LLM helped extract contacts")
                    elif phone or email:
                        extracted_phone = phone
                        extracted_email = email
                        intent = "CONTACTS_PARTIAL"
            except Exception as e:
                log.warning(f"[IDENTITY] LLM classification failed: {e}")
        
        # Step 3: Handle based on intent
        if intent == "REFUSE_CONTACTS":
            # Handle refusal flow
            if not sess.get("_identity_nudge_sent"):
                # First nudge
                log.info(f"[IDENTITY] User refused contact info, sending nudge")
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
        
        elif intent == "QUERY":
            # Answer question briefly and re-ask contacts
            log.info(f"[IDENTITY] User asked question about contacts, re-asking")
            name = profile.get("name", "there")
            contact_msg = format_message(IDENTITY_CONTACT_PROMPT, name=name)
            await mcp_wa_send(phone, contact_msg)
            _add_to_history(phone, bot_msg=contact_msg)
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        
        elif intent == "CONTACTS_OK" and extracted_phone and extracted_email:
            # Both collected - save profile
            name = profile.get("name", "")
            log.info(f"[IDENTITY] Phone and email collected: {extracted_phone}, {extracted_email}")
            
            # Try to save profile
            success, error = await save_profile(name, extracted_phone, extracted_email)
            
            if success:
                # Profile saved successfully
                profile["phone"] = extracted_phone
                profile["email"] = extracted_email
                sess["profile"] = profile
                sess["_identity_contact_collected"] = True
                sess["_identity_nudge_sent"] = False  # Reset for next time
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                
                # Transition to next state (Preferences)
                log.info(f"[IDENTITY] Profile saved, proceeding to PREFERENCES")
                sess["state"] = "PREFERENCES"
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                await _handle(phone, "__kick__")
                return
            else:
                # Save failed - ask once again
                log.warning(f"[IDENTITY] saveProfile failed, asking to retry")
                await mcp_wa_send(phone, IDENTITY_SAVE_RETRY)
                _add_to_history(phone, bot_msg=IDENTITY_SAVE_RETRY)
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
        
        elif intent == "CONTACTS_PARTIAL":
            # Only one provided - ask for missing one
            if extracted_phone and not extracted_email:
                log.info(f"[IDENTITY] Phone provided, missing email")
                await mcp_wa_send(phone, IDENTITY_MISSING_EMAIL)
                _add_to_history(phone, bot_msg=IDENTITY_MISSING_EMAIL)
            elif extracted_email and not extracted_phone:
                log.info(f"[IDENTITY] Email provided, missing phone")
                await mcp_wa_send(phone, IDENTITY_MISSING_PHONE)
                _add_to_history(phone, bot_msg=IDENTITY_MISSING_PHONE)
            else:
                # Both missing - re-ask
                name = profile.get("name", "there")
                contact_msg = format_message(IDENTITY_CONTACT_PROMPT, name=name)
                await mcp_wa_send(phone, contact_msg)
                _add_to_history(phone, bot_msg=contact_msg)
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        
        else:
            # AMBIGUOUS or other - re-ask contacts
            log.info(f"[IDENTITY] Ambiguous response, re-asking contacts")
            name = profile.get("name", "there")
            contact_msg = format_message(IDENTITY_CONTACT_PROMPT, name=name)
            await mcp_wa_send(phone, contact_msg)
            _add_to_history(phone, bot_msg=contact_msg)
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
