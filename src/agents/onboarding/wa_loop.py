"""
WhatsApp Onboarding Agent - MCP Orchestrator Client

This agent handles volunteer onboarding via WhatsApp with:
- MCP server-led orchestration for all conversation flow
- Pure orchestrator client: calls onboarding.next and executes returned instructions
- No local fallback logic: server controls all conversation and business logic
"""
import asyncio
import re
import json
import time
import uuid
import logging
from datetime import datetime, timedelta
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import httpx

from .config import settings
from .messages import (
    WELCOME, WELCOME_MAYBE_LATER,
    ELIGIBILITY_INTRO, ELIGIBILITY_Q1, ELIGIBILITY_Q2, ELIGIBILITY_Q3,
    ELIGIBILITY_INVALID_RESPONSE, REJECTED, ELIGIBILITY_PASSED,
    ASK_TEACHING_PREF, CONFIRM_TEACHING_PREF, EDIT_TEACHING_PREF, TEACHING_PREF_UNCLEAR,
    ASK_AVAILABILITY, CONSTRAINTS_ANNOUNCE, AVAILABILITY_PARSE_FAILED,
    CONFIRM_SLOT_TEMPLATE, CONFIRM_SLOT_INVALID, SLOT_NONE_OF_ABOVE,
    CONFIRM_BOOKING, CONFIRM_BOOKING_INVALID, BOOKING_IN_PROGRESS,
    DONE, ALREADY_DONE, RESTARTING, PERSUADE_COMMITMENT, PERSUADE_WEEKEND_ONLY,
    YES_WORDS, NO_WORDS, MAYBE_LATER, CONFIRM_WORDS, EDIT_WORDS,
    format_message, format_slot_options, format_subjects_list
)
from .validators import is_yes_response, is_no_response, normalize_phone
from .humanizer import humanize_weekday_confirmation
from .faq import looks_like_question, retrieve, compose_answer

log = logging.getLogger(__name__)

# ---------- Session & Config ----------
SESSIONS: dict[str, dict] = {}  # {phone: {"state": "...", "profile": {...}, "ts": epoch, ...}}
CONVERSATION_HISTORIES: dict[str, object] = {}  # {phone: ChatHistory()} - SK Memory
MCP_BASE = settings.MCP_BASE
MCP_JSONRPC_ENDPOINT = f"{MCP_BASE}/mcp/v1/jsonrpc"
MCP_INITIALIZED = False

# Semantic Kernel instance (lazy-loaded)
_SK_KERNEL = None

# Kafka serializers
def _js(v): return json.dumps(v).encode()
def _ks(k): return (k or "").encode()


# ---------- MCP Initialization ----------
async def _mcp_list_tools():
    """List available tools from MCP server"""
    payload = {
        "jsonrpc": "2.0",
        "id": str(uuid.uuid4()),
        "method": "tools/list",
        "params": {}
    }
    
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(MCP_JSONRPC_ENDPOINT, json=payload)
            r.raise_for_status()
            response = r.json()
            
            if "error" in response:
                log.error(f"[MCP] List tools error: {response['error']}")
                return []
            
            tools = response.get("result", {}).get("tools", [])
            log.info(f"[MCP] Available tools: {[t.get('name') for t in tools]}")
            return tools
    except Exception as e:
        log.error(f"[MCP] Failed to list tools: {e}")
        return []


async def _mcp_initialize():
    """Initialize the MCP session"""
    global MCP_INITIALIZED
    
    if MCP_INITIALIZED:
        return
    
    log.info("[MCP] Initializing MCP session...")
    
    init_payload = {
        "jsonrpc": "2.0",
        "id": str(uuid.uuid4()),
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {
                "roots": {"listChanged": True},
                "sampling": {}
            },
            "clientInfo": {
                "name": "serve-vm-agent-onboarding-v2",
                "version": "2.0.0"
            }
        }
    }
    
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(MCP_JSONRPC_ENDPOINT, json=init_payload)
            r.raise_for_status()
            init_response = r.json()
            
            if "error" in init_response:
                error = init_response['error']
                if error.get('message') == 'Already initialized':
                    log.info("[MCP] Session already initialized, continuing...")
                    MCP_INITIALIZED = True
                    await _mcp_list_tools()
                    return
                else:
                    log.error(f"[MCP] Initialize error: {error}")
                    raise RuntimeError(f"MCP initialization failed: {error['message']}")
            
            log.info("[MCP] Initialize response received")
            
            # Send initialized notification
            initialized_payload = {
                "jsonrpc": "2.0",
                "method": "notifications/initialized"
            }
            
            r = await client.post(MCP_JSONRPC_ENDPOINT, json=initialized_payload)
            r.raise_for_status()
            
            MCP_INITIALIZED = True
            log.info("[MCP] MCP session initialized successfully")
            await _mcp_list_tools()
            
    except Exception as e:
        log.error(f"[MCP] Failed to initialize: {e}", exc_info=True)
        raise


# ---------- JSON-RPC 2.0 MCP Helper ----------
async def _mcp_call(tool_name: str, arguments: dict, timeout: int = 15) -> dict:
    """
    Call MCP tool via JSON-RPC 2.0
    
    Args:
        tool_name: Name of the MCP tool
        arguments: Tool arguments
        timeout: Request timeout
        
    Returns:
        Parsed result from tool
    """
    await _mcp_initialize()
    
    request_id = str(uuid.uuid4())
    payload = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "tools/call",
        "params": {
            "name": tool_name,
            "arguments": arguments
        }
    }
    
    try:
        log.info(f"[MCP] Calling tool={tool_name}")
        
        async with httpx.AsyncClient(timeout=timeout) as client:
            r = await client.post(MCP_JSONRPC_ENDPOINT, json=payload)
            r.raise_for_status()
            response = r.json()
            
            # Check for JSON-RPC error
            if "error" in response:
                error = response["error"]
                log.error(f"[MCP] Tool error: {error['message']} (code: {error['code']})")
                raise RuntimeError(f"MCP tool '{tool_name}' failed: {error['message']}")
            
            # Extract result.content[0].text
            if "result" in response and "content" in response["result"]:
                content = response["result"]["content"]
                if content and len(content) > 0:
                    text = content[0].get("text", "{}")
                    try:
                        parsed = json.loads(text)
                        log.info(f"[MCP] Success: tool={tool_name}")
                        return parsed
                    except (json.JSONDecodeError, TypeError):
                        return {"text": text}
            
            return response.get("result", {})
            
    except Exception as e:
        log.error(f"[MCP] Error calling {tool_name}: {e}")
        raise


# ---------- MCP Tool Wrappers ----------
def _wa_sanitize(text: str) -> str:
    """Best-effort sanitize to avoid server-side encoding issues (temporary guard)."""
    if not isinstance(text, str):
        return str(text)
    safe = text.replace("–", "-").replace("—", "-")
    # If MCP bridge/tool can't handle emoji/non-ASCII, drop them
    try:
        safe.encode("latin-1")
    except Exception:
        safe = safe.encode("ascii", "ignore").decode()
    return safe


async def mcp_wa_send(to: str, text: str):
    """Send WhatsApp message via MCP"""
    return await _mcp_call("wa.send_message", {"to": to, "text": _wa_sanitize(text)}, timeout=10)

async def mcp_llm_generate_reply(prompt: str, context: str = ""):
    """Generate LLM reply via MCP"""
    return await _mcp_call("llm.generate_reply", {"prompt": prompt, "context": context}, timeout=15)


async def mcp_time_parse(text: str, duration=60, tz="Asia/Kolkata"):
    """Parse time options via MCP (fallback for complex parsing)"""
    return await _mcp_call("time.parse_options", {
        "text": text,
        "duration_minutes": duration,
        "tz": tz
    }, timeout=240)


async def mcp_time_refine(slots: list[dict], desired_count: int = 3, tz: str = "Asia/Kolkata"):
    """Refine or expand slot options via MCP (authoritative tool name: time_refine)."""
    return await _mcp_call(
        "time_refine",
        {
            "slots": slots,
            "desired_count": desired_count,
            "tz": tz,
            "policy": {
                "weekday_only": True,
                "window_24h": {"start": "08:00", "end": "15:00"},
                "map_phrases": True,
            },
        },
        timeout=60,
    )


async def mcp_onboarding_parse(text: str, locale: str = "en-IN", state: str | None = None) -> dict:
    """Unified understanding via MCP onboarding.parse_message."""
    try:
        payload = {"text": text, "locale": locale}
        if state:
            payload["state"] = state
        return await _mcp_call(
            "onboarding.parse_message",
            payload,
            timeout=10,
        )
    except Exception as e:
        log.warning(f"[PARSE] onboarding.parse_message failed: {e}")
        return {}


async def classify_eligibility_response(phone: str, question_type: str, user_input: str, question_text: str) -> dict:
    """
    Classify user response to eligibility questions using LLM via MCP
    
    Args:
        phone: Phone number
        question_type: "age", "device", or "commitment"
        user_input: User's response text
        question_text: The question that was asked
        
    Returns:
        Dict with classification result:
        {
            "classification": "YES" | "NO" | "UNCLEAR",
            "confidence": 0.0-1.0,
            "reasoning": "string",
            "extracted_info": {...}
        }
    """
    try:
        result = await _mcp_call(
            "llm.classify_response",
            {
                "question_type": question_type,
                "user_input": user_input,
                "context": {
                    "question_text": question_text,
                    "locale": "en-IN"
                }
            },
            timeout=10
        )
        
        log.info(f"[CLASSIFY] Question={question_type}, Input='{user_input}', Result={result.get('classification')}")
        return result
        
    except Exception as e:
        log.error(f"[CLASSIFY] Error classifying response: {e}")
        # Fallback: return UNCLEAR so we can re-ask
        return {
            "classification": "UNCLEAR",
            "confidence": 0.0,
            "reasoning": f"Error during classification: {str(e)}",
            "extracted_info": {}
        }


def _commitment_meets_thresholds(extracted_info: dict) -> tuple[bool, bool]:
    """
    Evaluate commitment against configured thresholds.
    Returns (meets, near_miss) where near_miss means within tolerance window for persuasion.
    """
    try:
        hours = float(extracted_info.get("hours_per_week", 0) or 0)
        months = float(extracted_info.get("months", 0) or 0)
    except (TypeError, ValueError):
        hours, months = 0.0, 0.0

    min_hours = settings.MIN_HOURS_PER_WEEK
    tol_hours = settings.HOURS_TOLERANCE_RATIO * min_hours
    min_months = settings.MIN_MONTHS
    tol_months = settings.MONTHS_TOLERANCE

    hours_ok = hours >= min_hours
    months_ok = months >= min_months

    meets = hours_ok and months_ok
    if meets:
        return True, False

    # Near-miss window check
    near_hours = (min_hours - tol_hours) <= hours < min_hours
    near_months = (min_months - tol_months) <= months < min_months
    near_miss = (near_hours and months_ok) or (near_months and hours_ok) or (near_hours and near_months)
    return False, near_miss


async def generate_persuasive_response(phone: str, user_input: str, context_type: str = "class_timing") -> str:
    """
    Generate a contextual, empathetic persuasion response using LLM via MCP.
    
    Args:
        phone: Phone number for conversation history
        user_input: User's hesitation/response (e.g., "let me think", "weekend only")
        context_type: Type of context ("class_timing" for weekday constraint)
        
    Returns:
        Generated persuasive message string, or None on error
    """
    try:
        history = _get_conversation_history(phone)
        conversation_context = []
        if history and hasattr(history, 'messages'):
            for msg in history.messages[-6:]:  # Last 6 messages for context
                if hasattr(msg, 'role') and hasattr(msg, 'content'):
                    role = "user" if msg.role.value.lower() == "user" else "assistant"
                    content = str(msg.content)[:200]
                    conversation_context.append(f"{role}: {content}")
        
        # Build context-aware prompt
        if context_type == "class_timing":
            system_context = """You are a friendly volunteer coordinator for an educational program.
Our live classes run only on weekdays between 8 AM and 3 PM (this is non-negotiable due to school schedules).
When volunteers express hesitation or say they can only do weekends, respond with:
- Empathy and understanding
- A brief, practical suggestion (e.g., lunch break slot, 20-30 minutes)
- A gentle explanation of why weekdays matter (aligns with school hours, reaches more students)
- End by asking if they can find a small weekday window

Keep response to 2-3 short sentences, warm and encouraging, not pushy."""
            
            user_prompt = f"""The volunteer just responded to our class timing constraint (weekdays 8 AM-3 PM) with: "{user_input}"

Recent conversation:
{chr(10).join(conversation_context[-4:] if conversation_context else ["(beginning of conversation)"])}

Generate a brief, empathetic response that acknowledges their concern and gently offers 1-2 practical weekday options (like lunch break, early morning, or a short 20-30 min slot). End by asking if they can try a weekday window."""
        
        result = await _mcp_call(
            "llm.call",
            {
                "messages": [
                    {"role": "system", "content": system_context},
                    {"role": "user", "content": user_prompt}
                ],
                "max_tokens": 150,
                "temperature": 0.7
            },
            timeout=15
        )
        
        # Extract generated text (adjust based on actual MCP response format)
        generated_text = result.get("content") or result.get("message") or result.get("text", "")
        if generated_text:
            log.info(f"[PERSUASION] Generated response for hesitation: '{user_input[:50]}...'")
            return generated_text.strip()
        
        log.warning("[PERSUASION] LLM returned empty response, falling back to template")
        return None
        
    except Exception as e:
        log.error(f"[PERSUASION] Error generating response: {e}", exc_info=True)
        return None


async def generate_humanizer_reply(phone: str, flow_state_summary: str, user_input: str) -> dict | None:
    """Call MCP humanizer tool to produce strict JSON for weekday confirmation step."""
    try:
        result = await _mcp_call(
            "llm.humanize_weekday_confirmation",
            {
                "flow_state_summary": flow_state_summary,
                "user_input": user_input,
                "locale": "en-IN"
            },
            timeout=12
        )
        # Expecting keys: label, tone_prefix, reply, bridge_question
        if all(k in result for k in ["label", "tone_prefix", "reply", "bridge_question"]):
            return result
        log.warning(f"[HUMANIZER] Incomplete result: {result}")
        return None
    except Exception as e:
        log.error(f"[HUMANIZER] Error: {e}")
        return None

async def handle_smart_welcome_from_registration(phone: str, registration_data: dict) -> str:
    """Generate personalized welcome message based on registration data using LLM"""
    try:
        name = registration_data.get('name', 'Volunteer')
        source = registration_data.get('source', 'our platform')
        preferences = registration_data.get('preferences', 'none')
        
        prompt = f"""Create a personalized welcome message for a new volunteer who just registered.

Registration Data:
- Name: {name}
- Registration source: {source}
- Any preferences mentioned: {preferences}

Create a warm welcome message that:
1. Thanks them for registering and explicitly mentions this is the onboarding process
2. Uses their name personally
3. If they mentioned preferences, acknowledge them specifically
4. Explains what happens next (onboarding steps)
5. Sets positive expectations about teaching children
6. Invites them to ask onboarding-related questions anytime (FAQ welcome)

Tone: Warm, professional, encouraging
Length: 2-3 sentences
Keep it conversational and personal"""

        result = await mcp_llm_generate_reply(prompt, "registration_welcome")
        
        if result and "content" in result and len(result["content"]) > 0:
            welcome_msg = result["content"][0]["text"]
            log.info(f"[SMART_WELCOME] Generated personalized welcome for {name}")
            return welcome_msg
        else:
            log.warning(f"[SMART_WELCOME] LLM response invalid, using default")
            return format_message(WELCOME, name=name)
            
    except Exception as e:
        log.error(f"[SMART_WELCOME] Failed to generate personalized welcome: {e}")
        return format_message(WELCOME, name=registration_data.get('name', 'Volunteer'))

async def parse_time_slots_hybrid(text: str) -> dict:
    """
    Parse time slots using SK hybrid parser (consistent with teaching preferences)
    
    Strategy:
    1. SK plugin tries rule-based first (fast, free)
    2. SK plugin calls MCP if rules fail
    
    Args:
        text: User's message with time preferences
        
    Returns:
        Dict with slots and metadata
    """
    try:
        log.info(f"[TIME-HYBRID] Starting hybrid time parse for: '{text}'")
        kernel = await _get_sk_kernel()
        
        log.info(f"[TIME-HYBRID] Kernel loaded, calling hybrid_time_parser plugin...")
        result = await kernel.invoke(
            function=kernel.plugins["hybrid_time_parser"]["parse_time_slots"],
            user_text=text,
            duration_minutes=60,
            timezone="Asia/Kolkata"
        )
        
        log.info(f"[TIME-HYBRID] Plugin returned: {str(result)[:200]}...")
        parsed = json.loads(str(result))
        
        log.info(f"[TIME-HYBRID] Parsed via {parsed.get('method', 'unknown')}: {len(parsed.get('slots', []))} slots")
        
        return parsed
        
    except Exception as e:
        log.error(f"[TIME-HYBRID] Parsing failed with exception: {e}", exc_info=True)
        return {
            "slots": [],
            "method": "error",
            "confidence": "low",
            "needs_clarification": True,
            "error": str(e)
        }


async def mcp_calendar_create(title: str, start_iso: str, end_iso: str, attendees: list[str], timezone="Asia/Kolkata", notes=None):
    """Create calendar event via MCP"""
    return await _mcp_call("calendar.create_event", {
        "title": title,
        "start_iso": start_iso,
        "end_iso": end_iso,
        "attendees": attendees,
        "timezone": timezone,
        "notes": notes
    }, timeout=15)


# ---------- New MCP Tool Wrappers (Phase: more human flow) ----------
async def mcp_consent_record(volunteer_id: str, consent: bool):
    return await _mcp_call("consent.record", {"volunteerId": volunteer_id, "consentGiven": consent}, timeout=10)


async def mcp_eligibility_check(age_years: int | None, has_device: bool | None, weekly_commitment_hours: float | None):
    return await _mcp_call(
        "eligibility.check",
        {
            "ageYears": age_years,
            "hasDevice": has_device,
            "weeklyCommitmentHours": weekly_commitment_hours,
        },
        timeout=12,
    )


async def mcp_preferences_save(volunteer_id: str, time_band: str):
    return await _mcp_call("preferences.save", {"volunteerId": volunteer_id, "timeBand": time_band}, timeout=10)


async def mcp_preferences_save_v2(volunteer_id: str, prefs: dict, policy_version: str | None = None, idempotency_key: str | None = None):
    payload = {"volunteerId": volunteer_id, "prefs": prefs}
    if policy_version:
        payload["policy_version"] = policy_version
    if idempotency_key:
        payload["idempotency_key"] = idempotency_key
    return await _mcp_call("preferences.save", payload, timeout=12)


async def mcp_policy_scheduling(region_id: str | None = None):
    payload = {}
    if region_id:
        payload["region_id"] = region_id
    return await _mcp_call("policy.scheduling", payload, timeout=10)

async def mcp_slots_propose(
    volunteer_id: str,
    time_band: str | None,
    days_whitelist: list[str] | None,
    limit: int = 2,
    seed_time_iso: str | None = None,
    seed_times_iso: list[str] | None = None,
):
    payload = {
        "volunteerId": volunteer_id,
        "limit": limit,
    }
    if time_band:
        payload["timeBand"] = time_band
    # Pass through None (null) when days are not specified; do not force empty list
    payload["daysWhitelist"] = days_whitelist if days_whitelist is not None else None
    if seed_time_iso:
        payload["seedTimeIso"] = seed_time_iso
    # If multiple seeds are available, pass them; server may center around these
    if seed_times_iso:
        payload["seedTimesIso"] = seed_times_iso
    return await _mcp_call("slots.propose", payload, timeout=12)


async def mcp_slot_hold(slot_id: str):
    return await _mcp_call("slot.hold", {"slotId": slot_id}, timeout=10)


async def mcp_slot_book(hold_id: str):
    return await _mcp_call("slot.book", {"holdId": hold_id}, timeout=12)


async def mcp_reminder_create(when_iso: str, reason: str, volunteer_id: str | None = None):
    return await _mcp_call("reminder.create", {"when_ISO": when_iso, "reason": reason, "volunteerId": volunteer_id}, timeout=10)


async def mcp_telemetry_emit(event: str, payload: dict):
    return await _mcp_call("telemetry.emit", {"event": event, "payload": payload}, timeout=8)


async def mcp_profile_get(volunteer_id: str):
    return await _mcp_call("profile.get", {"volunteerId": volunteer_id}, timeout=10)


# ---------- Intent Detection Helpers (GREET & CONSENT) ----------
def _detect_consent_yes(text: str) -> bool:
    """Detect CONSENT_YES intent"""
    pattern = r"\b(yes|yep|y|sure|okay|ok\+|let'?s\s+go|ready|works|proceed|continue|absolutely|definitely)\b"
    return bool(re.search(pattern, text.lower()))


def _detect_consent_no(text: str) -> bool:
    """Detect CONSENT_NO intent"""
    pattern = r"\b(no|not\s+now|not\s+interested|nope|can'?t|don'?t\s+want|decline)\b"
    return bool(re.search(pattern, text.lower()))


def _detect_query(text: str) -> bool:
    """Detect QUERY/FAQ intent using keyword buckets"""
    text_lower = text.lower()
    # About SERVE
    if re.search(r"\b(what\s+is\s+serve|who\s+runs|government|ngo|organization)\b", text_lower):
        return True
    # Process/Time
    if re.search(r"\b(how\s+(do\s+i\s+)?teach|travel|time|hours|online|when|schedule)\b", text_lower):
        return True
    # Benefits/Support
    if re.search(r"\b(certificate|training|orientation|support|help)\b", text_lower):
        return True
    # Question mark
    if "?" in text:
        return True
    return False


def _detect_deferral(text: str) -> bool:
    """Detect DEFERRAL intent"""
    pattern = r"\b(later|next\s+week|another\s+time|not\s+today|busy|travel(l)?ing|remind|maybe\s+later)\b"
    return bool(re.search(pattern, text.lower()))


def _detect_returning(text: str) -> bool:
    """Detect RETURNING intent (user thinks they already onboarded)"""
    pattern = r"\b(already\s+(did|done)|completed|onboarded|finished|did\s+this)\b"
    return bool(re.search(pattern, text.lower()))


def _detect_stop(text: str) -> bool:
    """Detect STOP/OPT-OUT intent"""
    pattern = r"\b(stop|unsubscribe|don'?t\s+message|opt\s+out)\b"
    return bool(re.search(pattern, text.lower()))


def _detect_ambiguous(text: str) -> bool:
    """Detect AMBIGUOUS intent (short/emoji/low signal)"""
    text_stripped = text.strip()
    # Very short or emoji-only
    if len(text_stripped) < 8 or (len(text_stripped) <= 3 and not text_stripped.isalnum()):
        # Check if it's just emojis
        if re.match(r"^[\U0001F300-\U0001F9FF\U0001FA00-\U0001FAFF\U00002700-\U000027BF]+$", text_stripped):
            return True
        # Very short ambiguous responses
        if text_stripped.lower() in ["hmm", "maybe", "ok", "huh"]:
            return True
    return False


def _extract_simple_hours(text: str) -> float | None:
    """
    Minimal numeric extraction fallback for hours.
    Only extracts obvious numeric patterns: "1 hour", "2 hours", "1.5 hours", "an hour", "one hour"
    Used as fallback when LLM extraction fails.
    """
    if not text:
        return None
    
    text_lower = text.lower().strip()
    
    # Word-to-number mapping for common cases
    word_to_num = {
        "an": 1, "one": 1, "a": 1,
        "two": 2, "three": 3, "four": 4, "five": 5,
        "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10
    }
    
    # Pattern 1: Numeric hours ("1 hour", "2 hours", "1.5 hours", "2.5 hours")
    m = re.search(r"\b(\d+(?:\.\d+)?)\s*hours?\b", text_lower)
    if m:
        try:
            return float(m.group(1))
        except Exception:
            pass
    
    # Pattern 2: Word numbers + "hour" ("an hour", "one hour", "two hours")
    m = re.search(r"\b(an|one|two|three|four|five|six|seven|eight|nine|ten)\s+hours?\b", text_lower)
    if m:
        word = m.group(1)
        if word in word_to_num:
            return float(word_to_num[word])
    
    # Pattern 3: "hour" with number before ("1 hr", "2 hrs", but not "maybe 1 hour" without number)
    # This is a catch-all for "an hour" when it's standalone
    if re.search(r"\b(?:an|one)\s+hour\b", text_lower) and not re.search(r"\b\d+\s*hours?\b", text_lower):
        return 1.0
    
    return None


async def mcp_deferral_create(volunteer_id: str, reason: str, until_iso: str, idempotency_key: str | None = None):
    """Create a deferral for the user"""
    payload = {"volunteerId": volunteer_id, "reason": reason, "until_ISO": until_iso}
    if idempotency_key:
        payload["idempotency_key"] = idempotency_key
    return await _mcp_call("deferral.create", payload, timeout=10)


async def mcp_state_get(volunteer_id: str):
    """Get current state for the user"""
    return await _mcp_call("state.get", {"volunteerId": volunteer_id}, timeout=10)


async def mcp_state_advance(volunteer_id: str, intent: str, idempotency_key: str | None = None):
    """Advance state based on intent"""
    payload = {"volunteerId": volunteer_id, "intent": intent}
    if idempotency_key:
        payload["idempotency_key"] = idempotency_key
    return await _mcp_call("state.advance", payload, timeout=10)


async def mcp_llm_classify_intent(text: str, state: str, context: dict) -> dict:
    """LLM fallback for intent classification when rules fail"""
    system_prompt = f"""You are "Sia", SERVE's Volunteer Onboarding Assistant.
Goal: Interpret the user's message at state={state} and return JSON:
- A single intent label from: [CONSENT_YES, CONSENT_NO, QUERY, DEFERRAL, STOP, RETURNING, AMBIGUOUS]
- A confidence score between 0 and 1
- A short, warm, human reply ("tone_reply") that fits the inferred intent and current context.
Important:
- Do NOT promise payment; this is volunteer-only.
- Do NOT perform state changes or bookings; you only generate text and intent.
- Prefer brevity (2–4 lines) and clarity. Keep it supportive and human.

Return ONLY valid JSON: {{"intent": "...", "confidence": 0.0, "tone_reply": "..."}}"""

    user_prompt = f"""Context: {json.dumps(context, indent=2)}\nUser message: {text}"""
    
    try:
        result = await _mcp_call("llm.call", {
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "temperature": 0.2,
            "max_tokens": 200
        }, timeout=15)
        
        # Parse response (may be in content/message/text field)
        content = result.get("content") or result.get("message") or result.get("text", "")
        if isinstance(content, str):
            try:
                parsed = json.loads(content)
                if isinstance(parsed, dict) and "intent" in parsed:
                    return parsed
            except Exception:
                pass
        
        return {"intent": "AMBIGUOUS", "confidence": 0.0, "tone_reply": ""}
    except Exception as e:
        log.warning(f"[LLM] Intent classification failed: {e}")
        return {"intent": "AMBIGUOUS", "confidence": 0.0, "tone_reply": ""}


async def mcp_knowledge_search(query: str, top_k: int = 5, policy_version: str | None = None) -> list[dict]:
    """Search knowledge base for FAQ snippets"""
    try:
        payload = {"query": query, "top_k": top_k}
        if policy_version:
            payload["policy_version"] = policy_version
        result = await _mcp_call("knowledge.search", payload, timeout=10)
        # Return list of snippets: [{"title": "...", "text": "...", "id": "..."}, ...]
        if isinstance(result, list):
            return result
        if isinstance(result, dict) and "snippets" in result:
            return result["snippets"]
        return []
    except Exception as e:
        log.warning(f"[KNOWLEDGE] knowledge.search failed: {e}")
        return []


async def mcp_llm_qa(question: str, snippets: list[dict], policy_version: str | None = None, knowledge_version: str | None = None, user_profile: dict | None = None) -> str:
    """Generate FAQ answer using LLM with RAG context"""
    system_prompt = """You are Sia, SERVE's Volunteer Onboarding Assistant. You're warm, friendly, and concise.

TASK: Answer the volunteer's question in 2–4 short lines. Keep it natural, human, and policy-correct.

GUIDELINES:
1. Use ONLY the provided snippets/policy context. Do NOT invent facts or make promises.
2. Do NOT promise payment — this is a volunteer role (no pay).
3. Keep tone warm and supportive. Use simple, clear language.
4. If the question isn't fully covered by snippets, acknowledge it briefly and suggest asking the coordinator in orientation.
5. ALWAYS end with: "Shall we schedule your orientation?" (this is required).
6. Be concise: 2–4 lines maximum. No long explanations.

TONE EXAMPLES:
✅ "Yes! You'll attend a 30-min online orientation, and a local coordinator supports you during classes. Shall we schedule your orientation?"
✅ "Most volunteers teach English, Math or Science for grades 5–8. We'll align your preferences during scheduling. Shall we schedule your orientation?"
❌ Avoid: Long paragraphs, technical jargon, or promises about payment/certificates not in snippets.

Return ONLY plain text (no JSON, no markdown, no code blocks)."""
    
    context_obj = {
        "policy_version": policy_version,
        "knowledge_version": knowledge_version,
        "snippets": snippets,
        "user_profile": user_profile or {}
    }
    
    user_prompt = f"""Context:
{json.dumps(context_obj, indent=2)}

User question: {question}

Generate a warm, concise answer (2-4 lines) using the snippets above. End with 'Shall we schedule your orientation?'"""
    
    try:
        result = await _mcp_call("llm.call", {
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "temperature": 0.3,
            "max_tokens": 300
        }, timeout=15)
        
        content = result.get("content") or result.get("message") or result.get("text", "")
        if isinstance(content, str) and content.strip():
            return content.strip()
        return ""
    except Exception as e:
        log.warning(f"[LLM] QA generation failed: {e}")
        return ""


# ---------- MCP Orchestrator (Server-led policy) ----------
def _build_session_snapshot(sess: dict) -> dict:
    try:
        return {
            "state": sess.get("state"),
            "profile": sess.get("profile", {}),
            "ts": sess.get("ts"),
        }
    except Exception:
        return {"state": sess.get("state"), "profile": {}}


async def mcp_onboarding_next(session_snapshot: dict, user_text: str, locale: str = "en-IN") -> dict:
    return await _mcp_call(
        "onboarding.next",
        {"session": session_snapshot, "user_text": user_text, "locale": locale},
        timeout=15,
    )


async def _execute_mcp_calls(calls: list[dict]):
    if not calls:
        return
    for c in calls:
        try:
            tool = (c.get("tool") or "").strip()
            args = c.get("args") or {}
            if tool == "consent.record":
                await mcp_consent_record(args.get("volunteerId"), bool(args.get("consentGiven")))
            elif tool == "eligibility.check":
                await mcp_eligibility_check(args.get("ageYears"), args.get("hasDevice"), args.get("weeklyCommitmentHours"))
            elif tool == "preferences.save":
                await mcp_preferences_save(args.get("volunteerId"), args.get("timeBand"))
            elif tool == "slots.propose":
                await mcp_slots_propose(args.get("volunteerId"), args.get("timeBand"), args.get("daysWhitelist"), args.get("limit", 2))
            elif tool == "slot.hold":
                await mcp_slot_hold(args.get("slotId"))
            elif tool == "slot.book":
                await mcp_slot_book(args.get("holdId"))
            elif tool == "reminder.create":
                await mcp_reminder_create(args.get("when_ISO"), args.get("reason"), args.get("volunteerId"))
            elif tool == "telemetry.emit":
                await mcp_telemetry_emit(args.get("event", "onboarding.event"), args.get("payload") or {})
            elif tool == "calendar.create_event":
                await mcp_calendar_create(
                    args.get("title"), args.get("start_iso"), args.get("end_iso"), args.get("attendees") or [], args.get("timezone", "Asia/Kolkata"), args.get("notes")
                )
            elif tool == "wa.send_message":
                await mcp_wa_send(args.get("to"), args.get("text", ""))
        except Exception as e:
            log.warning(f"[ORCH] Failed call {c}: {e}")

# ---------- SK Kernel Setup ----------
async def _get_sk_kernel():
    """Get or create SK kernel instance (lazy-loaded)"""
    global _SK_KERNEL
    
    if _SK_KERNEL is None:
        log.info("[SK] Initializing Semantic Kernel...")
        # Import here to avoid circular dependency
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parent.parent.parent))
        
        from sk_poc.kernel_setup import create_kernel
        _SK_KERNEL = await create_kernel()
        log.info("[SK] Kernel ready")
    
    return _SK_KERNEL


def _get_conversation_history(phone: str):
    """Get or create conversation history for a phone number"""
    if phone not in CONVERSATION_HISTORIES:
        try:
            from semantic_kernel.contents import ChatHistory
            CONVERSATION_HISTORIES[phone] = ChatHistory()
            log.info(f"[MEMORY] Created conversation history for {phone}")
        except ImportError:
            log.warning("[MEMORY] SK not available, conversation history disabled")
            CONVERSATION_HISTORIES[phone] = None
    
    return CONVERSATION_HISTORIES[phone]


def _add_to_history(phone: str, user_msg: str = None, bot_msg: str = None):
    """Add messages to conversation history"""
    history = _get_conversation_history(phone)
    
    if history is None:
        return  # SK not available
    
    try:
        if user_msg:
            history.add_user_message(user_msg)
            log.debug(f"[MEMORY] Added user message to history for {phone}")
        if bot_msg:
            history.add_assistant_message(bot_msg)
            log.debug(f"[MEMORY] Added bot message to history for {phone}")
    except Exception as e:
        log.warning(f"[MEMORY] Failed to add to history: {e}")


# ---------- SK-Powered Hybrid Parser ----------
async def handle_smart_edit_with_memory(phone: str, user_input: str, current_profile: dict) -> dict:
    """
    Handle smart edits using conversation context via MCP LLM
    
    Examples:
    - "Change to English" → Updates language field
    - "Add Science" → Adds to subjects
    - "Make it 9-10" → Updates grades
    
    Args:
        phone: User's phone number
        user_input: User's edit request
        current_profile: Current profile state
        
    Returns:
        Updated profile or None if edit couldn't be understood
    """
    try:
        history = _get_conversation_history(phone)
        
        if history is None:
            log.warning("[MEMORY] No conversation history, can't do smart edit")
            return None
        
        log.info(f"[MEMORY] Handling smart edit with context: '{user_input}'")
        
        # Format conversation history for MCP
        conversation_history = []
        for msg in history.messages[-10:]:  # Last 10 messages
            if hasattr(msg, 'role') and hasattr(msg, 'content'):
                role = "user" if msg.role.value.lower() == "user" else "assistant"
                content = str(msg.content)[:200]  # Limit length
                conversation_history.append(f"{role}: {content}")
        
        # Format current profile
        profile_data = {
            "subjects": current_profile.get('subjects', []),
            "grades": current_profile.get('grades', ''),
            "language": current_profile.get('language', '')
        }
        
        # Call MCP LLM to understand the edit
        result = await _mcp_call(
            "llm.handle_smart_edit",
            {
                "conversation_history": conversation_history,
                "current_profile": profile_data,
                "user_input": user_input
            },
            timeout=15
        )
        
        log.info(f"[MEMORY] MCP returned: {result}")
        
        # Parse result
        if not result.get("understood", False):
            log.warning("[MEMORY] Edit not understood by LLM")
            return None
        
        # Return updated profile
        updated = {
            "subjects": result.get("updated_subjects", current_profile.get("subjects", [])),
            "grades": result.get("updated_grades", current_profile.get("grades", "")),
            "language": result.get("updated_language", current_profile.get("language", "")),
            "explanation": result.get("explanation", "Updated")
        }
        
        log.info(f"[MEMORY] Smart edit successful: {updated}")
        return updated
        
    except Exception as e:
        log.error(f"[MEMORY] Smart edit failed: {e}", exc_info=True)
        return None


async def parse_teaching_preferences_hybrid(text: str) -> dict:
    """
    Parse teaching preferences using SK hybrid parser
    
    This uses the intelligent hybrid approach:
    - Rules first (fast, free)
    - LLM fallback (smart, flexible)
    
    Args:
        text: User's message with teaching preferences
        
    Returns:
        Dict with parsed data and metadata
    """
    try:
        log.info(f"[HYBRID] Starting hybrid parse for: '{text}'")
        kernel = await _get_sk_kernel()
        
        log.info(f"[HYBRID] Kernel loaded, calling hybrid_parser plugin...")
        result = await kernel.invoke(
            function=kernel.plugins["hybrid_parser"]["parse_teaching_preferences"],
            user_text=text
        )
        
        log.info(f"[HYBRID] Plugin returned: {str(result)[:200]}...")
        parsed = json.loads(str(result))
        log.info(f"[HYBRID] Parsed via {parsed.get('method', 'unknown')}: complete={parsed.get('complete')}, valid={parsed.get('valid')}")
        log.info(f"[HYBRID] Full result: {json.dumps(parsed, indent=2)}")
        
        return parsed
        
    except Exception as e:
        log.error(f"[HYBRID] Parsing failed with exception: {e}", exc_info=True)
        # Fallback to empty result
        return {
            "subjects": [],
            "grades": "",
            "language": "English",
            "confidence": "low",
            "method": "error",
            "complete": False,
            "valid": False,
            "errors": [str(e)]
        }


# ---------- Helper Functions ----------
async def _book_slot_and_finish(phone: str, chosen_slot: dict, profile: dict, name: str):
    """
    Book the orientation slot and send final confirmation
    
    Args:
        phone: User's phone number
        chosen_slot: The slot to book
        profile: User's profile data
        name: User's name
    """
    start_iso = chosen_slot.get("start_iso")
    end_iso = chosen_slot.get("end_iso")
    label = chosen_slot.get("label")
    
    title = "Serve Vriddhi - Volunteer Orientation"
    attendees = [phone]
    
    try:
        res = await mcp_calendar_create(title, start_iso, end_iso, attendees)
        meet_url = res.get("meeting_url", "https://meet.google.com/placeholder")
        
        profile["meeting_url"] = meet_url
        profile["meeting_start"] = start_iso
        
        # Send "All done" message
        await mcp_wa_send(phone, BOOKING_IN_PROGRESS)
        _add_to_history(phone, bot_msg=BOOKING_IN_PROGRESS)
        
        # Send final confirmation
        msg = format_message(
            DONE,
            name=name,
            subjects=format_subjects_list(profile.get("subjects", [])),
            grades=profile.get("grades", "N/A"),
            language=profile.get("language", "N/A"),
            slot_label=label,
            meet_link=meet_url
        )
        await mcp_wa_send(phone, msg)
        _add_to_history(phone, bot_msg=msg)
        
    except Exception as e:
        log.error(f"[BOOKING] Failed to book slot for {phone}: {e}", exc_info=True)
        error_msg = "Sorry, I couldn't book the slot. Please contact support or try again."
        await mcp_wa_send(phone, error_msg)
        _add_to_history(phone, bot_msg=error_msg)


# ---------- Entry Point ----------
async def start_onboarding(phone: str, name: str = "Volunteer", registration_data: dict = None):
    """
    Start onboarding for a volunteer with optional registration data
    
    Args:
        phone: Phone number
        name: Volunteer name (default: "Volunteer")
        registration_data: Optional registration data for personalized welcome
    """
    phone = normalize_phone(phone)
    log.info(f"[START] Starting onboarding for phone={phone}, name={name}")
    
    # Initialize fresh session
    SESSIONS[phone] = {
        "state": "WELCOME",
        "profile": {
            "name": name,
            "registration_data": registration_data,
            "uuid": None,
            "eligibility": {
                "q1_commitment": None,
                "q2_age": None,
                "q3_device": None,
                "passed": False,
                "rejection_reason": None
            },
            "subjects": [],
            "grades": "",
            "language": "",
            "parsing_method": "",
            "parsing_confidence": "",
            "slots": [],
            "chosen_slot": {},
            "meeting_url": "",
            "meeting_start": ""
        },
        "ts": time.time(),
        "_welcomed": False
    }
    
    try:
        # Send welcome message
        await _handle(phone, "__kick__")
        log.info(f"[START] Welcome message sent to phone={phone}")
    except Exception as e:
        log.error(f"[START] Failed to start onboarding for phone={phone}: {e}", exc_info=True)
        raise


# ---------- State Machine ----------
async def _handle(phone: str, text: str):
    """
    Main state machine handler
    
    Args:
        phone: Phone number
        text: User's message
    """
    phone = normalize_phone(phone)
    sess = SESSIONS.get(phone)
    
    if not sess:
        log.warning(f"[HANDLE] No session for {phone}, creating new one")
        # Initialize a complete default profile to avoid KeyError later
        sess = {
            "state": "WELCOME",
            "profile": {
                "name": "Volunteer",
                "registration_data": None,
                "uuid": None,
                "eligibility": {
                    "q1_commitment": None,
                    "q2_age": None,
                    "q3_device": None,
                    "passed": False,
                    "rejection_reason": None
                },
                "subjects": [],
                "grades": "",
                "language": "",
                "parsing_method": "",
                "parsing_confidence": "",
                "slots": [],
                "chosen_slot": {},
                "meeting_url": "",
                "meeting_start": ""
            },
            "ts": time.time(),
            "_welcomed": False
        }
        SESSIONS[phone] = sess
    
    state = sess["state"]
    profile = sess.get("profile", {})
    # Ensure eligibility structure exists (guard against partial sessions)
    if "eligibility" not in profile or not isinstance(profile.get("eligibility"), dict):
        profile["eligibility"] = {
            "q1_commitment": None,
            "q2_age": None,
            "q3_device": None,
            "passed": False,
            "rejection_reason": None
        }
    name = profile.get("name", "Volunteer")
    
    log.info(f"[HANDLE] phone={phone}, state={state}, text='{text[:30]}...'")
    
    # Deduplicate repeated messages within a short window to avoid double-handling on reloads
    last_text = sess.get("_last_msg_text")
    last_ts = sess.get("_last_msg_ts", 0)
    now_ts = time.time()
    if last_text == text and (now_ts - last_ts) < 3:
        log.warning("[HANDLE] Duplicate message suppressed (within 3s window)")
        return

    # Add user message to conversation history (SK Memory)
    if text != "__kick__":  # Don't add internal triggers
        _add_to_history(phone, user_msg=text)

    # Helper: detect reschedule intent
    text_lower_global = text.lower().strip()
    def _wants_reschedule() -> bool:
        return any(k in text_lower_global for k in [
            "reschedule", "change time", "different time", "another time", "new time",
            "change slot", "pick a different", "move it", "resched"
        ])
    
    # Lightweight FAQ intercept (strict: only explicit questions)
    if text != "__kick__" and ("?" in text or re.search(r"^(what|how|when|why|where|who|which|can|could|do|does|is|are)\b", text, re.I)):
        # If we're in commitment (ELIGIBILITY_PART2) and the question is about "same day 2 hours",
        # skip FAQ so the commitment handler can respond with the correct policy clarification.
        same_day_commitment = (
            state == "ELIGIBILITY_PART2" and re.search(
                r"\b(2\s*hours?|two\s*hours?)\b.*\b(same\s*day|same-day|today)\b|\b(same\s*day|same-day|today)\b.*\b(2\s*hours?|two\s*hours?)\b",
                text, re.I
            )
        )
        if not same_day_commitment:
            try:
                top = retrieve(text, k=3)
                if top:
                    ans = await compose_answer(text, top)
                    if ans:
                        await mcp_wa_send(phone, ans)
                        _add_to_history(phone, bot_msg=ans)
                        # Pause progression after FAQ; resume on next user message
                        sess["ts"] = time.time()
                        SESSIONS[phone] = sess
                        return
                else:
                    log.info("[FAQ] No KB match; skipping FAQ answer")
            except Exception as e:
                log.warning(f"[FAQ] Failed to answer FAQ: {e}")

    # Unified parse hook: opportunistic fast-forward (skip for trivial rule hits)
    parsed = {}
    should_skip_parse = False
    if text != "__kick__":
        # Skip parser for trivial yes/no in GREET and ELIGIBILITY states to save cost/latency
        if state in ["WELCOME", "ELIGIBILITY_PART1", "ELIGIBILITY_PART2"]:
            if is_yes_response(text) or is_no_response(text):
                should_skip_parse = True
        if not should_skip_parse:
            parsed = await mcp_onboarding_parse(text, state=state)
        # parsed example fields:
        # intents: [..], consent: {value, confidence}, constraints: {weekday_ok, weekend_only, confidence}
        # availability: [{day, start, end, confidence}]

    # Handle restart command
    if text.lower() == "restart":
        SESSIONS.pop(phone, None)
        CONVERSATION_HISTORIES.pop(phone, None)  # Clear memory too
        await mcp_wa_send(phone, RESTARTING)
        _add_to_history(phone, bot_msg=RESTARTING)
        return
    
    # ========== WELCOME & CONSENT STATE ==========
    if state == "WELCOME":
        if text == "__kick__" or not sess.get("_greet_sent"):
            # First time: send 3 messages
            log.info(f"[GREET] Sending welcome messages to {phone}")
            
            # Message 1: Personal greeting
            msg1 = f"Hey {name}! I'm Sia from the SERVE team — thank you for registering with us. It's really nice to see you here! 🌸\n\nI help amazing people like you start their journey as volunteer teachers with SERVE."
            await mcp_wa_send(phone, msg1)
            _add_to_history(phone, bot_msg=msg1)
            await asyncio.sleep(1.5)  # Natural pause
            
            # Message 2: Ask to continue
            msg2 = "Before we move ahead, let me quickly share what SERVE is about, just so we're on the same page. Shall we continue?"
            await mcp_wa_send(phone, msg2)
            _add_to_history(phone, bot_msg=msg2)
            # Now wait for the volunteer to reply before continuing
            sess["_greet_sent"] = True
            sess["_greet_step"] = "await_continue"
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        else:
            # If we're awaiting a simple continue, handle that first
            if sess.get("_greet_step") == "await_continue":
                proceed = False
                if is_yes_response(text):
                    proceed = True
                else:
                    try:
                        cobj = (parsed.get("consent") or {}) if parsed else {}
                        cval = (cobj.get("value") or "").lower()
                        if cval in ["yes", "agreed", "okay", "sure"]:
                            proceed = True
                    except Exception:
                        pass
                if proceed:
                    # Share SERVE info and then consent ask
                    msg3 = "SERVE has helped thousands of children learn English, Science, and Maths through volunteers like you.\nYou teach online — they learn in school — and our local coordinators make sure everything runs smoothly."
                    await mcp_wa_send(phone, msg3)
                    _add_to_history(phone, bot_msg=msg3)
                    await asyncio.sleep(1.0)
                    msg4 = f"Just to be clear — this is a volunteer opportunity, not a paid role. Your time makes a real difference to children's learning. 💛\n\nDoes that sound good, {name}?\nWould you like me to take you through the onboarding steps now?"
                    await mcp_wa_send(phone, msg4)
                    _add_to_history(phone, bot_msg=msg4)
                    sess["_greet_step"] = "shared_info"
                    sess["ts"] = time.time(); SESSIONS[phone] = sess
                    return
                elif is_no_response(text):
                    decline_msg = f"No problem, {name}. Totally understand — thank you for your time and interest. If you ever wish to volunteer later, I'll be right here to help."
                    await mcp_wa_send(phone, decline_msg)
                    _add_to_history(phone, bot_msg=decline_msg)
                    sess["state"] = "REJECTED"
                    sess["ts"] = time.time(); SESSIONS[phone] = sess
                    return
                else:
                    # Try unified parse consent again, then LLM classifier to avoid rigid loops
                    try:
                        cobj = (parsed.get("consent") or {}) if parsed else {}
                        cval = (cobj.get("value") or "").lower()
                        if cval in ["yes", "agreed", "okay", "sure"]:
                            proceed = True
                    except Exception:
                        pass
                    if proceed:
                        msg3 = "SERVE has helped thousands of children learn English, Science, and Maths through volunteers like you.\nYou teach online — they learn in school — and our local coordinators make sure everything runs smoothly."
                        await mcp_wa_send(phone, msg3)
                        _add_to_history(phone, bot_msg=msg3)
                        await asyncio.sleep(1.0)
                        msg4 = f"Just to be clear — this is a volunteer opportunity, not a paid role. Your time makes a real difference to children's learning. 💛\n\nDoes that sound good, {name}?\nWould you like me to take you through the onboarding steps now?"
                        await mcp_wa_send(phone, msg4)
                        _add_to_history(phone, bot_msg=msg4)
                        sess["_greet_step"] = "shared_info"
                        sess["ts"] = time.time(); SESSIONS[phone] = sess
                        return

                    # LLM classify as final fallback
                    try:
                        llm_ctx = {"last_prompt": "Shall we continue?", "state": "WELCOME"}
                        cls = await mcp_llm_classify_intent(text, "WELCOME", llm_ctx)
                        if isinstance(cls, dict):
                            label = (cls.get("intent") or cls.get("label") or "").upper()
                            conf = float(cls.get("confidence") or 0)
                            if label in ["CONSENT_YES", "YES"] and conf >= 0.6:
                                # proceed
                                msg3 = "SERVE has helped thousands of children learn English, Science, and Maths through volunteers like you.\nYou teach online — they learn in school — and our local coordinators make sure everything runs smoothly."
                                await mcp_wa_send(phone, msg3)
                                _add_to_history(phone, bot_msg=msg3)
                                await asyncio.sleep(1.0)
                                msg4 = f"Just to be clear — this is a volunteer opportunity, not a paid role. Your time makes a real difference to children's learning. 💛\n\nDoes that sound good, {name}?\nWould you like me to take you through the onboarding steps now?"
                                await mcp_wa_send(phone, msg4)
                                _add_to_history(phone, bot_msg=msg4)
                                sess["_greet_step"] = "shared_info"
                                sess["ts"] = time.time(); SESSIONS[phone] = sess
                                return
                            if label in ["CONSENT_NO", "NO"] and conf >= 0.6:
                                decline_msg = f"No problem, {name}. Totally understand — thank you for your time and interest. If you ever wish to volunteer later, I'll be right here to help."
                                await mcp_wa_send(phone, decline_msg)
                                _add_to_history(phone, bot_msg=decline_msg)
                                sess["state"] = "REJECTED"
                                sess["ts"] = time.time(); SESSIONS[phone] = sess
                                return
                    except Exception:
                        pass

                    clarify = "Just a quick yes or no — shall we continue?"
                    await mcp_wa_send(phone, clarify)
                    _add_to_history(phone, bot_msg=clarify)
                    sess["ts"] = time.time(); SESSIONS[phone] = sess
                    return

            # User replied to consent question - comprehensive intent handling
            text_lower = text.lower().strip()
            volunteer_id = profile.get("uuid") or phone
            intent_detected = None
            llm_called = False
            llm_result = None
            
            # 1) CONSENT_YES
            if _detect_consent_yes(text) or is_yes_response(text):
                intent_detected = "CONSENT_YES"
            # 2) CONSENT_NO
            elif _detect_consent_no(text) or is_no_response(text):
                intent_detected = "CONSENT_NO"
            # 3) QUERY (FAQ)
            elif _detect_query(text):
                intent_detected = "QUERY"
            # 4) DEFERRAL
            elif _detect_deferral(text):
                intent_detected = "DEFERRAL"
            # 5) RETURNING
            elif _detect_returning(text):
                intent_detected = "RETURNING"
            # 6) STOP / OPT-OUT
            elif _detect_stop(text):
                intent_detected = "STOP"
            # 7) AMBIGUOUS
            elif _detect_ambiguous(text):
                intent_detected = "AMBIGUOUS"
            # 8) Check unified parse as fallback
            else:
                try:
                    cobj = (parsed.get("consent") or {}) if parsed else {}
                    cval = (cobj.get("value") or "").lower()
                    if cval in ["yes", "agreed", "okay", "sure"]:
                        intent_detected = "CONSENT_YES"
                    elif cval in ["no", "declined", "not interested"]:
                        intent_detected = "CONSENT_NO"
                except Exception:
                    pass
            
            # LLM Fallback if still unclear
            if intent_detected is None or intent_detected == "AMBIGUOUS":
                log.info(f"[GREET] Calling LLM fallback for intent classification")
                llm_called = True
                context = {
                    "state": "WELCOME",
                    "user_profile": {"name": name, "locale": "en-IN"},
                    "last_agent_prompt": "Just to be clear — this is a volunteer opportunity, not a paid role. Your time makes a real difference to children's learning. Does that sound good, {name}? Would you like me to take you through the onboarding steps now?"
                }
                llm_result = await mcp_llm_classify_intent(text, "WELCOME", context)
                if llm_result.get("confidence", 0) >= 0.70:
                    intent_detected = llm_result.get("intent", "AMBIGUOUS")
                    log.info(f"[GREET] LLM classified intent: {intent_detected} (confidence: {llm_result.get('confidence', 0)})")
                else:
                    intent_detected = "AMBIGUOUS"
                    log.info(f"[GREET] LLM confidence too low, treating as AMBIGUOUS")
            
            # Generate idempotency key for this turn
            idempotency_key = f"{volunteer_id}_{intent_detected}_{int(time.time())}"
            
            # Route based on detected intent
            if intent_detected == "CONSENT_YES":
                # Record consent and advance state
                try:
                    await mcp_consent_record(volunteer_id, True)
                    await mcp_state_advance(volunteer_id, "to_ELIGIBILITY_PART1", idempotency_key)
                except Exception as e:
                    log.warning(f"[GREET] Failed to record consent/advance state: {e}")
                
                # Move to eligibility and send question immediately
                sess["state"] = "ELIGIBILITY_PART1"
                sess["_eligibility_step"] = "age"  # Start with age question
                sess["_eligibility_age_asked"] = False  # Will be set when question is sent
                sess["_eligibility_device_asked"] = False
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                log.info(f"[GREET] Consent recorded, moving to ELIGIBILITY_PART1")
                
                # Send acknowledgment
                ack_msg = f"That's wonderful, {name}! Let's start with a few quick questions to get you ready."
                await mcp_wa_send(phone, ack_msg)
                _add_to_history(phone, bot_msg=ack_msg)
                await asyncio.sleep(1.0)  # Small pause
                
                # Send first eligibility question (age only)
                age_msg = "First, to make sure you meet our policy — may I check if you're 18 or older?"
                await mcp_wa_send(phone, age_msg)
                _add_to_history(phone, bot_msg=age_msg)
                sess["_eligibility_age_asked"] = True
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                
                # Telemetry
                try:
                    await mcp_telemetry_emit("onboarding.consent_yes", {
                        "conversation_id": phone,
                        "user_id": volunteer_id,
                        "state_before": "WELCOME",
                        "state_after": "ELIGIBILITY_PART1",
                        "intent": intent_detected,
                        "llm_called": llm_called,
                        "rule_hit": not llm_called
                    })
                except Exception:
                    pass
                return
                
            elif intent_detected == "CONSENT_NO":
                # Record consent as no and move to rejected
                try:
                    await mcp_consent_record(volunteer_id, False)
                except Exception as e:
                    log.warning(f"[GREET] Failed to record consent: {e}")
                
                decline_msg = f"No problem, {name}. Totally understand — thank you for your time and interest. If you ever wish to volunteer later, I'll be right here to help."
                await mcp_wa_send(phone, decline_msg)
                _add_to_history(phone, bot_msg=decline_msg)
                sess["state"] = "REJECTED"
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                
                # Telemetry
                try:
                    await mcp_telemetry_emit("onboarding.consent_no", {
                        "conversation_id": phone,
                        "user_id": volunteer_id,
                        "intent": intent_detected,
                        "llm_called": llm_called
                    })
                except Exception:
                    pass
                return
                
            elif intent_detected == "QUERY":
                # Answer FAQ using local RAG
                try:
                    top = retrieve(text, k=3)
                    if top:
                        ans = await compose_answer(text, top)
                        if ans:
                            await mcp_wa_send(phone, ans)
                            _add_to_history(phone, bot_msg=ans)
                            await asyncio.sleep(1.0)  # Small pause
                            # Re-ask consent after FAQ
                            reask = f"Great question! Does that sound good, {name}? Would you like to go ahead and start?"
                            await mcp_wa_send(phone, reask)
                            _add_to_history(phone, bot_msg=reask)
                            return
                except Exception as e:
                    log.warning(f"[FAQ] Failed to answer FAQ: {e}")
                
                # Fallback if FAQ failed
                unclear = f"I'd be happy to answer your question, {name}. Could you rephrase it, or would you like to proceed with onboarding and ask later?"
                await mcp_wa_send(phone, unclear)
                _add_to_history(phone, bot_msg=unclear)
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
                
            elif intent_detected == "DEFERRAL":
                # Parse deferral time or default to 3-7 days
                from datetime import timezone
                until_date = datetime.now(timezone.utc) + timedelta(days=5)  # Default 5 days
                until_iso = until_date.isoformat()  # Will produce: 2025-11-06T19:33:08.334000+00:00
                
                # Try to extract date/time from text if mentioned
                # (Simple extraction - can be enhanced)
                
                try:
                    # Debug: Log the exact payload being sent
                    payload_debug = {
                        "volunteerId": volunteer_id,
                        "reason": "user_requested_later",
                        "until_ISO": until_iso,
                        "idempotency_key": idempotency_key
                    }
                    log.info(f"[DEFERRAL] Sending payload to deferral.create: {json.dumps(payload_debug, indent=2)}")
                    await mcp_deferral_create(volunteer_id, "user_requested_later", until_iso, idempotency_key)
                    defer_msg = f"No worries, {name}! I'll remind you in a few days. Ping me anytime if you want to start earlier."
                    await mcp_wa_send(phone, defer_msg)
                    _add_to_history(phone, bot_msg=defer_msg)
                    sess["state"] = "DEFERRED"
                    sess["ts"] = time.time()
                    SESSIONS[phone] = sess
                    return
                except Exception as e:
                    log.warning(f"[GREET] Failed to create deferral: {e}")
                    # Fallback: just acknowledge
                    defer_msg = f"No worries, {name}! Feel free to come back whenever you're ready."
                    await mcp_wa_send(phone, defer_msg)
                    _add_to_history(phone, bot_msg=defer_msg)
                    sess["state"] = "DEFERRED"
                    sess["ts"] = time.time()
                    SESSIONS[phone] = sess
                    return
                    
            elif intent_detected == "RETURNING":
                # Check if user has existing state
                try:
                    state_info = await mcp_state_get(volunteer_id)
                    existing_state = state_info.get("state") if isinstance(state_info, dict) else None
                    if existing_state and existing_state != "WELCOME":
                        # Fast-forward to existing state
                        msg = f"I see your status is {existing_state}. We can pick up from there. Ready to continue?"
                        await mcp_wa_send(phone, msg)
                        _add_to_history(phone, bot_msg=msg)
                        sess["state"] = existing_state
                        sess["ts"] = time.time()
                        SESSIONS[phone] = sess
                        return
                except Exception as e:
                    log.warning(f"[GREET] Failed to get existing state: {e}")
                
                # If no existing state or lookup failed, proceed normally
                msg = f"I'll help you continue, {name}. Let's pick up from where you left off. Ready to start?"
                await mcp_wa_send(phone, msg)
                _add_to_history(phone, bot_msg=msg)
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
                
            elif intent_detected == "STOP":
                # Respect opt-out immediately
                stop_msg = f"Understood, {name}. I'll stop messaging you. Thank you for your time."
                await mcp_wa_send(phone, stop_msg)
                _add_to_history(phone, bot_msg=stop_msg)
                sess["state"] = "OPTOUT"
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                
                # Telemetry
                try:
                    await mcp_telemetry_emit("onboarding.opt_out", {
                        "conversation_id": phone,
                        "user_id": volunteer_id,
                        "intent": intent_detected
                    })
                except Exception:
                    pass
                return
                
            else:  # AMBIGUOUS or unknown
                # Use LLM tone_reply if available, else default
                if llm_result and llm_result.get("tone_reply"):
                    unclear = llm_result["tone_reply"]
                else:
                    unclear = f"I think you're leaning towards continuing. If you'd like, I can start your onboarding now — or I can check back later. What works for you, {name}?"
                
                await mcp_wa_send(phone, unclear)
                _add_to_history(phone, bot_msg=unclear)
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                
                # Telemetry
                try:
                    await mcp_telemetry_emit("onboarding.ambiguous_response", {
                        "conversation_id": phone,
                        "user_id": volunteer_id,
                        "intent": intent_detected or "UNKNOWN",
                        "llm_called": llm_called,
                        "llm_confidence": llm_result.get("confidence", 0) if llm_result else 0
                    })
                except Exception:
                    pass
                return
    
    # ========== ELIGIBILITY (PART 1: age, then device) ==========
    elif state == "ELIGIBILITY_PART1":
        # Track which question we're on: "age" or "device"
        elig_step = sess.get("_eligibility_step", "age")
        volunteer_id = profile.get("uuid") or phone
        
        # Q1 - Age check (first question)
        if elig_step == "age":
            if not sess.get("_eligibility_age_asked"):
                # First time: ask age question
                log.info(f"[ELIG] Sending age question to {phone}")
                msg = "First, to make sure you meet our policy — may I check if you're 18 or older?"
                await mcp_wa_send(phone, msg)
                _add_to_history(phone, bot_msg=msg)
                sess["_eligibility_age_asked"] = True
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
            else:
                # User replied to age question
                # TRUST LLM-FIRST: Use onboarding.parse_message results (already called above)
                age_ok = None
                age_value = None
                
                # Primary source: onboarding.parse_message (LLM extraction)
                try:
                    hints = parsed.get("eligibility") or {} if parsed else {}
                    age_ok = hints.get("age_ok")
                    age_value = hints.get("age")  # Get numeric age if available
                    if age_ok is not None or age_value is not None:
                        log.info(f"[ELIG] LLM extracted age: ok={age_ok}, value={age_value}")
                        # If we have numeric age but not age_ok, derive it
                        if age_value is not None and age_ok is None:
                            age_ok = age_value >= 18
                except Exception as e:
                    log.warning(f"[ELIG] Failed to parse age from LLM: {e}")
                
                # Fallback: Simple yes/no for trivial responses only
                if age_ok is None:
                    # Simple yes (very obvious cases only)
                    if is_yes_response(text):
                        age_ok = True
                        log.info(f"[ELIG] Simple yes detected for age")
                    
                    # Simple no (very obvious cases only)
                    elif is_no_response(text):
                        age_ok = False
                        log.info(f"[ELIG] Simple no detected for age")
                
                # Handle unclear responses
                if age_ok is None:
                    unclear_msg = "I didn't quite catch that. Could you confirm: are you 18 or older? (Yes/No)"
                    await mcp_wa_send(phone, unclear_msg)
                    _add_to_history(phone, bot_msg=unclear_msg)
                    sess["ts"] = time.time()
                    SESSIONS[phone] = sess
                    return
                
                # HARD RULE: Age < 18 → immediate decline (no persuasion)
                if age_ok is False or (age_value is not None and age_value < 18):
                    decline_msg = f"Thanks for your enthusiasm 💛\nAt the moment, SERVE is open to adults (18+). Once you turn 18, I'll be happy to help you start your volunteering journey. 🌟"
                    await mcp_wa_send(phone, decline_msg)
                    _add_to_history(phone, bot_msg=decline_msg)
                    sess["state"] = "REJECTED"
                    sess["ts"] = time.time()
                    SESSIONS[phone] = sess
                    
                    # Telemetry
                    try:
                        await mcp_telemetry_emit("onboarding.age_decline", {
                            "conversation_id": phone,
                            "user_id": volunteer_id,
                            "age_value": age_value
                        })
                    except Exception:
                        pass
                    return
                
                # Age OK → Store and move to device question
                sess["elig.age"] = True
                sess["elig.age_value"] = age_value if age_value else 18
                sess["_eligibility_step"] = "device"
                sess["_eligibility_device_asked"] = True  # Mark as asked (we're about to send it)
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                
                # Send device question immediately
                await asyncio.sleep(0.5)  # Small pause
                device_msg = "Thank you! Do you have a smartphone or laptop and a stable internet connection to join the live classes?"
                await mcp_wa_send(phone, device_msg)
                _add_to_history(phone, bot_msg=device_msg)
                return
        
        # Q2 - Device check (second question)
        elif elig_step == "device":
            if not sess.get("_eligibility_device_asked"):
                # Shouldn't reach here, but handle just in case
                device_msg = "Thank you! Do you have a smartphone or laptop and a stable internet connection to join the live classes?"
                await mcp_wa_send(phone, device_msg)
                _add_to_history(phone, bot_msg=device_msg)
                sess["_eligibility_device_asked"] = True
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
            else:
                # User replied to device question
                # TRUST LLM-FIRST: Use onboarding.parse_message results (already called above)
                has_device = None
                
                # Primary source: onboarding.parse_message (LLM extraction)
                try:
                    hints = parsed.get("eligibility") or {} if parsed else {}
                    has_device = hints.get("has_device") or hints.get("device_ok")
                    if has_device is not None:
                        log.info(f"[ELIG] LLM extracted device: {has_device}")
                except Exception as e:
                    log.warning(f"[ELIG] Failed to parse device from LLM: {e}")
                
                # Fallback: Simple yes/no for trivial responses only
                if has_device is None:
                    # Simple yes (very obvious cases only)
                    if is_yes_response(text):
                        has_device = True
                        log.info(f"[ELIG] Simple yes detected for device")
                    
                    # Simple no (very obvious cases only)
                    elif is_no_response(text):
                        has_device = False
                        log.info(f"[ELIG] Simple no detected for device")
                
                # Handle unclear responses
                if has_device is None:
                    followup_msg = f"Could you clarify, {name}? Do you have a smartphone or laptop with internet access?"
                    await mcp_wa_send(phone, followup_msg)
                    _add_to_history(phone, bot_msg=followup_msg)
                    sess["ts"] = time.time()
                    SESSIONS[phone] = sess
                    return
                
                # Store device status
                sess["elig.device"] = has_device
                
                # If no device → offer deferral
                if has_device is False:
                    deferral_msg = f"No worries, {name} 😊 You'll just need a phone/laptop and steady internet to teach.\nI can check back once you have access — would you like me to remind you next week?"
                    await mcp_wa_send(phone, deferral_msg)
                    _add_to_history(phone, bot_msg=deferral_msg)
                    
                    # Wait for response (user will reply yes/no to deferral)
                    sess["_eligibility_device_deferral_asked"] = True
                    sess["ts"] = time.time()
                    SESSIONS[phone] = sess
                    return
                
                # Device OK → Both age and device confirmed, move to commitment
                ok_msg = "Great! 👍"
                await mcp_wa_send(phone, ok_msg)
                _add_to_history(phone, bot_msg=ok_msg)
                
                sess["state"] = "ELIGIBILITY_PART2"
                sess["_eligibility_part2_sent"] = True  # Mark as sent (we're about to send it)
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                
                # Send commitment question immediately
                await asyncio.sleep(0.5)  # Small pause
                commitment_msg = "Great 👍 Last quick one — would you be able to give about 2 hours a week for teaching?"
                await mcp_wa_send(phone, commitment_msg)
                _add_to_history(phone, bot_msg=commitment_msg)
                return
        
        # Handle deferral response for device
        if sess.get("_eligibility_device_deferral_asked"):
            if is_yes_response(text):
                # Create deferral
                from datetime import timezone
                until_date = datetime.now(timezone.utc) + timedelta(days=7)
                until_iso = until_date.isoformat()
                idempotency_key = f"{volunteer_id}_DEFERRAL_DEVICE_{int(time.time())}"
                
                try:
                    await mcp_deferral_create(volunteer_id, "NO_DEVICE", until_iso, idempotency_key)
                    defer_msg = f"Perfect! I'll remind you next week, {name}. Feel free to ping me anytime when you're ready."
                    await mcp_wa_send(phone, defer_msg)
                    _add_to_history(phone, bot_msg=defer_msg)
                    sess["state"] = "DEFERRED"
                    sess["ts"] = time.time()
                    SESSIONS[phone] = sess
                    return
                except Exception as e:
                    log.warning(f"[ELIG] Failed to create device deferral: {e}")
                    # Fallback
                    defer_msg = f"Got it, {name}! I'll check back with you later when you're ready."
                    await mcp_wa_send(phone, defer_msg)
                    _add_to_history(phone, bot_msg=defer_msg)
                    sess["state"] = "DEFERRED"
                    sess["ts"] = time.time()
                    SESSIONS[phone] = sess
                    return
            else:
                # User declined deferral, ask device again
                device_msg = "No problem. Could you confirm if you have a smartphone or laptop with internet?"
                await mcp_wa_send(phone, device_msg)
                _add_to_history(phone, bot_msg=device_msg)
                sess["_eligibility_device_deferral_asked"] = False
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
    
    # ========== ELIGIBILITY (PART 2: commitment with persuasion) ==========
    elif state == "ELIGIBILITY_PART2":
        volunteer_id = profile.get("uuid") or phone
        persuasion_attempts = sess.get("_commitment_persuasion_attempts", 0)
        
        if not sess.get("_eligibility_part2_sent"):
            # First time: send commitment question
            log.info(f"[ELIG] Sending commitment question to {phone}")
            msg = "Last quick one — would you be able to give about 2 hours a week for teaching?"
            await mcp_wa_send(phone, msg)
            _add_to_history(phone, bot_msg=msg)
            sess["_eligibility_part2_sent"] = True
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        else:
            # User replied to commitment question
            # TRUST LLM-FIRST: Use onboarding.parse_message results (already called above)
            commit_hours = None
            commit_ok = None
            same_day_request = False

            # Read parsed eligibility hints (from server prompt) and log for visibility
            try:
                hints = parsed.get("eligibility") or {} if parsed else {}
                # same_day_request from server
                if isinstance(hints.get("same_day_request"), bool):
                    same_day_request = hints.get("same_day_request")
                # Optional: quick log of parsed payload
                log.info(f"[ELIG] Parsed hints: hours={hints.get('weekly_commitment_hours')}, same_day_request={same_day_request}, confidence={hints.get('confidence')}")
            except Exception:
                pass

            # Broadened same-day detection (client guard) as a backup to server flag
            if not same_day_request and re.search(r"\b(same\s*day|same-day|sameday|today)\b", text, re.I):
                same_day_request = True

            # Commitment policy clarification takes precedence if same-day was requested
            if same_day_request:
                clarify_policy = (
                    "Thanks for checking. The 2 hours need to be split across different weekdays during school hours (8–15), "
                    "not all on the same day. Would two short weekday slots work for you—say 30–45 minutes each on different days?"
                )
                await mcp_wa_send(phone, clarify_policy)
                _add_to_history(phone, bot_msg=clarify_policy)
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
            
            # Primary source: onboarding.parse_message (LLM extraction)
            try:
                hints = parsed.get("eligibility") or {} if parsed else {}
                commit_hours_raw = hints.get("weekly_commitment_hours")
                if commit_hours_raw is not None:
                    commit_hours = float(commit_hours_raw)
                    commit_ok = commit_hours >= 2.0
                    log.info(f"[ELIG] LLM extracted commitment: {commit_hours} hours, ok={commit_ok}")
            except Exception as e:
                log.warning(f"[ELIG] Failed to parse commitment from LLM: {e}")
            
            # Fallback 1: Minimal numeric extraction (when LLM fails but text contains obvious hours)
            if commit_hours is None:
                extracted_hours = _extract_simple_hours(text)
                if extracted_hours is not None:
                    commit_hours = extracted_hours
                    commit_ok = extracted_hours >= 2.0
                    log.info(f"[ELIG] Minimal fallback extracted: {commit_hours} hours, ok={commit_ok}")
            
            # Fallback 2: Simple yes/no for trivial responses only (avoid LLM cost for obvious cases)
            if commit_hours is None and commit_ok is None:
                text_lower = text.lower().strip()
                
                # Simple yes (very obvious cases only)
                if is_yes_response(text):
                    commit_ok = True
                    commit_hours = 2.0
                    log.info(f"[ELIG] Simple yes detected, defaulting to 2.0 hours")
                
                # Simple no (very obvious cases only)
                elif is_no_response(text):
                    commit_ok = False
                    log.info(f"[ELIG] Simple no detected")
            
            # If still unclear, ask for clarification (max 2 times to prevent loops)
            if commit_hours is None and commit_ok is None:
                clarification_count = sess.get("_commitment_clarification_count", 0)
                
                if clarification_count >= 2:
                    # Too many clarifications → treat as hesitant and offer deferral/persuasion
                    log.warning(f"[ELIG] Max clarifications reached for commitment, treating as hesitant")
                    commit_ok = False
                    sess["elig.commitment"] = False
                    # Will trigger persuasion logic below
                else:
                    sess["_commitment_clarification_count"] = clarification_count + 1
                    clarifier = "Could you confirm: can you spare around 2 hours per week for teaching?"
                    await mcp_wa_send(phone, clarifier)
                    _add_to_history(phone, bot_msg=clarifier)
                    sess["ts"] = time.time()
                    SESSIONS[phone] = sess
                    return
            
            # If we have hours but commit_ok wasn't set, set it based on hours
            if commit_hours is not None and commit_ok is None:
                commit_ok = commit_hours >= 2.0
                log.info(f"[ELIG] Commit_ok set from hours: {commit_hours} >= 2.0 = {commit_ok}")
            
            # Store commitment
            sess["elig.commitment"] = commit_ok
            sess["elig.commitment_hours"] = commit_hours if commit_hours else (2.0 if commit_ok else None)
            
            # POSITIVE: All three collected → Call eligibility.check
            if commit_ok is True:
                # All eligibility info collected: age, device, commitment
                age_val = sess.get("elig.age_value", 18)
                device_ok = sess.get("elig.device", True)
                
                # Call server with all three answers
                try:
                    idempotency_key = f"{volunteer_id}_ELIG_CHECK_{int(time.time())}"
                    elig = await mcp_eligibility_check(
                        age_years=int(age_val) if age_val else 18,
                        has_device=bool(device_ok),
                        weekly_commitment_hours=float(commit_hours) if commit_hours else 2.0
                    )
                    
                    eligible = bool(elig.get("eligible", False))
                    reasons = elig.get("reasons", [])
                    
                    if eligible:
                        # All eligible → Advance to next stage
                        ok_msg = "Awesome! 🎉"
                        await mcp_wa_send(phone, ok_msg)
                        _add_to_history(phone, bot_msg=ok_msg)
                        # Move to Day & Time Preferences state
                        sess["state"] = "PREFS_DAYTIME"
                        sess["_prefs_init_done"] = True  # we'll send M1/M2 now
                        # Fetch scheduling policy and send M1/M2 immediately
                        weekend_gate = False
                        policy_version = None
                        try:
                            pol = await mcp_policy_scheduling(profile.get("region_id"))
                            weekend_gate = bool(pol.get("weekend_gate", False)) if isinstance(pol, dict) else False
                            policy_version = pol.get("policy_version") if isinstance(pol, dict) else None
                        except Exception:
                            weekend_gate = False
                            policy_version = None
                        sess["_weekend_gate"] = weekend_gate
                        sess["_policy_version"] = policy_version
                        sess["ts"] = time.time()
                        SESSIONS[phone] = sess
                        # Outbound M1 and M2
                        m1 = (
                            "What days usually work for you to take class?\n"
                            "You can pick 2–3 days (e.g., Mon, Wed, Sat) — or just type what suits you."
                        )
                        if weekend_gate:
                            m1 += "\n\nNote: Weekends are reserved — weekdays are best."
                        await mcp_wa_send(phone, m1)
                        _add_to_history(phone, bot_msg=m1)
                        # Wait for volunteer's reply before asking time
                        sess["_prefs_last_prompt"] = "ask_days"
                        SESSIONS[phone] = sess
                        
                        # Telemetry
                        try:
                            await mcp_telemetry_emit("onboarding.eligibility_passed", {
                                "conversation_id": phone,
                                "user_id": volunteer_id,
                                "age": age_val,
                                "device": device_ok,
                                "commitment_hours": commit_hours
                            })
                        except Exception:
                            pass
                        return
                    else:
                        # Server says not eligible → Decline with reasons
                        decline_msg = "Thanks for your interest! Right now, the teaching program needs volunteers who can meet these requirements. We will keep you posted on other ways to contribute."
                        await mcp_wa_send(phone, decline_msg)
                        _add_to_history(phone, bot_msg=decline_msg)
                        sess["state"] = "REJECTED"
                        sess["ts"] = time.time()
                        SESSIONS[phone] = sess
                        return
                        
                except Exception as e:
                    log.warning(f"[ELIG] MCP eligibility.check failed: {e}")
                    # Fallback: if we have all three positive, assume eligible
                    if age_val and device_ok and commit_ok:
                        ok_msg = "Awesome! 🎉"
                        await mcp_wa_send(phone, ok_msg)
                        _add_to_history(phone, bot_msg=ok_msg)
                        sess["state"] = "DONE"  # Temporary
                        sess["ts"] = time.time()
                        SESSIONS[phone] = sess
                        return
                    else:
                        # Fallback decline
                        decline_msg = "Thanks for your interest! Right now, we need volunteers who meet all requirements."
                        await mcp_wa_send(phone, decline_msg)
                        _add_to_history(phone, bot_msg=decline_msg)
                        sess["state"] = "REJECTED"
                        sess["ts"] = time.time()
                        SESSIONS[phone] = sess
                        return
            
            # HESITANT/NEGATIVE: Persuasion logic (max 2 attempts)
            if commit_ok is False:
                if persuasion_attempts == 0:
                    # First hesitant response detected → send persuasion
                    sess["_commitment_persuasion_attempts"] = 1
                    persuasion_msg = f"I understand, {name} 😊 Even 2 hours a week can make a big difference for the children — and you can pick times that suit you!\nDo you think that might work?"
                    await mcp_wa_send(phone, persuasion_msg)
                    _add_to_history(phone, bot_msg=persuasion_msg)
                    sess["ts"] = time.time()
                    SESSIONS[phone] = sess
                    return
                
                elif persuasion_attempts == 1:
                    # User replied to first persuasion → check if they said yes now
                    # Re-check their response after persuasion
                    if is_yes_response(text):
                        # They agreed after persuasion → treat as positive, call eligibility.check
                        commit_ok = True
                        if commit_hours is None:
                            commit_hours = 2.0
                        sess["elig.commitment"] = True
                        sess["elig.commitment_hours"] = commit_hours
                        
                        # All eligibility info collected: age, device, commitment
                        age_val = sess.get("elig.age_value", 18)
                        device_ok = sess.get("elig.device", True)
                        
                        # Call server with all three answers
                        try:
                            idempotency_key = f"{volunteer_id}_ELIG_CHECK_{int(time.time())}"
                            elig = await mcp_eligibility_check(
                                age_years=int(age_val) if age_val else 18,
                                has_device=bool(device_ok),
                                weekly_commitment_hours=float(commit_hours) if commit_hours else 2.0
                            )
                            
                            eligible = bool(elig.get("eligible", False))
                            
                            if eligible:
                                # All eligible → Advance to next stage
                                ok_msg = "Awesome! 🎉"
                                await mcp_wa_send(phone, ok_msg)
                                _add_to_history(phone, bot_msg=ok_msg)
                                
                                # TODO: Move to next section (TIME_PREF)
                                sess["state"] = "DONE"  # Temporary - will be TIME_PREF later
                                sess["ts"] = time.time()
                                SESSIONS[phone] = sess
                                
                                # Telemetry
                                try:
                                    await mcp_telemetry_emit("onboarding.eligibility_passed", {
                                        "conversation_id": phone,
                                        "user_id": volunteer_id,
                                        "age": age_val,
                                        "device": device_ok,
                                        "commitment_hours": commit_hours,
                                        "persuaded": True
                                    })
                                except Exception:
                                    pass
                                return
                            else:
                                # Server says not eligible → Decline
                                decline_msg = "Thanks for your interest! Right now, the teaching program needs volunteers who can meet these requirements. We will keep you posted on other ways to contribute."
                                await mcp_wa_send(phone, decline_msg)
                                _add_to_history(phone, bot_msg=decline_msg)
                                sess["state"] = "REJECTED"
                                sess["ts"] = time.time()
                                SESSIONS[phone] = sess
                                return
                                
                        except Exception as e:
                            log.warning(f"[ELIG] MCP eligibility.check failed after persuasion: {e}")
                            # Fallback: if we have all three positive, assume eligible
                            if age_val and device_ok:
                                ok_msg = "Awesome! 🎉"
                                await mcp_wa_send(phone, ok_msg)
                                _add_to_history(phone, bot_msg=ok_msg)
                                sess["state"] = "DONE"  # Temporary
                                sess["ts"] = time.time()
                                SESSIONS[phone] = sess
                                return
                    else:
                        # Still hesitant/no after persuasion → Stop persuading, offer deferral
                        deferral_msg = f"Totally fine, {name} 💛 I'll note that you'd like to start later and remind you in a few days."
                        await mcp_wa_send(phone, deferral_msg)
                        _add_to_history(phone, bot_msg=deferral_msg)
                        
                        # Create deferral
                        from datetime import timezone
                        until_date = datetime.now(timezone.utc) + timedelta(days=5)
                        until_iso = until_date.isoformat()
                        idempotency_key = f"{volunteer_id}_DEFERRAL_COMMITMENT_{int(time.time())}"
                        
                        try:
                            await mcp_deferral_create(volunteer_id, "NO_COMMITMENT", until_iso, idempotency_key)
                            sess["state"] = "DEFERRED"
                        except Exception as e:
                            log.warning(f"[ELIG] Failed to create commitment deferral: {e}")
                            sess["state"] = "DEFERRED"  # Set state anyway
                        
                        sess["ts"] = time.time()
                        SESSIONS[phone] = sess
                        return
            
            
            # Should not reach here
            log.warning(f"[ELIG] Unexpected state in ELIGIBILITY_PART2 for {phone}")
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
    
    # ========== PREFS_DAYTIME (Day & Time Preferences) ==========
    elif state == "PREFS_DAYTIME":
        volunteer_id = profile.get("uuid") or phone
        # One-time initialization
        if not sess.get("_prefs_init_done"):
            try:
                pol = await mcp_policy_scheduling(profile.get("region_id"))
                sess["_weekend_gate"] = bool(pol.get("weekend_gate", False)) if isinstance(pol, dict) else False
                sess["_policy_version"] = pol.get("policy_version") if isinstance(pol, dict) else None
            except Exception:
                sess["_weekend_gate"] = False
                sess["_policy_version"] = None
            sess["_prefs_init_done"] = True
            SESSIONS[phone] = sess
            # M1 only (wait for reply before asking time)
            m1 = (
                "What days usually work for you to take class?\n"
                "You can pick 2–3 days (e.g., Mon, Wed, Sat) — or just type what suits you."
            )
            if sess.get("_weekend_gate"):
                m1 += "\n\nNote: Weekends are reserved — weekdays are best."
            await mcp_wa_send(phone, m1)
            _add_to_history(phone, bot_msg=m1)
            sess["_prefs_last_prompt"] = "ask_days"
            SESSIONS[phone] = sess
            return
        
        # Parse user reply for days and times (LLM-first merge)
        text_lower = text.lower()
        llm_days = []
        llm_windows = []
        try:
            if parsed:
                if isinstance(parsed.get("days"), list):
                    llm_days = [d for d in parsed.get("days") if isinstance(d, str)]
                if isinstance(parsed.get("time_windows"), list):
                    for w in parsed.get("time_windows"):
                        if isinstance(w, dict) and w.get("start") and w.get("end"):
                            llm_windows.append({"start": w.get("start"), "end": w.get("end")})
        except Exception:
            pass
        day_map = {"mon":"MON","monday":"MON","tue":"TUE","tues":"TUE","tuesday":"TUE","wed":"WED","weds":"WED","wednesday":"WED","thu":"THU","thur":"THU","thurs":"THU","thursday":"THU","fri":"FRI","friday":"FRI","sat":"SAT","saturday":"SAT","sun":"SUN","sunday":"SUN"}
        days_found = list(llm_days) if llm_days else []
        for token, iso in day_map.items():
            if re.search(rf"\b{re.escape(token)}\b", text_lower):
                if iso not in days_found:
                    days_found.append(iso)
        if re.search(r"\bweekdays?\b", text_lower):
            for iso in ["MON","TUE","WED","THU","FRI"]:
                if iso not in days_found:
                    days_found.append(iso)
        if re.search(r"\bweekends?\b", text_lower):
            for iso in ["SAT","SUN"]:
                if iso not in days_found:
                    days_found.append(iso)
        days_capped = days_found[:3] if days_found else []
        
        # Time bands and exact time
        time_windows = list(llm_windows) if llm_windows else []
        def add_window(start:str, end:str):
            time_windows.append({"start": start, "end": end})
        if re.search(r"\bmorn(ing)?\b", text_lower):
            add_window("08:00","11:00")
        if re.search(r"\bafternoon\b|\bnoon\b", text_lower):
            add_window("12:00","16:00")
        if re.search(r"\beve(ning)?\b", text_lower):
            add_window("17:00","20:00")
        m12 = re.search(r"\b(1[0-2]|0?[1-9]):?([0-5]?\d)?\s*(am|pm)\b", text_lower)
        m24 = re.search(r"\b([01]?\d|2[0-3]):?([0-5]?\d)\b", text_lower)
        def expand_around(hour:int, minute:int=0):
            start_h = max(8, hour-1); end_h = min(20, hour+2)
            return f"{start_h:02d}:{minute:02d}", f"{end_h:02d}:{minute:02d}"
        if m12:
            h = int(m12.group(1)); mm = int(m12.group(2) or 0); ap = m12.group(3)
            if ap == "pm" and h != 12: h += 12
            if ap == "am" and h == 12: h = 0
            s,e = expand_around(h, mm); add_window(s,e)
        elif m24:
            h = int(m24.group(1)); mm = int(m24.group(2) or 0)
            s,e = expand_around(h, mm); add_window(s,e)
        # Merge with any partial session choices
        prev_days = sess.get("_prefs_days") or []
        prev_windows = sess.get("_prefs_windows") or []
        merged_days = days_capped or prev_days or []
        merged_windows = time_windows or prev_windows or []

        # If neither present, ask combined clarifier
        if not merged_days and not merged_windows:
            clar = "Could you pick 2–3 days (e.g., Mon, Wed) and a time band — Morning (8–11), Afternoon (12–4), or Evening (5–8) in IST?"
            await mcp_wa_send(phone, clar)
            _add_to_history(phone, bot_msg=clar)
            sess["ts"] = time.time(); SESSIONS[phone] = sess
            return

        # If only days present, ask time once (suppress duplicates)
        if merged_days and not merged_windows:
            if sess.get("_prefs_last_prompt") != "ask_time":
                ask_time = "Great. Do mornings (8AM –11AM) or afternoons (12PM–3PM) work best in IST?"
                await mcp_wa_send(phone, ask_time)
                _add_to_history(phone, bot_msg=ask_time)
                sess["_prefs_last_prompt"] = "ask_time"
            sess["_prefs_days"] = merged_days
            sess["ts"] = time.time(); SESSIONS[phone] = sess
            return

        # If only windows present, ask days once (suppress duplicates)
        if merged_windows and not merged_days:
            if sess.get("_prefs_last_prompt") != "ask_days":
                ask_days = "Could you pick two weekdays (Mon–Fri)?"
                await mcp_wa_send(phone, ask_days)
                _add_to_history(phone, bot_msg=ask_days)
                sess["_prefs_last_prompt"] = "ask_days"
            sess["_prefs_windows"] = merged_windows
            sess["ts"] = time.time(); SESSIONS[phone] = sess
            return

        # Both present → confirm and save
        prefs = {
            "days": merged_days,
            "time_windows": merged_windows,
            "timezone": profile.get("tz","Asia/Kolkata")
        }
        days_str = ", ".join([d.title()[:3] for d in prefs["days"]])
        tw = merged_windows[0]; band_str = f"{tw['start'][:-3]}–{tw['end'][:-3]}"
        if sess.get("_weekend_gate") and any(d in ["SAT","SUN"] for d in prefs["days"]):
            confirm = f"I’ve noted {days_str}, though weekends are limited in your region. I’ll prioritize weekdays in {band_str} IST. 👍"
        else:
            confirm = f"Noted 👍 I’ll look for {days_str} in {band_str} IST."
        await mcp_wa_send(phone, confirm)
        _add_to_history(phone, bot_msg=confirm)

        try:
            idk = f"{volunteer_id}_PREFS_{int(time.time())}"
            await mcp_preferences_save_v2(volunteer_id, prefs, sess.get("_policy_version"), idk)
            # Clear partials (teaching preferences are separate from orientation scheduling)
            sess.pop("_prefs_days", None)
            sess.pop("_prefs_windows", None)
            sess.pop("_prefs_last_prompt", None)
            sess["state"] = "QA_WINDOW"
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            log.info(f"[PREFS] Preferences saved, transitioning to QA_WINDOW for {phone}")
            # Immediately trigger QA_WINDOW entry message
            await _handle(phone, "__kick__")
            return
        except Exception as e:
            log.warning(f"[PREFS] preferences.save failed: {e}")
            msg = "Could you pick two weekdays (Mon–Fri)?" if not prefs["days"] else "Do mornings (8–11), afternoons (12–4), or evenings (5–8) work best?"
            await mcp_wa_send(phone, msg)
            _add_to_history(phone, bot_msg=msg)
            # Persist partials for next turn
            sess["_prefs_days"] = prefs.get("days")
            sess["_prefs_windows"] = prefs.get("time_windows")
            sess["ts"] = time.time(); SESSIONS[phone] = sess
            return

    # ========== QA_WINDOW (Questions & Answers) ==========
    elif state == "QA_WINDOW":
        log.info(f"[QA] QA_WINDOW handler triggered for {phone}, text='{text[:30]}...'")
        volunteer_id = profile.get("uuid") or phone
        name = profile.get("name") or "there"
        qa_count = sess.get("_qa_count", 0)
        
        # Entry: send initial QA prompt
        if text == "__kick__":
            log.info(f"[QA] Sending QA entry message to {phone}")
            entry_msg = (
                f"Before we wrap up, do you have any quick questions for me? "
                f"(training, certificate, subjects, tech setup…)\n\n"
                f"I'll keep it short and clear. 🙂"
            )
            await mcp_wa_send(phone, entry_msg)
            _add_to_history(phone, bot_msg=entry_msg)
            sess["_qa_count"] = 0
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            log.info(f"[QA] QA entry message sent to {phone}")
            return
        
        # Rule-based routing (deterministic intents)
        text_lower = text.lower()
        route = "RULE"  # or "LLM"
        faq_bucket = None
        classifier_conf = None
        
        # Check if user said "no" or "not now" to the QA entry question (before any schedule prompt)
        if not sess.get("_qa_waiting_for_schedule_response"):
            if is_no_response(text) or re.search(r"\b(not now|no questions|no questions?|nothing|no)\b", text_lower):
                # User doesn't have questions - proceed to orientation scheduling
                mandatory_msg = "This is the final segment — An orientation is mandatory for all volunteers. It's a quick 30-minute session to help you get started.\n\nShall we schedule your orientation?"
                await mcp_wa_send(phone, mandatory_msg)
                _add_to_history(phone, bot_msg=mandatory_msg)
                sess["_qa_waiting_for_schedule_response"] = True
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
        
        # Check if user is responding to "Shall we schedule your orientation?" prompt
        if sess.get("_qa_waiting_for_schedule_response"):
            if is_yes_response(text):
                # Transition to orientation consent
                try:
                    idk = f"{volunteer_id}_ORIENT_CONSENT_{int(time.time())}"
                    await mcp_consent_record(volunteer_id, True)
                    await mcp_state_advance(volunteer_id, "ORIENTATION_CONSENT", idk)
                except Exception as e:
                    log.warning(f"[QA] Orientation consent failed: {e}")
                sess.pop("_qa_waiting_for_schedule_response", None)
                sess["state"] = "ORIENTATION_CONSENT"
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                # Trigger next step
                await _handle(phone, "__kick__")
                return
            elif is_no_response(text) or re.search(r"\b(not now|maybe later|later)\b", text_lower):
                # User wants to defer orientation - but we should still note it's mandatory
                # Defer orientation
                until_date = datetime.now() + timedelta(days=5)
                until_iso = until_date.isoformat()
                idk = f"{volunteer_id}_QA_DEFER_{int(time.time())}"
                try:
                    await mcp_deferral_create(volunteer_id, "ORIENTATION_LATER", until_iso, idk)
                    defer_msg = "No worries 😊 I can check back later. When should I remind you — Tue 10am, Thu 6pm, or Sat 10am?"
                    await mcp_wa_send(phone, defer_msg)
                    _add_to_history(phone, bot_msg=defer_msg)
                    sess["state"] = "DEFERRED"
                    sess.pop("_qa_waiting_for_schedule_response", None)
                    sess["ts"] = time.time()
                    SESSIONS[phone] = sess
                    try:
                        await mcp_telemetry_emit("onboarding.qa_deferral", {
                            "conversation_id": phone,
                            "user_id": volunteer_id,
                            "qa_count": qa_count
                        })
                    except Exception:
                        pass
                    return
                except Exception as e:
                    log.warning(f"[QA] Deferral creation failed: {e}")
                sess.pop("_qa_waiting_for_schedule_response", None)
                # Fall through to treat as new question if deferral fails
        
        # A) STOP / OPT-OUT
        if re.search(r"\b(stop|unsubscribe|don'?t message|no more messages)\b", text_lower):
            ack = "Understood. I'll stop messages. If you change your mind, just say \"Hi\" here anytime. 💛"
            await mcp_wa_send(phone, ack)
            _add_to_history(phone, bot_msg=ack)
            sess["state"] = "OPTOUT"
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            try:
                await mcp_telemetry_emit("onboarding.qa_stop", {
                    "conversation_id": phone,
                    "user_id": volunteer_id,
                    "qa_count": qa_count
                })
            except Exception:
                pass
            return
        
        # B) DEFERRAL
        if re.search(r"\b(later|next week|not today|busy|remind|check back)\b", text_lower):
            until_date = datetime.now() + timedelta(days=5)
            until_iso = until_date.isoformat()
            idk = f"{volunteer_id}_QA_DEFER_{int(time.time())}"
            try:
                await mcp_deferral_create(volunteer_id, "ORIENTATION_LATER", until_iso, idk)
                defer_msg = "No worries 😊 I can check back later. When should I remind you — Tue 10am, Thu 6pm, or Sat 10am?"
                await mcp_wa_send(phone, defer_msg)
                _add_to_history(phone, bot_msg=defer_msg)
                sess["state"] = "DEFERRED"
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                try:
                    await mcp_telemetry_emit("onboarding.qa_deferral", {
                        "conversation_id": phone,
                        "user_id": volunteer_id,
                        "qa_count": qa_count
                    })
                except Exception:
                    pass
                return
            except Exception as e:
                log.warning(f"[QA] Deferral creation failed: {e}")
                # Continue to answer as FAQ if deferral fails
        
        # C) RETURNING
        if re.search(r"\b(already (did|done)|completed|onboarded|finished)\b", text_lower):
            try:
                server_state = await mcp_state_get(volunteer_id)
                if server_state and server_state.get("state") not in ["WELCOME", "QA_WINDOW"]:
                    # Fast-forward to server state
                    sess["state"] = server_state.get("state", "QA_WINDOW")
                    sess["ts"] = time.time()
                    SESSIONS[phone] = sess
                    # Trigger next step
                    await _handle(phone, "__kick__")
                    return
            except Exception:
                pass
        
        # D) FAQ keyword buckets (no LLM needed)
        faq_answers = {
            "about_serve": {
                "pattern": r"\b(what is serve|who runs|government|ngo|organization)\b",
                "answer": (
                    "SERVE helps thousands of children learn English, Science, and Maths through volunteers like you. "
                    "You teach online — they learn in school — and our local coordinators make sure everything runs smoothly."
                )
            },
            "time_process": {
                "pattern": r"\b(hours?|time|how teach|online|travel|duration|how long)\b",
                "answer": (
                    "You'll teach live online while students sit in their school smart classroom.\n"
                    "Usually ~2 hours/week."
                )
            },
            "support": {
                "pattern": r"\b(training|orientation|help|support|guidance|assistance)\b",
                "answer": (
                    "Yes! You'll attend a 30-min online orientation, and a local coordinator supports you during classes."
                )
            },
            "certificate": {
                "pattern": r"\b(certificate|letter|proof|document|completion)\b",
                "answer": (
                    "We provide a volunteer certificate after you complete the required sessions as per policy."
                )
            },
            "subjects_grades": {
                "pattern": r"\b(subject|grade|class|what (teach|teach)|math|english|science)\b",
                "answer": (
                    "Most volunteers teach English, Math or Science for grades 5–8 (varies by school).\n"
                    "We'll align your preferences during scheduling."
                )
            },
            "tech": {
                "pattern": r"\b(internet|wifi|laptop|phone|meet|zoom|google meet|tech|technical|device)\b",
                "answer": (
                    "A phone or laptop with stable internet is enough. We'll share the Meet link for sessions."
                )
            }
        }
        
        matched_bucket = None
        for bucket_name, bucket_data in faq_answers.items():
            if re.search(bucket_data["pattern"], text_lower):
                matched_bucket = bucket_name
                faq_bucket = bucket_name
                answer = bucket_data["answer"]
                await mcp_wa_send(phone, answer)
                _add_to_history(phone, bot_msg=answer)
                qa_count += 1
                sess["_qa_count"] = qa_count
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                
                # Timebox check: if >= 2 QAs, nudge forward
                if qa_count >= 2:
                    nudge = "Happy to answer more in orientation too 🙂\n\nBefore we proceed — orientation is mandatory for all volunteers. It's a quick 30-minute session to help you get started.\n\nShall we pick a slot now?"
                    await mcp_wa_send(phone, nudge)
                    _add_to_history(phone, bot_msg=nudge)
                    sess["_qa_waiting_for_schedule_response"] = True
                else:
                    # Ask if they want to continue or schedule
                    await asyncio.sleep(0.5)  # Small pause between FAQ answer and scheduling prompt
                    continue_prompt = "Before we proceed — orientation is mandatory for all volunteers. It's a quick 30-minute session to help you get started.\n\nShall we schedule your orientation?"
                    await mcp_wa_send(phone, continue_prompt)
                    _add_to_history(phone, bot_msg=continue_prompt)
                    sess["_qa_waiting_for_schedule_response"] = True
                
                try:
                    await mcp_telemetry_emit("onboarding.qa_answered", {
                        "conversation_id": phone,
                        "user_id": volunteer_id,
                        "qa_count": qa_count,
                        "route": "RULE",
                        "faq_bucket": faq_bucket,
                        "policy_version": sess.get("_policy_version"),
                        "knowledge_version": sess.get("_knowledge_version")
                    })
                except Exception:
                    pass
                return
        
        # E) LLM + RAG (free-text, multilingual, mixed questions)
        if not matched_bucket:
            route = "LLM"
            policy_version = sess.get("_policy_version")
            knowledge_version = sess.get("_knowledge_version")
            
            # Search knowledge base
            snippets = []
            try:
                snippets = await mcp_knowledge_search(text, top_k=5, policy_version=policy_version)
            except Exception as e:
                log.warning(f"[QA] knowledge.search failed: {e}")
            
            # Generate answer using LLM
            answer = ""
            if snippets:
                try:
                    answer = await mcp_llm_qa(
                        text,
                        snippets,
                        policy_version=policy_version,
                        knowledge_version=knowledge_version,
                        user_profile={"name": name, "tz": profile.get("tz", "Asia/Kolkata")}
                    )
                except Exception as e:
                    log.warning(f"[QA] LLM QA generation failed: {e}")
            
            # Fallback if LLM failed or no snippets
            if not answer:
                answer = (
                    "I might not have the perfect answer right now. Our coordinator will cover this in orientation."
                )
            
            await mcp_wa_send(phone, answer)
            _add_to_history(phone, bot_msg=answer)
            qa_count += 1
            sess["_qa_count"] = qa_count
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            
            # Timebox check
            if qa_count >= 2:
                nudge = "Happy to answer more in orientation too 🙂\n\nBefore we proceed — orientation is mandatory for all volunteers. It's a quick 30-minute session to help you get started.\n\nShall we pick a slot now?"
                await mcp_wa_send(phone, nudge)
                _add_to_history(phone, bot_msg=nudge)
                sess["_qa_waiting_for_schedule_response"] = True
            else:
                continue_prompt = "Before we proceed — orientation is mandatory for all volunteers. It's a quick 30-minute session to help you get started.\n\nShall we schedule your orientation?"
                await mcp_wa_send(phone, continue_prompt)
                _add_to_history(phone, bot_msg=continue_prompt)
                sess["_qa_waiting_for_schedule_response"] = True
            
            try:
                await mcp_telemetry_emit("onboarding.qa_answered", {
                    "conversation_id": phone,
                    "user_id": volunteer_id,
                    "qa_count": qa_count,
                    "route": "LLM",
                    "classifier_conf": classifier_conf,
                    "faq_bucket": faq_bucket,
                    "snippet_ids": [s.get("id") for s in snippets if isinstance(s, dict) and s.get("id")],
                    "policy_version": policy_version,
                    "knowledge_version": knowledge_version
                })
            except Exception:
                pass
            return
        
        # Should not reach here, but handle gracefully
        unclear = "I'd be happy to answer your question. Could you rephrase it, or would you like to proceed with scheduling orientation?"
        await mcp_wa_send(phone, unclear)
        _add_to_history(phone, bot_msg=unclear)
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return

    # ========== ORIENTATION_CONSENT (Entry & Slot Proposal) ==========
    elif state == "ORIENTATION_CONSENT":
        volunteer_id = profile.get("uuid") or phone
        name = profile.get("name") or "there"
        
        log.info(f"[ORIENT] ORIENTATION_CONSENT handler triggered for {phone}, text='{text[:30]}...'")
        
        # Entry: send ASK_AVAILABILITY message
        if text == "__kick__":
            log.info(f"[ORIENT] Sending orientation availability request to {phone}")
            await mcp_wa_send(phone, ASK_AVAILABILITY)
            _add_to_history(phone, bot_msg=ASK_AVAILABILITY)
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        
        # User provided time slots - parse and propose slots
        text_lower = text.lower()
        
        # Parse time slots from user input (LLM-first + deterministic), always include time.parse_options
        slots_parsed = []
        try:
            # Use onboarding.parse_message to extract any availability objects
            if parsed and isinstance(parsed.get("availability"), list):
                slots_parsed = list(parsed.get("availability", []))
        except Exception as e:
            log.warning(f"[ORIENT] Slot parsing from onboarding.parse_message failed: {e}")
        # Always attempt deterministic time parse and merge
        try:
            time_result = await mcp_time_parse(text, duration=30, tz=profile.get("tz", "Asia/Kolkata"))
            if isinstance(time_result, dict) and isinstance(time_result.get("slots"), list):
                for s in time_result.get("slots", []):
                    if isinstance(s, dict):
                        slots_parsed.append(s)
        except Exception as e:
            log.warning(f"[ORIENT] Time parsing failed: {e}")
        
        # If parsing failed, ask for clarification
        if not slots_parsed:
            await mcp_wa_send(phone, AVAILABILITY_PARSE_FAILED)
            _add_to_history(phone, bot_msg=AVAILABILITY_PARSE_FAILED)
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        
        # Determine time band and days from parsed slots or raw text (orientation is separate from teaching preferences)
        time_band = "MORNING"  # Default
        days_whitelist = None  # Don't restrict by days unless user specified

        # Prepare seed time(s) early: collect all full ISO times from parsed slots
        seed_time_iso = None
        seed_times_iso: list[str] = []
        for s in slots_parsed:
            if isinstance(s, dict):
                cand = s.get("start_iso") or s.get("start")
                if isinstance(cand, str) and "T" in cand:
                    if seed_time_iso is None:
                        seed_time_iso = cand
                    if cand not in seed_times_iso:
                        seed_times_iso.append(cand)

        # Collect days strictly from raw text (do not infer from parser constraints)
        days_found: list[str] = []

        # Infer time band from first parsed slot's time, else from raw text
        inferred_hour = None
        if slots_parsed:
            first_slot = slots_parsed[0] if isinstance(slots_parsed[0], dict) else {}
            start_time = (first_slot.get("start") or first_slot.get("start_iso") or "") if isinstance(first_slot, dict) else ""
            try:
                from datetime import datetime
                if start_time and "T" in start_time:
                    dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                    inferred_hour = dt.hour
            except Exception:
                pass
            if inferred_hour is None and start_time:
                m_ap = re.search(r"\b(1[0-2]|0?[1-9])(?::([0-5]?\d))?\s*(am|pm)\b", start_time, re.I)
                m_24 = re.search(r"\b([01]?\d|2[0-3])(?::[0-5]?\d)?\b", start_time)
                if m_ap:
                    h = int(m_ap.group(1)); ap = m_ap.group(3).lower()
                    if ap == "pm" and h != 12: h += 12
                    if ap == "am" and h == 12: h = 0
                    inferred_hour = h
                elif m_24:
                    inferred_hour = int(m_24.group(1))

        # If still no hour, infer from the raw user text
        if inferred_hour is None:
            m_ap_text = re.search(r"\b(1[0-2]|0?[1-9])(?::([0-5]?\d))?\s*(am|pm)\b", text, re.I)
            m_24_text = re.search(r"\b([01]?\d|2[0-3])(?::[0-5]?\d)?\b", text)
            if m_ap_text:
                h = int(m_ap_text.group(1)); ap = m_ap_text.group(3).lower()
                if ap == "pm" and h != 12: h += 12
                if ap == "am" and h == 12: h = 0
                inferred_hour = h
            elif m_24_text:
                inferred_hour = int(m_24_text.group(1))

        if inferred_hour is not None:
            if 8 <= inferred_hour < 12:
                time_band = "MORNING"
            elif 12 <= inferred_hour < 16:
                time_band = "AFTERNOON"
            else:
                time_band = "EVENING"

        # Extract days from raw text (tokens)
        day_map = {"mon":"MON","monday":"MON","tue":"TUE","tues":"TUE","tuesday":"TUE","wed":"WED","weds":"WED","wednesday":"WED","thu":"THU","thur":"THU","thurs":"THU","thursday":"THU","fri":"FRI","friday":"FRI","sat":"SAT","saturday":"SAT","sun":"SUN","sunday":"SUN"}
        tl = text.lower()
        for token, iso in day_map.items():
            if re.search(rf"\b{re.escape(token)}\b", tl):
                if iso not in days_found:
                    days_found.append(iso)

        # If multiple seeds span different days, do not constrain days (show all)
        unique_seed_days: list[str] = []
        for iso_str in (seed_times_iso or []):
            try:
                from datetime import datetime
                dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
                iso_day = ["MON","TUE","WED","THU","FRI","SAT","SUN"][dt.weekday()]
                if iso_day not in unique_seed_days:
                    unique_seed_days.append(iso_day)
            except Exception:
                continue

        if len(unique_seed_days) > 1:
            days_whitelist = None
        else:
            days_whitelist = days_found or (unique_seed_days if unique_seed_days else None)
        
        # Omit timeBand: server infers from seedTimeIso (preferred two-step path)
        time_band = None

        # Call slots.propose for orientation (seed takes precedence server-side)
        try:
            log.info(f"[ORIENT] Proposing orientation slots for {phone}, seeds={seed_times_iso or seed_time_iso}, days={days_whitelist}")
            slots_result = await mcp_slots_propose(
                volunteer_id,
                None,
                days_whitelist,
                limit=3,
                seed_time_iso=seed_time_iso,
                seed_times_iso=seed_times_iso if seed_times_iso else None,
            )
            
            if not slots_result or not isinstance(slots_result, dict):
                log.warning(f"[ORIENT] slots_propose returned invalid result: {slots_result}")
                await mcp_wa_send(phone, SLOT_NONE_OF_ABOVE)
                _add_to_history(phone, bot_msg=SLOT_NONE_OF_ABOVE)
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
            
            slots = slots_result.get("slots", [])
            if not slots:
                await mcp_wa_send(phone, SLOT_NONE_OF_ABOVE)
                _add_to_history(phone, bot_msg=SLOT_NONE_OF_ABOVE)
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
            
            # Store slots in session for next state
            sess["_orientation_slots"] = slots
            sess["_orientation_slots_raw"] = slots_result
            
            # Format and send slot options
            slot_options_text = format_slot_options(slots)
            confirm_msg = format_message(CONFIRM_SLOT_TEMPLATE, slot_options=slot_options_text)
            await mcp_wa_send(phone, confirm_msg)
            _add_to_history(phone, bot_msg=confirm_msg)
            
            # Transition to ORIENTATION_SCHEDULING
            sess["state"] = "ORIENTATION_SCHEDULING"
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            log.info(f"[ORIENT] Slot options sent, transitioning to ORIENTATION_SCHEDULING for {phone}")
            return
            
        except Exception as e:
            log.error(f"[ORIENT] Failed to propose slots: {e}", exc_info=True)
            await mcp_wa_send(phone, "Sorry, I couldn't find available slots right now. Could you try again with different times?")
            _add_to_history(phone, bot_msg="Sorry, I couldn't find available slots right now. Could you try again with different times?")
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return

    # ========== ORIENTATION_SCHEDULING (Slot Selection & Booking) ==========
    elif state == "ORIENTATION_SCHEDULING":
        volunteer_id = profile.get("uuid") or phone
        name = profile.get("name") or "there"
        
        log.info(f"[SCHED] ORIENTATION_SCHEDULING handler triggered for {phone}, text='{text[:30]}...'")
        
        slots = sess.get("_orientation_slots", [])
        if not slots:
            log.warning(f"[SCHED] No slots found in session for {phone}, asking for availability again")
            sess["state"] = "ORIENTATION_CONSENT"
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            await _handle(phone, "__kick__")
            return
        
        # Parse user's selection
        text_lower = text.lower()
        selected_slot = None
        selected_index = None
        
        # Check if user said "Yes" (pick first option)
        if is_yes_response(text) or text_lower.strip() == "1":
            selected_index = 0
            selected_slot = slots[0] if slots else None
        # Check if user provided a number (1, 2, 3, etc.)
        elif re.search(r"^\s*(\d+)\s*$", text_lower):
            match = re.search(r"^\s*(\d+)\s*$", text_lower)
            if match:
                idx = int(match.group(1)) - 1  # Convert to 0-based index
                if 0 <= idx < len(slots):
                    selected_index = idx
                    selected_slot = slots[idx]
        # Check if user provided a day/time that matches a slot
        else:
            for i, slot in enumerate(slots):
                slot_label = slot.get("label", "").lower()
                slot_start = slot.get("start_iso", "").lower()
                # Check if user's text contains day/time from slot
                if slot_label and any(word in text_lower for word in slot_label.split() if len(word) > 2):
                    selected_index = i
                    selected_slot = slot
                    break
                # Check if user mentioned time that matches
                if slot_start:
                    try:
                        from datetime import datetime
                        dt = datetime.fromisoformat(slot_start.replace("Z", "+00:00"))
                        time_str = dt.strftime("%I:%M %p").lower()
                        if time_str.split()[0] in text_lower or time_str.split()[1] in text_lower:
                            selected_index = i
                            selected_slot = slot
                            break
                    except Exception:
                        pass
        
        # If no slot selected, ask for clarification
        if not selected_slot:
            await mcp_wa_send(phone, CONFIRM_SLOT_INVALID)
            _add_to_history(phone, bot_msg=CONFIRM_SLOT_INVALID)
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        
        # Hold the slot
        slot_id = selected_slot.get("slot_id") or selected_slot.get("id")
        if not slot_id:
            log.error(f"[SCHED] Selected slot has no ID: {selected_slot}")
            await mcp_wa_send(phone, "Sorry, there was an issue with that slot. Please pick another one.")
            _add_to_history(phone, bot_msg="Sorry, there was an issue with that slot. Please pick another one.")
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        
        try:
            log.info(f"[SCHED] Holding slot {slot_id} for {phone}")
            hold_result = await mcp_slot_hold(slot_id)
            hold_id = None
            if isinstance(hold_result, dict):
                hold_id = hold_result.get("hold_id") or hold_result.get("holdId") or hold_result.get("id")
            
            if not hold_id:
                log.error(f"[SCHED] Failed to hold slot {slot_id}: {hold_result}")
                await mcp_wa_send(phone, "Sorry, that slot is no longer available. Please pick another one.")
                _add_to_history(phone, bot_msg="Sorry, that slot is no longer available. Please pick another one.")
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
            
            # Book the slot
            log.info(f"[SCHED] Booking slot with hold_id {hold_id} for {phone}")
            await mcp_wa_send(phone, CONFIRM_BOOKING)
            _add_to_history(phone, bot_msg=CONFIRM_BOOKING)
            
            booking_result = await mcp_slot_book(hold_id)
            
            if not booking_result or not isinstance(booking_result, dict):
                log.error(f"[SCHED] Failed to book slot: {booking_result}")
                await mcp_wa_send(phone, "Sorry, I couldn't complete the booking. Please try again or contact support.")
                _add_to_history(phone, bot_msg="Sorry, I couldn't complete the booking. Please try again or contact support.")
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
            
            # Prepare chosen_slot for _book_slot_and_finish
            chosen_slot = {
                "start_iso": selected_slot.get("start_iso") or selected_slot.get("start"),
                "end_iso": selected_slot.get("end_iso") or selected_slot.get("end"),
                "label": selected_slot.get("label") or f"Slot {selected_index + 1}"
            }
            
            # Clean up session
            sess.pop("_orientation_slots", None)
            sess.pop("_orientation_slots_raw", None)
            
            # Book and finish
            await _book_slot_and_finish(phone, chosen_slot, profile, name)
            
            # Transition to final state (could be DONE or COMPLETE)
            sess["state"] = "COMPLETE"
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            
            try:
                await mcp_telemetry_emit("onboarding.orientation_booked", {
                    "conversation_id": phone,
                    "user_id": volunteer_id,
                    "slot_id": slot_id,
                    "hold_id": hold_id
                })
            except Exception:
                pass
            
            return
            
        except Exception as e:
            log.error(f"[SCHED] Error during slot booking: {e}", exc_info=True)
            await mcp_wa_send(phone, "Sorry, there was an error booking your slot. Please try again or contact support.")
            _add_to_history(phone, bot_msg="Sorry, there was an error booking your slot. Please try again or contact support.")
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return

    # Default: unknown state
    log.warning(f"[HANDLE] Unknown state: {state}")
    await mcp_wa_send(phone, "Sorry, something went wrong. Please type 'restart' to try again.")
    sess["ts"] = time.time()
    SESSIONS[phone] = sess

# ---------- Kafka Loop ----------
async def wa_loop():
    """
    Main Kafka consumer loop for WhatsApp messages
    """
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA_BROKERS,
        value_serializer=_js,
        key_serializer=_ks
    )
    consumer = AIOKafkaConsumer(
        settings.TOPIC_WA_IN,
        bootstrap_servers=settings.KAFKA_BROKERS,
        group_id="vm-agent-onboarding-wa",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode()),
        key_deserializer=lambda k: k.decode() if k else None
    )
    
    await producer.start()
    await consumer.start()
    
    log.info("[KAFKA] Consumer started, listening for WhatsApp messages...")
    
    try:
        async for rec in consumer:
            evt = rec.value
            
            if evt.get("type") != "wa.inbound.v1":
                continue

            data = evt.get("data") or {}
            phone = normalize_phone(data.get("from") or "")
            text = (data.get("text") or "").strip()

            # Ignore empty messages
            if not phone or not text:
                continue

            log.info(f"[KAFKA] Received from {phone}: '{text[:30]}...'")
            
            # Handle message through state machine
            try:
                await _handle(phone, text)
            except Exception as e:
                log.error(f"[KAFKA] Error handling message from {phone}: {e}", exc_info=True)
                try:
                    await mcp_wa_send(phone, "Sorry, something went wrong. Please type 'restart' to try again.")
                except:
                    pass  # Don't crash the loop
    
    finally:
        await consumer.stop()
        await producer.stop()
        log.info("[KAFKA] Consumer stopped")
