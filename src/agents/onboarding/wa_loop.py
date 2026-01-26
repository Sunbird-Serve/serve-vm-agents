# -*- coding: utf-8 -*-
"""
WhatsApp Onboarding Agent - client-led orchestrator

This agent handles volunteer onboarding via WhatsApp with:
- Client-owned state machine, prompts, and branching
- MCP tools for classification, retrieval, and scheduling support
- No dependency on onboarding.next; all conversation logic lives in the client
"""
import asyncio
import re
import json
import time
import uuid
import hashlib
import logging
import os
import pathlib
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta, timezone
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import httpx
import jsonschema
from jsonschema import ValidationError

from .config import settings
from .messages import (
    WELCOME, WELCOME_MAYBE_LATER,
    WELCOME_INTRO, WELCOME_INSTRUCTIONS, WELCOME_START_BUTTONS, WELCOME_VIDEO_INTRO, WELCOME_VIDEO_FOOTER,
    GENERIC_DEFERRED_MSG, WELCOME_SERVE_OVERVIEW, WELCOME_CONSENT_ACK, WELCOME_CONSENT_REMINDER,
    WELCOME_FAQ_FOLLOWUP, WELCOME_VIDEO_CONTINUE,
    WELCOME_STATEMENT_ACK,
    INTENT_PROMPT, INTENT_EXIT,
    ELIGIBILITY_PROMPT, ELIGIBILITY_EXIT,
    ELIGIBILITY_INTRO, ELIGIBILITY_Q1, ELIGIBILITY_Q2, ELIGIBILITY_Q3,
    ELIGIBILITY_INVALID_RESPONSE, REJECTED, ELIGIBILITY_PASSED,
    ELIGIBILITY_AGE_PROMPT, ELIGIBILITY_AGE_UNCLEAR, ELIGIBILITY_UNDERAGE_DECLINE,
    ELIGIBILITY_AGE_ACK,
    ELIGIBILITY_DEVICE_PROMPT, ELIGIBILITY_DEVICE_CLARIFY,
    ELIGIBILITY_DEVICE_DEFERRAL, ELIGIBILITY_DEVICE_DEFERRAL_CONFIRM,
    ELIGIBILITY_DEVICE_DEFERRAL_FALLBACK, ELIGIBILITY_DEVICE_REASK,
    ELIGIBILITY_DEVICE_OK, ELIGIBILITY_DEVICE_ACK,
    ASK_TEACHING_PREF, CONFIRM_TEACHING_PREF, EDIT_TEACHING_PREF, TEACHING_PREF_UNCLEAR,
    ASK_AVAILABILITY, CONSTRAINTS_ANNOUNCE, AVAILABILITY_PARSE_FAILED,
    BOOKING_IN_PROGRESS, DONE, ALREADY_DONE, RESTARTING,
    PERSUADE_COMMITMENT, PERSUADE_WEEKEND_ONLY, ELIGIBILITY_COMMIT_PROMPT,
    ELIGIBILITY_COMMIT_CLARIFY, ELIGIBILITY_COMMIT_POLICY, ELIGIBILITY_COMMIT_SUCCESS,
    ELIGIBILITY_PREFERENCES_PROMPT, ELIGIBILITY_PREFERENCES_WEEKEND_NOTE,
    ELIGIBILITY_COMMIT_PERSUADE, ELIGIBILITY_COMMIT_DEFERRAL, ELIGIBILITY_COMMIT_DEFERRAL_CONFIRM,
    ELIGIBILITY_DECLINE_REQUIREMENTS, ELIGIBILITY_DECLINE_GENERIC, ELIGIBILITY_SUMMARY,
    PREFS_INTRO_COLLAB, PREFS_FOLLOWUP_DAYS, PREFS_FOLLOWUP_TIME,
    PREFS_WEEKEND_NOTE, PREFS_EVENING_NUDGE, PREFS_CONFIRM_DEFAULT, PREFS_SUMMARY_FALLBACK,
    QA_SUMMARY_WITH_QUESTIONS, QA_SUMMARY_NO_QUESTIONS,
    PREFS_EVENING_POLICY, PREFS_EVENING_DEFERRAL,
    QA_ENTRY_PROMPT, QA_MANDATORY_ORIENT, QA_CONTINUE_PROMPT, QA_NUDGE,
    QA_DEFERRAL_PROMPT, QA_STOP_ACK,
    QA_FAQ_ABOUT_SERVE, QA_FAQ_TIME_PROCESS, QA_FAQ_SUPPORT,
    QA_FAQ_CERTIFICATE, QA_FAQ_SUBJECTS_GRADES, QA_FAQ_TECH,
    ORIENT_INTRO, ORIENT_SHOW_OPTIONS, ORIENT_CONFIRM,
    ORIENT_INVALID_PICK, ORIENT_LATER_NOTE,
    ORIENT_AVAILABILITY_ACK, ORIENT_PROPOSAL_NO_SLOTS,
    ORIENT_PROPOSAL_ERROR, ORIENT_SLOT_UNAVAILABLE,
    ORIENT_BOOKING_CONFIRM, ORIENT_BOOKING_FAILURE,
    YES_WORDS, NO_WORDS, MAYBE_LATER, CONFIRM_WORDS, EDIT_WORDS,
    VIDEO_INTRO, VIDEO_FOOTER, VIDEO_DONE_PROMPT, VIDEO_ERROR_MSG,
    PEEK_VIDEO_PROMPT,
    PEEK_NEEDS_PROMPT, PEEK_REQUIREMENTS_NOTE, PEEK_SKIP_MESSAGE, PEEK_MAYBE_MESSAGE,
    format_message, format_subjects_list
)
from .validators import is_yes_response, is_no_response, is_defer_response, is_resume_response, normalize_phone
from .faq import looks_like_question, send_global_faq_response
from .prompts.master_prompt import MASTER_SYSTEM_PROMPT
from .prompts.state_prompts import STATE_TASK_PROMPTS, DEFAULT_TASK_PROMPT
from .prompts.few_shots import FEW_SHOT_EXAMPLES
from .prompts.context import build_llm_context
from .states.intent import handle_intent
from .states.readiness_check import handle_readiness_check
from .states.video import handle_video
from .states.needs_preview import handle_needs_preview
from .states.feedback import handle_feedback
from .states.continue_confirm import handle_continue_confirm
from .states.eligibility import handle_eligibility
from .states.identity import handle_identity
from .states.preferences import handle_preferences
from .states.qa import handle_qa_window
from runtime.phone_lock import get_phone_lock
from storage.db import get_db_session
from storage.session_store import get_or_create_session, get_last_inbound_id, set_last_inbound_id
from storage.event_logger import log_event

log = logging.getLogger(__name__)

# ---------- Session & Config ----------
SESSIONS: dict[str, dict] = {}  # {phone: {"state": "...", "profile": {...}, "ts": epoch, ...}}
CONVERSATION_HISTORIES: dict[str, object] = {}  # {phone: ChatHistory()} - SK Memory
MCP_BASE = settings.MCP_BASE
MCP_JSONRPC_ENDPOINT = f"{MCP_BASE}/mcp/v1/jsonrpc"
MCP_INITIALIZED = False

WELCOME_ALLOWED_INTENTS = {"CONSENT_YES", "CONSENT_NO", "QUERY", "DEFERRAL", "STOP", "RETURNING", "AMBIGUOUS"}
ELIGIBILITY_PART1_ALLOWED_INTENTS = {
    "AGE_OK",
    "AGE_UNDER",
    "AGE_UNCLEAR",
    "DEVICE_OK",
    "DEVICE_NO",
    "DEVICE_UNCLEAR",
    "DEFERRAL",
    "QUERY",
    "AMBIGUOUS",
}
ELIGIBILITY_PART2_ALLOWED_INTENTS = {
    "COMMIT_OK",
    "COMMIT_TOO_LOW",
    "COMMIT_SAME_DAY_ONLY",
    "COMMIT_UNSURE",
    "DEFERRAL",
    "COMMIT_NO",
    "QUERY",
    "AMBIGUOUS",
}
PREFS_DAYTIME_ALLOWED_INTENTS = {
    "PREFS_DAYS_AND_TIME_OK",
    "PREFS_DAYS_ONLY",
    "PREFS_TIME_ONLY",
    "PREFS_WEEKEND_ONLY",
    "PREFS_EVENING_ONLY",
    "PREFS_FAQ",
    "PREFS_LATER_OR_DEFERRAL",
    "PREFS_AMBIGUOUS",
}

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
            r = await client.post(
                MCP_JSONRPC_ENDPOINT, 
                json=payload,
                headers={"Content-Type": "application/json; charset=utf-8"}
            )
            r.raise_for_status()
            r.encoding = "utf-8"
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
            r = await client.post(
                MCP_JSONRPC_ENDPOINT, 
                json=init_payload,
                headers={"Content-Type": "application/json; charset=utf-8"}
            )
            r.raise_for_status()
            r.encoding = "utf-8"
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
            
            r = await client.post(
                MCP_JSONRPC_ENDPOINT, 
                json=initialized_payload,
                headers={"Content-Type": "application/json; charset=utf-8"}
            )
            r.raise_for_status()
            r.encoding = "utf-8"
            
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
            # Ensure UTF-8 encoding for JSON payload
            r = await client.post(
                MCP_JSONRPC_ENDPOINT, 
                json=payload,
                headers={"Content-Type": "application/json; charset=utf-8"}
            )
            r.raise_for_status()
            # Ensure response is decoded as UTF-8
            r.encoding = "utf-8"
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


# ---------- LLM Helpers ----------
async def _llm_call_messages(
    messages: list[dict], *, temperature: float = 0.2, max_tokens: int = 200, timeout: int = 15
) -> dict:
    payload = {"messages": messages, "temperature": temperature, "max_tokens": max_tokens}
    return await _mcp_call("llm.call", payload, timeout=timeout)


INTENT_RESPONSE_SCHEMA = {
    "type": "object",
    "required": ["intent", "confidence"],
    "properties": {
        "intent": {"type": "string"},
        "confidence": {"type": ["number", "string"]},
        "tone_reply": {"type": ["string", "null"]},
    },
}

PEEK_PLANNER_SCHEMA = {
    "type": "object",
    "required": ["action", "tone_reply"],
    "properties": {
        "action": {
            "type": "string",
            "enum": ["SHOW_VIDEO", "SHOW_NEEDS", "SHOW_BOTH", "SKIP", "CLARIFY"],
        },
        "tone_reply": {"type": "string"},
        "confidence": {"type": ["number", "string"]},
    },
    "additionalProperties": False,
}

PREFS_INTERPRET_RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "preferred_days": {
            "type": "array",
            "items": {"type": "string"},
        },
        "preferred_time_band": {"type": ["string", "null"]},
        "preferred_language": {"type": ["string", "null"]},
        "followup": {"type": ["string", "null"]},
        "followup_tag": {"type": ["string", "null"]},
        "deferral": {
            "anyOf": [
                {"type": "null"},
                {
                    "type": "object",
                    "properties": {
                        "message": {"type": "string"},
                        "until_iso": {"type": "string"},
                    },
                    "required": ["message", "until_iso"],
                    "additionalProperties": False,
                },
            ]
        },
        "topics": {
            "type": "array",
            "items": {"type": "string"},
        },
    },
    "additionalProperties": False,
}


async def _llm_call_structured(
    messages: list[dict],
    *,
    schema: dict,
    temperature: float = 0.2,
    max_tokens: int = 200,
    timeout: int = 15,
) -> dict:
    payload = {
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "response_format": "json",
    }
    result = await _mcp_call("llm.call", payload, timeout=timeout)
    raw = _extract_llm_text(result)
    if not raw:
        raise ValueError("LLM returned empty response")
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"LLM response is not valid JSON: {raw}") from exc
    try:
        jsonschema.validate(parsed, schema)
    except ValidationError as exc:
        if isinstance(parsed, dict) and schema.get("properties"):
            allowed_keys = set(schema["properties"].keys())
            pruned = {k: v for k, v in parsed.items() if k in allowed_keys}
            if pruned != parsed:
                try:
                    jsonschema.validate(pruned, schema)
                    parsed = pruned
                except ValidationError as inner_exc:
                    raise ValueError(f"LLM response failed schema validation after pruning: {inner_exc.message}") from inner_exc
            else:
                raise ValueError(f"LLM response failed schema validation: {exc.message}") from exc
        else:
            raise ValueError(f"LLM response failed schema validation: {exc.message}") from exc
    return parsed


async def _peek_planner_llm(user_text: str, stage: str) -> dict:
    if stage == "VIDEO":
        stage_prompt = "The user was asked if they'd like to watch a short class glimpse."
        allowed = "- SHOW_VIDEO: user wants to watch the class video\n- SKIP: user wants to skip\n- CLARIFY: unclear response"
    else:
        stage_prompt = "The user was asked if they'd like to see a quick preview of current requirements."
        allowed = "- SHOW_NEEDS: user wants to see requirements preview\n- SKIP: user wants to skip\n- CLARIFY: unclear response"
    prompt = (
        "You are helping decide the next step in a volunteer onboarding flow.\n"
        f"{stage_prompt}\n"
        "Allowed actions:\n"
        f"{allowed}\n"
        "Return ONLY valid JSON with keys: action, tone_reply.\n"
        "tone_reply should be a short, friendly response (1-2 lines).\n"
        "If user is unclear, tone_reply should ask a concise clarification."
    )
    messages = [
        {"role": "system", "content": MASTER_SYSTEM_PROMPT},
        {"role": "system", "content": prompt},
        {"role": "user", "content": user_text},
    ]
    return await _llm_call_structured(
        messages,
        schema=PEEK_PLANNER_SCHEMA,
        temperature=0.2,
        max_tokens=120,
        timeout=12,
    )


def _extract_llm_text(result: dict) -> str:
    if not isinstance(result, dict):
        return ""
    content = result.get("content")
    if isinstance(content, list):
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text" and item.get("text"):
                return item["text"]
    for key in ("reply", "tone_reply", "text", "message"):
        value = result.get(key)
        if isinstance(value, str):
            return value
    return ""


def _sanitize_llm_message(text: str) -> str:
    if not isinstance(text, str):
        return ""
    cleaned = text.strip().strip('"').strip()
    if "The enhanced reply" in cleaned:
        cleaned = cleaned.split("The enhanced reply", 1)[0].strip()
    lines: list[str] = []
    for line in cleaned.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith(("-", "•", "*")):
            continue
        lines.append(stripped)
    cleaned = " ".join(lines) if lines else cleaned
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


# ---------- MCP Tool Wrappers ----------
def _wa_sanitize(text: str) -> str:
    """Sanitize text while preserving UTF-8 encoding and emojis."""
    if not isinstance(text, str):
        return str(text)
    # Normalize some special characters but preserve UTF-8/emojis
    safe = text.replace("–", "-").replace("—", "-")
    # Ensure text is valid UTF-8 (don't strip emojis/non-ASCII)
    try:
        safe.encode("utf-8")
    except UnicodeEncodeError:
        # If encoding fails, try to fix invalid characters but preserve valid UTF-8
        safe = safe.encode("utf-8", "replace").decode("utf-8")
    return safe


async def mcp_wa_send_template(to: str, template_name: str, language_code: str = "en") -> Optional[str]:
    """
    Send WhatsApp template message via MCP (required for first outbound message).
    
    Args:
        to: Phone number
        template_name: Template name (e.g., "serve_welcome")
        language_code: Template language code (default: "en")
    
    Returns:
        Optional[str]: Outbound message ID if available, None otherwise
    """
    args = {
        "to": to,
        "type": "template",
        "template": {
            "name": template_name,
            "language": {
                "code": language_code
            }
        }
    }
    result = await _mcp_call("wa.send_message", args, timeout=10)
    
    # Extract outbound message ID from MCP response
    message_id = None
    if isinstance(result, dict):
        # Try direct message_id field
        if "message_id" in result:
            message_id = str(result["message_id"])
        # Try wamid field
        elif "wamid" in result:
            message_id = str(result["wamid"])
        # Try nested in result
        elif "result" in result and isinstance(result["result"], dict):
            if "message_id" in result["result"]:
                message_id = str(result["result"]["message_id"])
            elif "wamid" in result["result"]:
                message_id = str(result["result"]["wamid"])
    
    # Update last_outbound_msg_id in database (best-effort, non-blocking)
    if message_id:
        try:
            from storage.db import get_db_session
            from storage.session_store import update_session_state_and_tool_state
            
            with get_db_session() as db:
                # Get current state from in-memory session
                sess = SESSIONS.get(to, {})
                current_state = sess.get("state", "WELCOME")
                sub_state = sess.get("sub_state")
                
                # Update last_outbound_msg_id without changing state
                update_session_state_and_tool_state(
                    db=db,
                    wa_phone=to,
                    state=current_state,
                    sub_state=sub_state,
                    last_outbound_msg_id=message_id
                )
        except Exception as e:
            log.warning(f"[MCP_WA_SEND_TEMPLATE] Failed to update last_outbound_msg_id for {to}: {e}")
            # Don't block on persistence failure
    
    return message_id


async def mcp_wa_send(to: str, text: str, buttons: list[str] = None) -> Optional[str]:
    """
    Send WhatsApp message via MCP
    
    Args:
        to: Phone number
        text: Message text
        buttons: Optional list of button labels (e.g., ["✅ Yes", "❌ No", "ℹ️ Tell me more"])
    
    Returns:
        Optional[str]: Outbound message ID if available, None otherwise
    """
    args = {"to": to, "text": _wa_sanitize(text)}
    if buttons:
        args["buttons"] = buttons
    result = await _mcp_call("wa.send_message", args, timeout=10)
    
    # Extract outbound message ID from MCP response
    message_id = None
    if isinstance(result, dict):
        # Try direct message_id field
        if "message_id" in result:
            message_id = str(result["message_id"])
        # Try wamid field
        elif "wamid" in result:
            message_id = str(result["wamid"])
        # Try nested in result
        elif "result" in result and isinstance(result["result"], dict):
            if "message_id" in result["result"]:
                message_id = str(result["result"]["message_id"])
            elif "wamid" in result["result"]:
                message_id = str(result["result"]["wamid"])
    
    # Update last_outbound_msg_id in database (best-effort, non-blocking)
    if message_id:
        try:
            from storage.db import get_db_session
            from storage.session_store import update_session_state_and_tool_state
            
            with get_db_session() as db:
                # Get current state from in-memory session
                sess = SESSIONS.get(to, {})
                current_state = sess.get("state", "WELCOME")
                sub_state = sess.get("sub_state")
                
                # Update last_outbound_msg_id without changing state
                update_session_state_and_tool_state(
                    db=db,
                    wa_phone=to,
                    state=current_state,
                    sub_state=sub_state,
                    last_outbound_msg_id=message_id
                )
        except Exception as e:
            log.warning(f"[MCP_WA_SEND] Failed to update last_outbound_msg_id for {to}: {e}")
            # Don't block on persistence failure
    
    return message_id


async def mcp_wa_send_list(
    to: str,
    header: str,
    body: Optional[str] = None,
    footer: Optional[str] = None,
    sections: List[Dict[str, Any]] = None,
    button_label: str = "View options",
) -> Optional[str]:
    """
    Send WhatsApp interactive list message (carousel) via MCP.

    Expected MCP payload shape:
    {
      "to": "<phone>",
      "list": {
        "body": "Message body text",
        "button": "View Options",
        "header": "Optional header",
        "footer": "Optional footer",
        "sections": [ { "rows": [ { "id": "...", "title": "...", "description": "..." } ] } ]
      }
    }
    """
    # Validate and trim fields to WhatsApp limits
    header_trimmed = header[:60] if header else ""
    body_text = body or header_trimmed or "Here are some options you can consider:"
    body_trimmed = body_text[:1024]
    footer_trimmed = footer[:60] if footer else ""
    button_trimmed = (button_label or "View options")[:20]

    list_obj: Dict[str, Any] = {
        "body": body_trimmed,
        "button": button_trimmed,
    }
    if header_trimmed:
        list_obj["header"] = header_trimmed
    if footer_trimmed:
        list_obj["footer"] = footer_trimmed

    # Validate and format sections/rows
    formatted_sections: List[Dict[str, Any]] = []
    if sections:
        for section in sections[:10]:  # Max 10 sections
            rows = section.get("rows", [])
            if not rows:
                continue

            formatted_section: Dict[str, Any] = {}
            if section.get("title"):
                formatted_section["title"] = str(section["title"])[:24]

            formatted_rows = []
            for row in rows[:10]:  # Max 10 rows total per WhatsApp docs
                formatted_row: Dict[str, Any] = {
                    "id": str(row.get("id", ""))[:200],
                    "title": str(row.get("title", ""))[:24],
                }
                if row.get("description"):
                    formatted_row["description"] = str(row["description"])[:72]
                formatted_rows.append(formatted_row)

            if formatted_rows:
                formatted_section["rows"] = formatted_rows
                formatted_sections.append(formatted_section)

    if formatted_sections:
        list_obj["sections"] = formatted_sections

    args = {
        "to": to,
        "list": list_obj,
    }
    
    try:
        result = await _mcp_call("wa.send_message", args, timeout=10)
        
        # Extract outbound message ID from MCP response
        message_id = None
        if isinstance(result, dict):
            if "message_id" in result:
                message_id = str(result["message_id"])
            elif "wamid" in result:
                message_id = str(result["wamid"])
            elif "result" in result and isinstance(result["result"], dict):
                if "message_id" in result["result"]:
                    message_id = str(result["result"]["message_id"])
                elif "wamid" in result["result"]:
                    message_id = str(result["result"]["wamid"])
        
        # Update last_outbound_msg_id in database (best-effort, non-blocking)
        if message_id:
            try:
                from storage.db import get_db_session
                from storage.session_store import update_session_state_and_tool_state
                
                with get_db_session() as db:
                    sess = SESSIONS.get(to, {})
                    current_state = sess.get("state", "WELCOME")
                    sub_state = sess.get("sub_state")
                    
                    update_session_state_and_tool_state(
                        db=db,
                        wa_phone=to,
                        state=current_state,
                        sub_state=sub_state,
                        last_outbound_msg_id=message_id
                    )
            except Exception as e:
                log.warning(f"[MCP_WA_SEND_LIST] Failed to update last_outbound_msg_id for {to}: {e}")
        
        return message_id
    except Exception as e:
        log.error(f"[MCP_WA_SEND_LIST] Failed to send interactive list to {to}: {e}", exc_info=True)
        return None


# ---------- Media Upload & Video Sending ----------
# In-memory cache for media_id (keyed by file path)
_MEDIA_ID_CACHE: dict[str, str] = {}
_MEDIA_CACHE_FILE = ".media_cache.json"


def _load_media_cache() -> dict[str, str]:
    """Load media_id cache from file if it exists."""
    if os.path.exists(_MEDIA_CACHE_FILE):
        try:
            with open(_MEDIA_CACHE_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            log.warning(f"[MEDIA] Failed to load cache file: {e}")
    return {}


def _save_media_cache(cache: dict[str, str]):
    """Save media_id cache to file."""
    try:
        with open(_MEDIA_CACHE_FILE, "w") as f:
            json.dump(cache, f)
    except Exception as e:
        log.warning(f"[MEDIA] Failed to save cache file: {e}")


async def mcp_wa_send_class_video(to_phone: str) -> Optional[str]:
    """
    Send class preview video to WhatsApp recipient.
    The MCP server handles loading and sending the video file internally.
    
    Args:
        to_phone: Recipient phone number (required)
        
    Returns:
        message_id if successful, None otherwise
    """
    log.info(f"[VIDEO] Requesting class video send to {to_phone}")
    
    try:
        # Call serve.whatsapp.send_class_video MCP tool
        # Server handles loading the video file internally
        result = await _mcp_call(
            "serve.whatsapp.send_class_video",
            {
                "to_phone": to_phone
            },
            timeout=60  # Longer timeout for file upload and send
        )
        
        # Extract message_id from result (tool sends video and returns message_id)
        if isinstance(result, dict):
            # Check for success indicator
            if result.get("ok") is True:
                # Extract message_id - check wa_message_id first (actual field name)
                message_id = result.get("wa_message_id") or result.get("message_id") or result.get("id") or result.get("wamid")
                if message_id:
                    log.info(f"[VIDEO] MCP tool sent video successfully, message_id: {message_id}")
                    return str(message_id)
                else:
                    # Video was sent (ok: true) but no message_id - still consider success
                    log.info(f"[VIDEO] MCP tool sent video successfully (ok: true), but no message_id in response")
                    return "success"  # Return a success indicator even without message_id
            elif result.get("ok") is False:
                # Explicit failure
                error_msg = result.get("error") or "Unknown error"
                log.error(f"[VIDEO] MCP tool failed: {error_msg}")
                return None
            else:
                # No 'ok' field - try to extract message_id anyway (backward compatibility)
                message_id = result.get("wa_message_id") or result.get("message_id") or result.get("id") or result.get("wamid")
                if message_id:
                    log.info(f"[VIDEO] MCP tool sent video successfully, message_id: {message_id}")
                    return str(message_id)
        
        log.warning(f"[VIDEO] MCP tool returned unexpected format: {result}")
        return None
        
    except Exception as e:
        log.error(f"[VIDEO] Failed to send class video: {e}")
        return None


async def mcp_wa_send_welcome_video(to_phone: str) -> Optional[str]:
    """
    Send welcome video to WhatsApp recipient.
    The MCP server handles loading and sending the video file internally.
    
    Args:
        to_phone: Recipient phone number (required)
        
    Returns:
        message_id if successful, None otherwise
    """
    log.info(f"[VIDEO] Requesting welcome video send to {to_phone}")
    
    try:
        result = await _mcp_call(
            "serve.whatsapp.send_welcome_video",
            {
                "to_phone": to_phone
            },
            timeout=60  # Longer timeout for file upload and send
        )
        
        if isinstance(result, dict):
            if result.get("ok") is True:
                message_id = result.get("wa_message_id") or result.get("message_id") or result.get("id") or result.get("wamid")
                if message_id:
                    log.info(f"[VIDEO] MCP tool sent welcome video successfully, message_id: {message_id}")
                    return str(message_id)
                log.info("[VIDEO] MCP tool sent welcome video successfully (ok: true), but no message_id in response")
                return "success"
            elif result.get("ok") is False:
                error_msg = result.get("error") or "Unknown error"
                log.error(f"[VIDEO] MCP tool failed: {error_msg}")
                return None
            else:
                message_id = result.get("wa_message_id") or result.get("message_id") or result.get("id") or result.get("wamid")
                if message_id:
                    log.info(f"[VIDEO] MCP tool sent welcome video (legacy response), message_id: {message_id}")
                    return str(message_id)
        
        log.warning(f"[VIDEO] MCP tool returned unexpected format: {result}")
        return None
        
    except Exception as e:
        log.error(f"[VIDEO] Failed to send welcome video: {e}")
        return None


async def mcp_wa_send_thankyou_video(to_phone: str) -> Optional[str]:
    """
    Send thank-you video to WhatsApp recipient.
    The MCP server handles loading and sending the video file internally.
    
    Args:
        to_phone: Recipient phone number (required)
        
    Returns:
        message_id if successful, None otherwise
    """
    log.info(f"[VIDEO] Requesting thank-you video send to {to_phone}")
    
    try:
        result = await _mcp_call(
            "serve.whatsapp.send_thankyou_video",
            {
                "to_phone": to_phone
            },
            timeout=60  # Longer timeout for file upload and send
        )
        
        if isinstance(result, dict):
            if result.get("ok") is True:
                message_id = result.get("wa_message_id") or result.get("message_id") or result.get("id") or result.get("wamid")
                if message_id:
                    log.info(f"[VIDEO] MCP tool sent thank-you video successfully, message_id: {message_id}")
                    return str(message_id)
                log.info("[VIDEO] MCP tool sent thank-you video successfully (ok: true), but no message_id in response")
                return "success"
            elif result.get("ok") is False:
                error_msg = result.get("error") or "Unknown error"
                log.error(f"[VIDEO] MCP tool failed: {error_msg}")
                return None
            else:
                message_id = result.get("wa_message_id") or result.get("message_id") or result.get("id") or result.get("wamid")
                if message_id:
                    log.info(f"[VIDEO] MCP tool sent thank-you video (legacy response), message_id: {message_id}")
                    return str(message_id)
        
        log.warning(f"[VIDEO] MCP tool returned unexpected format: {result}")
        return None
        
    except Exception as e:
        log.error(f"[VIDEO] Failed to send thank-you video: {e}")
        return None


async def mcp_wa_send_video(to: str, media_id: str, caption: Optional[str] = None) -> Optional[str]:
    """
    Send a WhatsApp video message using media_id.
    
    Args:
        to: Phone number
        media_id: Media ID from upload
        caption: Optional caption text
        
    Returns:
        Optional[str]: Outbound message ID if available, None otherwise
    """
    try:
        args = {
            "to": to,
            "type": "video",
            "video": {
                "id": media_id
            }
        }
        
        if caption:
            args["video"]["caption"] = _wa_sanitize(caption)
        
        result = await _mcp_call("wa.send_message", args, timeout=30)
        
        # Extract outbound message ID
        if isinstance(result, dict):
            if "message_id" in result:
                return str(result["message_id"])
            if "wamid" in result:
                return str(result["wamid"])
            if "result" in result and isinstance(result["result"], dict):
                if "message_id" in result["result"]:
                    return str(result["result"]["message_id"])
                if "wamid" in result["result"]:
                    return str(result["result"]["wamid"])
        
        return None
        
    except Exception as e:
        log.error(f"[VIDEO] Failed to send video message: {e}")
        raise


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


def _format_weekly_hours(hours: float | int | None) -> str | None:
    try:
        if hours is None:
            return None
        value = float(hours)
    except (TypeError, ValueError):
        return None

    if value <= 0:
        return None

    rounded = round(value)
    if abs(value - rounded) < 0.01:
        return f"{int(rounded)} hours"
    return f"{value:.1f} hours"


def _build_eligibility_summary(sess: dict, commitment_hours: float | int | None) -> str | None:
    if not sess.get("elig.age") or not sess.get("elig.device"):
        return None

    age_val = sess.get("elig.age_value")
    age_phrase = None
    if isinstance(age_val, (int, float)) and age_val >= 18:
        rounded_age = round(age_val)
        if abs(age_val - rounded_age) < 0.5:
            age_phrase = f"{int(rounded_age)}+"
        else:
            age_phrase = f"{age_val:.0f}+"

    if not age_phrase:
        age_phrase = "18+"

    hours_phrase = _format_weekly_hours(commitment_hours or sess.get("elig.commitment_hours"))
    if not hours_phrase:
        return None

    return format_message(
        ELIGIBILITY_SUMMARY,
        age_phrase=age_phrase,
        commitment_phrase=hours_phrase,
    )


async def _generate_eligibility_summary_phone(
    phone: str,
    sess: dict,
    profile: dict,
    *,
    commit_hours: float | int | None,
    volunteer_name: str | None = None,
) -> str | None:
    """Deterministic eligibility summary renderer."""
    return _build_eligibility_summary(sess, commit_hours)


async def _generate_prefs_summary_phone(
    phone: str,
    sess: dict,
    profile: dict,
    *,
    volunteer_name: str | None,
    days: list[str],
    time_band: str | None,
    days_label: str,
    band_label: str,
) -> str | None:
    """Deterministic acknowledgement of captured preferences."""
    if not days or not time_band:
        return None

    fallback = format_message(
        PREFS_SUMMARY_FALLBACK,
        days_label=days_label,
        band_label=band_label,
    )
    return fallback


async def _generate_qa_summary_phone(
    phone: str,
    profile: dict,
    *,
    volunteer_name: str | None,
    had_questions: bool,
    topics: list[str],
) -> str | None:
    """Deterministic reflection before wrap-up."""
    return QA_SUMMARY_WITH_QUESTIONS if had_questions else QA_SUMMARY_NO_QUESTIONS


async def _send_orientation_summary(phone: str, sess: dict, profile: dict):
    """Send summary before wrap-up."""
    if sess.get("_qa_summary_sent"):
        return

    topics = sess.get("_qa_topics") or []
    had_questions = bool(topics) or bool(sess.get("_qa_count", 0))

    summary_msg = await _generate_qa_summary_phone(
        phone=phone,
        profile=profile,
        volunteer_name=profile.get("name"),
        had_questions=had_questions,
        topics=list(topics),
    )

    if summary_msg:
        await asyncio.sleep(0.3)
        await mcp_wa_send(phone, summary_msg)
        _add_to_history(phone, bot_msg=summary_msg)

    sess["_qa_summary_sent"] = True
    sess.pop("_qa_topics", None)


async def _generate_prefs_interpretation(
    phone: str,
    profile: dict,
    volunteer_name: str | None,
    text: str,
    sess: dict,
) -> dict:
    """Use LLM to interpret availability reply; return structured hints + follow-up text."""
    fallback = {
        "days": [],
        "time_band": None,
        "language": None,
        "followup": None,
        "followup_tag": None,
        "deferral": None,
        "topics": [],
    }

    try:
        messages: list[dict] = [
            {"role": "system", "content": MASTER_SYSTEM_PROMPT},
            {
                "role": "system",
                "content": (
                    "You interpret availability replies. Return strict JSON with keys: preferred_days (array of ISO weekday strings), "
                    "preferred_time_band (MORNING/AFTERNOON/EVENING or null), preferred_language (string or null), "
                    "followup (string or null), followup_tag (string or null), "
                    "deferral (object with keys message and until_iso or null), topics (array)."
                ),
            },
        ]
        messages += FEW_SHOT_EXAMPLES.get("PREFS_INTERPRET", [])
        user_prompt = json.dumps(
            {
                "volunteer_name": volunteer_name or profile.get("name") or "there",
                "existing_days": sess.get("_prefs_days", []),
                "existing_time_band": sess.get("_prefs_time_band"),
                "user_text": text,
            },
            ensure_ascii=False,
        )
        payload = await _llm_call_structured(
            messages + [{"role": "user", "content": user_prompt}],
            schema=PREFS_INTERPRET_RESPONSE_SCHEMA,
            temperature=0.2,
            max_tokens=220,
            timeout=12,
        )
        interpretation = {
            "days": payload.get("preferred_days") or [],
            "time_band": payload.get("preferred_time_band"),
            "language": payload.get("preferred_language"),
            "deferral": payload.get("deferral"),
            "topics": payload.get("topics") or [],
            "followup": payload.get("followup"),
            "followup_tag": payload.get("followup_tag"),
        }
        return interpretation
    except Exception as e:
        log.warning(f"[PREFS_INTERPRET] LLM interpretation failed: {e}")
    return fallback


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
        
        messages = [
            {"role": "system", "content": MASTER_SYSTEM_PROMPT},
            {
                "role": "system",
                "content": (
                    "Write a short (2-3 sentence) personalized WhatsApp welcome message for a new volunteer who just registered."
                ),
            },
        ]
        messages += FEW_SHOT_EXAMPLES.get("SMART_WELCOME", [])

        user_prompt = json.dumps(
            {
                "name": name,
                "source": source,
                "preferences": preferences,
            },
            ensure_ascii=False,
        )

        result = await _llm_call_messages(messages + [{"role": "user", "content": user_prompt}], max_tokens=180, timeout=12)

        welcome_msg = _sanitize_llm_message(_extract_llm_text(result))
        if welcome_msg:
            log.info(f"[SMART_WELCOME] Generated personalized welcome for {name}")
            return welcome_msg

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
    pattern = (
        r"\b("
        r"later|next\s+week|another\s+(time|day)|tomorrow|"
        r"not\s+today|not\s+now|not\s+right\s+now|not\s+sure|"
        r"busy|travel(l)?ing|remind|maybe\s+later|do\s+this\s+later|"
        r"come\s+back|check\s+back|ping\s+me\s+later"
        r")\b"
    )
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
    task_prompt = STATE_TASK_PROMPTS.get(state)
    if not task_prompt:
        task_prompt = DEFAULT_TASK_PROMPT.format(state=state)

    user_prompt = f"""Context: {json.dumps(context, indent=2)}\nUser message: {text}"""

    try:
        few_shots = FEW_SHOT_EXAMPLES.get(state, [])
        messages = [
            {"role": "system", "content": MASTER_SYSTEM_PROMPT},
            {"role": "system", "content": task_prompt},
        ] + few_shots + [
            {"role": "user", "content": user_prompt},
        ]
        parsed = await _llm_call_structured(
            messages,
            schema=INTENT_RESPONSE_SCHEMA,
            temperature=0.2,
            max_tokens=200,
            timeout=15,
        )

        intent = str(parsed.get("intent", "AMBIGUOUS") or "").upper()
        confidence_raw = parsed.get("confidence", 0.0)
        try:
            confidence = float(confidence_raw)
        except (TypeError, ValueError):
            confidence = 0.0
        confidence = max(0.0, min(1.0, confidence))

        tone_reply = parsed.get("tone_reply")
        if not isinstance(tone_reply, str):
            tone_reply = ""

        if state == "WELCOME" and intent not in WELCOME_ALLOWED_INTENTS:
            intent = "AMBIGUOUS"

        if state == "ELIGIBILITY_PART1" and intent not in ELIGIBILITY_PART1_ALLOWED_INTENTS:
            intent = "AMBIGUOUS"

        if state == "ELIGIBILITY_PART1" and confidence < 0.6:
            if intent.startswith("AGE_"):
                intent = "AGE_UNCLEAR"
            elif intent.startswith("DEVICE_"):
                intent = "DEVICE_UNCLEAR"
            elif intent == "DEFERRAL":
                intent = "AMBIGUOUS"

        if state == "ELIGIBILITY_PART2" and intent not in ELIGIBILITY_PART2_ALLOWED_INTENTS:
            intent = "AMBIGUOUS"

        if state == "ELIGIBILITY_PART2" and confidence < 0.7:
            if intent == "COMMIT_OK":
                intent = "COMMIT_UNSURE"
            elif intent in {"COMMIT_TOO_LOW", "COMMIT_NO", "DEFERRAL"}:
                pass
            else:
                intent = "AMBIGUOUS"

        if state == "PREFS_DAYTIME" and intent not in PREFS_DAYTIME_ALLOWED_INTENTS:
            intent = "PREFS_AMBIGUOUS"

        if state == "PREFS_DAYTIME" and confidence < 0.5:
            intent = "PREFS_AMBIGUOUS"

        if "intent" not in parsed:
            intent = "AMBIGUOUS"

        return {"intent": intent, "confidence": confidence, "tone_reply": tone_reply}
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
    qa_task_prompt = """Guidelines for answering volunteer questions:

1. Answer in 2–4 short lines using the provided snippets/policy context.
2. Do NOT invent facts or promise payment (this role is volunteer-only).
3. Keep the tone warm, supportive, and clear.
4. If the snippets don't fully cover the question, invite them to message here anytime for help.
5. Do not mention orientation, scheduling, or internal stages.
6. Output plain text only (no JSON/markdown)."""

    context_obj = {
        "policy_version": policy_version,
        "knowledge_version": knowledge_version,
        "snippets": snippets,
        "user_profile": user_profile or {}
    }
    
    user_prompt = f"""Context:
{json.dumps(context_obj, indent=2)}

User question: {question}

Generate a warm, concise answer (2-4 lines) using the snippets above. Avoid mentioning orientation, scheduling, or internal stages."""
    
    try:
        few_shots = FEW_SHOT_EXAMPLES.get("FAQ", [])
        messages = (
            [{"role": "system", "content": MASTER_SYSTEM_PROMPT},
             {"role": "system", "content": qa_task_prompt}]
            + few_shots
            + [{"role": "user", "content": user_prompt}]
        )
        result = await _mcp_call("llm.call", {"messages": messages, "temperature": 0.3, "max_tokens": 300}, timeout=15)
        
        content = result.get("content") or result.get("message") or result.get("text", "")
        if isinstance(content, str) and content.strip():
            return content.strip()
        return ""
    except Exception as e:
        log.warning(f"[LLM] QA generation failed: {e}")
        return ""


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
async def _reask_pending_question(phone: str, state: str, sess: dict) -> bool:
    """Re-send the outstanding question after handling an FAQ reply."""
    prompt_text: str | None = None

    if state == "WELCOME":
        prompt_text = WELCOME_CONSENT_REMINDER
    elif state == "ELIGIBILITY_PART1":
        step = sess.get("_eligibility_step", "age")
        if step == "age":
            prompt_text = ELIGIBILITY_AGE_PROMPT
            sess["_eligibility_age_asked"] = True
        elif step == "device":
            prompt_text = ELIGIBILITY_DEVICE_PROMPT
            sess["_eligibility_device_asked"] = True
    elif state == "ELIGIBILITY_PART2":
        prompt_text = ELIGIBILITY_COMMIT_PROMPT
        sess["_eligibility_part2_sent"] = True
    elif state == "PREFS_DAYTIME":
        prompt_text = sess.get("_prefs_last_prompt_text")
        if not prompt_text:
            prompt_text = PREFS_INTRO_COLLAB
            sess["_prefs_last_prompt"] = "intro"
            sess["_prefs_last_prompt_text"] = prompt_text

    if not prompt_text:
        return False

    await mcp_wa_send(phone, prompt_text)
    _add_to_history(phone, bot_msg=prompt_text)
    sess["ts"] = time.time()
    SESSIONS[phone] = sess
    return True


async def _book_slot_and_finish(phone: str, chosen_slot: dict, profile: dict, name: str, *, send_orientation_confirm: bool = False):
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
    
    title = "Serve Vriddhi - Volunteer Welcome Session"
    attendees = [phone]
    
    try:
        res = await mcp_calendar_create(title, start_iso, end_iso, attendees)
        meet_url = res.get("meeting_url", "https://meet.google.com/placeholder")
        
        profile["meeting_url"] = meet_url
        profile["meeting_start"] = start_iso
        
        # Send final confirmation (keep quick acknowledgement + final details)
        confirmation_lines = [
            f"Session: {label}",
            f"Join link: {meet_url}",
            "",
            f"Welcome to the SERVE Volunteer Community, {name}!",
            "Every hour you share helps a child learn better. See you soon!"
        ]
        confirm_msg = "\n".join(confirmation_lines).strip()
        await mcp_wa_send(phone, confirm_msg)
        _add_to_history(phone, bot_msg=confirm_msg)
        
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
            "uuid": phone,
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


# ---------- Idempotency & Locking ----------
def _extract_inbound_msg_id(evt: dict, phone: str, text: str) -> str:
    """
    Extract inbound message ID from Kafka event payload.
    
    Priority:
    1) payload["message_id"] if present
    2) payload["wamid"] if present
    3) payload["meta"]["messages"][0]["id"] if present
    4) Generate deterministic hash from (wa_phone + text + timestamp)
    
    Args:
        evt: Kafka event payload
        phone: WhatsApp phone number
        text: Message text
        
    Returns:
        str: Inbound message ID
    """
    data = evt.get("data") or {}
    meta = evt.get("meta") or {}
    
    # Priority 1: message_id
    if "message_id" in data:
        return str(data["message_id"])
    
    # Priority 2: wamid
    if "wamid" in data:
        return str(data["wamid"])
    
    # Priority 3: meta.messages[0].id
    if "messages" in meta and isinstance(meta["messages"], list) and len(meta["messages"]) > 0:
        msg = meta["messages"][0]
        if isinstance(msg, dict) and "id" in msg:
            return str(msg["id"])
    
    # Priority 4: Generate deterministic hash
    timestamp = evt.get("timestamp") or data.get("timestamp") or str(time.time())
    hash_input = f"{phone}:{text}:{timestamp}"
    hash_value = hashlib.sha256(hash_input.encode()).hexdigest()[:16]
    return f"hash_{hash_value}"


async def _handle_with_idempotency(phone: str, text: str, inbound_msg_id: str, evt: dict):
    """
    Handle inbound message with idempotency check and phone lock.
    
    Args:
        phone: WhatsApp phone number
        text: Message text
        inbound_msg_id: Inbound message ID for idempotency
        evt: Original Kafka event (for context)
    """
    # Check for demo shortcuts
    try:
        # Selection Agent shortcut
        from agents.selection.config import settings as selection_settings
        if selection_settings.DEMO_SELECTION_SHORTCUT:
            text_lower = text.lower().strip()
            if text_lower in ["select", "selection"]:
                log.info(f"[ROUTING] Demo shortcut triggered for {phone}, routing to Selection Agent")
                sess = SESSIONS.get(phone, {})
                sess["state"] = "SEL_START"
                sess["agent"] = "selection"
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                
                from agents.selection.handler import handle_selection
                await handle_selection(phone, "__kick__", sess)
                return
        
        # Fulfillment Agent shortcut
        from agents.fulfillment.config import settings as fulfillment_settings
        if fulfillment_settings.DEMO_NEEDS_SHORTCUT:
            text_lower = text.lower().strip()
            if text_lower in ["needs", "opportunities"]:
                log.info(f"[ROUTING] Demo shortcut triggered for {phone}, routing to Fulfillment Agent")
                sess = SESSIONS.get(phone, {})
                sess["state"] = "FULFILL_INTRO"
                sess["agent"] = "fulfillment"
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                
                from agents.fulfillment.handler import handle_fulfillment
                await handle_fulfillment(phone, "__kick__", sess)
                return
    except Exception as e:
        log.warning(f"[ROUTING] Failed to check demo shortcuts: {e}")
    
    # Get phone lock
    lock = get_phone_lock(phone)
    
    async with lock:
        # Get or create session and check idempotency
        with get_db_session() as db:
            db_session = get_or_create_session(db, wa_phone=phone, agent_name="onboarding")
            
            # Check for duplicate
            last_inbound_id = get_last_inbound_id(db, phone)
            if last_inbound_id == inbound_msg_id:
                log.info(f"[IDEMPOTENCY] Duplicate message ignored for {phone}: {inbound_msg_id}")
                # Log DUPLICATE_IGNORED event (best-effort)
                try:
                    log_event(
                        db=db,
                        wa_phone=phone,
                        agent_name=settings.AGENT_NAME,
                        event_type="DUPLICATE_IGNORED",
                        event_source="onboarding_agent",
                        state=db_session.get("state"),
                        status="ignored",
                        details={"inbound_msg_id": inbound_msg_id}
                    )
                except:
                    pass  # Best-effort
                return  # Early return - don't process duplicate
        
        # Check current agent and route accordingly
        sess = SESSIONS.get(phone, {})
        current_agent = sess.get("agent", "onboarding")
        
        if current_agent == "selection":
            # Route to Selection Agent
            try:
                from agents.selection.handler import handle_selection
                await handle_selection(phone, text, sess)
                # Update idempotency after processing
                try:
                    with get_db_session() as db:
                        set_last_inbound_id(db, phone, inbound_msg_id, None)
                except Exception as e:
                    log.warning(f"[IDEMPOTENCY] Failed to update idempotency for {phone}: {e}")
                return
            except Exception as e:
                log.error(f"[ROUTING] Error in Selection Agent: {e}", exc_info=True)
                # Fall through to onboarding handler
        elif current_agent == "fulfillment":
            # Route to Fulfillment Agent
            try:
                from agents.fulfillment.handler import handle_fulfillment
                await handle_fulfillment(phone, text, sess)
                # Update idempotency after processing
                try:
                    with get_db_session() as db:
                        set_last_inbound_id(db, phone, inbound_msg_id, None)
                except Exception as e:
                    log.warning(f"[IDEMPOTENCY] Failed to update idempotency for {phone}: {e}")
                return
            except Exception as e:
                log.error(f"[ROUTING] Error in Fulfillment Agent: {e}", exc_info=True)
                # Fall through to onboarding handler
        
        # Process message normally (onboarding agent or fallback)
        outbound_msg_id = None
        try:
            # Call original _handle
            await _handle(phone, text, evt)
            
            # Note: outbound_msg_id would ideally be captured from mcp_wa_send return value
            # For now, we update idempotency after processing
            # Future enhancement: track outbound_msg_id from mcp_wa_send calls
            
        finally:
            # Update idempotency after processing (even if error occurred)
            # Only update if we actually processed (not duplicate)
            try:
                with get_db_session() as db:
                    set_last_inbound_id(db, phone, inbound_msg_id, outbound_msg_id)
            except Exception as e:
                log.warning(f"[IDEMPOTENCY] Failed to update idempotency for {phone}: {e}")


# ---------- State Machine ----------
async def _handle(phone: str, text: str, evt: Optional[Dict] = None):
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
        db_session = None
        try:
            from storage.db import get_db_session
            from storage.session_store import get_or_create_session
            with get_db_session() as db:
                db_session = get_or_create_session(
                    db, wa_phone=phone, agent_name=settings.AGENT_NAME
                )
        except Exception as e:
            log.warning(f"[PERSISTENCE] Failed to read DB session for {phone}: {e}", exc_info=True)
        
        if db_session:
            db_state = db_session.get("state") or "WELCOME"
            db_sub_state = db_session.get("sub_state")
            profile_name = db_session.get("temp_name") or "Volunteer"
            created_at = db_session.get("created_at")
            updated_at = db_session.get("updated_at")
            has_context = any([
                db_session.get("tool_state"),
                db_state not in ["WELCOME", None],
                db_sub_state,
                db_session.get("last_outbound_msg_id"),
                db_session.get("temp_name"),
                db_session.get("temp_email"),
                db_session.get("temp_phone"),
                db_session.get("eligibility_status"),
            ])
            looks_new = False
            if created_at and updated_at:
                try:
                    looks_new = abs((updated_at - created_at).total_seconds()) < 1
                except Exception:
                    looks_new = False
            is_restored = has_context or not looks_new
            if db_state == "ONBOARDING":
                sess_state = db_sub_state or "WELCOME"
                agent_name = "onboarding"
            elif db_state == "SELECTION":
                sess_state = db_sub_state or "SEL_START"
                agent_name = "selection"
            elif db_state == "FULFILLMENT":
                sess_state = db_sub_state or "FULFILL_INTRO"
                agent_name = "fulfillment"
            else:
                sess_state = db_state
                agent_name = "onboarding"
            sess = {
                "state": sess_state,
                "agent": agent_name,
                "sub_state": db_sub_state,
                "profile": {
                    "name": profile_name,
                    "registration_data": None,
                    "uuid": phone,
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
                "tool_state": db_session.get("tool_state") or {},
                "ts": time.time(),
                "_welcomed": False,
                "_db_session_id": db_session.get("session_id"),
            }
            # Hydrate session fields from tool_state for better resume behavior
            tool_state = sess.get("tool_state", {})
            if isinstance(tool_state, dict):
                profile = sess.get("profile", {})
                reg = tool_state.get("registration")
                if isinstance(reg, dict):
                    if reg.get("name") and profile.get("name") == "Volunteer":
                        profile["name"] = reg.get("name")
                    if reg.get("email"):
                        profile["email"] = reg.get("email")
                    reg_phone = reg.get("wa_phone") or reg.get("phone")
                    if reg_phone:
                        profile["phone"] = reg_phone
                    serve_block = reg.get("serve", {})
                    if isinstance(serve_block, dict) and serve_block.get("volunteer_id"):
                        profile["volunteer_id"] = serve_block.get("volunteer_id")
                    sess["profile"] = profile

                eligibility = tool_state.get("eligibility")
                if isinstance(eligibility, dict):
                    profile.setdefault("eligibility", {})
                    for key in ["q1_commitment", "q2_age", "q3_device", "passed", "rejection_reason"]:
                        if key in eligibility:
                            profile["eligibility"][key] = eligibility.get(key)
                    sess["profile"] = profile
                    if eligibility.get("prompted_at"):
                        sess["_eligibility_prompted"] = True

                preferences = tool_state.get("preferences")
                if isinstance(preferences, dict):
                    profile.setdefault("preferences", {})
                    if "days" in preferences:
                        profile["preferences"]["days"] = preferences.get("days")
                    if "time_band" in preferences:
                        profile["preferences"]["time_band"] = preferences.get("time_band")
                    if "language" in preferences:
                        profile["preferences"]["language"] = preferences.get("language")
                    sess["profile"] = profile
                    if preferences.get("confirmed_at"):
                        sess["_prefs_confirmed"] = True

                welcome_state = tool_state.get("welcome")
                if isinstance(welcome_state, dict):
                    if welcome_state.get("template_sent_at"):
                        sess["_template_sent"] = True
                    if welcome_state.get("sent_at"):
                        sess["_greet_sent"] = True

                selection_state = tool_state.get("selection")
                if isinstance(selection_state, dict):
                    sess.setdefault("tool_state", {}).setdefault("selection", {})
                    knowing = selection_state.get("knowing_volunteer", {})
                    if isinstance(knowing, dict):
                        if isinstance(knowing.get("profile"), dict):
                            sess["tool_state"]["selection"]["profile"] = knowing.get("profile")
                        discussed_fields = knowing.get("discussed_fields")
                        if discussed_fields:
                            sess["tool_state"]["selection"]["discussed_fields"] = set(discussed_fields)
                        questions_asked = knowing.get("questions_asked")
                        if isinstance(questions_asked, int):
                            sess["tool_state"]["selection"]["question_index"] = questions_asked
                    if isinstance(selection_state.get("discussed_fields"), list):
                        sess["tool_state"]["selection"]["discussed_fields"] = set(selection_state.get("discussed_fields"))

            log.info(f"[PERSISTENCE] Restored session from DB for {phone} (state: {db_state}, sub_state: {db_sub_state})")
            if is_restored:
                try:
                    from storage.db import get_db_session
                    from storage.event_logger import log_event
                    with get_db_session() as db:
                        log_event(
                            db=db,
                            wa_phone=phone,
                            agent_name=settings.AGENT_NAME,
                            event_type="SESSION_RESTORED",
                            event_source="onboarding_agent",
                            state=db_state,
                            sub_state=db_sub_state,
                            status="restored",
                            details={"agent": agent_name},
                            session_id=db_session.get("session_id")
                        )
                except Exception as e:
                    log.warning(f"[PERSISTENCE] Failed to log SESSION_RESTORED for {phone}: {e}", exc_info=True)
            else:
                try:
                    from storage.db import get_db_session
                    from storage.event_logger import log_event
                    with get_db_session() as db:
                        log_event(
                            db=db,
                            wa_phone=phone,
                            agent_name=settings.AGENT_NAME,
                            event_type="SESSION_STARTED",
                            event_source="onboarding_agent",
                            state="WELCOME",
                            session_id=db_session.get("session_id")
                        )
                except Exception as e:
                    log.warning(f"[PERSISTENCE] Failed to log SESSION_STARTED for {phone}: {e}", exc_info=True)
        else:
            # Initialize a complete default profile to avoid KeyError later
            sess = {
                "state": "WELCOME",
                "profile": {
                    "name": "Volunteer",
                    "registration_data": None,
                    "uuid": phone,
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
        
        # Persistence: Create/upsert session in DB (checkpoint 1) if we didn't restore
        if not db_session:
            try:
                from storage.db import get_db_session
                from storage.session_store import get_or_create_session
                from storage.event_logger import log_event
                from .config import settings
                
                with get_db_session() as db:
                    db_session = get_or_create_session(
                        db, wa_phone=phone, agent_name=settings.AGENT_NAME
                    )
                    sess["_db_session_id"] = db_session["session_id"]
                    # Log SESSION_STARTED event
                    log_event(
                        db=db,
                        wa_phone=phone,
                        agent_name=settings.AGENT_NAME,
                        event_type="SESSION_STARTED",
                        event_source="onboarding_agent",
                        state="WELCOME",
                        session_id=db_session["session_id"]
                    )
                    log.info(f"[PERSISTENCE] Created/updated session for {phone}")
            except Exception as e:
                log.warning(f"[PERSISTENCE] Failed to create session for {phone}: {e}", exc_info=True)
                # Continue without DB - don't block flow
    
    # Ensure DB session exists even if in-memory session already existed
    if sess and not sess.get("_db_session_id"):
        try:
            from storage.db import get_db_session
            from storage.session_store import get_or_create_session
            from storage.event_logger import log_event
            from .config import settings
            
            with get_db_session() as db:
                db_session = get_or_create_session(
                    db, wa_phone=phone, agent_name=settings.AGENT_NAME
                )
                sess["_db_session_id"] = db_session["session_id"]
                log.info(f"[PERSISTENCE] Ensured DB session exists for {phone} (session_id: {db_session['session_id']})")
        except Exception as e:
            log.warning(f"[PERSISTENCE] Failed to ensure DB session for {phone}: {e}", exc_info=True)
            # Continue without DB - don't block flow
    
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

    # Global deferral handling (outside WELCOME/DEFERRED)
    if (
        text != "__kick__"
        and state not in ["WELCOME", "DEFERRED", "OPTOUT", "REJECTED", "COMPLETE"]
        and is_defer_response(text)
    ):
        defer_msg = GENERIC_DEFERRED_MSG
        await mcp_wa_send(phone, defer_msg)
        _add_to_history(phone, bot_msg=defer_msg)
        sess["_deferred_prev_state"] = state
        sess["_deferred_reason"] = "USER_DEFERRED"
        sess["state"] = "DEFERRED"
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
    
    # Per-turn flag for state-level FAQ handling
    if text != "__kick__":
        sess["_state_handled_question"] = False
        SESSIONS[phone] = sess
    
    def _is_question(text_value: str) -> bool:
        if not text_value:
            return False
        if "?" in text_value:
            return True
        # Allow question words at start or after a short preface (e.g., "sure, ... can I ...")
        return bool(
            re.search(
                r"^(?:\w+\s+){0,3}(what|how|when|why|where|who|which|can|could|do|does|is|are|will|would|should)\b",
                text_value.strip(),
                re.I,
            )
        )
    
    async def _maybe_answer_global_faq(text_value: str) -> bool:
        """Return True if global FAQ answered the question."""
        try:
            add_bridge = state not in {"COMPLETE"}
            handled = await send_global_faq_response(
                phone=phone,
                question=text_value,
                send_fn=mcp_wa_send,
                add_history_fn=_add_to_history,
                add_bridge=add_bridge,
            )
            if handled:
                sess["_state_handled_question"] = True
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return True
            log.info("[FAQ] No KB match; skipping FAQ answer")
        except Exception as e:
            log.warning(f"[FAQ] Failed to answer FAQ: {e}")
        return False
    
    async def _handle_offtopic_redirect() -> None:
        redirect_msg = (
            "I’m here to help with SERVE volunteering. If you’d like to continue, "
            "I can guide you through the next step."
        )
        await mcp_wa_send(phone, redirect_msg)
        _add_to_history(phone, bot_msg=redirect_msg)
        sess["_state_handled_question"] = True
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        await _reask_pending_question(phone, state, sess)

    # Shared ambiguity resolver (split answer from mixed Q+A)
    def _has_answer_indicators(text_value: str) -> bool:
        if not text_value:
            return False
        text_lower = text_value.lower()
        return bool(
            re.search(r"\b(yes|yeah|yep|no|nah|ready|continue|ok|okay|sure|done)\b", text_lower)
            or "@" in text_lower
            or re.search(r"\b\d{10}\b", text_lower)
            or re.search(r"\b(my name is|i am|i'm|im|name is)\b", text_lower)
            or re.search(r"\b(mon(day)?|tue(sday)?|wed(nesday)?|thu(rsday)?|fri(day)?|sat(urday)?|sun(day)?|weekend|weekday|morning|afternoon|evening)\b", text_lower)
        )
    
    def _split_question_and_answer(text_value: str) -> tuple[str | None, str, bool]:
        if not text_value or not _is_question(text_value):
            return (None, text_value, False)
        if len(text_value.split()) < 6:
            return (None, text_value, False)
        if not _has_answer_indicators(text_value):
            return (None, text_value, False)
        last_q = text_value.rfind("?")
        if last_q == -1:
            return (None, text_value, False)
        question_part = text_value[: last_q + 1].strip()
        answer_part = text_value[last_q + 1 :].strip()
        if answer_part and _has_answer_indicators(answer_part):
            return (question_part, answer_part, True)
        return (None, text_value, False)
    
    resume_intent = text != "__kick__" and is_resume_response(text)
    mixed_question, text_for_state, is_mixed_qna = (None, text, False)
    if text != "__kick__":
        mixed_question, text_for_state, is_mixed_qna = _split_question_and_answer(text)
    
    if is_mixed_qna and mixed_question:
        answered = False
        if state == "WELCOME":
            welcome_faq = [
                (r"\b(what is this|what is serve|what is this about|about serve)\b", "This is SERVE’s volunteer onboarding on WhatsApp — I’ll guide you step by step."),
                (r"\b(how long|how much time|how long will this take)\b", "About 5–10 minutes if you’re ready now."),
                (r"\b(what will you ask|what do i need to do|what is the process)\b", "Just a few basics — your interest, availability, and contact details."),
                (r"\b(is this paid|paid role|payment|stipend)\b", "It’s a volunteer role with no payment."),
                (r"\b(laptop|tablet|device|phone)\b", "You’ll need a tablet or laptop with stable internet for classes."),
            ]
            for pattern, answer in welcome_faq:
                if re.search(pattern, text_lower_global):
                    await mcp_wa_send(phone, answer)
                    _add_to_history(phone, bot_msg=answer)
                    answered = True
                    break
        if not answered and not resume_intent:
            answered = await _maybe_answer_global_faq(mixed_question)
        if not answered:
            fallback = "I can share more on that soon — for now, let’s continue."
            await mcp_wa_send(phone, fallback)
            _add_to_history(phone, bot_msg=fallback)
        sess["_state_handled_question"] = True
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
    
    # Lightweight FAQ intercept (strict: only explicit questions)
    # Behavior: answer FAQ and then continue normal state flow (no pause),
    # except when _reask_pending_question explicitly handles the follow-up and returns True.
    awaiting_simple_consent = state == "WELCOME" and sess.get("_greet_step") == "await_continue"
    deferral_like = state == "WELCOME" and _detect_deferral(text)
    # Skip FAQ intercept inside QA_WINDOW so QA_WINDOW owns all Q&A behavior.
    if (
        text != "__kick__"
        and state != "QA_WINDOW"
        and not deferral_like
        and not resume_intent
        and _is_question(text)
        and state not in {"WELCOME", "PEEK_CHOICE", "PEEK_NEEDS_OFFER", "ELIGIBILITY"}
        and not sess.get("_state_handled_question")
        and not is_mixed_qna
    ):
        # If we're in commitment (ELIGIBILITY_PART2) and the question is about "same day 2 hours",
        # skip FAQ so the commitment handler can respond with the correct policy clarification.
        same_day_commitment = (
            state == "ELIGIBILITY_PART2" and re.search(
                r"\b(2\s*hours?|two\s*hours?)\b.*\b(same\s*day|same-day|today)\b|\b(same\s*day|same-day|today)\b.*\b(2\s*hours?|two\s*hours?)\b",
                text, re.I
            )
        )
        if not same_day_commitment:
            answered = await _maybe_answer_global_faq(text)
            if answered:
                return
            await _handle_offtopic_redirect()
            return

    # Unified parse hook: opportunistic fast-forward (skip for trivial rule hits)
    parsed = {}
    should_skip_parse = False
    if text != "__kick__":
        # Skip parser for trivial yes/no in GREET and ELIGIBILITY states to save cost/latency
        if state in ["WELCOME", "ELIGIBILITY_PART1", "ELIGIBILITY_PART2"]:
            if is_yes_response(text) or is_no_response(text):
                should_skip_parse = True
        if not should_skip_parse:
            parsed = await mcp_onboarding_parse(text_for_state, state=state)
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
    
    # ========== WELCOME STATE ==========
    # State 1: First outbound message - template message (required by Meta), then welcome text
    if state == "WELCOME":
        if text == "__kick__" or not sess.get("_greet_sent"):
            log.info(f"[GREET] Sending welcome message to {phone}")

            # Step 1: Send template message first (required by Meta for first outbound)
            template_sent = sess.get("_template_sent", False)
            from .config import settings
            if not template_sent:
                try:
                    template_name = settings.WHATSAPP_WELCOME_TEMPLATE_NAME
                    language_code = settings.WHATSAPP_TEMPLATE_LANGUAGE_CODE
                    
                    log.info(f"[GREET] Sending template message '{template_name}' to {phone}")
                    template_msg_id = await mcp_wa_send_template(phone, template_name, language_code)
                    
                    # Mark template as sent
                    sess["_template_sent"] = True
                    
                    # Persistence: record template sent
                    try:
                        from datetime import datetime, timezone
                        from storage.db import get_db_session
                        from storage.session_store import update_session_state_and_tool_state
                        from storage.event_logger import log_event
                        
                        now_iso = datetime.now(timezone.utc).isoformat()
                        with get_db_session() as db:
                            session_id = sess.get("_db_session_id")
                            update_session_state_and_tool_state(
                                db=db,
                                wa_phone=phone,
                                state="ONBOARDING",
                                sub_state="WELCOME",
                                last_outbound_msg_id=template_msg_id,
                                tool_state_updates={
                                    "welcome": {
                                        "template_sent_at": now_iso,
                                        "template_name": template_name
                                    }
                                },
                            )
                            log_event(
                                db=db,
                                wa_phone=phone,
                                agent_name=settings.AGENT_NAME,
                                event_type="WELCOME_TEMPLATE_SENT",
                                event_source="agent",
                                state="ONBOARDING",
                                sub_state="WELCOME",
                                status="SUCCESS",
                                details={"template_name": template_name, "language_code": language_code},
                                session_id=session_id
                            )
                    except Exception as e:
                        log.warning(f"[GREET] Failed to persist template: {e}", exc_info=True)
                    
                    # Small delay to ensure template is delivered before sending text messages
                    await asyncio.sleep(1.0)
                    
                except Exception as e:
                    log.error(f"[GREET] Failed to send template message to {phone}: {e}", exc_info=True)
                    # Don't block - continue with text messages (but this may fail if Meta rejects)
            
            # Step 2: Send the welcome intro message (now that 24-hour session is open)
            intro_msg = WELCOME_INTRO
            intro_msg_id = await mcp_wa_send(phone, intro_msg)
            _add_to_history(phone, bot_msg=intro_msg)

            # Step 3: Immediately send instructions message
            instructions_msg = WELCOME_INSTRUCTIONS
            instructions_msg_id = await mcp_wa_send(phone, instructions_msg, buttons=WELCOME_START_BUTTONS)
            _add_to_history(phone, bot_msg=instructions_msg)

            # Persistence: record welcome text messages in sessions + events
            try:
                from datetime import datetime, timezone
                from storage.db import get_db_session
                from storage.session_store import update_session_state_and_tool_state
                from storage.event_logger import log_event
                from .config import settings

                now_iso = datetime.now(timezone.utc).isoformat()
                last_msg_id = instructions_msg_id or intro_msg_id
                with get_db_session() as db:
                    session_id = sess.get("_db_session_id")
                    
                    # Read existing welcome from tool_state to preserve template info
                    from sqlalchemy import select
                    from storage.tables import serve_agent_sessions
                    stmt = select(serve_agent_sessions.c.tool_state).where(
                        serve_agent_sessions.c.wa_phone == phone
                    )
                    result = db.execute(stmt).first()
                    existing_welcome = {}
                    if result and result[0] and isinstance(result[0], dict):
                        existing_welcome = result[0].get("welcome", {})
                    
                    welcome_update = existing_welcome.copy()
                    welcome_update.update({
                        "sent_at": now_iso,
                    })
                    
                    update_session_state_and_tool_state(
                        db=db,
                        wa_phone=phone,
                        state="ONBOARDING",
                        sub_state="WELCOME",
                        last_outbound_msg_id=last_msg_id,
                        tool_state_updates={
                            "welcome": welcome_update
                        },
                    )
                    log_event(
                        db=db,
                        wa_phone=phone,
                        agent_name=settings.AGENT_NAME,
                        event_type="WELCOME_PROMPT_SENT",
                        event_source="agent",
                        state="ONBOARDING",
                        sub_state="WELCOME",
                        status="SUCCESS",
                        details={"messages": ["WELCOME_INTRO", "WELCOME_INSTRUCTIONS"]},
                        session_id=session_id,
                    )
            except Exception as e:
                log.warning(f"[WELCOME] Failed to persist welcome state: {e}", exc_info=True)

            sess["_greet_sent"] = True
            sess["sub_state"] = "WELCOME"
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        else:
            # Handle deferral early to avoid FAQ + reminder loops
            deferral_payload = ""
            deferral_title = ""
            if evt:
                data = evt.get("data") or {}
                deferral_payload = data.get("payload") or data.get("button_id") or data.get("button_payload") or ""
                deferral_title = data.get("title") or data.get("text") or data.get("button_text") or ""
            deferral_text = f"{text} {deferral_payload} {deferral_title}".lower().strip()
            deferral_text_norm = re.sub(r"[^a-z0-9]+", " ", deferral_text).strip()
            if (
                is_defer_response(text)
                or _detect_deferral(text)
                or (isinstance(deferral_payload, str) and deferral_payload.lower() in {"ill_do_this_later", "later", "defer", "do_later"})
                or "do this later" in deferral_text_norm
                or "ill do this later" in deferral_text_norm
            ):
                await mcp_wa_send(phone, GENERIC_DEFERRED_MSG)
                _add_to_history(phone, bot_msg=GENERIC_DEFERRED_MSG)
                sess["_deferred_prev_state"] = "WELCOME"
                sess["_deferred_reason"] = "WELCOME_LATER"
                sess["state"] = "DEFERRED"
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
            
            # Handle welcome start button text robustly (strip punctuation/emojis)
            normalized_text = re.sub(r"[^a-z0-9]+", " ", text_lower_global).strip()
            if normalized_text in {"lets start", "let s start", "start"}:
                log.info(f"[GREET] Start detected, transitioning to WELCOME_VIDEO")
                sess["state"] = "WELCOME_VIDEO"
                sess["sub_state"] = "WELCOME_VIDEO"
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                await _handle(phone, "__kick__")
                return

            # Welcome FAQ handling: answer common questions and keep in WELCOME
            if _is_question(text) and not sess.get("_state_handled_question"):
                welcome_faq = [
                    (r"\b(what is this|what is serve|what is this about|about serve)\b", "This is SERVE’s volunteer onboarding on WhatsApp — I’ll guide you step by step."),
                    (r"\b(how long|how much time|how long will this take)\b", "About 5–10 minutes if you’re ready now."),
                    (r"\b(what will you ask|what do i need to do|what is the process)\b", "Just a few basics — your interest, availability, and contact details."),
                    (r"\b(is this paid|paid role|payment|stipend)\b", "It’s a volunteer role with no payment."),
                    (r"\b(laptop|tablet|device|phone)\b", "You’ll need a tablet or laptop with stable internet for classes."),
                ]
                answered = False
                for pattern, answer in welcome_faq:
                    if re.search(pattern, text_lower_global):
                        await mcp_wa_send(phone, answer)
                        _add_to_history(phone, bot_msg=answer)
                        sess["_state_handled_question"] = True
                        answered = True
                        break
                
                if not answered:
                    answered = await _maybe_answer_global_faq(text)
                    if not answered:
                        await _handle_offtopic_redirect()
                        return
                return
            
            # Acknowledge non-question statements and re-ask to begin
            if (
                text
                and not _is_question(text)
                and not is_yes_response(text)
                and not is_no_response(text)
                and not is_defer_response(text)
                and not _detect_deferral(text)
            ):
                await mcp_wa_send(phone, WELCOME_STATEMENT_ACK)
                _add_to_history(phone, bot_msg=WELCOME_STATEMENT_ACK)
                await mcp_wa_send(phone, WELCOME_FAQ_FOLLOWUP, buttons=WELCOME_START_BUTTONS)
                _add_to_history(phone, bot_msg=WELCOME_FAQ_FOLLOWUP)
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
            
            # Otherwise, start with welcome video
            log.info(f"[GREET] User responded after welcome message, transitioning to WELCOME_VIDEO")
            sess["state"] = "WELCOME_VIDEO"
            sess["sub_state"] = "WELCOME_VIDEO"
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            await _handle(phone, "__kick__")
            return
    
    # ========== PEEK_CHOICE STATE (Video/Requirements choice) ==========
    if state == "PEEK_CHOICE":
        if text == "__kick__" or not sess.get("_peek_video_prompted"):
            await mcp_wa_send(phone, PEEK_VIDEO_PROMPT)
            _add_to_history(phone, bot_msg=PEEK_VIDEO_PROMPT)
            sess["_peek_video_prompted"] = True
            sess["_peek_stage"] = "VIDEO"
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        
        stage = sess.get("_peek_stage") or "VIDEO"
        text_lower = text.lower().strip()
        if text_lower in {"maybe", "maybe later"}:
            action = "SKIP"
            tone_reply = ""
            sess["_peek_soft_deferral"] = True
        elif text_lower in {"no", "nope", "nah", "no thanks", "not now", "skip"}:
            action = "SKIP"
            tone_reply = ""
        else:
            try:
                plan = await _peek_planner_llm(text, stage=stage)
                action = (plan.get("action") or "").upper()
                tone_reply = (plan.get("tone_reply") or "").strip()
            except Exception as e:
                log.warning(f"[PEEK_CHOICE] Planner failed: {e}")
                action = ""
                tone_reply = ""
        
        if tone_reply and action != "SKIP":
            await mcp_wa_send(phone, tone_reply)
            _add_to_history(phone, bot_msg=tone_reply)
        
        if stage == "VIDEO":
            if action == "SHOW_VIDEO":
                sess["_video_next_state"] = "PEEK_NEEDS_OFFER"
                sess["state"] = "VIDEO"
                sess["sub_state"] = "VIDEO"
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                await _handle(phone, "__kick__")
                return
            if action == "SKIP":
                if not sess.get("_peek_skip_message_sent"):
                    skip_msg = PEEK_MAYBE_MESSAGE if sess.get("_peek_soft_deferral") else PEEK_SKIP_MESSAGE
                    await mcp_wa_send(phone, skip_msg)
                    _add_to_history(phone, bot_msg=skip_msg)
                    sess["_peek_skip_message_sent"] = True
                sess.pop("_peek_soft_deferral", None)
                sess["state"] = "PEEK_NEEDS_OFFER"
                sess["sub_state"] = "PEEK_NEEDS_OFFER"
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                await _handle(phone, "__kick__")
                return
            # CLARIFY or unknown
            await mcp_wa_send(phone, PEEK_VIDEO_PROMPT)
            _add_to_history(phone, bot_msg=PEEK_VIDEO_PROMPT)
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return

        # Fallback: if stage not recognized, re-ask
        await mcp_wa_send(phone, PEEK_VIDEO_PROMPT)
        _add_to_history(phone, bot_msg=PEEK_VIDEO_PROMPT)
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return

    # ========== PEEK_NEEDS_OFFER STATE (Optional requirements preview) ==========
    if state == "PEEK_NEEDS_OFFER":
        if text == "__kick__" or not sess.get("_peek_needs_prompted"):
            await mcp_wa_send(phone, PEEK_NEEDS_PROMPT)
            _add_to_history(phone, bot_msg=PEEK_NEEDS_PROMPT)
            sess["_peek_needs_prompted"] = True
            sess["_peek_stage"] = "NEEDS"
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        
        text_lower = text.lower().strip()
        if text_lower in {"maybe", "maybe later"}:
            action = "SKIP"
            tone_reply = ""
            sess["_peek_soft_deferral"] = True
        elif text_lower in {"no", "nope", "nah", "no thanks", "not now", "skip"}:
            action = "SKIP"
            tone_reply = ""
        else:
            try:
                plan = await _peek_planner_llm(text, stage="NEEDS")
                action = (plan.get("action") or "").upper()
                tone_reply = (plan.get("tone_reply") or "").strip()
            except Exception as e:
                log.warning(f"[PEEK_NEEDS_OFFER] Planner failed: {e}")
                action = ""
                tone_reply = ""
        
        if tone_reply and action != "SKIP":
            await mcp_wa_send(phone, tone_reply)
            _add_to_history(phone, bot_msg=tone_reply)
        
        if action == "SHOW_NEEDS":
            sess["_needs_preview_next_state"] = "ELIGIBILITY"
            sess["_needs_preview_note"] = PEEK_REQUIREMENTS_NOTE
            sess["state"] = "NEEDS_PREVIEW"
            sess["sub_state"] = "NEEDS_PREVIEW"
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            await _handle(phone, "__kick__")
            return
        
        if action == "SKIP":
            if not sess.get("_peek_skip_message_sent"):
                skip_msg = PEEK_MAYBE_MESSAGE if sess.get("_peek_soft_deferral") else PEEK_SKIP_MESSAGE
                await mcp_wa_send(phone, skip_msg)
                _add_to_history(phone, bot_msg=skip_msg)
                sess["_peek_skip_message_sent"] = True
            sess.pop("_peek_soft_deferral", None)
            sess["state"] = "ELIGIBILITY"
            sess["sub_state"] = "ELIGIBILITY"
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            await _handle(phone, "__kick__")
            return
        
        # CLARIFY or unknown
        await mcp_wa_send(phone, PEEK_NEEDS_PROMPT)
        _add_to_history(phone, bot_msg=PEEK_NEEDS_PROMPT)
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
    
    # (PEEK_VIDEO_OFFER state removed; handled by micro-planner in PEEK_CHOICE)
    
    # ========== INTENT STATE (State 2: Purpose Acknowledgement) ==========
    if state == "INTENT":
        await handle_intent(phone, text_for_state, sess, profile)
        return

    # ========== WELCOME_VIDEO STATE (Quick hello video) ==========
    if state == "WELCOME_VIDEO":
        from .messages import QA_STOP_ACK, WELCOME_MAYBE_LATER
        text_lower = text.lower().strip()
        
        if text == "__kick__" or not sess.get("_welcome_video_sent"):
            await mcp_wa_send(phone, WELCOME_VIDEO_INTRO)
            _add_to_history(phone, bot_msg=WELCOME_VIDEO_INTRO)
            # Send mp4 using existing class video tool
            await mcp_wa_send_welcome_video(phone)
            _add_to_history(phone, bot_msg="[VIDEO]")
            # Short delay to reduce footer arriving before media
            await asyncio.sleep(2.0)
            await mcp_wa_send(phone, WELCOME_VIDEO_FOOTER)
            _add_to_history(phone, bot_msg=WELCOME_VIDEO_FOOTER)
            sess["_welcome_video_sent"] = True
            sess["_welcome_video_response_received"] = False
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        
        if re.search(r"\b(stop|unsubscribe|leave|quit|exit|end)\b", text_lower):
            await mcp_wa_send(phone, QA_STOP_ACK)
            _add_to_history(phone, bot_msg=QA_STOP_ACK)
            sess["state"] = "OPTOUT"
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        
        # Handle deferral / clear decline
        if (
            is_no_response(text)
            or is_defer_response(text)
            or _detect_deferral(text)
            or re.search(r"\b(not interested|don'?t want|do not want|dont want)\b", text_lower)
        ):
            await mcp_wa_send(phone, WELCOME_MAYBE_LATER)
            _add_to_history(phone, bot_msg=WELCOME_MAYBE_LATER)
            sess["_deferred_prev_state"] = "WELCOME_VIDEO"
            sess["_deferred_reason"] = "WELCOME_LATER"
            sess["state"] = "DEFERRED"
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        
        # If user asks a question, answer then move on (no footer re-ask)
        if _is_question(text):
            answered = await _maybe_answer_global_faq(text)
            if not answered and "video" in text_lower:
                about_msg = "It’s a quick hello from our team and a peek into real classrooms."
                await mcp_wa_send(phone, about_msg)
                _add_to_history(phone, bot_msg=about_msg)
                sess["_state_handled_question"] = True
            if not answered and not sess.get("_state_handled_question"):
                fallback = "Thanks for asking — I’ll share more as we go."
                await mcp_wa_send(phone, fallback)
                _add_to_history(phone, bot_msg=fallback)
                sess["_state_handled_question"] = True
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            # Continue to intent after answering
            thanks_intent_msg = f"{WELCOME_VIDEO_CONTINUE} {INTENT_PROMPT}"
            await mcp_wa_send(phone, thanks_intent_msg)
            _add_to_history(phone, bot_msg=thanks_intent_msg)
            sess["_intent_prompted"] = True
            sess["state"] = "INTENT"
            sess["sub_state"] = "INTENT"
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        
        done_keywords = ["done", "watched", "viewed", "finished", "completed", "ok", "okay", "yes", "sure", "y", "ready"]
        appreciation_keywords = ["wow", "nice", "great", "awesome", "love", "loved", "thanks", "thank you", "cool", "amazing"]
        if any(k in text_lower for k in done_keywords + appreciation_keywords):
            if any(k in text_lower for k in appreciation_keywords):
                await mcp_wa_send(phone, "Glad you liked it!")
                _add_to_history(phone, bot_msg="Glad you liked it!")
            sess["_welcome_video_response_received"] = True
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            # Send thank-you + intent question, then move to INTENT (prompt already sent)
            thanks_intent_msg = f"Thank you for watching! {INTENT_PROMPT}"
            await mcp_wa_send(phone, thanks_intent_msg)
            _add_to_history(phone, bot_msg=thanks_intent_msg)
            sess["_intent_prompted"] = True
            sess["state"] = "INTENT"
            sess["sub_state"] = "INTENT"
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        
        # Ambiguous response: use LLM to decide; default to continue
        intent_detected = None
        try:
            llm_context = build_llm_context("WELCOME", sess, last_prompt=WELCOME_VIDEO_FOOTER)
            llm_result = await mcp_llm_classify_intent(text, "WELCOME", llm_context)
            llm_intent = (llm_result.get("intent") or "").upper()
            llm_conf = float(llm_result.get("confidence") or 0.0)
            if llm_conf >= 0.7:
                intent_detected = llm_intent
            elif llm_intent == "DEFERRAL" and llm_conf >= 0.3:
                intent_detected = "DEFERRAL"
            else:
                intent_detected = "AMBIGUOUS"
        except Exception as e:
            log.warning(f"[WELCOME_VIDEO] LLM classification failed: {e}")
            intent_detected = "AMBIGUOUS"
        
        if intent_detected in {"STOP"}:
            await mcp_wa_send(phone, QA_STOP_ACK)
            _add_to_history(phone, bot_msg=QA_STOP_ACK)
            sess["state"] = "OPTOUT"
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        if intent_detected in {"DEFERRAL", "CONSENT_NO"}:
            await mcp_wa_send(phone, WELCOME_MAYBE_LATER)
            _add_to_history(phone, bot_msg=WELCOME_MAYBE_LATER)
            sess["_deferred_prev_state"] = "WELCOME_VIDEO"
            sess["_deferred_reason"] = "WELCOME_LATER"
            sess["state"] = "DEFERRED"
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        
        # Default: continue
        thanks_intent_msg = f"{WELCOME_VIDEO_CONTINUE} {INTENT_PROMPT}"
        await mcp_wa_send(phone, thanks_intent_msg)
        _add_to_history(phone, bot_msg=thanks_intent_msg)
        sess["_intent_prompted"] = True
        sess["state"] = "INTENT"
        sess["sub_state"] = "INTENT"
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
    
    # ========== VIDEO STATE (State 2.5: Show class preview video) ==========
    if state == "VIDEO":
        await handle_video(phone, text_for_state, sess, profile)
        updated_sess = SESSIONS.get(phone, sess)
        if _is_question(text) and not updated_sess.get("_state_handled_question"):
            answered = await _maybe_answer_global_faq(text)
            if not answered:
                await _handle_offtopic_redirect()
        return
    
    # ========== NEEDS_PREVIEW STATE (State 2.7: Show needs preview) ==========
    if state == "NEEDS_PREVIEW":
        await handle_needs_preview(phone, text_for_state, sess, profile)
        return
    
    # ========== CONTINUE_CONFIRM STATE (State 2.8: Confirm continuation with time expectation) ==========
    if state == "CONTINUE_CONFIRM":
        await handle_continue_confirm(phone, text_for_state, sess, profile)
        return
    
    # ========== ELIGIBILITY STATE (State 3: Eligibility Check) ==========
    if state == "ELIGIBILITY":
        await handle_eligibility(phone, text_for_state, sess, profile)
        updated_sess = SESSIONS.get(phone, sess)
        if _is_question(text) and not updated_sess.get("_state_handled_question"):
            answered = await _maybe_answer_global_faq(text)
            if not answered:
                await _handle_offtopic_redirect()
        return
    
    # ========== IDENTITY STATE (State 4: Name, Phone, Email Collection) ==========
    if state == "IDENTITY":
        try:
            log.info(f"[HANDLE] Calling handle_identity for {phone}, text='{text[:50]}...'")
            await handle_identity(phone, text_for_state, sess, profile)
            log.info(f"[HANDLE] handle_identity completed for {phone}")
        except Exception as e:
            log.error(f"[HANDLE] Error in handle_identity for {phone}: {e}", exc_info=True)
            await mcp_wa_send(phone, "Sorry, there was an error processing your message. Please try again.")
        return
    
    # ========== PREFERENCES STATE (State 5: Day & Time Preferences) ==========
    if state == "PREFERENCES":
        log.info(f"[HANDLE] Calling handle_preferences for {phone}, text='{text[:50]}...', "
                f"state={sess.get('state')}, _prefs_confirmed={sess.get('_prefs_confirmed')}")
        await handle_preferences(phone, text_for_state, sess, profile)
        log.info(f"[HANDLE] handle_preferences returned for {phone}, new_state={sess.get('state')}")
        return
    
    # ========== QA_WINDOW STATE (State 6: Questions & Answers) ==========
    if state == "QA_WINDOW":
        await handle_qa_window(phone, text_for_state, sess, profile)
        return

    # ========== FEEDBACK STATE ==========
    if state == "FEEDBACK":
        await handle_feedback(phone, text_for_state, sess, profile)
        return
    
    # ========== REJECTED STATE (Eligibility Not Met) ==========
    if state == "REJECTED":
        # User was rejected but can re-enter by sending a message
        # Reset to ELIGIBILITY state and re-show prompt
        log.info(f"[REJECTED] User messaging after rejection, resetting to ELIGIBILITY")
        sess["state"] = "ELIGIBILITY"
        sess["_eligibility_prompted"] = False
        sess["_eligibility_clarification_sent"] = False
        sess["_eligibility_missing_req"] = None
        sess["_eligibility_clarification_step"] = None
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        # Trigger ELIGIBILITY handler
        await _handle(phone, "__kick__")
        return
    
    # ========== COMPLETE STATE (Final State) ==========
    if state == "COMPLETE":
        # Final state - just acknowledge any messages
        if text == "__kick__":
            # Send completion message if not already sent
            if not sess.get("_complete_message_sent"):
                # Persistence: Finalize onboarding (checkpoint 3)
                try:
                    from storage.db import get_db_session
                    from storage.session_store import finalize_onboarding
                    from storage.event_logger import log_event
                    from .config import settings
                    
                    # Determine eligibility status
                    # Default to ELIGIBLE if passed is True or not set (assume passed if reached COMPLETE)
                    eligibility_passed = profile.get("eligibility", {}).get("passed")
                    if sess.get("state") == "REJECTED":
                        eligibility_status = "REJECTED"
                    elif eligibility_passed is False:
                        # Explicitly set to False means rejected
                        eligibility_status = "REJECTED"
                    else:
                        # passed is True or None/not set - if we reached COMPLETE, assume ELIGIBLE
                        eligibility_status = "ELIGIBLE"
                    
                    # Get preferences
                    prefs = profile.get("preferences", {})
                    available_days = prefs.get("days", [])
                    available_time_bands = [prefs.get("time_band")] if prefs.get("time_band") else None
                    
                    with get_db_session() as db:
                        finalize_onboarding(
                            db,
                            wa_phone=phone,
                            eligibility_status=eligibility_status,
                            available_days=available_days if available_days else None,
                            available_time_bands=available_time_bands,
                            end_reason="completed"
                        )
                        # Log SESSION_ENDED event
                        session_id = sess.get("_db_session_id")
                        log_event(
                            db=db,
                            wa_phone=phone,
                            agent_name=settings.AGENT_NAME,
                            event_type="SESSION_ENDED",
                            event_source="onboarding_agent",
                            state="COMPLETE",
                            status="completed",
                            session_id=session_id
                        )
                        log.info(f"[PERSISTENCE] Finalized onboarding for {phone}")
                except Exception as e:
                    log.warning(f"[PERSISTENCE] Failed to finalize onboarding for {phone}: {e}", exc_info=True)
                    # Continue without DB - don't block flow
                
                # Get volunteer name from profile
                name = profile.get("name") or "there"
                # Combine "Let's continue" message with Selection intro
                combined_msg = (
                    f"Here's a quick note from our team, {name} - we're in the last stretch now 🙂"
                )
                await mcp_wa_send(phone, combined_msg)
                _add_to_history(phone, bot_msg=combined_msg)
                sess["_complete_message_sent"] = True
                # Mark that selection intro was already sent
                sess["_selection_intro_sent"] = True
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                
                # Transition to Selection Agent
                try:
                    from agents.selection.handler import handle_selection
                    sess["state"] = "SEL_START"
                    sess["agent"] = "selection"
                    sess["ts"] = time.time()
                    SESSIONS[phone] = sess
                    await handle_selection(phone, "__kick__", sess)
                    return
                except Exception as e:
                    log.error(f"[ONBOARDING] Failed to transition to Selection Agent: {e}", exc_info=True)
                    # Continue with existing flow if transition fails
            return
        else:
            # User sent a message after completion - just acknowledge
            name = profile.get("name") or "there"
            ack = f"Thanks, {name}! I'm here if you need anything. 💛"
            await mcp_wa_send(phone, ack)
            _add_to_history(phone, bot_msg=ack)
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
    
    # Old WELCOME handler code (kept for reference, but should not be reached)
    if False and state == "WELCOME":
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
                    overview_msg = WELCOME_SERVE_OVERVIEW
                    await mcp_wa_send(phone, overview_msg)
                    _add_to_history(phone, bot_msg=overview_msg)
                    await asyncio.sleep(1.0)
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
                    try:
                        cobj = (parsed.get("consent") or {}) if parsed else {}
                        cval = (cobj.get("value") or "").lower()
                        if cval in ["yes", "agreed", "okay", "sure"]:
                            proceed = True
                    except Exception:
                        pass

                    if proceed:
                        overview_msg = WELCOME_SERVE_OVERVIEW
                        await mcp_wa_send(phone, overview_msg)
                        _add_to_history(phone, bot_msg=overview_msg)
                        await asyncio.sleep(1.0)
                        sess["_greet_step"] = "shared_info"
                        sess["ts"] = time.time(); SESSIONS[phone] = sess
                        return
                    # Fall through to comprehensive intent handling below for deferrals, queries, etc.

            # User replied to consent question - comprehensive intent handling
            text_lower = text.lower().strip()
            volunteer_id = profile.get("uuid") or phone
            intent_detected = None
            llm_called = False
            llm_result = None
            
            # 1) DEFERRAL (check first to avoid "not sure"/"later" being treated as consent)
            if _detect_deferral(text):
                intent_detected = "DEFERRAL"
            # 2) CONSENT_YES
            else:
                # 2) Check simple consent heuristics
                if _detect_consent_yes(text) or is_yes_response(text):
                    intent_detected = "CONSENT_YES"
                elif _detect_consent_no(text) or is_no_response(text):
                    intent_detected = "CONSENT_NO"
                # 3) High-confidence parser consent
                # 4) STOP / OPT-OUT
                if intent_detected is None and _detect_stop(text):
                    intent_detected = "STOP"
                # 5) DEFERRAL
                elif intent_detected is None and _detect_deferral(text):
                    intent_detected = "DEFERRAL"
                # 6) RETURNING
                elif intent_detected is None and _detect_returning(text):
                    intent_detected = "RETURNING"
                # 7) QUERY (FAQ)
                elif intent_detected is None and _detect_query(text):
                    intent_detected = "QUERY"

            # LLM fallback only if still unknown
            if intent_detected is None:
                try:
                    log.info(f"[GREET] Calling LLM fallback for intent classification")
                    llm_context = build_llm_context("WELCOME", sess, last_prompt=WELCOME_SERVE_OVERVIEW)
                    llm_result = await mcp_llm_classify_intent(text, "WELCOME", llm_context)
                    llm_intent = (llm_result.get("intent") or "").upper()
                    llm_conf = float(llm_result.get("confidence") or 0.0)

                    if llm_conf >= 0.7:
                        intent_detected = llm_intent
                        llm_called = True
                        log.info(f"[GREET] LLM classified intent: {intent_detected} (confidence: {llm_conf})")
                    elif llm_intent == "DEFERRAL" and llm_conf >= 0.3:
                        intent_detected = "DEFERRAL"
                        llm_called = True
                    else:
                        log.info(f"[GREET] LLM confidence ({llm_conf}) too low, treating as AMBIGUOUS")
                        intent_detected = "AMBIGUOUS"
                        llm_called = True
                except Exception as e:
                    log.warning(f"[GREET] LLM classification failed: {e}")
                    intent_detected = "AMBIGUOUS"
            
            # Generate idempotency key for this turn
            idempotency_key = f"{volunteer_id}_{intent_detected}_{int(time.time())}"
            
            # Route based on detected intent
            if intent_detected == "CONSENT_YES":
                # Record consent and advance state
                try:
                    await mcp_consent_record(volunteer_id, True)
                except Exception as e:
                    log.warning(f"[GREET] Failed to record consent: {e}")
                
                # Move to eligibility and send question immediately
                sess["state"] = "ELIGIBILITY_PART1"
                sess["_eligibility_step"] = "age"  # Start with age question
                sess["_eligibility_age_asked"] = False  # Will be set when question is sent
                sess["_eligibility_device_asked"] = False
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                log.info(f"[GREET] Consent recorded, moving to ELIGIBILITY_PART1")
                
                # Send acknowledgment
                ack_msg = WELCOME_CONSENT_ACK
                await mcp_wa_send(phone, ack_msg)
                _add_to_history(phone, bot_msg=ack_msg)
                await asyncio.sleep(1.0)  # Small pause
                
                # Send first eligibility question (age only)
                age_msg = ELIGIBILITY_AGE_PROMPT
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
                    sess["_deferred_prev_state"] = state
                    sess["_deferred_reason"] = "WELCOME_USER_LATER"
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
                    sess["_deferred_prev_state"] = state
                    sess["_deferred_reason"] = "WELCOME_USER_LATER"
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
    
    # ============================================================================
    # NOTE: All states below (ELIGIBILITY_PART1 onwards) are COMMENTED OUT
    # They will be moved to separate files in states/ directory as we work on them.
    # State 1 (WELCOME) remains active in this file.
    # ============================================================================
    
    # ========== ELIGIBILITY (PART 1: age, then device) ==========
    # COMMENTED OUT - Will be moved to states/eligibility_part1.py
    elif False and state == "ELIGIBILITY_PART1":  # Disabled - will be in separate file
        # Track which question we're on: "age" or "device"
        elig_step = sess.get("_eligibility_step", "age")
        volunteer_id = profile.get("uuid") or phone
        
        # Q1 - Age check (first question)
        if elig_step == "age":
            if not sess.get("_eligibility_age_asked"):
                # First time: ask age question
                log.info(f"[ELIG] Sending age question to {phone}")
                msg = ELIGIBILITY_AGE_PROMPT
                await mcp_wa_send(phone, msg)
                _add_to_history(phone, bot_msg=msg)
                sess["_eligibility_age_asked"] = True
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
            else:
                # User replied to age question
                age_ok = None
                age_value = None
                # Primary source: onboarding.parse_message (LLM extraction)
                try:
                    hints = parsed.get("eligibility") or {} if parsed else {}
                    age_ok = hints.get("age_ok")
                    age_value = hints.get("age")
                    if age_ok is not None or age_value is not None:
                        log.info(f"[ELIG] LLM extracted age: ok={age_ok}, value={age_value}")
                        if age_value is not None and age_ok is None:
                            age_ok = age_value >= 18
                except Exception as e:
                    log.warning(f"[ELIG] Failed to parse age from LLM: {e}")

                # Fallback: Simple yes/no for trivial responses only
                if age_ok is None:
                    if is_yes_response(text):
                        age_ok = True
                        log.info(f"[ELIG] Simple yes detected for age")
                    elif is_no_response(text):
                        age_ok = False
                        log.info(f"[ELIG] Simple no detected for age")

                # LLM fallback classifier if still unclear
                if age_ok is None:
                    try:
                        llm_context = build_llm_context(
                            "ELIGIBILITY_PART1",
                            sess,
                            last_prompt=ELIGIBILITY_AGE_PROMPT,
                        )
                        llm_result = await mcp_llm_classify_intent(text, "ELIGIBILITY_PART1", llm_context)
                        llm_intent = (llm_result.get("intent") or "").upper()
                        if llm_intent == "AGE_OK":
                            age_ok = True
                        elif llm_intent == "AGE_UNDER":
                            age_ok = False
                        elif llm_intent in {"AGE_UNCLEAR", "AMBIGUOUS", "QUERY"}:
                            unclear_msg = ELIGIBILITY_AGE_UNCLEAR
                            await mcp_wa_send(phone, unclear_msg)
                            _add_to_history(phone, bot_msg=unclear_msg)
                            sess["ts"] = time.time()
                            SESSIONS[phone] = sess
                            return
                    except Exception as e:
                        log.warning(f"[ELIG] Age LLM fallback failed: {e}")

                # Handle unclear responses
                if age_ok is None:
                    unclear_msg = ELIGIBILITY_AGE_UNCLEAR
                    await mcp_wa_send(phone, unclear_msg)
                    _add_to_history(phone, bot_msg=unclear_msg)
                    sess["ts"] = time.time()
                    SESSIONS[phone] = sess
                    return

                # HARD RULE: Age < 18 → immediate decline (no persuasion)
                if age_ok is False or (age_value is not None and age_value < 18):
                    decline_msg = format_message(ELIGIBILITY_UNDERAGE_DECLINE)
                    await mcp_wa_send(phone, decline_msg)
                    _add_to_history(phone, bot_msg=decline_msg)
                    sess["state"] = "REJECTED"
                    sess["ts"] = time.time()
                    SESSIONS[phone] = sess
                    try:
                        await mcp_telemetry_emit("onboarding.age_decline", {
                            "conversation_id": phone,
                            "user_id": volunteer_id,
                            "age_value": age_value
                        })
                    except Exception:
                        pass
                    return

                sess["elig.age"] = True
                sess["elig.age_value"] = age_value if age_value else 18
                profile_elig = profile.setdefault("eligibility", {})
                profile_elig["q2_age"] = True
                if age_value is not None:
                    profile_elig["age_years"] = age_value
                sess["_eligibility_step"] = "device"
                sess["_eligibility_device_asked"] = True
                sess["ts"] = time.time()
                SESSIONS[phone] = sess

                name = profile.get("name") or "Volunteer"
                ack_line = format_message(ELIGIBILITY_AGE_ACK, name=name).strip()
                device_prompt = format_message(ELIGIBILITY_DEVICE_PROMPT, name=name).strip()
                transition_msg = f"{ack_line}\n\n{device_prompt}"

                await asyncio.sleep(0.5)
                await mcp_wa_send(phone, transition_msg)
                _add_to_history(phone, bot_msg=transition_msg)
                return

        # Q2 - Device check (second question)
        elif elig_step == "device":
            if not sess.get("_eligibility_device_asked"):
                device_msg = ELIGIBILITY_DEVICE_PROMPT
                await mcp_wa_send(phone, device_msg)
                _add_to_history(phone, bot_msg=device_msg)
                sess["_eligibility_device_asked"] = True
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
            else:
                has_device = None
                llm_suggests_deferral = False

                try:
                    hints = parsed.get("eligibility") or {} if parsed else {}
                    has_device = hints.get("has_device") or hints.get("device_ok")
                    if has_device is not None:
                        log.info(f"[ELIG] LLM extracted device: {has_device}")
                except Exception as e:
                    log.warning(f"[ELIG] Failed to parse device from LLM: {e}")

                if has_device is None:
                    if is_yes_response(text):
                        has_device = True
                        log.info(f"[ELIG] Simple yes detected for device")
                    elif is_no_response(text):
                        has_device = False
                        log.info(f"[ELIG] Simple no detected for device")

                if has_device is None:
                    text_lower = text.lower()
                    negative_device_patterns = [
                        r"no\s+(proper|stable|good)\s+(net|network|internet|wifi)",
                        r"no\s+(internet|wifi|broadband)",
                        r"not\s+able\s+to\s+(join|connect)",
                        r"poor\s+(internet|network)",
                        r"bad\s+(internet|network)",
                        r"unstable\s+(internet|network|wifi)",
                    ]
                    if any(re.search(pat, text_lower) for pat in negative_device_patterns):
                        has_device = False
                        log.info("[ELIG] Detected unreliable internet phrasing; treating as no device")

                if has_device is None:
                    try:
                        llm_context = build_llm_context(
                            "ELIGIBILITY_PART1",
                            sess,
                            last_prompt=ELIGIBILITY_DEVICE_PROMPT,
                        )
                        llm_result = await mcp_llm_classify_intent(text, "ELIGIBILITY_PART1", llm_context)
                        llm_intent = (llm_result.get("intent") or "").upper()

                        if llm_intent == "DEVICE_OK":
                            has_device = True
                        elif llm_intent == "DEVICE_NO":
                            has_device = False
                        elif llm_intent == "DEFERRAL":
                            has_device = False
                            llm_suggests_deferral = True
                        elif llm_intent in {"DEVICE_UNCLEAR", "AMBIGUOUS", "QUERY"}:
                            followup_msg = format_message(ELIGIBILITY_DEVICE_CLARIFY, name=name)
                            await mcp_wa_send(phone, followup_msg)
                            _add_to_history(phone, bot_msg=followup_msg)
                            sess["ts"] = time.time()
                            SESSIONS[phone] = sess
                            return
                    except Exception as e:
                        log.warning(f"[ELIG] Device LLM fallback failed: {e}")

                if has_device is None:
                    followup_msg = format_message(ELIGIBILITY_DEVICE_CLARIFY, name=name)
                    await mcp_wa_send(phone, followup_msg)
                    _add_to_history(phone, bot_msg=followup_msg)
                    sess["ts"] = time.time()
                    SESSIONS[phone] = sess
                    return

                if has_device is False:
                    deferral_msg = format_message(ELIGIBILITY_DEVICE_DEFERRAL, name=name)
                    await mcp_wa_send(phone, deferral_msg)
                    _add_to_history(phone, bot_msg=deferral_msg)
                    sess["_eligibility_device_deferral_asked"] = True
                    sess["ts"] = time.time()
                    SESSIONS[phone] = sess

                    if llm_suggests_deferral:
                        sess["_eligibility_from_llm_deferral"] = True
                    return

                sess["elig.device"] = True
                profile_elig = profile.setdefault("eligibility", {})
                profile_elig["q3_device"] = True
                if has_device not in (None, True, False) and isinstance(has_device, str):
                    profile_elig["device_type"] = has_device

                sess["state"] = "ELIGIBILITY_PART2"
                sess["_eligibility_part2_sent"] = True
                sess["ts"] = time.time()
                SESSIONS[phone] = sess

                name = profile.get("name") or "Volunteer"
                ack_line = format_message(ELIGIBILITY_DEVICE_ACK, name=name).strip()
                commit_prompt = ELIGIBILITY_COMMIT_PROMPT.strip()
                transition_msg = f"{ack_line}\n\n{commit_prompt}"

                await asyncio.sleep(0.5)
                await mcp_wa_send(phone, transition_msg)
                _add_to_history(phone, bot_msg=transition_msg)
                return
    
    # ========== ELIGIBILITY (PART 2: commitment with persuasion) ==========
    # COMMENTED OUT - Will be moved to states/eligibility_part2.py
    elif False and state == "ELIGIBILITY_PART2":  # Disabled - will be in separate file
        volunteer_id = profile.get("uuid") or phone
        persuasion_attempts = sess.get("_commitment_persuasion_attempts", 0)
        
        if not sess.get("_eligibility_part2_sent"):
            # First time: send commitment question
            log.info(f"[ELIG] Sending commitment question to {phone}")
            msg = ELIGIBILITY_COMMIT_PROMPT
            await mcp_wa_send(phone, msg)
            _add_to_history(phone, bot_msg=msg)
            sess["_eligibility_part2_sent"] = True
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        else:
            # User replied to commitment question
            commit_hours = None
            commit_ok = None
            same_day_request = False
            llm_commit_intent = None

            try:
                hints = parsed.get("eligibility") or {} if parsed else {}
                if isinstance(hints.get("same_day_request"), bool):
                    same_day_request = hints.get("same_day_request")
            except Exception:
                pass

            if not same_day_request and re.search(r"\b(same\s*day|same-day|sameday|today)\b", text, re.I):
                same_day_request = True

            if same_day_request:
                clarify_policy = ELIGIBILITY_COMMIT_POLICY
                await mcp_wa_send(phone, clarify_policy)
                _add_to_history(phone, bot_msg=clarify_policy)
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return

            try:
                hints = parsed.get("eligibility") or {} if parsed else {}
                commit_hours_raw = hints.get("weekly_commitment_hours")
                if commit_hours_raw is not None:
                    commit_hours = float(commit_hours_raw)
                    commit_ok = commit_hours >= 2.0
                    log.info(f"[ELIG] LLM extracted commitment: {commit_hours} hours, ok={commit_ok}")
            except Exception as e:
                log.warning(f"[ELIG] Failed to parse commitment from LLM: {e}")

            if commit_hours is None:
                extracted_hours = _extract_simple_hours(text)
                if extracted_hours is not None:
                    commit_hours = extracted_hours
                    commit_ok = extracted_hours >= 2.0
                    log.info(f"[ELIG] Minimal fallback extracted: {commit_hours} hours, ok={commit_ok}")

            if commit_hours is None and commit_ok is None:
                text_lower = text.lower().strip()

                if is_yes_response(text):
                    commit_ok = True
                    commit_hours = 2.0
                    log.info(f"[ELIG] Simple yes detected, defaulting to 2.0 hours")
                elif is_no_response(text):
                    commit_ok = False
                    log.info(f"[ELIG] Simple no detected")

            llm_result = None
            if commit_hours is None and commit_ok is None:
                try:
                    llm_context = build_llm_context(
                        "ELIGIBILITY_PART2",
                        sess,
                        last_prompt=ELIGIBILITY_COMMIT_PROMPT,
                    )
                    llm_result = await mcp_llm_classify_intent(text, "ELIGIBILITY_PART2", llm_context)
                    llm_commit_intent = (llm_result.get("intent") or "").upper()
                except Exception as e:
                    log.warning(f"[ELIG] Commitment LLM fallback failed: {e}")

            if llm_commit_intent == "COMMIT_OK":
                commit_ok = True
                if commit_hours is None:
                    commit_hours = 2.0
            elif llm_commit_intent == "COMMIT_TOO_LOW":
                commit_ok = False
                if commit_hours is None:
                    commit_hours = 1.0
            elif llm_commit_intent == "COMMIT_SAME_DAY_ONLY":
                clarify_policy = ELIGIBILITY_COMMIT_POLICY
                await mcp_wa_send(phone, clarify_policy)
                _add_to_history(phone, bot_msg=clarify_policy)
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
            elif llm_commit_intent == "COMMIT_UNSURE":
                commit_ok = False
            elif llm_commit_intent == "DEFERRAL":
                commit_ok = False
                sess["_commitment_llm_deferral"] = True
            elif llm_commit_intent == "COMMIT_NO":
                commit_ok = False
            elif llm_commit_intent == "QUERY":
                clarifier = ELIGIBILITY_COMMIT_CLARIFY
                await mcp_wa_send(phone, clarifier)
                _add_to_history(phone, bot_msg=clarifier)
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
            elif llm_commit_intent == "AMBIGUOUS":
                clarifier = ELIGIBILITY_COMMIT_CLARIFY
                await mcp_wa_send(phone, clarifier)
                _add_to_history(phone, bot_msg=clarifier)
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return

            if commit_ok is False:
                if llm_commit_intent == "COMMIT_NO":
                    decline_msg = ELIGIBILITY_DECLINE_REQUIREMENTS
                    await mcp_wa_send(phone, decline_msg)
                    _add_to_history(phone, bot_msg=decline_msg)
                    sess["state"] = "REJECTED"
                    sess["ts"] = time.time()
                    SESSIONS[phone] = sess
                    return

                if llm_commit_intent == "COMMIT_TOO_LOW":
                    sess["_commitment_persuasion_attempts"] = persuasion_attempts + 1
                    persuasion_msg = format_message(ELIGIBILITY_COMMIT_PERSUADE, name=name)
                    await mcp_wa_send(phone, persuasion_msg)
                    _add_to_history(phone, bot_msg=persuasion_msg)
                    sess["ts"] = time.time()
                    SESSIONS[phone] = sess
                    return

                if persuasion_attempts == 0 and llm_commit_intent in {"COMMIT_UNSURE", None}:
                    sess["_commitment_persuasion_attempts"] = 1
                    persuasion_msg = format_message(ELIGIBILITY_COMMIT_PERSUADE, name=name)
                    await mcp_wa_send(phone, persuasion_msg)
                    _add_to_history(phone, bot_msg=persuasion_msg)
                    sess["ts"] = time.time()
                    SESSIONS[phone] = sess
                    return

                elif persuasion_attempts == 1:
                    if is_yes_response(text):
                        commit_ok = True
                        if commit_hours is None:
                            commit_hours = 2.0
                    else:
                        deferral_msg = format_message(ELIGIBILITY_COMMIT_DEFERRAL, name=name)
                        await mcp_wa_send(phone, deferral_msg)
                        _add_to_history(phone, bot_msg=deferral_msg)

                        until_date = datetime.now(timezone.utc) + timedelta(days=5)
                        until_iso = until_date.isoformat()
                        idempotency_key = f"{volunteer_id}_DEFERRAL_COMMITMENT_{int(time.time())}"

                        try:
                            await mcp_deferral_create(volunteer_id, "NO_COMMITMENT", until_iso, idempotency_key)
                            defer_confirm = format_message(ELIGIBILITY_COMMIT_DEFERRAL_CONFIRM, name=name)
                            await mcp_wa_send(phone, defer_confirm)
                            _add_to_history(phone, bot_msg=defer_confirm)
                            sess["_deferred_prev_state"] = state
                            sess["_deferred_reason"] = "COMMITMENT_INSUFFICIENT"
                            sess["state"] = "DEFERRED"
                        except Exception as e:
                            log.warning(f"[ELIG] Failed to create commitment deferral: {e}")
                            sess["_deferred_prev_state"] = state
                            sess["_deferred_reason"] = "COMMITMENT_INSUFFICIENT"
                            sess["state"] = "DEFERRED"

                        sess["ts"] = time.time()
                        SESSIONS[phone] = sess
                        return

            if commit_hours is None and commit_ok is None:
                clarification_count = sess.get("_commitment_clarification_count", 0)

                if clarification_count >= 2:
                    log.warning(f"[ELIG] Max clarifications reached for commitment, treating as hesitant")
                    commit_ok = False
                    sess["elig.commitment"] = False
                else:
                    sess["_commitment_clarification_count"] = clarification_count + 1
                    clarifier = ELIGIBILITY_COMMIT_CLARIFY
                    await mcp_wa_send(phone, clarifier)
                    _add_to_history(phone, bot_msg=clarifier)
                    sess["ts"] = time.time()
                    SESSIONS[phone] = sess
                    return

            if commit_hours is not None and commit_ok is None:
                commit_ok = commit_hours >= 2.0
                log.info(f"[ELIG] Commit_ok set from hours: {commit_hours} >= 2.0 = {commit_ok}")

            if llm_commit_intent == "DEFERRAL" or sess.get("_commitment_llm_deferral"):
                deferral_msg = format_message(ELIGIBILITY_COMMIT_DEFERRAL, name=name)
                await mcp_wa_send(phone, deferral_msg)
                _add_to_history(phone, bot_msg=deferral_msg)
                until_date = datetime.now(timezone.utc) + timedelta(days=5)
                until_iso = until_date.isoformat()
                idempotency_key = f"{volunteer_id}_DEFERRAL_COMMITMENT_{int(time.time())}"

                try:
                    await mcp_deferral_create(volunteer_id, "NO_COMMITMENT", until_iso, idempotency_key)
                    defer_confirm = format_message(ELIGIBILITY_COMMIT_DEFERRAL_CONFIRM, name=name)
                    await mcp_wa_send(phone, defer_confirm)
                    _add_to_history(phone, bot_msg=defer_confirm)
                    sess["_deferred_prev_state"] = state
                    sess["_deferred_reason"] = "COMMITMENT_INSUFFICIENT"
                    sess["state"] = "DEFERRED"
                except Exception as e:
                    log.warning(f"[ELIG] Failed to create commitment deferral: {e}")
                    sess["_deferred_prev_state"] = state
                    sess["_deferred_reason"] = "COMMITMENT_INSUFFICIENT"
                    sess["state"] = "DEFERRED"

                sess.pop("_commitment_llm_deferral", None)
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return

            if commit_ok is True:
                if commit_hours is None:
                    commit_hours = 2.0

                # Parse any availability hints in the same message
                eligibility_days = []
                eligibility_windows = []
                try:
                    if parsed:
                        if isinstance(parsed.get("days"), list):
                            eligibility_days = [d for d in parsed["days"] if isinstance(d, str)]
                        if isinstance(parsed.get("time_windows"), list):
                            for w in parsed["time_windows"]:
                                if isinstance(w, dict) and w.get("start") and w.get("end"):
                                    eligibility_windows.append({"start": w["start"], "end": w["end"]})
                except Exception:
                    pass
                if eligibility_days:
                    sess.setdefault("_prefs_days", [])
                    for d in eligibility_days:
                        if d not in sess["_prefs_days"]:
                            sess["_prefs_days"].append(d)
                if eligibility_windows:
                    sess.setdefault("_prefs_windows", [])
                    for w in eligibility_windows:
                        if w not in sess["_prefs_windows"]:
                            sess["_prefs_windows"].append(w)

                sess["elig.commitment"] = True
                sess["elig.commitment_hours"] = commit_hours
                age_val = sess.get("elig.age_value", 18)
                device_ok = sess.get("elig.device", True)

                eligible = True
                try:
                    elig = await mcp_eligibility_check(
                        age_years=int(age_val) if age_val else 18,
                        has_device=bool(device_ok),
                        weekly_commitment_hours=float(commit_hours)
                    )
                    eligible = bool(elig.get("eligible", True))
                except Exception as e:
                    log.warning(f"[ELIG] eligibility.check failed (proceeding optimistically): {e}")

                if eligible:
                    success_msg = ELIGIBILITY_COMMIT_SUCCESS
                    await mcp_wa_send(phone, success_msg)
                    _add_to_history(phone, bot_msg=success_msg)

                    summary_msg = None
                    if not sess.get("_elig_summary_sent"):
                        summary_msg = await _generate_eligibility_summary_phone(
                            phone,
                            sess,
                            profile,
                            commit_hours=commit_hours,
                            volunteer_name=profile.get("name"),
                        )

                    if summary_msg:
                        await asyncio.sleep(0.4)
                        await mcp_wa_send(phone, summary_msg)
                        _add_to_history(phone, bot_msg=summary_msg)
                        sess["_elig_summary_sent"] = True

                    # Mark profile eligibility snapshot
                    profile.setdefault("eligibility", {})
                    profile["eligibility"]["q1_commitment"] = True
                    profile["eligibility"]["passed"] = True
                    sess["elig.age"] = sess.get("elig.age", True)
                    sess["elig.device"] = sess.get("elig.device", True)
                    sess["elig.commitment"] = True
                    sess["elig.commitment_hours"] = commit_hours

                    sess["state"] = "PREFS_DAYTIME"
                    sess.pop("_commitment_persuasion_attempts", None)
                    sess.pop("_commitment_clarification_count", None)
                    sess["_prefs_last_prompt"] = None
                    sess["_prefs_last_prompt_text"] = None
                    sess["ts"] = time.time()
                    SESSIONS[phone] = sess

                    try:
                        await mcp_telemetry_emit("onboarding.eligibility_passed", {
                            "conversation_id": phone,
                            "user_id": volunteer_id,
                            "age": age_val,
                            "device": device_ok,
                            "commitment_hours": commit_hours,
                            "persuaded": persuasion_attempts > 0
                        })
                    except Exception:
                        pass

                    await asyncio.sleep(0.5)
                    await _handle(phone, "__kick__")
                    return

                decline_msg = ELIGIBILITY_DECLINE_REQUIREMENTS
                await mcp_wa_send(phone, decline_msg)
                _add_to_history(phone, bot_msg=decline_msg)
                sess["state"] = "REJECTED"
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return

            sess["elig.commitment"] = commit_ok
            sess["elig.commitment_hours"] = commit_hours if commit_hours else (2.0 if commit_ok else None)
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return

    # ========== PREFS_DAYTIME (Day & Time Preferences) ==========
    # COMMENTED OUT - Will be moved to states/prefs_daytime.py
    elif False and state == "PREFS_DAYTIME":  # Disabled - will be in separate file
        if text == "__kick__" or not sess.get("_prefs_prompted"):
            await mcp_wa_send(phone, PREFS_INTRO_COLLAB)
            _add_to_history(phone, bot_msg=PREFS_INTRO_COLLAB)
            sess["_prefs_prompted"] = True
            sess.setdefault("_prefs_days", [])
            sess.setdefault("_prefs_time_band", None)
            sess["_prefs_evening_attempts"] = 0
            sess["_prefs_last_prompt"] = "intro"
            sess["_prefs_last_prompt_text"] = PREFS_INTRO_COLLAB
            sess["ts"] = time.time(); SESSIONS[phone] = sess
            return

        interpretation = await _generate_prefs_interpretation(
            phone=phone,
            profile=profile,
            volunteer_name=profile.get("name"),
            text=text,
            sess=sess,
        )

        days = sess.setdefault("_prefs_days", [])
        time_band = sess.get("_prefs_time_band")
        had_evening = time_band == "EVENING"

        if interpretation.get("days"):
            for iso in interpretation["days"]:
                if iso not in days:
                    days.append(iso)

        if interpretation.get("time_band"):
            time_band = interpretation["time_band"]
            sess["_prefs_time_band"] = time_band

        if interpretation.get("topics"):
            topics = sess.setdefault("_qa_topics", [])
            for topic in interpretation["topics"]:
                if topic not in topics:
                    topics.append(topic)

        if not interpretation.get("days"):
            inferred_days: list[str] = []
            text_lower_local = text.lower()
            day_patterns = {
                "monday": "MON",
                "mon": "MON",
                "tuesday": "TUE",
                "tue": "TUE",
                "wednesday": "WED",
                "wed": "WED",
                "thursday": "THU",
                "thu": "THU",
                "thur": "THU",
                "friday": "FRI",
                "fri": "FRI",
                "saturday": "SAT",
                "sat": "SAT",
                "sunday": "SUN",
                "sun": "SUN",
            }
            for token, iso in day_patterns.items():
                if re.search(rf"\b{re.escape(token)}\b", text_lower_local):
                    if iso not in inferred_days:
                        inferred_days.append(iso)
            if inferred_days:
                for iso in inferred_days:
                    if iso not in days:
                        days.append(iso)

        if interpretation.get("deferral"):
            await mcp_deferral_create(
                profile.get("uuid") or phone,
                "PREFS_LATER",
                interpretation["deferral"]["until_iso"],
                f"{phone}_PREFS_DEFER_{int(time.time())}"
            )
            await mcp_wa_send(phone, interpretation["deferral"]["message"])
            _add_to_history(phone, bot_msg=interpretation["deferral"]["message"])
            sess["_deferred_prev_state"] = "PREFS_DAYTIME"
            sess["_deferred_reason"] = "PREFS_LATER"
            sess["state"] = "DEFERRED"
            sess["ts"] = time.time(); SESSIONS[phone] = sess
            return
        elif interpretation.get("followup"):
            followup = interpretation["followup"]
            followup_tag = (interpretation.get("followup_tag") or "").lower()
            followup_lower = followup.lower()
            # If time is already captured, ignore time followups
            if time_band and ("time" in followup_tag or "time" in followup_lower):
                log.info("[PREFS] Ignoring time followup since time_band already set")
            # If days already captured, ignore day followups
            elif days and ("day" in followup_tag or "day" in followup_lower):
                log.info("[PREFS] Ignoring day followup since days already set")
            else:
                await mcp_wa_send(phone, followup)
                _add_to_history(phone, bot_msg=followup)
                sess["_prefs_last_prompt"] = interpretation.get("followup_tag")
                sess["_prefs_last_prompt_text"] = followup
                sess["ts"] = time.time(); SESSIONS[phone] = sess
                return

        if not days:
            followup = PREFS_FOLLOWUP_DAYS
            await mcp_wa_send(phone, followup)
            _add_to_history(phone, bot_msg=followup)
            sess["_prefs_last_prompt"] = "days_followup"
            sess["_prefs_last_prompt_text"] = followup
            sess["ts"] = time.time(); SESSIONS[phone] = sess
            return

        day_label_map = {
            "MON": "Monday", "TUE": "Tuesday", "WED": "Wednesday",
            "THU": "Thursday", "FRI": "Friday", "SAT": "Saturday", "SUN": "Sunday"
        }
        human_days = [day_label_map.get(d, d) for d in days[:3]]
        if len(human_days) == 1:
            days_str = human_days[0]
        elif len(human_days) == 2:
            days_str = f"{human_days[0]} & {human_days[1]}"
        else:
            days_str = ", ".join(human_days[:-1]) + f" & {human_days[-1]}"

        band_label_map = {
            "MORNING": "morning slots",
            "AFTERNOON": "lunch or early-afternoon slots",
            "EVENING": "evening slots"
        }
        band_str = band_label_map.get(time_band, "your preferred time")

        profile.setdefault("preferences", {})
        profile["preferences"]["days"] = days
        profile["preferences"]["time_band"] = time_band

        confirm = format_message(PREFS_CONFIRM_DEFAULT, days=days_str, band=band_str)
        await mcp_wa_send(phone, confirm)
        _add_to_history(phone, bot_msg=confirm)
        sess["_prefs_last_prompt"] = None
        sess["_prefs_last_prompt_text"] = None
        sess.pop("_prefs_evening_attempts", None)

        vid = profile.get("uuid")
        if vid and str(vid).upper() not in {"NONE", "UNKNOWN"}:
            try:
                await mcp_preferences_save(vid, time_band)
            except Exception as e:
                log.debug(f"[PREFS] preferences.save skipped: {e}")

        summary_msg = await _generate_prefs_summary_phone(
            phone=phone,
            sess=sess,
            profile=profile,
            volunteer_name=profile.get("name"),
            days=days,
            time_band=time_band,
            days_label=days_str,
            band_label=band_str,
        )
        if summary_msg:
            await asyncio.sleep(0.4)
            await mcp_wa_send(phone, summary_msg)
            _add_to_history(phone, bot_msg=summary_msg)

        sess["state"] = "QA_WINDOW"
        sess["_qa_count"] = 0
        sess["_qa_topics"] = []
        sess["_qa_summary_sent"] = False
        sess["ts"] = time.time()
        SESSIONS[phone] = sess

        await asyncio.sleep(0.5)
        await _handle(phone, "__kick__")
        return

    # ========== QA_WINDOW (Questions & Answers) ==========
    # COMMENTED OUT - Will be moved to states/qa_window.py
    elif False and state == "QA_WINDOW":  # Disabled - will be in separate file
        log.info(f"[QA] QA_WINDOW handler triggered for {phone}, text='{text[:30]}...'")
        volunteer_id = profile.get("uuid") or phone
        name = profile.get("name") or "there"
        qa_count = sess.get("_qa_count", 0)
        qa_topics = sess.setdefault("_qa_topics", [])
        
        # Entry: send initial QA prompt
        if text == "__kick__":
            log.info(f"[QA] Sending QA entry message to {phone}")
            entry_msg = QA_ENTRY_PROMPT
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
        
        # If user indicates they're done with questions, move directly to orientation scheduling
        # COMMENTED OUT: Orientation scheduling disabled
        # if is_no_response(text) or re.search(r"\b(not now|no questions|no questions?|nothing|no)\b", text_lower):
        #     await _send_orientation_summary(phone, sess, profile)
        #     sess["state"] = "ORIENTATION_SLOT"
        #     sess["ts"] = time.time()
        #     sess.pop("_orientation_phase", None)
        #     sess.pop("_orientation_slots", None)
        #     SESSIONS[phone] = sess
        #     await _handle(phone, "__kick__")
        #     return
        # Instead, transition to COMPLETE when done with questions
        if is_no_response(text) or re.search(r"\b(not now|no questions|no questions?|nothing|no)\b", text_lower):
            sess["state"] = "COMPLETE"
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            await _handle(phone, "__kick__")
            return
        
        # A) STOP / OPT-OUT
        if re.search(r"\b(stop|unsubscribe|don'?t message|no more messages)\b", text_lower):
            ack = QA_STOP_ACK
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
                defer_msg = QA_DEFERRAL_PROMPT
                await mcp_wa_send(phone, defer_msg)
                _add_to_history(phone, bot_msg=defer_msg)
                sess["_deferred_prev_state"] = state
                sess["_deferred_reason"] = "ORIENTATION_LATER"
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
                "answer": QA_FAQ_ABOUT_SERVE
            },
            "time_process": {
                "pattern": r"\b(hours?|time|how teach|online|travel|duration|how long)\b",
                "answer": QA_FAQ_TIME_PROCESS
            },
            "support": {
                "pattern": r"\b(training|orientation|help|support|guidance|assistance)\b",
                "answer": QA_FAQ_SUPPORT
            },
            "certificate": {
                "pattern": r"\b(certificate|letter|proof|document|completion)\b",
                "answer": QA_FAQ_CERTIFICATE
            },
            "subjects_grades": {
                "pattern": r"\b(subject|grade|class|what (teach|teach)|math|english|science)\b",
                "answer": QA_FAQ_SUBJECTS_GRADES
            },
            "tech": {
                "pattern": r"\b(internet|wifi|laptop|phone|meet|zoom|google meet|tech|technical|device)\b",
                "answer": QA_FAQ_TECH
            }
        }
        
        matched_bucket = None
        for bucket_name, bucket_data in faq_answers.items():
            if re.search(bucket_data["pattern"], text_lower):
                matched_bucket = bucket_name
                faq_bucket = bucket_name
                answer = bucket_data["answer"]
                if bucket_name not in qa_topics:
                    qa_topics.append(bucket_name)
                await mcp_wa_send(phone, answer)
                _add_to_history(phone, bot_msg=answer)
                qa_count += 1
                sess["_qa_count"] = qa_count
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                
                await asyncio.sleep(0.5)
                # COMMENTED OUT: Orientation scheduling disabled
                # await _send_orientation_summary(phone, sess, profile)
                # sess["state"] = "ORIENTATION_SLOT"
                # sess["ts"] = time.time()
                # sess.pop("_orientation_phase", None)
                # sess.pop("_orientation_slots", None)
                # SESSIONS[phone] = sess
                # await _handle(phone, "__kick__")
                # Instead, transition to COMPLETE
                sess["state"] = "COMPLETE"
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                await _handle(phone, "__kick__")
                return

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
                    "I might not have the perfect answer right now. You can message here anytime and we’ll help."
                )
            
            if "custom" not in qa_topics:
                qa_topics.append("custom")
            await mcp_wa_send(phone, answer)
            _add_to_history(phone, bot_msg=answer)
            qa_count += 1
            sess["_qa_count"] = qa_count
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            
            await asyncio.sleep(0.5)
            # COMMENTED OUT: Orientation scheduling disabled
            # await _send_orientation_summary(phone, sess, profile)
            # sess["state"] = "ORIENTATION_SLOT"
            # sess["ts"] = time.time()
            # sess.pop("_orientation_phase", None)
            # sess.pop("_orientation_slots", None)
            # SESSIONS[phone] = sess
            # Instead, transition to COMPLETE
            sess["state"] = "COMPLETE"
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            await _handle(phone, "__kick__")
            return

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
        unclear = "I'd be happy to answer your question. Could you rephrase it, or would you like to continue?"
        await mcp_wa_send(phone, unclear)
        _add_to_history(phone, bot_msg=unclear)
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return

    # ========== ORIENTATION_SLOT (Availability Capture & Slot Proposal) ==========
    # COMMENTED OUT - Will be moved to states/orientation_slot.py
    elif False and state == "ORIENTATION_SLOT":  # Disabled - will be in separate file
        volunteer_id = profile.get("uuid") or phone
        name = profile.get("name") or "there"
        
        log.info(f"[ORIENT] ORIENTATION_SLOT handler triggered for {phone}, text='{text[:30]}...'")
        
        # Entry: send ASK_AVAILABILITY message
        if text == "__kick__":
            log.info(f"[ORIENT] Sending orientation intro to {phone}")
            await mcp_wa_send(phone, ORIENT_INTRO)
            _add_to_history(phone, bot_msg=ORIENT_INTRO)
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        
        # User provided time slots - parse and propose slots
        text_lower = text.lower()

        # Handle stop/opt-out requests
        if re.search(r"\b(stop|unsubscribe|don'?t message|no more messages)\b", text_lower):
            ack = QA_STOP_ACK
            await mcp_wa_send(phone, ack)
            _add_to_history(phone, bot_msg=ack)
            sess["state"] = "OPTOUT"
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return

        # Handle orientation deferral requests (quick heuristic before LLM)
        if re.search(r"\b(later|next week|not today|busy|remind|check back)\b", text_lower):
            until_date = datetime.now() + timedelta(days=5)
            until_iso = until_date.isoformat()
            idk = f"{volunteer_id}_QA_DEFER_{int(time.time())}"
            try:
                await mcp_deferral_create(volunteer_id, "ORIENTATION_LATER", until_iso, idk)
                defer_msg = ORIENT_LATER_NOTE
                await mcp_wa_send(phone, defer_msg)
                _add_to_history(phone, bot_msg=defer_msg)
                sess["state"] = "DEFERRED"
                sess["orientation_pending"] = True
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                try:
                    await mcp_telemetry_emit("onboarding.qa_deferral", {
                        "conversation_id": phone,
                        "user_id": volunteer_id,
                        "qa_count": sess.get("_qa_count", 0)
                    })
                except Exception:
                    pass
                return
            except Exception as e:
                log.warning(f"[ORIENT] Deferral creation failed: {e}")

        # LLM classification for orientation intents
        llm_intent = None
        llm_conf = 0.0
        try:
            llm_context = build_llm_context(
                "ORIENTATION_SLOT",
                sess,
                last_prompt=ORIENT_INTRO,
            )
            llm_result = await mcp_llm_classify_intent(text, "ORIENTATION_SLOT", llm_context)
            llm_intent = (llm_result.get("intent") or "").upper()
            llm_conf = float(llm_result.get("confidence") or 0.0)
        except Exception as e:
            log.warning(f"[ORIENT] LLM classification failed: {e}")

        accept_llm = False
        if llm_intent:
            accept_llm = llm_conf >= 0.6
            if not accept_llm and llm_intent == "ORIENT_LATER_OR_DEFERRAL" and llm_conf >= 0.35:
                accept_llm = True

        async def _send_and_track(message: str):
            await mcp_wa_send(phone, message)
            _add_to_history(phone, bot_msg=message)
            sess["ts"] = time.time()
            SESSIONS[phone] = sess

        if accept_llm and llm_intent:
            if llm_intent == "ORIENT_LATER_OR_DEFERRAL":
                reply = ORIENT_LATER_NOTE
                await _send_and_track(reply)
                until_date = datetime.now() + timedelta(days=5)
                until_iso = until_date.isoformat()
                idk = f"{volunteer_id}_ORIENT_DEFER_{int(time.time())}"
                try:
                    await mcp_deferral_create(volunteer_id, "ORIENTATION_LATER", until_iso, idk)
                except Exception as e:
                    log.warning(f"[ORIENT] Deferral creation via LLM intent failed: {e}")
                sess["state"] = "DEFERRED"
                sess["orientation_pending"] = True
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                try:
                    await mcp_telemetry_emit("onboarding.qa_deferral", {
                        "conversation_id": phone,
                        "user_id": volunteer_id,
                        "qa_count": sess.get("_qa_count", 0),
                        "source": "llm"
                    })
                except Exception:
                    pass
                return

            if llm_intent == "ORIENT_FAQ":
                reply = QA_MANDATORY_ORIENT
                await _send_and_track(reply)
                return

            if llm_intent == "ORIENT_INVALID_PICK":
                reply = ORIENT_INVALID_PICK
                await _send_and_track(reply)
                return

            if llm_intent == "ORIENT_AMBIGUOUS":
                reply = "Would you like me to suggest a couple of slots based on your availability?"
                await _send_and_track(reply)
                return

            # COMMENTED OUT: Orientation scheduling disabled
            # if llm_intent == "ORIENT_PICK_OPTION":
            #     await _send_and_track(ORIENT_BOOKING_CONFIRM)
            #     sess["state"] = "ORIENTATION_SCHEDULING"
            #     sess["ts"] = time.time()
            #     SESSIONS[phone] = sess
            #     await _handle(phone, text)
            #     return

            if llm_intent != "ORIENT_PROVIDE_PREFERENCES":
                # Unknown intent even after acceptance – fall through to parsing
                log.info(f"[ORIENT] Accepted LLM intent {llm_intent} but no handler; falling back to parsing.")
            else:
                await _send_and_track(ORIENT_AVAILABILITY_ACK)
        
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
            ack_msg = ORIENT_AVAILABILITY_ACK
            await mcp_wa_send(phone, ack_msg)
            _add_to_history(phone, bot_msg=ack_msg)

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
                await mcp_wa_send(phone, ORIENT_PROPOSAL_ERROR)
                _add_to_history(phone, bot_msg=ORIENT_PROPOSAL_ERROR)
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
            
            slots = slots_result.get("slots", [])
            if not slots:
                await mcp_wa_send(phone, ORIENT_PROPOSAL_NO_SLOTS)
                _add_to_history(phone, bot_msg=ORIENT_PROPOSAL_NO_SLOTS)
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
            
            # Keep at most two options for a simple choice
            slots = list(slots[:2])
            
            # Store slots in session for next state
            sess["_orientation_slots"] = slots
            sess["_orientation_slots_raw"] = slots_result
            
            # Format and send slot options
            option_lines = []
            for idx, slot in enumerate(slots[:2], start=1):
                label = slot.get("label", f"Option {idx}")
                option_lines.append(f"{idx}️⃣ {label}")
            options_text = "\n".join(option_lines) if option_lines else "1️⃣ Option 1"
            confirm_msg = format_message(ORIENT_SHOW_OPTIONS, options=options_text)
            await mcp_wa_send(phone, confirm_msg)
            _add_to_history(phone, bot_msg=confirm_msg)
            
            # COMMENTED OUT: Orientation scheduling disabled
            # Transition to ORIENTATION_SCHEDULING
            # sess["state"] = "ORIENTATION_SCHEDULING"
            # sess["ts"] = time.time()
            # SESSIONS[phone] = sess
            # log.info(f"[ORIENT] Slot options sent, transitioning to ORIENTATION_SCHEDULING for {phone}")
            # return
            # Instead, transition to COMPLETE
            sess["state"] = "COMPLETE"
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            log.info(f"[ORIENT] Orientation scheduling disabled, transitioning to COMPLETE for {phone}")
            return
            
        except Exception as e:
            log.error(f"[ORIENT] Failed to propose slots: {e}", exc_info=True)
            await mcp_wa_send(phone, ORIENT_PROPOSAL_ERROR)
            _add_to_history(phone, bot_msg=ORIENT_PROPOSAL_ERROR)
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return

    # ========== DEFERRED (Waiting for volunteer to return) ==========
    if state == "DEFERRED":
        prev_state = sess.pop("_deferred_prev_state", None) or "WELCOME"
        sess.pop("_deferred_reason", None)

        if prev_state == "PREFS_DAYTIME":
            sess.pop("_prefs_evening_attempts", None)

        sess["state"] = prev_state
        sess["ts"] = time.time()
        sess.pop("_last_msg_text", None)
        sess.pop("_last_msg_ts", None)
        SESSIONS[phone] = sess

        # If user explicitly wants to resume, advance past WELCOME or re-ask pending question.
        if text != "__kick__" and is_resume_response(text):
            if prev_state == "WELCOME":
                sess["state"] = "WELCOME_VIDEO"
                sess["sub_state"] = "WELCOME_VIDEO"
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                await _handle(phone, "__kick__")
                return
            if await _reask_pending_question(phone, prev_state, sess):
                return

        await _handle(phone, text)
        return

    # ========== ORIENTATION_SCHEDULING (Slot Selection & Booking) ==========
    # COMMENTED OUT - Will be moved to states/orientation_scheduling.py
    elif False and state == "ORIENTATION_SCHEDULING":  # Disabled - will be in separate file
        volunteer_id = profile.get("uuid") or phone
        name = profile.get("name") or "there"
        
        log.info(f"[SCHED] ORIENTATION_SCHEDULING handler triggered for {phone}, text='{text[:30]}...'")
        
        slots = sess.get("_orientation_slots", [])
        if not slots:
            log.warning(f"[SCHED] No slots found in session for {phone}, asking for availability again")
            # COMMENTED OUT: Orientation scheduling disabled
            # sess["state"] = "ORIENTATION_SLOT"
            # sess["ts"] = time.time()
            # SESSIONS[phone] = sess
            # await _handle(phone, "__kick__")
            # return
            # Instead, transition to COMPLETE
            sess["state"] = "COMPLETE"
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
            await mcp_wa_send(phone, ORIENT_INVALID_PICK)
            _add_to_history(phone, bot_msg=ORIENT_INVALID_PICK)
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            return
        
        # Hold the slot
        slot_id = selected_slot.get("slot_id") or selected_slot.get("id")
        if not slot_id:
            log.error(f"[SCHED] Selected slot has no ID: {selected_slot}")
            await mcp_wa_send(phone, ORIENT_SLOT_UNAVAILABLE)
            _add_to_history(phone, bot_msg=ORIENT_SLOT_UNAVAILABLE)
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
                await mcp_wa_send(phone, ORIENT_SLOT_UNAVAILABLE)
                _add_to_history(phone, bot_msg=ORIENT_SLOT_UNAVAILABLE)
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
            
            # Book the slot
            log.info(f"[SCHED] Booking slot with hold_id {hold_id} for {phone}")
            await mcp_wa_send(phone, ORIENT_BOOKING_CONFIRM)
            _add_to_history(phone, bot_msg=ORIENT_BOOKING_CONFIRM)
            
            booking_result = await mcp_slot_book(hold_id)
            
            if not booking_result or not isinstance(booking_result, dict):
                log.error(f"[SCHED] Failed to book slot: {booking_result}")
                await mcp_wa_send(phone, ORIENT_BOOKING_FAILURE)
                _add_to_history(phone, bot_msg=ORIENT_BOOKING_FAILURE)
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
            await _book_slot_and_finish(phone, chosen_slot, profile, name, send_orientation_confirm=True)
            
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
    # End of commented-out state handlers
    # ============================================================================

    # Default: unknown state (only reached if state is not WELCOME and not in commented section)
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
            
            # Extract inbound message ID for idempotency
            inbound_msg_id = _extract_inbound_msg_id(evt, phone, text)
            
            # Handle message through state machine with lock and idempotency
            try:
                await _handle_with_idempotency(phone, text, inbound_msg_id, evt)
            except Exception as e:
                log.error(f"[KAFKA] Error handling message from {phone}: {e}", exc_info=True)
                # Log ERROR event
                try:
                    with get_db_session() as db:
                        log_event(
                            db=db,
                            wa_phone=phone,
                            agent_name=settings.AGENT_NAME,
                            event_type="ERROR",
                            event_source="onboarding_agent",
                            state=None,
                            status="error",
                            details={"error": str(e), "inbound_msg_id": inbound_msg_id}
                        )
                except:
                    pass  # Best-effort event logging
                try:
                    await mcp_wa_send(phone, "Sorry, something went wrong. Please type 'restart' to try again.")
                except:
                    pass  # Don't crash the loop
    
    finally:
        await consumer.stop()
        await producer.stop()
        log.info("[KAFKA] Consumer stopped")
