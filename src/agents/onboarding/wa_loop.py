import asyncio, json, time, uuid, logging
from datetime import datetime, timedelta
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from .config import settings
import httpx

# ---------- Session & Config ----------
log = logging.getLogger(__name__)
SESSIONS: dict[str, dict] = {}  # {phone: {"state": "...", "profile": {...}, "ts": epoch, ...}}
MCP_BASE = settings.MCP_BASE
MCP_JSONRPC_ENDPOINT = f"{MCP_BASE}/mcp/v1/jsonrpc"
MCP_INITIALIZED = False  # Track if MCP session is initialized

CATALOG = {
    "subjects": ["Math","English","Science","Social Studies","Computer Science","Hindi"],
    "grades":   ["1-3","4-5","6-8","9-10","11-12"]
}

YES_WORDS = {"yes","y","ok","okay","sure","go ahead","continue","proceed","ya","haan"}

def _js(v): return json.dumps(v).encode()
def _ks(k): return (k or "").encode()
def norm_phone(p: str) -> str: return (p or "").lstrip("+").strip()

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
    
    # Step 1: Send initialize request
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
                "name": "serve-vm-agent-onboarding",
                "version": "1.0.0"
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
                # "Already initialized" is not a fatal error - just means session exists
                if error.get('message') == 'Already initialized':
                    log.info("[MCP] Session already initialized, continuing...")
                    MCP_INITIALIZED = True
                    # List available tools
                    await _mcp_list_tools()
                    return
                else:
                    log.error(f"[MCP] Initialize error: {error}")
                    raise RuntimeError(f"MCP initialization failed: {error['message']}")
            
            log.info(f"[MCP] Initialize response: {json.dumps(init_response, indent=2)}")
            
            # Step 2: Send initialized notification
            initialized_payload = {
                "jsonrpc": "2.0",
                "method": "notifications/initialized"
            }
            
            r = await client.post(MCP_JSONRPC_ENDPOINT, json=initialized_payload)
            r.raise_for_status()
            
            MCP_INITIALIZED = True
            log.info("[MCP] MCP session initialized successfully")
            
            # List available tools
            await _mcp_list_tools()
            
    except Exception as e:
        log.error(f"[MCP] Failed to initialize: {e}", exc_info=True)
        raise

# ---------- JSON-RPC 2.0 MCP helpers ----------
async def _mcp_call(tool_name: str, arguments: dict, timeout: int = 15) -> dict:
    """
    Make a JSON-RPC 2.0 call to the MCP server.
    Returns the parsed result from result.content[0].text (as JSON if applicable).
    """
    # Ensure MCP session is initialized
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
        log.info(f"[MCP] Calling tool={tool_name} to {MCP_JSONRPC_ENDPOINT}")
        log.debug(f"[MCP] Request payload: {json.dumps(payload, indent=2)}")
        
        async with httpx.AsyncClient(timeout=timeout) as client:
            r = await client.post(MCP_JSONRPC_ENDPOINT, json=payload)
            r.raise_for_status()
            response = r.json()
            
            log.info(f"[MCP] Response for {tool_name}: {json.dumps(response, indent=2)}")
            
            # Check for JSON-RPC error
            if "error" in response:
                error = response["error"]
                log.error(f"[MCP] Tool call error: {error['message']} (code: {error['code']})")
                raise RuntimeError(f"MCP tool '{tool_name}' failed: {error['message']}")
            
            # Extract result.content[0].text
            if "result" in response and "content" in response["result"]:
                content = response["result"]["content"]
                if content and len(content) > 0:
                    text = content[0].get("text", "{}")
                    # Try to parse as JSON if it looks like JSON
                    try:
                        parsed = json.loads(text)
                        log.info(f"[MCP] Success: tool={tool_name}, result extracted")
                        return parsed
                    except (json.JSONDecodeError, TypeError):
                        log.info(f"[MCP] Success: tool={tool_name}, text response")
                        return {"text": text}
            
            # Fallback if response structure is unexpected
            log.warning(f"[MCP] Unexpected response structure for tool={tool_name}. Full response: {json.dumps(response)}")
            return response.get("result", {})
            
    except httpx.HTTPStatusError as e:
        log.error(f"[MCP] HTTP Error calling {tool_name}: {e.response.status_code} - {e.response.text}")
        raise
    except httpx.RequestError as e:
        log.error(f"[MCP] Request Error calling {tool_name}: {e}")
        raise
    except Exception as e:
        log.error(f"[MCP] Unexpected error calling {tool_name}: {e}", exc_info=True)
        raise

async def mcp_wa_send(to: str, text: str):
    """Send WhatsApp message via MCP JSON-RPC 2.0"""
    return await _mcp_call("wa.send_message", {"to": to, "text": text}, timeout=10)

async def mcp_extract(state, text, catalog, locale="en-IN"):
    """Extract profile fields via MCP JSON-RPC 2.0"""
    return await _mcp_call("llm.extract_profile_fields", {
        "state": state,
        "text": text,
        "catalog": catalog,
        "locale": locale
    }, timeout=15)

async def mcp_reply(purpose, state, profile_partial, locale="en-IN"):
    """Generate reply via MCP JSON-RPC 2.0"""
    result = await _mcp_call("llm.generate_reply", {
        "purpose": purpose,
        "state": state,
        "profile_partial": profile_partial,
        "locale": locale
    }, timeout=15)
    return result.get("text", "")

async def mcp_calendar_create(title: str, start_iso: str, end_iso: str, attendees: list[str], timezone="Asia/Kolkata", notes=None):
    """Create calendar event via MCP JSON-RPC 2.0"""
    return await _mcp_call("calendar.create_event", {
        "title": title,
        "start_iso": start_iso,
        "end_iso": end_iso,
        "attendees": attendees,
        "timezone": timezone,
        "notes": notes
    }, timeout=15)


async def start_onboarding(phone: str):
    phone = norm_phone(phone)
    log.info(f"[START] Starting onboarding for phone={phone}")
    # initialize a fresh session and send the welcome message
    SESSIONS[phone] = {"state": "WELCOME", "profile": {}, "ts": time.time(), "_welcomed": False}
    try:
        await _handle(phone, "__kick__")  # this will send the welcome and wait for "Yes"
        log.info(f"[START] Welcome message sent successfully to phone={phone}")
    except Exception as e:
        log.error(f"[START] Failed to start onboarding for phone={phone}: {e}", exc_info=True)
        raise


# ---------- Slot helpers (stub) ----------
def iso_kolkata(dt: datetime) -> str:
    # Simple formatting with +05:30; for production use zoneinfo.
    return dt.strftime("%Y-%m-%dT%H:%M:%S+05:30")

def next_evening_slots():
    now = datetime.now()
    base = now + timedelta(days=1)
    opts = []
    for h in (19, 20, 21):  # 7pm, 8pm, 9pm IST, 30-min each
        start = base.replace(hour=h, minute=0, second=0, microsecond=0)
        end = start + timedelta(minutes=30)
        label = start.strftime("%a %d %b %I:%M %p")
        opts.append((iso_kolkata(start), iso_kolkata(end), label))
    return opts  # [(start_iso, end_iso, label), ...]

# ---------- Conversation engine ----------
async def _handle_edit(phone: str, sess: dict):
    """Handle user request to edit/go back to previous answer"""
    state = sess["state"]
    profile = sess.get("profile", {})
    
    # Map states to their previous state
    state_flow = {
        "ASK_SUBJECTS": "WELCOME",
        "ASK_GRADES": "ASK_SUBJECTS",
        "ASK_LANG": "ASK_GRADES",
        "ASK_AVAILABILITY": "ASK_LANG",
        "CONFIRM_SLOT": "ASK_AVAILABILITY",
        "CONFIRM_BOOKING": "CONFIRM_SLOT"
    }
    
    previous_state = state_flow.get(state)
    if not previous_state:
        await mcp_wa_send(phone, "You're at the beginning. Nothing to go back to!")
        return
    
    # Show what they can edit
    if previous_state == "WELCOME":
        await mcp_wa_send(phone, 
            "Going back to the start.\n\n"
            "Ready to begin? Reply *Yes* to continue."
        )
        sess["state"] = "WELCOME"
        sess["_welcomed"] = True
    elif previous_state == "ASK_SUBJECTS":
        await mcp_wa_send(phone,
            "*Step 1 of 5: Subject Preferences*\n\n"
            f"Current: *{', '.join(profile.get('subjects', []))}*\n\n"
            "Which subject(s) would you like to teach?\n"
            "(e.g., Math, English, Science)"
        )
        sess["state"] = "ASK_SUBJECTS"
    elif previous_state == "ASK_GRADES":
        await mcp_wa_send(phone,
            "*Step 2 of 5: Grade Levels*\n\n"
            f"Current: *{profile.get('grades', 'N/A')}*\n\n"
            "Which grade levels would you like to teach?\n"
            "(e.g., 6-8, 9-10, 11-12)"
        )
        sess["state"] = "ASK_GRADES"
    elif previous_state == "ASK_LANG":
        await mcp_wa_send(phone,
            "*Step 3 of 5: Teaching Language*\n\n"
            f"Current: *{profile.get('language', 'N/A')}*\n\n"
            "Which language are you comfortable teaching in?\n"
            "(e.g., English, Hindi, or Both)"
        )
        sess["state"] = "ASK_LANG"
    elif previous_state == "ASK_AVAILABILITY":
        await mcp_wa_send(phone,
            "*Step 4 of 5: Schedule Orientation*\n\n"
            "Please share 2-3 time options that work for you in IST.\n"
            "(e.g., 'tomorrow 7pm', 'Saturday 8:30pm', 'Monday 6pm')"
        )
        sess["state"] = "ASK_AVAILABILITY"
    elif previous_state == "CONFIRM_SLOT":
        # Re-show slots
        slots = sess.get("slots", [])
        if slots:
            msg = "*Step 5 of 5: Confirm Your Slot*\n\n"
            msg += "Here are the time slots:\n\n"
            for i, s in enumerate(slots, start=1):
                msg += f"*{i}.* {s['label']}\n"
            msg += f"*4.* None of the above, suggest different times\n\n"
            msg += "Please reply with *1*, *2*, *3*, or *4*."
            await mcp_wa_send(phone, msg)
            sess["state"] = "CONFIRM_SLOT"
        else:
            await mcp_wa_send(phone,
                "*Step 4 of 5: Schedule Orientation*\n\n"
                "Please share 2-3 time options that work for you in IST.\n"
                "(e.g., 'tomorrow 7pm', 'Saturday 8:30pm', 'Monday 6pm')"
            )
            sess["state"] = "ASK_AVAILABILITY"
    elif previous_state == "CONFIRM_BOOKING":
        # Go back to slot confirmation
        chosen = sess.get("chosen_slot", {})
        if chosen:
            await mcp_wa_send(phone, 
                f"You selected: *{chosen.get('label', 'N/A')}*\n\n"
                f"I'll now create a calendar invite and send you the meeting link.\n\n"
                f"Reply *okay* to confirm and proceed."
            )
            sess["state"] = "CONFIRM_BOOKING"
        else:
            # No slot chosen, go back to slot selection
            slots = sess.get("slots", [])
            if slots:
                msg = "*Step 5 of 5: Confirm Your Slot*\n\n"
                msg += "Here are the time slots:\n\n"
                for i, s in enumerate(slots, start=1):
                    msg += f"*{i}.* {s['label']}\n"
                msg += f"*4.* None of the above, suggest different times\n\n"
                msg += "Please reply with *1*, *2*, *3*, or *4*."
                await mcp_wa_send(phone, msg)
                sess["state"] = "CONFIRM_SLOT"
            else:
                await mcp_wa_send(phone,
                    "*Step 4 of 5: Schedule Orientation*\n\n"
                    "Please share 2-3 time options that work for you in IST.\n"
                    "(e.g., 'tomorrow 7pm', 'Saturday 8:30pm', 'Monday 6pm')"
                )
                sess["state"] = "ASK_AVAILABILITY"

async def _handle(phone: str, text: str):
    phone = norm_phone(phone)
    sess = SESSIONS.get(phone) or {"state": "WELCOME", "profile": {}, "ts": time.time()}

    # Handle edit/back commands
    if text.lower() in ["edit", "back", "previous"] and sess["state"] not in ["WELCOME", "DONE"]:
        await _handle_edit(phone, sess)
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
    
    if sess["state"] == "WELCOME":
        # Only send welcome once per new session
        if not sess.get("_welcomed"):
            msg = (
                "Hi! Thank you for registering with *Project SERVE*.\n"
                "SERVE connects passionate volunteers like you with schools that need support in teaching.\n\n"
                "I'll now collect a few of your preferences so we can match you with the right school.\n"
                "Let's get you onboarded.\n\n"
                "Shall I go ahead? (Please reply *Yes* to continue)"
            )
            await mcp_wa_send(phone, msg)
            sess["_welcomed"] = True
            # Stay in WELCOME; wait for reply
        else:
            if text.lower() in YES_WORDS:
                await mcp_wa_send(phone, 
                    "*Step 1 of 5: Subject Preferences*\n\n"
                    "Which subject(s) would you like to teach?\n"
                    "(e.g., Math, English, Science)\n\n"
                    "_Type 'back' anytime to edit previous answers_"
                )
                sess["state"] = "ASK_SUBJECTS"
            else:
                await mcp_wa_send(phone, "No problem. Reply *Yes* whenever you're ready to continue.")
                # remain in WELCOME

    elif sess["state"] == "ASK_SUBJECTS":
        try:
            x = await mcp_extract("ASK_SUBJECTS", text, CATALOG)
            subjects = x.get("subjects") or []
        except Exception as e:
            log.error(f"[ERROR] mcp_extract failed: {e}")
            subjects = []

        if subjects:
            sess["profile"]["subjects"] = subjects  # list
            await mcp_wa_send(phone, 
                f"*Step 2 of 5: Grade Levels*\n\n"
                f"Great! You selected: *{', '.join(subjects)}*\n\n"
                f"Which grade levels would you like to teach?\n"
                f"(e.g., 6-8, 9-10, 11-12)"
            )
            sess["state"] = "ASK_GRADES"
        else:
            await mcp_wa_send(phone, "Please share the subject(s) you'd like to teach (e.g., Math, English, Science).")

    elif sess["state"] == "ASK_GRADES":
        grades = text.strip()
        if not grades:
            await mcp_wa_send(phone, "Please mention the grade levels (e.g., 6-8, 9-10, 11-12).")
        else:
            sess["profile"]["grades"] = grades
            await mcp_wa_send(phone, 
                f"*Step 3 of 5: Teaching Language*\n\n"
                f"Perfect! Grades: *{grades}*\n\n"
                f"Which language are you comfortable teaching in?\n"
                f"(e.g., English, Hindi, or Both)"
            )
            sess["state"] = "ASK_LANG"

    elif sess["state"] == "ASK_LANG":
        lang = text.strip()
        if not lang:
            await mcp_wa_send(phone, "Please share your preferred teaching language (e.g., English, Hindi, Both).")
        else:
            sess["profile"]["language"] = lang
            # Confirm & say stored
            p = sess["profile"]
            subjects_str = ", ".join(p.get("subjects", [])) if isinstance(p.get("subjects"), list) else p.get("subjects", "N/A")
            grades_str = p.get("grades", "N/A")
            await mcp_wa_send(phone,
                f"*Your Teaching Preferences:*\n"
                f"- Subjects: {subjects_str}\n"
                f"- Grades: {grades_str}\n"
                f"- Language: {p.get('language','N/A')}\n\n"
                "Great! I've saved your preferences."
            )
            # Orientation intro
            await mcp_wa_send(phone,
                "*Step 4 of 5: Schedule Orientation*\n\n"
                "Every volunteer attends a short online *Orientation Session* to get started.\n\n"
                "Please share 2-3 time options that work for you in IST.\n"
                "(e.g., 'tomorrow 7pm', 'Saturday 8:30pm', 'Monday 6pm')"
            )
            sess["state"] = "ASK_AVAILABILITY"

    elif sess["state"] == "ASK_AVAILABILITY":
        res = await mcp_time_parse(text)
        slots = res.get("slots", [])
        if not slots:
            await mcp_wa_send(phone,
                "I couldn't parse the time options you provided.\n\n"
                "Please share your available times again.\n"
                "(e.g., 'tomorrow 7pm', 'Saturday 8:30pm')"
            )
            # Stay in ASK_AVAILABILITY to retry
        else:
            sess["slots"] = slots
            msg = "*Step 5 of 5: Confirm Your Slot*\n\n"
            msg += "Here are the time slots I understood:\n\n"
            for i, s in enumerate(slots, start=1):
                msg += f"*{i}.* {s['label']}\n"
            msg += f"*4.* None of the above, suggest different times\n\n"
            msg += "Please reply with *1*, *2*, *3*, or *4*."
            await mcp_wa_send(phone, msg)
            sess["state"] = "CONFIRM_SLOT"
    
    elif sess["state"] == "CONFIRM_SLOT":
        if text not in {"1","2","3","4"}:
            await mcp_wa_send(phone, "Please reply with 1, 2, 3, or 4.")
        elif text == "4":
            # User wants different times
            await mcp_wa_send(phone,
                "No problem! Please share 2-3 different time options that work better for you.\n"
                "(e.g., 'tomorrow 7pm', 'Saturday 8:30pm')"
            )
            sess["state"] = "ASK_AVAILABILITY"
        elif not sess.get("slots"):
            await mcp_wa_send(phone, "Please reply with 1, 2, 3, or 4.")
        else:
            choice = int(text) - 1
            chosen = sess["slots"][choice]
            sess["chosen_slot"] = chosen
            await mcp_wa_send(phone, 
                f"You selected: *{chosen['label']}*\n\n"
                f"I'll now create a calendar invite and send you the meeting link.\n\n"
                f"Reply *okay* to confirm and proceed."
            )
            sess["state"] = "CONFIRM_BOOKING"
    
    elif sess["state"] == "CONFIRM_BOOKING":
        if text.lower() in YES_WORDS or text.lower() in ["okay", "ok", "confirm"]:
            chosen = sess.get("chosen_slot")
            if not chosen:
                await mcp_wa_send(phone, "Sorry, I lost the slot info. Please type 'restart' to start over.")
                sess["state"] = "DONE"
            else:
                await mcp_wa_send(phone, "Great! Booking your orientation session now...")
                
                # Execute booking immediately
                start_iso = chosen.get("start_iso")
                end_iso   = chosen.get("end_iso")
                label     = chosen.get("label")

                title = "Serve Vriddhi – Volunteer Orientation"
                attendees = [phone]  # later map to email
                res = await mcp_calendar_create(title, start_iso, end_iso, attendees)
                meet = res.get("meeting_url", "https://meet.google.com/placeholder")
                sess["profile"]["meeting_url"] = meet
                sess["profile"]["meeting_start"] = start_iso

                p = sess["profile"]
                subjects_str = ", ".join(p.get("subjects", [])) if isinstance(p.get("subjects"), list) else p.get("subjects", "N/A")
                grades_str = p.get("grades", "N/A")

                await mcp_wa_send(phone,
                    f"*All Set! Welcome to Project SERVE!*\n\n"
                    f"*Your Profile:*\n"
                    f"- Subjects: {subjects_str}\n"
                    f"- Grades: {grades_str}\n"
                    f"- Language: {p.get('language','N/A')}\n\n"
                    f"*Orientation Session:*\n"
                    f"- Time: {label}\n"
                    f"- Meeting Link: {meet}\n\n"
                    f"See you there! We're excited to have you on board."
                )
                sess["state"] = "DONE"
        else:
            await mcp_wa_send(phone, "Please reply *okay* to confirm, or type *back* to choose a different slot.")


    else:
        await mcp_wa_send(phone, "We’ve captured your details. Type 'restart' to start over.")

    sess["ts"] = time.time()
    SESSIONS[phone] = sess

async def mcp_time_parse(text: str, duration=60, tz="Asia/Kolkata"):
    """Parse time options via MCP JSON-RPC 2.0"""
    return await _mcp_call("time.parse_options", {
        "text": text,
        "duration_minutes": duration,
        "tz": tz
    }, timeout=240)


# ---------- Kafka loop ----------
async def wa_loop():
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA_BROKERS,
        value_serializer=_js, key_serializer=_ks
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
    try:
        async for rec in consumer:
            evt = rec.value
            if evt.get("type") != "wa.inbound.v1":
                continue

            data = evt.get("data") or {}
            phone = norm_phone(data.get("from") or "")
            text  = (data.get("text") or "").strip()

            # Ignore delivery/read receipts or empties
            if not phone or not text:
                continue

            if text.lower() == "restart":
                SESSIONS.pop(phone, None)
                await mcp_wa_send(phone, "Restarting your onboarding.")
                # fall-through to welcome on next real message
                continue

            # Drive the state machine with actual user text
            await _handle(phone, text)
    finally:
        await consumer.stop()
        await producer.stop()
