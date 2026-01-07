"""
Selection Agent - Main Handler

Handles the Selection Agent state machine for volunteer screening and recommendation.
"""
import logging
import time
from typing import Optional

from .types import SelectionState, RecommendationOutcome
from .prompts import (
    get_sel_video_intro,
    get_sel_video_done_prompt,
    get_sel_video_followup,
    get_sel_about_you,
    get_sel_recommended_msg,
    WELCOME_VIDEO_URL,
    SEL_NOT_RECOMMENDED_MSG,
)
from .config import settings
from .knowing_volunteer_engine import (
    run_knowing_volunteer_step,
    init_volunteer_profile,
    KnowingVolunteerResult,
)

log = logging.getLogger(__name__)

# These will be imported from the main routing/dispatcher
# For now, we'll use late imports to avoid circular dependencies
_mcp_wa_send = None
_log_event = None
_handle_fulfillment = None


def _get_mcp_wa_send():
    """Lazy import of mcp_wa_send to avoid circular dependencies"""
    global _mcp_wa_send
    if _mcp_wa_send is None:
        from agents.onboarding.wa_loop import mcp_wa_send
        _mcp_wa_send = mcp_wa_send
    return _mcp_wa_send


def _get_log_event():
    """Lazy import of log_event to avoid circular dependencies"""
    global _log_event
    if _log_event is None:
        from storage.event_logger import log_event
        _log_event = log_event
    return _log_event


def _get_handle_fulfillment():
    """Lazy import of fulfillment handler to avoid circular dependencies"""
    global _handle_fulfillment
    if _handle_fulfillment is None:
        from agents.fulfillment.handler import handle_fulfillment
        _handle_fulfillment = handle_fulfillment
    return _handle_fulfillment


async def handle_selection(phone: str, text: str, session: dict):
    """
    Main handler for Selection Agent state machine.
    
    Args:
        phone: WhatsApp phone number
        text: Inbound message text
        session: Current session data (state, profile, etc.) - modified in place
    """
    state = session.get("state", SelectionState.START)
    
    log.info(f"[SELECTION] Handling state={state} for {phone}, text='{text[:30]}...'")
    
    # Route to appropriate state handler
    if state == SelectionState.START:
        await handle_selection_start(phone, text, session)
    elif state == SelectionState.WAIT_VIDEO_DONE:
        await handle_selection_wait_video_done(phone, text, session)
    elif state == SelectionState.KNOWING_VOLUNTEER_LOOP:
        await handle_selection_knowing_volunteer_loop(phone, text, session)
    elif state == SelectionState.EVALUATE:
        await handle_selection_evaluate(phone, session)
    elif state == SelectionState.RECOMMENDED:
        await handle_selection_recommended(phone, session)
    elif state == SelectionState.NOT_RECOMMENDED:
        await handle_selection_not_recommended(phone, session)
    elif state == SelectionState.STOP:
        await handle_selection_stop(phone, session)
    else:
        log.warning(f"[SELECTION] Unknown state: {state}, defaulting to START")
        session["state"] = SelectionState.START
        await handle_selection_start(phone, "__kick__", session)


async def handle_selection_start(phone: str, text: str, session: dict):
    """
    Handle SEL_START state: entry point, send video intro + video link + done prompt.
    
    On __kick__: send video intro, video URL, done prompt, and advance to SEL_WAIT_VIDEO_DONE.
    """
    if text == "__kick__" or not session.get("_selection_started"):
        log.info(f"[SELECTION] Starting selection for {phone}")
        
        # Log SELECTION_STARTED event
        try:
            from storage.db import get_db_session
            log_event = _get_log_event()
            with get_db_session() as db:
                log_event(
                    db=db,
                    wa_phone=phone,
                    agent_name=settings.AGENT_NAME,
                    event_type="SELECTION_STARTED",
                    event_source="selection_agent",
                    state=SelectionState.START,
                    status="started"
                )
        except Exception as e:
            log.warning(f"[SELECTION] Failed to log SELECTION_STARTED event: {e}")
        
        # Get volunteer name from profile
        profile = session.get("profile", {})
        name = profile.get("name") or "there"
        
        # Send video intro
        mcp_wa_send = _get_mcp_wa_send()
        video_intro = get_sel_video_intro(name)
        await mcp_wa_send(phone, video_intro)
        
        # Send video URL
        await mcp_wa_send(phone, WELCOME_VIDEO_URL)
        
        # Send done prompt
        done_prompt = get_sel_video_done_prompt()
        await mcp_wa_send(phone, done_prompt)
        
        # Update session
        session["_selection_started"] = True
        session["state"] = SelectionState.WAIT_VIDEO_DONE
        session["ts"] = time.time()
        
        # Store in SESSIONS (imported from wa_loop)
        from agents.onboarding.wa_loop import SESSIONS
        SESSIONS[phone] = session


async def handle_selection_wait_video_done(phone: str, text: str, session: dict):
    """
    Handle SEL_WAIT_VIDEO_DONE state: wait for video acknowledgement.
    
    Interpret responses:
    - "done", "watched", "yes", "ok", "okay", "completed" -> proceed
    - Question -> answer briefly and re-ask
    - Cannot watch -> allow "skip" and proceed
    """
    text_lower = text.lower().strip() if text else ""
    
    # Check for done/confirmation keywords
    done_keywords = ["done", "watched", "yes", "ok", "okay", "completed", "finished", "viewed"]
    skip_keywords = ["skip", "cannot", "can't", "cant", "not now", "later"]
    
    if any(keyword in text_lower for keyword in done_keywords) or any(keyword in text_lower for keyword in skip_keywords):
        # Proceed to followup and about-you question
        log.info(f"[SELECTION] Video acknowledged by {phone}, proceeding")
        
        # Get volunteer name
        profile = session.get("profile", {})
        name = profile.get("name") or "there"
        
        mcp_wa_send = _get_mcp_wa_send()
        
        # Send followup
        followup = get_sel_video_followup()
        await mcp_wa_send(phone, followup)
        
        # Send about-you question
        about_you = get_sel_about_you(name)
        await mcp_wa_send(phone, about_you)
        
        # Store the about-you question as last agent prompt
        session["_last_agent_prompt"] = about_you
        
        # Transition to knowing volunteer loop (do NOT send kickoff question)
        session["state"] = SelectionState.KNOWING_VOLUNTEER_LOOP
        session["_knowing_volunteer_started"] = True  # Mark as started so it doesn't send kickoff
        session["ts"] = time.time()
        
        from agents.onboarding.wa_loop import SESSIONS
        SESSIONS[phone] = session
        
    elif "?" in text_lower or any(qword in text_lower for qword in ["what", "how", "when", "where", "why", "can you", "is it"]):
        # Question asked - answer briefly and re-ask
        log.info(f"[SELECTION] Question asked by {phone} while waiting for video")
        mcp_wa_send = _get_mcp_wa_send()
        await mcp_wa_send(phone, "It's a short welcome video from the SERVE team. When you're done watching, just reply *Done*")
        
        session["ts"] = time.time()
        from agents.onboarding.wa_loop import SESSIONS
        SESSIONS[phone] = session
    else:
        # Ambiguous response - re-ask
        log.info(f"[SELECTION] Ambiguous response from {phone}, re-asking video done prompt")
        mcp_wa_send = _get_mcp_wa_send()
        await mcp_wa_send(phone, get_sel_video_done_prompt())
        
        session["ts"] = time.time()
        from agents.onboarding.wa_loop import SESSIONS
        SESSIONS[phone] = session


async def handle_selection_knowing_volunteer_loop(phone: str, text: str, session: dict):
    """
    Handle SEL_KNOWING_VOLUNTEER_LOOP state: multi-turn conversation to understand volunteer.
    
    On entry (__kick__): send first question.
    For each user message: extract signals, send response, check completion.
    """
    # Initialize tool_state.selection if needed
    if "tool_state" not in session:
        session["tool_state"] = {}
    if "selection" not in session["tool_state"]:
        session["tool_state"]["selection"] = {}
    if "profile" not in session["tool_state"]["selection"]:
        session["tool_state"]["selection"]["profile"] = init_volunteer_profile()
        session["tool_state"]["selection"]["discussed_fields"] = set()
    if "question_index" not in session["tool_state"]["selection"]:
        session["tool_state"]["selection"]["question_index"] = 0
    
    # Get last agent prompt from session
    last_agent_prompt = session.get("_last_agent_prompt")
    
    # Get conversation history
    history_messages = []
    try:
        from agents.onboarding.wa_loop import _get_conversation_history
        history = _get_conversation_history(phone)
        if history and hasattr(history, 'messages'):
            for msg in history.messages[-6:]:  # Last 6 messages
                if hasattr(msg, 'role') and hasattr(msg, 'content'):
                    role = "user" if msg.role.value.lower() == "user" else "assistant"
                    content = str(msg.content)
                    history_messages.append({"role": role, "content": content})
    except Exception as e:
        log.warning(f"[SELECTION] Failed to get conversation history: {e}")
    
    # On first entry, check if we should send kickoff question
    # Only send if not already started (i.e., not coming from video flow)
    if text == "__kick__" and not session.get("_knowing_volunteer_started"):
        log.info(f"[SELECTION] Starting knowing volunteer loop for {phone}")
        session["_knowing_volunteer_started"] = True
        
        # Get volunteer name
        profile = session.get("profile", {})
        name = profile.get("name") or "there"
        
        # Send initial question (only if not already asked in video flow)
        if not session.get("_last_agent_prompt"):
            initial_question = get_sel_about_you(name)
            mcp_wa_send = _get_mcp_wa_send()
            await mcp_wa_send(phone, initial_question)
            
            # Store as last agent prompt
            session["_last_agent_prompt"] = initial_question
            
            # Add to history
            try:
                from agents.onboarding.wa_loop import _add_to_history
                _add_to_history(phone, bot_msg=initial_question)
            except:
                pass
        
        session["ts"] = time.time()
        from agents.onboarding.wa_loop import SESSIONS
        SESSIONS[phone] = session
        return
    
    # If user sends a message (not __kick__), process it
    if text != "__kick__":
        # Process user message normally
        pass
    
    # Process user message
    log.info(f"[SELECTION] Processing knowing volunteer step for {phone}")
    
    # Log SELECTION_KNOWING_VOLUNTEER_STEP event
    question_index = session["tool_state"]["selection"].get("question_index", 0)
    try:
        from storage.db import get_db_session
        log_event = _get_log_event()
        with get_db_session() as db:
            log_event(
                db=db,
                wa_phone=phone,
                agent_name=settings.AGENT_NAME,
                event_type="SELECTION_KNOWING_VOLUNTEER_STEP",
                event_source="selection_agent",
                state=SelectionState.KNOWING_VOLUNTEER_LOOP,
                status="processing",
                details={"question_index": question_index}
            )
    except Exception as e:
        log.warning(f"[SELECTION] Failed to log KNOWING_VOLUNTEER_STEP event: {e}")
    
    # Run knowing volunteer step
    try:
        result = await run_knowing_volunteer_step(
            session=session,
            user_text=text,
            last_agent_prompt=last_agent_prompt,
            history_messages=history_messages
        )
    except Exception as e:
        log.error(f"[SELECTION] Error in knowing volunteer step: {e}", exc_info=True)
        # Fallback: continue with ambiguous response
        result = {
            "intent": "AMBIGUOUS",
            "confidence": 0.0,
            "assistant_text": "I see. Could you tell me a bit more about yourself?",
            "signals": session["tool_state"]["selection"].get("profile", {}),
            "decision": KnowingVolunteerResult.CONTINUE.value
        }
    
    decision = result.get("decision")
    assistant_text = result.get("assistant_text", "")
    
    # Add user message to history
    try:
        from agents.onboarding.wa_loop import _add_to_history
        _add_to_history(phone, user_msg=text)
    except:
        pass
    
    # Handle decision
    if decision == KnowingVolunteerResult.CONTINUE.value:
        # Continue: send assistant text and remain in loop
        if assistant_text:
            mcp_wa_send = _get_mcp_wa_send()
            await mcp_wa_send(phone, assistant_text)
            session["_last_agent_prompt"] = assistant_text
            
            # Add to history
            try:
                from agents.onboarding.wa_loop import _add_to_history
                _add_to_history(phone, bot_msg=assistant_text)
            except:
                pass
        
        session["ts"] = time.time()
        from agents.onboarding.wa_loop import SESSIONS
        SESSIONS[phone] = session
        
    elif decision in [KnowingVolunteerResult.COMPLETE.value, KnowingVolunteerResult.COMPLETE_INSUFFICIENT_INFO.value]:
        # Complete: check if we should send final response, then transition to evaluation
        log.info(f"[SELECTION] Knowing volunteer complete for {phone}, decision: {decision}")
        
        # Only send assistant_text if it doesn't contain a question (to avoid orphaned questions)
        # The "thanks" will be included in the recommended message instead
        if assistant_text and "?" not in assistant_text:
            mcp_wa_send = _get_mcp_wa_send()
            await mcp_wa_send(phone, assistant_text)
            session["_last_agent_prompt"] = assistant_text
            
            # Add to history
            try:
                from agents.onboarding.wa_loop import _add_to_history
                _add_to_history(phone, bot_msg=assistant_text)
            except:
                pass
        elif assistant_text and "?" in assistant_text:
            log.info(f"[SELECTION] Skipping question in assistant_text since we're completing: {assistant_text[:50]}...")
        
        session["state"] = SelectionState.EVALUATE
        session["ts"] = time.time()
        
        from agents.onboarding.wa_loop import SESSIONS
        SESSIONS[phone] = session
        
        # Evaluate
        await handle_selection_evaluate(phone, session)
        
    elif decision == KnowingVolunteerResult.STOP.value:
        # Stop: transition to stop state
        log.info(f"[SELECTION] Volunteer {phone} requested to stop")
        session["state"] = SelectionState.STOP
        session["ts"] = time.time()
        
        from agents.onboarding.wa_loop import SESSIONS
        SESSIONS[phone] = session
        
        # Handle stop
        await handle_selection_stop(phone, session)


async def handle_selection_evaluate(phone: str, session: dict):
    """
    Handle SEL_EVALUATE state: internal evaluation, compute decision.
    
    Evaluation logic:
    - Default is recommended=true
    - Hold for human follow-up if ANY of these are true:
      - user_stop_requested == True (STOP state)
      - profile.teaching_interest == "no"
      - profile.commitment_horizon == "no"
    
    Routing:
    - If recommended -> transition to Fulfillment
    - If hold_for_human -> send coordinator follow-up message and end (no Fulfillment)
    """
    log.info(f"[SELECTION] Evaluating for {phone}")
    
    # Get profile from tool_state
    profile = session.get("tool_state", {}).get("selection", {}).get("profile", {})
    
    # Derive basic signals
    teaching_interest = profile.get("teaching_interest")
    commitment_horizon = profile.get("commitment_horizon")
    
    # User stop requested (via STOP state)
    user_stop_requested = session.get("state") == SelectionState.STOP
    
    # Compute hold_for_human based on guardrails
    reason_codes = []
    if user_stop_requested:
        reason_codes.append("USER_STOP_REQUESTED")
    if teaching_interest == "no":
        reason_codes.append("TEACHING_INTEREST_NO")
    if commitment_horizon == "no":
        reason_codes.append("COMMITMENT_HORIZON_NO")
    
    hold_for_human = len(reason_codes) > 0
    
    # Recommended is simply the inverse of hold_for_human
    recommended = not hold_for_human
    
    outcome = RecommendationOutcome.RECOMMENDED if recommended else RecommendationOutcome.NOT_RECOMMENDED
    
    # Count signals present
    signals_present_count = sum([
        1 if profile.get("motivation") else 0,
        1 if profile.get("has_teaching_experience") is not None else 0,
        1 if profile.get("commitment_horizon") else 0,
        1 if profile.get("language") or profile.get("language_comfort") else 0,  # Language signal counts if either field is present
        1 if profile.get("teaching_interest") else 0,
    ])
    
    # Store decision in session
    session["selection"] = session.get("selection", {})
    session["selection"]["recommended"] = recommended
    session["selection"]["outcome"] = outcome.value
    
    # Store in tool_state.selection.outcome
    if "tool_state" not in session:
        session["tool_state"] = {}
    if "selection" not in session["tool_state"]:
        session["tool_state"]["selection"] = {}
    session["tool_state"]["selection"]["outcome"] = {
        "recommended": recommended,
        "hold_for_human": hold_for_human,
        "reason_codes": reason_codes,
        "mode": "lite",
        "signals": profile.copy()
    }
    
    # Log SELECTION_EVALUATED event
    try:
        from storage.db import get_db_session
        log_event = _get_log_event()
        with get_db_session() as db:
            log_event(
                db=db,
                wa_phone=phone,
                agent_name=settings.AGENT_NAME,
                event_type="SELECTION_EVALUATED",
                event_source="selection_agent",
                state=SelectionState.EVALUATE,
                status="completed",
                details={
                    "recommended": recommended,
                    "hold_for_human": hold_for_human,
                    "reason_codes": reason_codes,
                    "signals_present_count": signals_present_count
                }
            )
    except Exception as e:
        log.warning(f"[SELECTION] Failed to log SELECTION_COMPLETED event: {e}")
    
    # Move to appropriate state
    if recommended:
        session["state"] = SelectionState.RECOMMENDED
        session["ts"] = time.time()
        
        from agents.onboarding.wa_loop import SESSIONS
        SESSIONS[phone] = session
        
        # Handle recommended outcome
        await handle_selection_recommended(phone, session)
    else:
        session["state"] = SelectionState.NOT_RECOMMENDED
        session["ts"] = time.time()
        
        from agents.onboarding.wa_loop import SESSIONS
        SESSIONS[phone] = session
        
        # Handle not recommended outcome
        await handle_selection_not_recommended(phone, session)


async def handle_selection_stop(phone: str, session: dict):
    """
    Handle SEL_STOP state: user requested to stop, send graceful exit message.
    """
    log.info(f"[SELECTION] Volunteer {phone} stopped selection")
    
    # Send graceful exit message
    exit_msg = """No problem at all 🌿 

If you'd like to continue later, you can message me here anytime.

Thank you for your interest in SERVE! 💛"""
    
    mcp_wa_send = _get_mcp_wa_send()
    await mcp_wa_send(phone, exit_msg)
    
    # Mark session as ended
    session["state"] = "CLOSE"
    session["ended"] = True
    session["ts"] = time.time()
    
    from agents.onboarding.wa_loop import SESSIONS
    SESSIONS[phone] = session


async def handle_selection_recommended(phone: str, session: dict):
    """
    Handle SEL_RECOMMENDED state: send message and transition to Fulfillment agent.
    """
    log.info(f"[SELECTION] Volunteer {phone} is recommended, transitioning to Fulfillment")
    
    # Get volunteer name from profile
    profile = session.get("profile", {})
    name = profile.get("name") or "there"
    
    # Send recommended message with thanks
    mcp_wa_send = _get_mcp_wa_send()
    recommended_msg = get_sel_recommended_msg(name)
    await mcp_wa_send(phone, recommended_msg)
    
    # Transition to Fulfillment agent
    session["state"] = "FULFILL_INTRO"  # Fulfillment agent entry state
    session["agent"] = "fulfillment"  # Mark current agent
    session["ts"] = time.time()
    
    from agents.onboarding.wa_loop import SESSIONS
    SESSIONS[phone] = session
    
    # Call Fulfillment handler with __kick__
    handle_fulfillment = _get_handle_fulfillment()
    await handle_fulfillment(phone, "__kick__", session)


async def handle_selection_not_recommended(phone: str, session: dict):
    """
    Handle SEL_NOT_RECOMMENDED state: send coordinator follow-up message and exit gracefully.
    """
    log.info(f"[SELECTION] Volunteer {phone} is on hold for human follow-up")
    
    # Get volunteer name from profile (if available)
    profile = session.get("profile", {})
    name = profile.get("name") or "there"
    
    # Coordinator follow-up message (no transition to Fulfillment)
    hold_msg = (
        f"Thank you, {name} 💛 A SERVE Coordinator will get in touch with you to guide the next step. "
        "If you have questions meanwhile, you can message here anytime."
    )
    
    mcp_wa_send = _get_mcp_wa_send()
    await mcp_wa_send(phone, hold_msg)
    
    # Mark session as ended
    session["state"] = "CLOSE"
    session["ended"] = True
    session["ts"] = time.time()
    
    from agents.onboarding.wa_loop import SESSIONS
    SESSIONS[phone] = session
