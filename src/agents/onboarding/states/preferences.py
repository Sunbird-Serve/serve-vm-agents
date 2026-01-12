"""
PREFERENCES State Handler (State 5: Day & Time Preferences Collection)
Reuses the old PREFS_DAYTIME logic
"""
import logging
import time
import re
import asyncio
from typing import Dict, Any
from ..messages import (
    PREFS_INTRO_COLLAB, PREFS_FOLLOWUP_DAYS, PREFS_FOLLOWUP_TIME,
    PREFS_WEEKEND_NOTE, PREFS_EVENING_NUDGE, PREFS_CONFIRM_DEFAULT,
    PREFS_SUMMARY_FALLBACK, format_message
)

log = logging.getLogger(__name__)

# Weekend-only detection and nudge message
PREFS_WEEKEND_ONLY_NUDGE = """Noted 😊 Most sessions run on weekdays. If you can do any one weekday (Mon–Fri), tell me. If weekends are the only option, reply: Weekend only."""


async def handle_preferences(phone: str, text: str, sess: Dict[str, Any], profile: Dict[str, Any]) -> None:
    """
    Handle PREFERENCES state - collect day and time preferences
    
    Args:
        phone: Phone number (session key)
        text: User's message
        sess: Session dict
        profile: Profile dict
    """
    # Late import to avoid circular dependency
    from ..wa_loop import (
        mcp_wa_send, _add_to_history, _handle, SESSIONS,
        _generate_prefs_interpretation, _generate_prefs_summary_phone,
        mcp_preferences_save, mcp_deferral_create
    )
    
    # LOG: Entry point
    log.info(f"[PREFS] Handler called for {phone}, text='{text[:50]}...', state={sess.get('state')}, "
             f"_prefs_confirmed={sess.get('_prefs_confirmed')}, "
             f"_prefs_confirmation_sent={sess.get('_prefs_confirmation_sent')}, "
             f"_prefs_summary_sent={sess.get('_prefs_summary_sent')}")
    
    # Early return if already transitioned to next state (idempotency)
    if sess.get("state") != "PREFERENCES" and sess.get("state") != "PREFS_DAYTIME":
        log.info(f"[PREFS] State is already {sess.get('state')}, skipping preferences handler")
        return
    
    # Early return if preferences have already been confirmed (idempotency)
    if sess.get("_prefs_confirmed"):
        log.warning(f"[PREFS] DUPLICATE CALL DETECTED: Preferences already confirmed for {phone}, "
                   f"state={sess.get('state')}, skipping handler")
        # Just ensure state is correct and transition if needed
        if sess.get("state") != "QA_WINDOW":
            sess["state"] = "QA_WINDOW"
            sess["_qa_count"] = 0
            sess["_qa_topics"] = []
            sess["_qa_summary_sent"] = False
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            await asyncio.sleep(0.5)
            await _handle(phone, "__kick__")
        return
    
    if text == "__kick__" or not sess.get("_prefs_prompted"):
        await mcp_wa_send(phone, PREFS_INTRO_COLLAB)
        _add_to_history(phone, bot_msg=PREFS_INTRO_COLLAB)
        sess["_prefs_prompted"] = True
        sess.setdefault("_prefs_days", [])
        sess.setdefault("_prefs_time_band", None)
        sess["_prefs_evening_attempts"] = 0
        sess["_prefs_last_prompt"] = "intro"
        sess["_prefs_last_prompt_text"] = PREFS_INTRO_COLLAB
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
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
        sess["_deferred_prev_state"] = "PREFERENCES"
        sess["_deferred_reason"] = "PREFS_LATER"
        sess["state"] = "DEFERRED"
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return
    elif interpretation.get("followup"):
        await mcp_wa_send(phone, interpretation["followup"])
        _add_to_history(phone, bot_msg=interpretation["followup"])
        sess["_prefs_last_prompt"] = interpretation.get("followup_tag")
        sess["_prefs_last_prompt_text"] = interpretation["followup"]
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        return

    # Check for weekend-only input BEFORE checking if days is empty
    text_lower = text.lower().strip()
    weekend_only_patterns = [
        r"\bweekend\s+only\b",
        r"\bonly\s+weekend\b",
        r"\bweekends?\s+only\b",
        r"\bonly\s+weekends?\b",
        r"\b(sat|saturday|sun|sunday)\s+only\b",
        r"\bonly\s+(sat|saturday|sun|sunday)\b"
    ]
    
    # Check if user explicitly confirms "Weekend only" after nudge
    if sess.get("_prefs_weekend_nudge_sent"):
        if "weekend only" in text_lower or any(re.search(pattern, text_lower) for pattern in weekend_only_patterns):
            # User confirmed weekend-only - accept and proceed
            log.info(f"[PREFS] User confirmed weekend-only availability for {phone}")
            sess["weekend_only"] = True
            sess["available_days"] = ["Sat", "Sun"]
            # Set days to weekend days for processing
            days = ["SAT", "SUN"]
            sess["_prefs_days"] = days
            sess.pop("_prefs_weekend_nudge_sent", None)
            # Continue with normal flow
        else:
            # User provided weekday(s) after nudge - clear nudge flag and continue
            sess.pop("_prefs_weekend_nudge_sent", None)
    
    # Detect weekend-only input (if nudge not already sent)
    if not sess.get("_prefs_weekend_nudge_sent"):
        # Check if only weekend days are mentioned
        weekend_days_in_text = []
        weekday_days_in_text = []
        
        day_patterns = {
            "monday": "MON", "mon": "MON",
            "tuesday": "TUE", "tue": "TUE",
            "wednesday": "WED", "wed": "WED",
            "thursday": "THU", "thu": "THU", "thur": "THU",
            "friday": "FRI", "fri": "FRI",
            "saturday": "SAT", "sat": "SAT",
            "sunday": "SUN", "sun": "SUN",
        }
        
        for token, iso in day_patterns.items():
            if re.search(rf"\b{re.escape(token)}\b", text_lower):
                if iso in ["SAT", "SUN"]:
                    weekend_days_in_text.append(iso)
                else:
                    weekday_days_in_text.append(iso)
        
        # Check for explicit weekend-only phrases
        has_weekend_only_phrase = any(re.search(pattern, text_lower) for pattern in weekend_only_patterns)
        
        # If only weekend days mentioned AND no weekdays, send nudge
        if (weekend_days_in_text and not weekday_days_in_text) or has_weekend_only_phrase:
            if not sess.get("_prefs_weekend_nudge_sent"):
                log.info(f"[PREFS] Weekend-only input detected for {phone}, sending nudge")
                await mcp_wa_send(phone, PREFS_WEEKEND_ONLY_NUDGE)
                _add_to_history(phone, bot_msg=PREFS_WEEKEND_ONLY_NUDGE)
                sess["_prefs_weekend_nudge_sent"] = True
                sess["_prefs_last_prompt"] = "weekend_nudge"
                sess["_prefs_last_prompt_text"] = PREFS_WEEKEND_ONLY_NUDGE
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                return
    
    if not days:
        followup = PREFS_FOLLOWUP_DAYS
        await mcp_wa_send(phone, followup)
        _add_to_history(phone, bot_msg=followup)
        sess["_prefs_last_prompt"] = "days_followup"
        sess["_prefs_last_prompt_text"] = followup
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
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

    # Double-check idempotency right before sending messages (defense in depth)
    if sess.get("_prefs_confirmed"):
        log.warning(f"[PREFS] Preferences already confirmed for {phone} (duplicate call detected), skipping messages")
        # Just ensure state is correct and transition
        if sess.get("state") != "QA_WINDOW":
            sess["state"] = "QA_WINDOW"
            sess["_qa_count"] = 0
            sess["_qa_topics"] = []
            sess["_qa_summary_sent"] = False
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            await asyncio.sleep(0.5)
            await _handle(phone, "__kick__")
        return

    # Mark as confirmed IMMEDIATELY before any message sending to prevent race conditions
    # This must happen before any await calls to ensure flag is set synchronously
    log.info(f"[PREFS] Setting _prefs_confirmed=True for {phone} before sending messages")
    sess["_prefs_confirmed"] = True
    sess["ts"] = time.time()
    # Persist immediately to make flag visible to concurrent calls
    SESSIONS[phone] = sess
    log.info(f"[PREFS] Persisted _prefs_confirmed=True to SESSIONS for {phone}")

    # Try to save preferences first (will fail for now, but that's okay)
    vid = profile.get("uuid")
    if vid and str(vid).upper() not in {"NONE", "UNKNOWN"}:
        try:
            await mcp_preferences_save(vid, time_band)
            log.info(f"[PREFS] preferences.save succeeded for {phone}")
        except Exception as e:
            # Non-blocking: log warning but continue flow
            log.warning(f"[PREFS] preferences.save failed (continuing anyway): {e}")

    # Send only summary message (with idempotency check)
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
        if not sess.get("_prefs_summary_sent"):
            log.info(f"[PREFS] Sending summary message for {phone}")
            sess["_prefs_summary_sent"] = True
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            await mcp_wa_send(phone, summary_msg)
            _add_to_history(phone, bot_msg=summary_msg)
            log.info(f"[PREFS] Summary message sent for {phone}")
        else:
            log.warning(f"[PREFS] DUPLICATE: Summary already sent for {phone}, skipping duplicate")
    
    sess["_prefs_last_prompt"] = None
    sess["_prefs_last_prompt_text"] = None
    sess.pop("_prefs_evening_attempts", None)

    # Transition to QA_WINDOW state
    log.info(f"[PREFS] Transitioning to QA_WINDOW for {phone}")
    sess["state"] = "QA_WINDOW"
    sess["_qa_count"] = 0
    sess["_qa_topics"] = []
    sess["_qa_summary_sent"] = False
    sess["ts"] = time.time()
    SESSIONS[phone] = sess
    log.info(f"[PREFS] State transitioned to QA_WINDOW and persisted for {phone}")

    await asyncio.sleep(0.5)
    log.info(f"[PREFS] Calling _handle(phone, '__kick__') for {phone} to trigger QA_WINDOW")
    await _handle(phone, "__kick__")
    log.info(f"[PREFS] Handler completed for {phone}")
    return
