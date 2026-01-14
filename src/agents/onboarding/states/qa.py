"""
QA_WINDOW State Handler (State 6: Questions & Answers)
"""
import logging
import time
import re
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any
from ..messages import (
    QA_ENTRY_PROMPT, QA_STOP_ACK, QA_DEFERRAL_PROMPT,
    QA_FAQ_ABOUT_SERVE, QA_FAQ_TIME_PROCESS, QA_FAQ_SUPPORT,
    QA_FAQ_CERTIFICATE, QA_FAQ_SUBJECTS_GRADES, QA_FAQ_TECH
)
from ..validators import is_no_response

log = logging.getLogger(__name__)


async def handle_qa_window(phone: str, text: str, sess: Dict[str, Any], profile: Dict[str, Any]) -> None:
    """
    Handle QA_WINDOW state - short Q&A window before completion
    
    Args:
        phone: Phone number
        text: User's message
        sess: Session dict
        profile: Profile dict
    """
    # Late import to avoid circular dependency
    from ..wa_loop import (
        mcp_wa_send, _add_to_history, _handle, SESSIONS,
        mcp_telemetry_emit, mcp_deferral_create, mcp_state_get,
    )
    from ..faq import retrieve, compose_answer
    
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
    
    # If user indicates they're done with questions, transition to COMPLETE
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
            sess["_deferred_prev_state"] = sess.get("state")
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
            
            # Emit telemetry
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
            
            # After answering, check if max turns reached, otherwise stay in QA_WINDOW
            if qa_count >= 2:
                # Max turns reached, transition to COMPLETE
                await asyncio.sleep(0.5)
                sess["state"] = "COMPLETE"
                sess["ts"] = time.time()
                SESSIONS[phone] = sess
                await _handle(phone, "__kick__")
            return
    
    # E) LLM + KB (single pipeline: retrieve from faqs.jsonl + compose_answer via faq.answer)
    if not matched_bucket:
        route = "LLM"
        
        # Retrieve top KB entries from local FAQ KB
        context_entries = retrieve(text, k=3)
        answer = ""
        try:
            if context_entries:
                answer = await compose_answer(text, context_entries)
        except Exception as e:
            log.warning(f"[QA] FAQ compose_answer failed: {e}")
        
        # Fallback if LLM+KB failed or no context
        if not answer:
            answer = (
                "I might not have the perfect answer right now. Our coordinator will cover this in orientation."
            )
        
        if "custom" not in qa_topics:
            qa_topics.append("custom")
        await mcp_wa_send(phone, answer)
        _add_to_history(phone, bot_msg=answer)
        qa_count += 1
        sess["_qa_count"] = qa_count
        sess["ts"] = time.time()
        SESSIONS[phone] = sess
        
        # Emit telemetry
        try:
            await mcp_telemetry_emit("onboarding.qa_answered", {
                "conversation_id": phone,
                "user_id": volunteer_id,
                "qa_count": qa_count,
                "route": "LLM",
                "classifier_conf": classifier_conf,
                "faq_bucket": faq_bucket,
                "policy_version": sess.get("_policy_version"),
                "knowledge_version": sess.get("_knowledge_version"),
            })
        except Exception:
            pass
        
        # After answering, check if max turns reached, otherwise stay in QA_WINDOW
        if qa_count >= 2:
            # Max turns reached, transition to COMPLETE
            await asyncio.sleep(0.5)
            sess["state"] = "COMPLETE"
            sess["ts"] = time.time()
            SESSIONS[phone] = sess
            await _handle(phone, "__kick__")
        return
    
    # Should not reach here, but handle gracefully
    unclear = "I'd be happy to answer your question. Could you rephrase it, or would you like to proceed?"
    await mcp_wa_send(phone, unclear)
    _add_to_history(phone, bot_msg=unclear)
    sess["ts"] = time.time()
    SESSIONS[phone] = sess
    return

