from typing import Any, Dict, List, Optional


def determine_conversation_stage(sess: Dict[str, Any], state: str) -> str:
    """
    Return a coarse hint about where we are in the conversation.
    """
    if state != "WELCOME":
        return "progressed"

    if not sess.get("_greet_sent"):
        return "first_contact"

    greet_step = sess.get("_greet_step")
    if greet_step in {None, "await_continue"}:
        return "consent_followup"

    if sess.get("_welcomed"):
        return "reengagement"

    return "consent_followup"


def build_llm_context(
    state: str,
    sess: Dict[str, Any],
    *,
    last_prompt: Optional[str] = None,
    history_snippet: Optional[List[Dict[str, str]]] = None,
) -> Dict[str, Any]:
    profile: Dict[str, Any] = dict(sess.get("profile", {}) or {})
    profile.setdefault("locale", "en-IN")

    context: Dict[str, Any] = {
        "state": state,
        "user_profile": profile,
        "conversation_stage_hint": determine_conversation_stage(sess, state),
    }

    if last_prompt:
        context["last_agent_prompt"] = last_prompt

    if history_snippet:
        context["history_snippet"] = history_snippet

    return context

