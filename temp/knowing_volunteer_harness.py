import asyncio
import json

from agents.selection import knowing_volunteer_engine as kve


class StubMCP:
    def __init__(self):
        self.turn = 0

    async def __call__(self, method, payload, timeout=20):
        self.turn += 1
        # Identify planner vs extractor
        system_text = " ".join(
            m.get("content", "") for m in payload.get("messages", []) if m.get("role") == "system"
        )
        if "planning module" in system_text:
            # Simple planner: ask commitment first, then stop
            if self.turn == 1:
                resp = {
                    "next_target": "commitment_horizon",
                    "question": "Would you be open to volunteering for around 3 months?",
                    "expected_answer_type": "yes_no_maybe",
                    "why_internal": "critical rubric",
                }
            else:
                resp = {
                    "next_target": "stop",
                    "question": "Thanks for sharing — I have enough to suggest a next step.",
                    "expected_answer_type": "free_text",
                    "why_internal": "critical resolved",
                }
            return {"content": [{"type": "text", "text": json.dumps(resp)}]}

        # Extractor: map common replies
        user_text = payload.get("messages", [])[-1].get("content", "")
        if "not sure" in user_text.lower():
            resp = {
                "extracted": {
                    "commitment_horizon": {"value": "maybe", "confidence": 0.6}
                },
                "rubric_status_updates": {"commitment_horizon": "partial"},
                "needs_clarification": True,
                "clarification_question": "Would it help if you could try for a shorter window first?",
                "notes_internal": "uncertain commitment",
            }
        else:
            resp = {
                "extracted": {
                    "commitment_horizon": {"value": "yes", "confidence": 0.9}
                },
                "rubric_status_updates": {"commitment_horizon": "resolved"},
                "needs_clarification": False,
                "clarification_question": "",
                "notes_internal": "clear commitment",
            }
        return {"content": [{"type": "text", "text": json.dumps(resp)}]}


async def main():
    # Monkeypatch MCP call
    kve._mcp_call = StubMCP()
    session = {"tool_state": {"selection": {"profile": kve.init_volunteer_profile()}}}

    # Simulate a turn with unsure commitment
    result = await kve.run_knowing_volunteer_step(
        session=session,
        user_text="I am not sure",
        last_agent_prompt="Would you be open to volunteering for around 3 months?",
        history_messages=None,
    )
    print("Result:", result)


if __name__ == "__main__":
    asyncio.run(main())

