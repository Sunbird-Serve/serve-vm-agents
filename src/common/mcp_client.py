import httpx, uuid, time, logging, os
log = logging.getLogger("mcp")

class MCPClient:
    def __init__(self, base: str):
        self.base = base.rstrip("/")

    async def parse_onboarding_answer(self, user_text: str, locale: str = "en-IN") -> dict:
        # For MVP, return a deterministic, "already-complete" profile
        return {
            "normalized_delta": {
                "subjects": ["Math"],
                "grades": [6],
                "languages": ["en","hi"],
                "timezone": "Asia/Kolkata",
                "availability": [{"dow":"Sat","start":"10:00","end":"11:00"}],
                "device_ok": True, "bandwidth_ok": True, "consent": True
            }
        }

    async def next_question(self, frame: dict, last_user_text: str, locale: str = "en-IN") -> dict:
        # For MVP, immediately finish
        return { "done": True, "ask": None }

    async def meet_create(self, title: str, start: str, end: str, attendees: list[dict]) -> dict:
        # Stub a "Google Meet" result
        return {
            "meet_link": f"https://meet.google.com/dev-{uuid.uuid4().hex[:6]}",
            "calendar_event_id": f"cal_dev_{uuid.uuid4().hex[:8]}",
            "start": start, "end": end
        }
