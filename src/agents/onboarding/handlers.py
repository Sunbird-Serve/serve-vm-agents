import time, uuid, logging
from common.mcp_client import MCPClient
from .conversation import run_conversation

log = logging.getLogger("onboarding.handlers")

def _mock_profile(volunteer_id: str) -> dict:
    return {
      "volunteer_id": volunteer_id,
      "profile": {
        "subjects": ["Math"],
        "grades": [6],
        "languages": ["en","hi"],
        "timezone": "Asia/Kolkata",
        "availability": [{"dow":"Sat","start":"10:00","end":"11:00"}],
        "device_ok": True, "bandwidth_ok": True, "consent": True
      }
    }


async def handle_message(evt, key, publish, mcp, topic_onboarding):
    etype = evt.get("type")

    if etype == "onboarding.task.start.v1":
        data = evt.get("data", {})
        vid = data.get("volunteer_id")
        locale = data.get("locale", "en-IN")

        # 🗨️ Start interactive conversation
        collected = await run_conversation(vid, locale, mcp)

        out = {
            "type": "onboarding.profile.collected.v1",
            "id": str(uuid.uuid4()),
            "time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "data": collected,
        }
        await publish(topic_onboarding, key=vid, value=out)
        log.info("Published profile for %s", vid)
