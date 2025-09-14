import logging
from common.aconsole import ainput
from common.mcp_client import MCPClient

log = logging.getLogger("conversation")

async def run_conversation(volunteer_id: str, locale: str, mcp: MCPClient) -> dict:
    frame = {
        "subjects": [],
        "grades": [],
        "languages": [],
        "timezone": None,
        "availability": [],
        "device_ok": None,
        "bandwidth_ok": None,
        "consent": None,
    }

    print(f"\n👋 Hello {volunteer_id}! Let's set up your profile.\n")

    # Q1
    ans = await ainput("1) Which subjects would you like to teach? (e.g., Math, English) ")
    delta = await mcp.parse_onboarding_answer(ans, locale)
    frame.update(delta["normalized_delta"])

    # Q2
    ans = await ainput("2) Which grades can you handle? (e.g., 6, 7, 8) ")
    delta = await mcp.parse_onboarding_answer(ans, locale)
    frame.update(delta["normalized_delta"])

    # Q3
    ans = await ainput("3) Your timezone? (e.g., Asia/Kolkata) ")
    delta = await mcp.parse_onboarding_answer(ans, locale)
    frame.update(delta["normalized_delta"])

    # Q4
    ans = await ainput("4) When are you available? (e.g., Sat 10:00-11:00, Sun 16:00-17:00) ")
    delta = await mcp.parse_onboarding_answer(ans, locale)
    frame.update(delta["normalized_delta"])

    # Q5
    ans = await ainput("5) Do you have a working device and stable internet? (yes/no) ")
    delta = await mcp.parse_onboarding_answer(ans, locale)
    frame.update(delta["normalized_delta"])

    # Q6
    ans = await ainput("6) Do you consent to be contacted for classes? (yes/no) ")
    delta = await mcp.parse_onboarding_answer(ans, locale)
    frame.update(delta["normalized_delta"])

    print("\n✅ Thanks! Collected your profile.\n")
    return {"volunteer_id": volunteer_id, "profile": frame}
