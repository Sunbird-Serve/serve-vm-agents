import os, uvicorn
from agents.onboarding.app import build_app as build_onboarding
from common.logging import setup_logging

AGENT_NAME = os.getenv("AGENT_NAME","onboarding")

if AGENT_NAME != "onboarding":
    raise RuntimeError(f"Only onboarding is wired in this slice. Got AGENT_NAME={AGENT_NAME}")

app = build_onboarding()

if __name__ == "__main__":
    # Fast local run; uvicorn CLI also works
    port = int(os.getenv("PORT","8001"))
    uvicorn.run("service.main:app", host="0.0.0.0", port=port, reload=False)
