import os, uvicorn
from agents.onboarding.app import build_app as build_onboarding
from common.logging import setup_logging
from fastapi import FastAPI, Body
from pydantic import BaseModel
from typing import Optional, Dict, Any
import asyncio
from agents.onboarding.wa_loop import wa_loop, start_onboarding

AGENT_NAME = os.getenv("AGENT_NAME","onboarding")

app = FastAPI(title="serve-vm-agents")

if AGENT_NAME != "onboarding":
    raise RuntimeError(f"Only onboarding is wired in this slice. Got AGENT_NAME={AGENT_NAME}")

app = build_onboarding()

if __name__ == "__main__":
    # Fast local run; uvicorn CLI also works
    port = int(os.getenv("PORT","8001"))
    uvicorn.run("service.main:app", host="0.0.0.0", port=port, reload=False)

@app.on_event("startup")
async def startup():
    asyncio.create_task(wa_loop())

class OnboardingRequest(BaseModel):
    phone: str
    name: str = "Volunteer"
    registration_data: Optional[Dict[str, Any]] = None

@app.post("/agents/onboarding/start")
async def start_onboarding_api(request: OnboardingRequest):
    """Start onboarding for a phone number with optional registration data"""
    phone = (request.phone or "").lstrip("+").strip()
    if not phone:
        return {"ok": False, "error": "phone required (E.164 without +, e.g. 9198xxxxxxx)"}
    
    await start_onboarding(phone, request.name, request.registration_data)
    return {"ok": True, "message": f"Onboarding started for {phone}"}

@app.get("/healthz")
async def healthz():
    return {"ok": True}
