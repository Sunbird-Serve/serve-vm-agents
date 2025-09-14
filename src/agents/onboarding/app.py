import asyncio, logging
from fastapi import FastAPI
from common.bus import Bus
from common.logging import setup_logging
from common.mcp_client import MCPClient
from .config import settings
from .handlers import handle_message

def build_app() -> FastAPI:
    setup_logging()
    app = FastAPI(title="vm-agent-onboarding")
    bus = Bus(brokers=settings.KAFKA_BROKERS)
    mcp = MCPClient(base=settings.MCP_BASE)

    @app.get("/healthz")
    def healthz(): return {"ok": True}

    async def loop():
        await bus.start_producer()
        await bus.start_consumer(settings.TOPIC_ONBOARDING, group_id=settings.GROUP_ID)
        try:
            async for msg in bus.consumer:
                await handle_message(msg.value, msg.key, bus.publish, mcp, settings.TOPIC_ONBOARDING)
        finally:
            await bus.stop()

    @app.on_event("startup")
    async def startup():
        app.state.t = asyncio.create_task(loop())

    @app.on_event("shutdown")
    async def shutdown():
        app.state.t.cancel()

    return app
