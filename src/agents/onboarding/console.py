import asyncio, logging, time, uuid
from common.logging import setup_logging
from common.bus import Bus
from common.mcp_client import MCPClient
from .config import settings
from .conversation import run_conversation

log = logging.getLogger("onboarding.console")

async def main():
    setup_logging()
    brokers = settings.KAFKA_BROKERS
    topic = settings.TOPIC_ONBOARDING
    group = settings.GROUP_ID
    mcp = MCPClient(base=settings.MCP_BASE)

    bus = Bus(brokers=brokers)
    await bus.start_producer()
    await bus.start_consumer(topic, group_id=group)
    try:
        # avoid emoji to keep Windows CP1252 happy
        print(f"[Console] Listening on topic '{topic}' (group: {group})…")

        async for msg in bus.consumer:
            evt, key = msg.value, msg.key
            if evt.get("type") != "onboarding.task.start.v1":
                continue

            data = evt.get("data", {})
            vid = data.get("volunteer_id")
            locale = data.get("locale", "en-IN")
            if not vid:
                log.error("task missing volunteer_id")
                continue

            collected = await run_conversation(vid, locale, mcp)

            out = {
                "type": "onboarding.profile.collected.v1",
                "id": str(uuid.uuid4()),
                "time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "data": collected,
            }
            await bus.publish(topic, key=vid, value=out)
            log.info("Published onboarding.profile.collected.v1 for %s", vid)
    finally:
        # ensure clean shutdown so you don't see "Unclosed AIOKafkaProducer/Consumer"
        await bus.stop()

if __name__ == "__main__":
    asyncio.run(main())
