import asyncio
from typing import Dict

from aiostream import streamcontext

from app.contracts.models import StandardDisasterEvent
from app.modules.disaster_scanner.pipeline import build_disaster_pipeline
from app.modules.disaster_scanner.repository import save_event_to_csv

ACTIVE_DISASTERS_CACHE: Dict[str, StandardDisasterEvent] = {}


def get_current_disasters() -> list[StandardDisasterEvent]:
    return list(ACTIVE_DISASTERS_CACHE.values())


async def stream_consumer_worker():
    print(">>> [WORKER] Background stream consumer started!", flush=True)
    pipeline = build_disaster_pipeline()

    try:
        async with streamcontext(pipeline) as streamer:
            async for event in streamer:
                try:
                    print(f">>> [PIPELINE] Processed: {event.source.value} - {event.title}", flush=True)
                    ACTIVE_DISASTERS_CACHE[event.id] = event
                    await save_event_to_csv(event)
                except Exception as e:
                    print(f">>> [PIPELINE ERROR]: {e}", flush=True)

    except asyncio.CancelledError:
        print(">>> [WORKER] Shutting down.", flush=True)
    except Exception as e:
        print(f">>> [FATAL PIPELINE CRASH]: {e}", flush=True)
