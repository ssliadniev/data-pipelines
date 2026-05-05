import asyncio
from typing import AsyncIterator

import aiohttp


async def fetch_nasa_stream() -> AsyncIterator[dict]:
    while True:
        print(">>> [NASA] Waking up and fetching...", flush=True)
        try:
            headers = {"Accept": "application/json"}

            async with aiohttp.ClientSession() as session:
                async with session.get("https://eonet.gsfc.nasa.gov/api/v3/events", headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json(content_type=None)
                        events = data.get("events", [])
                        print(f">>> [NASA] SUCCESS! Found {len(events)} events.", flush=True)
                        for event in events:
                            yield event
                    else:
                        print(f">>> [NASA] API returned status {resp.status}", flush=True)
        except Exception as e:
            print(f">>> [NASA] ERROR: {e}", flush=True)

        await asyncio.sleep(10)


async def fetch_pdc_stream() -> AsyncIterator[dict]:
    while True:
        print(">>> [PDC] Waking up and fetching...", flush=True)
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get("https://disasteralert.pdc.org/disasteralert/") as resp:
                    if resp.status == 200:
                        data = await resp.json(content_type=None)
                        if isinstance(data, list):
                            print(f">>> [PDC] SUCCESS! Found {len(data)} events.", flush=True)
                            for event in data:
                                if isinstance(event, dict):
                                    yield event
        except Exception as e:
            print(f">>> [PDC] ERROR (Expected if HTML): {e}", flush=True)

        await asyncio.sleep(10)
