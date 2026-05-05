import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routers import disasters
from app.modules.disaster_scanner.broadcaster import stream_consumer_worker

background_tasks = set()


@asynccontextmanager
async def lifespan(app: FastAPI):
    print(">>> [APP] Starting FastAPI Lifespan...", flush=True)

    task = asyncio.create_task(stream_consumer_worker())
    background_tasks.add(task)

    yield

    print(">>> [APP] Shutting down FastAPI...", flush=True)
    task.cancel()


app = FastAPI(lifespan=lifespan)
app.include_router(disasters.router)
