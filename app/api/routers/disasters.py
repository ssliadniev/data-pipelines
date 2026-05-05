from fastapi import APIRouter
from typing import List
from app.contracts.models import StandardDisasterEvent

from app.modules.disaster_scanner.broadcaster import get_current_disasters

router = APIRouter()


@router.get("/api/v1/disasters/active", response_model=List[StandardDisasterEvent])
async def get_active_disasters():
    return get_current_disasters()
