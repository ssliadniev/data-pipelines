from datetime import datetime
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


class DisasterSource(str, Enum):
    NASA = "NASA"
    PDC = "PDC"


class StandardDisasterEvent(BaseModel):
    """
    Immutable contract representing a normalized disaster event.
    """

    model_config = ConfigDict(frozen=True)

    id: str = Field(..., description="Unique identifier from the source API")
    title: str
    category: str
    latitude: float
    longitude: float
    date: datetime
    source: DisasterSource
