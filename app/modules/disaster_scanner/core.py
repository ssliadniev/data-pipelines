from datetime import datetime, timezone

from app.contracts.models import DisasterSource, StandardDisasterEvent


def normalize_nasa_event(raw_event: dict) -> StandardDisasterEvent:
    """
    Pure function mapping NASA JSON to StandardDisasterEvent.
    """

    sorted_geometry = sorted(
        raw_event.get("geometry", []),
        key=lambda g: g.get("date", ""),
        reverse=True
    )
    latest_geometry = sorted_geometry[0] if sorted_geometry else {}
    coords = latest_geometry.get("coordinates", [0.0, 0.0])
    categories = raw_event.get("categories", [])

    return StandardDisasterEvent(
        id=raw_event.get("id", "UNKNOWN"),
        title=raw_event.get("title", "Untitled Event"),
        category=categories[0].get("title", "Unknown") if categories else "Unknown",
        longitude=float(coords[0]),
        latitude=float(coords[1]),
        date=datetime.fromisoformat(
            latest_geometry.get("date", "1970-01-01T00:00:00Z").replace("Z", "+00:00")
        ),
        source=DisasterSource.NASA
    )


def normalize_pdc_event(raw_event: dict) -> StandardDisasterEvent:
    """
    Pure function mapping PDC JSON to StandardDisasterEvent.
    """

    raw_date = raw_event.get("update_Date") or raw_event.get("start_Date", "0")
    timestamp_seconds = int(raw_date) / 1000.0 if raw_date else 0.0

    return StandardDisasterEvent(
        id=raw_event.get("uuid", "UNKNOWN"),
        title=raw_event.get("hazard_Name", "Untitled Event"),
        category=raw_event.get("type_ID", "Unknown"),
        latitude=float(raw_event.get("latitude", 0.0)),
        longitude=float(raw_event.get("longitude", 0.0)),
        date=datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc),
        source=DisasterSource.PDC
    )


def normalize_router(raw_event: dict) -> StandardDisasterEvent:
    """
    Higher-order router to determine which pure normalizer to apply.
    """

    if "geometry" in raw_event:
        return normalize_nasa_event(raw_event)
    return normalize_pdc_event(raw_event)
