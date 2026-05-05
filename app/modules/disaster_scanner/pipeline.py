from aiostream import pipe, stream

from app.modules.disaster_scanner.core import normalize_router
from app.modules.disaster_scanner.operators import deduplicator
from app.modules.disaster_scanner.streams import (fetch_nasa_stream,
                                                  fetch_pdc_stream)


def build_disaster_pipeline():
    nasa_gen = fetch_nasa_stream()
    # pdc_gen = fetch_pdc_stream()

    combined_stream = stream.merge(nasa_gen)

    return (
            combined_stream
            | pipe.map(normalize_router)
            | deduplicator.pipe(key_extractor=lambda event: event.id)
    )
