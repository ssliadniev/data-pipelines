import csv
import os
import logging
from app.contracts.models import StandardDisasterEvent


CSV_FILE_PATH = "disasters_test_data.csv"

logger = logging.getLogger(__name__)


async def save_event_to_csv(event: StandardDisasterEvent):
    """
    The New Sink: Takes an event from the stream and appends it to a CSV file.
    """
    file_exists = os.path.isfile(CSV_FILE_PATH)

    try:
        with open(CSV_FILE_PATH, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)

            if not file_exists:
                writer.writerow(["ID", "Title", "Category", "Latitude", "Longitude", "Date", "Source"])

            writer.writerow([
                event.id,
                event.title,
                event.category,
                event.latitude,
                event.longitude,
                event.date.isoformat(),
                event.source.value
            ])

    except Exception as e:
        logger.error(f"Failed to save event {event.id} to CSV: {e}")
