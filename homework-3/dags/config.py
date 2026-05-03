import os
from datetime import timedelta

STORAGE_BASE = "/opt/airflow/dags/data"
RAW_DIR = os.path.join(STORAGE_BASE, "raw")
PROCESSED_DIR = os.path.join(STORAGE_BASE, "processed")
DB_PATH = "/opt/airflow/dags/weather.db"

CITIES = {
    "Lviv": {"lat": 49.842957, "lon": 24.031111},
    "Kyiv": {"lat": 50.450001, "lon": 30.523333},
    "Kharkiv": {"lat": 49.988358, "lon": 36.232845},
    "Odesa": {"lat": 46.482952, "lon": 30.712481},
    "Zhmerynka": {"lat": 49.03705, "lon": 28.11201}
}

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
}
