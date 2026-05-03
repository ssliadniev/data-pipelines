import json
import logging
import os
import sqlite3
from datetime import datetime

import requests
from airflow.models import Variable
from config import DB_PATH, PROCESSED_DIR, RAW_DIR

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)


def extract_to_storage(city: str, lat: float, lon: float, **context) -> None:
    """
    Fetches API data and saves it to a raw json file.
    """

    logical_date = context["logical_date"]
    ds = context["ds"]

    url = "https://api.openweathermap.org/data/3.0/onecall/timemachine"
    params = {
        "lat": lat,
        "lon": lon,
        "dt": logical_date.int_timestamp,
        "appid": Variable.get("WEATHER_API_KEY"),
        "units": "metric"
    }

    response = requests.get(url, params=params)
    response.raise_for_status()

    file_path = os.path.join(RAW_DIR, f"{city}_{ds}.json")
    with open(file_path, "w") as file:
        json.dump(response.json(), file)

    logging.info(f"Raw data saved to {file_path}")


def transform_from_storage(city: str, **context) -> None:
    """
    Reads raw json, transforms it, and saves to a processed json file.
    """

    ds = context["ds"]
    raw_file = os.path.join(RAW_DIR, f"{city}_{ds}.json")
    processed_file = os.path.join(PROCESSED_DIR, f"{city}_{ds}.json")

    with open(raw_file, "r") as file:
        raw_data = json.load(file)

    data = raw_data["data"][0]

    result = {
        "city": city,
        "date": datetime.fromtimestamp(data["dt"]).strftime("%Y-%m-%d"),
        "temperature": data["temp"],
        "humidity": data["humidity"],
        "cloudiness": data["clouds"],
        "wind_speed": data["wind_speed"]
    }

    with open(processed_file, "w") as file:
        json.dump(result, file)

    logging.info(f"Processed data saved to {processed_file}")


def data_quality_check(city: str, **context) -> None:
    """
    Validates the processed data before allowing it to be loaded.
    """

    ds = context["ds"]
    processed_file = os.path.join(PROCESSED_DIR, f"{city}_{ds}.json")

    with open(processed_file, "r") as file:
        data = json.load(file)

    assert -80 < data["temperature"] < 60, f"Temperature {data['temperature']} out of realistic bounds!"
    assert 0 <= data["humidity"] <= 100, f"Humidity {data['humidity']} must be 0-100!"
    assert data["wind_speed"] >= 0, "Wind speed cannot be negative!"
    assert data["city"] == city, f"City mismatch: Expected {city}, got {data['city']}"

    logging.info(f"Data quality checks PASSED for {city}.")


def load_to_db(city: str, **context) -> None:
    """
    Reads processed data and inserts it into SQLite.
    """

    ds = context["ds"]
    processed_file = os.path.join(PROCESSED_DIR, f"{city}_{ds}.json")

    with open(processed_file, "r") as file:
        data = json.load(file)

    connection = sqlite3.connect(DB_PATH, timeout=20.0)
    cursor = connection.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS measures
        (
            city VARCHAR,
            date DATE,
            temperature FLOAT,
            humidity FLOAT,
            cloudiness FLOAT,
            wind_speed FLOAT
        );
        """)

    cursor.execute(
        """
        INSERT INTO measures (city, date, temperature, humidity, cloudiness, wind_speed)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            data['city'], data['date'], data['temperature'], data['humidity'], data['cloudiness'], data['wind_speed']
        )
    )

    connection.commit()
    connection.close()
    logging.info(f"Successfully loaded {city} data into SQLite.")
