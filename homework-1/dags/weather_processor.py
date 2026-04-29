import json
import logging

import pendulum
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.task_group import TaskGroup


API_URL = "data/3.0/onecall/timemachine"

CITIES = {
    "Lviv": {"lat": 49.842957, "lon": 24.031111},
    "Kyiv": {"lat": 50.450001, "lon": 30.523333},
    "Kharkiv": {"lat": 49.988358, "lon": 36.232845},
    "Odesa": {"lat": 46.482952, "lon": 30.712481},
    "Zhmerynka": {"lat": 49.03705, "lon": 28.11201}
}


def _process_weather(ti, city: str, extract_task_id: str) -> dict:
    info = ti.xcom_pull(task_ids=extract_task_id)
    data = info["data"][0]

    date = datetime.fromtimestamp(data["dt"]).strftime("%Y-%m-%d")

    result = {
        "city": city,
        "date": date,
        "temperature": data["temp"],
        "humidity": data["humidity"],
        "cloudiness": data["clouds"],
        "wind_speed": data["wind_speed"]
    }

    logging.info(f"Successfully processed data for {city}: {result}")
    return result


with DAG(
    dag_id="weather_processor",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 3, 13, tz="UTC"),
    catchup=True,
    tags=["weather"],
    max_active_runs=1,
) as dag:
    db_create = SQLExecuteQueryOperator(
        task_id="create_table_sqlite",
        conn_id="weather_connection",
        sql="""
            CREATE TABLE IF NOT EXISTS measures (
                city VARCHAR,
                date DATE,
                temperature FLOAT,
                humidity FLOAT,
                cloudiness FLOAT,
                wind_speed FLOAT
            );
        """
    )

    for city, coords in CITIES.items():
        with TaskGroup(group_id=f"process_{city.lower()}") as city_group:
            check_api = HttpSensor(
                task_id="check_api",
                http_conn_id="weather_connection_http",
                endpoint=API_URL,
                request_params={
                    "appid": "{{ var.value.WEATHER_API_KEY }}",
                    "lat": coords["lat"],
                    "lon": coords["lon"],
                    "dt": "{{ logical_date.int_timestamp }}",
                    "units": "metric"
                }
            )

            extract_data = HttpOperator(
                task_id="extract_data",
                http_conn_id="weather_connection_http",
                endpoint=API_URL,
                data={
                    "appid": "{{ var.value.WEATHER_API_KEY }}",
                    "lat": coords["lat"],
                    "lon": coords["lon"],
                    "dt": "{{ logical_date.int_timestamp }}",
                    "units": "metric"
                },
                method="GET",
                response_filter=lambda x: json.loads(x.text),
                log_response=True
            )

            process_data = PythonOperator(
                task_id="process_data",
                python_callable=_process_weather,
                op_kwargs={
                    "city": city,
                    "extract_task_id": f"process_{city.lower()}.extract_data"
                }
            )

            inject_data = SQLExecuteQueryOperator(
                task_id="inject_data",
                conn_id="weather_connection",
                sql=f"""
                    INSERT INTO measures (city, date, temperature, humidity, cloudiness, wind_speed) 
                    VALUES (
                        '{{{{ ti.xcom_pull(task_ids='process_{city.lower()}.process_data')['city'] }}}}', 
                        '{{{{ ti.xcom_pull(task_ids='process_{city.lower()}.process_data')['date'] }}}}', 
                        {{{{ ti.xcom_pull(task_ids='process_{city.lower()}.process_data')['temperature'] }}}}, 
                        {{{{ ti.xcom_pull(task_ids='process_{city.lower()}.process_data')['humidity'] }}}}, 
                        {{{{ ti.xcom_pull(task_ids='process_{city.lower()}.process_data')['cloudiness'] }}}}, 
                        {{{{ ti.xcom_pull(task_ids='process_{city.lower()}.process_data')['wind_speed'] }}}}
                    );
                """
            )

            check_api >> extract_data >> process_data >> inject_data

        db_create >> city_group
