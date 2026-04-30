import json
import logging

import pendulum
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.task_group import TaskGroup


API_URL = "data/3.0/onecall/timemachine"

WIND_SPEED_ALERT_THRESHOLD = 5.0

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
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
}


def _transform_weather(ti, city: str, extract_task_id: str) -> dict:
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


def _branch_wind_speed(ti, transform_task_id: str, normal_task_id: str, alert_task_id: str) -> str:
    data = ti.xcom_pull(task_ids=transform_task_id)
    wind_speed = data["wind_speed"]

    if wind_speed > WIND_SPEED_ALERT_THRESHOLD:
        logging.info(f"High wind detected ({wind_speed} m/s)! Routing to ALERT path.")
        return alert_task_id
    else:
        logging.info(f"Normal wind ({wind_speed} m/s). Routing to NORMAL path.")
        return normal_task_id


def _log_alert(ti, transform_task_id: str) -> None:
    data = ti.xcom_pull(task_ids=transform_task_id)
    logging.warning(f"ALERT! High wind speed of {data['wind_speed']} m/s recorded in {data['city']} on {data['date']}!")


with DAG(
    dag_id="weather_processor",
    default_args=DEFAULT_ARGS,
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
                response_filter=lambda x: json.loads(x.text)
            )

            transform_data = PythonOperator(
                task_id="transform_data",
                python_callable=_transform_weather,
                op_kwargs={
                    "city": city,
                    "extract_task_id": f"process_{city.lower()}.extract_data"
                }
            )

            branch_task = BranchPythonOperator(
                task_id="branch_wind_speed",
                python_callable=_branch_wind_speed,
                op_kwargs={
                    "transform_task_id": f"process_{city.lower()}.transform_data",
                    "normal_task_id": f"process_{city.lower()}.normal_load",
                    "alert_task_id": f"process_{city.lower()}.alert_action"
                }
            )

            normal_load = SQLExecuteQueryOperator(
                task_id="normal_load",
                conn_id="weather_connection",
                sql=f"""
                    INSERT INTO measures (city, date, temperature, humidity, cloudiness, wind_speed) VALUES (
                        '{{{{ ti.xcom_pull(task_ids='process_{city.lower()}.transform_data')['city'] }}}}', 
                        '{{{{ ti.xcom_pull(task_ids='process_{city.lower()}.transform_data')['date'] }}}}', 
                        {{{{ ti.xcom_pull(task_ids='process_{city.lower()}.transform_data')['temperature'] }}}}, 
                        {{{{ ti.xcom_pull(task_ids='process_{city.lower()}.transform_data')['humidity'] }}}}, 
                        {{{{ ti.xcom_pull(task_ids='process_{city.lower()}.transform_data')['cloudiness'] }}}}, 
                        {{{{ ti.xcom_pull(task_ids='process_{city.lower()}.transform_data')['wind_speed'] }}}}
                    );
                """
            )

            alert_action = PythonOperator(
                task_id="alert_action",
                python_callable=_log_alert,
                op_kwargs={"transform_task_id": f"process_{city.lower()}.transform_data"}
            )

            alert_load = SQLExecuteQueryOperator(
                task_id="alert_load",
                conn_id="weather_connection",
                sql=f"""
                    INSERT INTO measures (city, date, temperature, humidity, cloudiness, wind_speed) VALUES (
                        '{{{{ ti.xcom_pull(task_ids='process_{city.lower()}.transform_data')['city'] }}}}', 
                        '{{{{ ti.xcom_pull(task_ids='process_{city.lower()}.transform_data')['date'] }}}}', 
                        {{{{ ti.xcom_pull(task_ids='process_{city.lower()}.transform_data')['temperature'] }}}}, 
                        {{{{ ti.xcom_pull(task_ids='process_{city.lower()}.transform_data')['humidity'] }}}}, 
                        {{{{ ti.xcom_pull(task_ids='process_{city.lower()}.transform_data')['cloudiness'] }}}}, 
                        {{{{ ti.xcom_pull(task_ids='process_{city.lower()}.transform_data')['wind_speed'] }}}}
                    );
                """
            )

            check_api >> extract_data >> transform_data >> branch_task
            branch_task >> normal_load
            branch_task >> alert_action >> alert_load

        db_create >> city_group
