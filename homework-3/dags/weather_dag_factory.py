import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.http.sensors.http import HttpSensor

from config import CITIES, DEFAULT_ARGS
from weather_etl import (data_quality_check, extract_to_storage, load_to_db,
                         transform_from_storage)


API_URL = "data/3.0/onecall/timemachine"


def create_ingestion_dag(city: str, coords: dict) -> DAG:
    """
    Factory function returning an ingestion DAG for a specific city.
    """

    dag = DAG(
        dag_id=f"{city.lower()}_weather_ingestion_dag",
        default_args=DEFAULT_ARGS,
        schedule="@daily",
        start_date=pendulum.datetime(2026, 5, 1, tz="UTC"),
        catchup=True,
        max_active_runs=1,
        params={"api_endpoint": API_URL},
        tags=["homework-3", "ingestion", city.lower()]
    )

    with dag:
        check_api = HttpSensor(
            task_id="check_api",
            http_conn_id="weather_connection_http",
            endpoint="{{ params.api_endpoint }}",
            request_params={
                "appid": "{{ var.value.WEATHER_API_KEY }}",
                "lat": coords["lat"],
                "lon": coords["lon"],
                "dt": "{{ logical_date.int_timestamp }}",
                "units": "metric"
            }
        )

        extract_raw = PythonOperator(
            task_id="extract_to_storage",
            python_callable=extract_to_storage,
            op_kwargs={"city": city, "lat": coords["lat"], "lon": coords["lon"]}
        )

        trigger_processing = TriggerDagRunOperator(
            task_id="trigger_processing_dag",
            trigger_dag_id=f"{city.lower()}_weather_processing_dag",
            execution_date="{{ logical_date }}",
            wait_for_completion=False,
            reset_dag_run=True
        )

        check_api >> extract_raw >> trigger_processing

    return dag


def create_processing_dag(city: str) -> DAG:
    """
    Factory function returning a processing DAG for a specific city.
    """

    dag = DAG(
        dag_id=f"{city.lower()}_weather_processing_dag",
        default_args=DEFAULT_ARGS,
        schedule=None,
        start_date=pendulum.datetime(2026, 5, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,
        tags=["homework-3", "processing", city.lower()]
    )

    with dag:
        transform_data = PythonOperator(
            task_id="transform_from_storage",
            python_callable=transform_from_storage,
            op_kwargs={"city": city}
        )

        dq_check = PythonOperator(
            task_id="data_quality_check",
            python_callable=data_quality_check,
            op_kwargs={"city": city}
        )

        load_data = PythonOperator(
            task_id="load_to_db",
            python_callable=load_to_db,
            op_kwargs={"city": city}
        )

        transform_data >> dq_check >> load_data

    return dag


for city_name, city_coords in CITIES.items():
    ingestion_dag = create_ingestion_dag(city_name, city_coords)
    globals()[ingestion_dag.dag_id] = ingestion_dag

    processing_dag = create_processing_dag(city_name)
    globals()[processing_dag.dag_id] = processing_dag
