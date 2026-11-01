"""Prueba manual de la instalación y del montaje de src/inventory."""

from airflow.sdk import dag, task


@dag(
    dag_id="airflow_smoke",
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["laboratorio"],
)
def airflow_smoke():
    @task
    def check_inventory_import():
        from inventory.forecasting import forecast_week

        assert callable(forecast_week)

    check_inventory_import()


airflow_smoke()
