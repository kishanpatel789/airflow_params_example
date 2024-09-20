import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from src.python_runner import first_function

with DAG(
    dag_id="01_simple_dag",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2024, 9, 1, tz="UTC"),
    catchup=False,
    tags=["level:lame", "usability:confusing"],
    params={},
) as dag:
    empty_task = EmptyOperator(
        task_id="empty_task",
    )

    bash_task = BashOperator(
        task_id="bash_task",
        bash_command="pip freeze",
    )

    python_task = PythonOperator(
        task_id="python_task",
        python_callable=first_function,
    )

    empty_task >> bash_task >> python_task
